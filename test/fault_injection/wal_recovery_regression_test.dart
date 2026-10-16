import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/io_faults.dart';
import 'package:reaxdb_dart/src/core/storage/lsm_storage_engine.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

Future<List<File>> _walFiles(Directory dir) async {
  final files = <File>[];
  await for (final e in dir.list()) {
    if (e is File && e.path.endsWith('.wal')) files.add(e);
  }
  files.sort((a, b) => a.path.compareTo(b.path));
  return files;
}

/// Offsets of every frame in a WAL file (header is 6 bytes; every test
/// record here is shorter than 128 bytes, so its length is one byte).
List<int> _frameOffsets(Uint8List bytes) {
  final out = <int>[];
  var off = 6;
  while (off < bytes.length) {
    out.add(off);
    off += 4 + 1 + bytes[off + 4];
  }
  return out;
}

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('wal_recovery_regression_');
  });

  tearDown(() async {
    IoFaults.hook = null;
    if (await dir.exists()) await dir.delete(recursive: true);
  });

  group('WAL write failure (ENOSPC/EIO mid-append)', () {
    for (final mode in SyncMode.values) {
      test('torn append is rolled back; log stays readable ($mode)', () async {
        final wal = await WriteAheadLog.open(
          directory: dir.path,
          syncMode: mode,
        );
        for (var i = 0; i < 5; i++) {
          await wal.append(WalEntry.put(_b('ok-$i'), _b('v$i')));
        }
        // Force buffered bytes out so the failing write below is real.
        await wal.syncTo(SyncMode.os);
        final seqBefore = wal.lastSequenceNumber;
        // The next write reaches the disk only partially, then fails.
        IoFaults.hook = (op, path, length) =>
            op == IoOp.walWrite ? length ~/ 2 : null;
        await expectLater(
          wal.appendAll([
            WalEntry.put(_b('lost'), Uint8List(70000)),
            WalEntry.put(_b('lost-2'), _b('x')),
          ]),
          throwsA(isA<FileSystemException>()),
        );
        IoFaults.hook = null;
        expect(wal.lastSequenceNumber, seqBefore, reason: 'not consumed');
        for (var i = 0; i < 5; i++) {
          await wal.append(WalEntry.put(_b('after-$i'), _b('w$i')));
        }
        await wal.close();

        final reopened = await WriteAheadLog.open(directory: dir.path);
        final keys = await reopened
            .replay()
            .map((e) => utf8.decode(e.key!))
            .toList();
        expect(keys, [
          for (var i = 0; i < 5; i++) 'ok-$i',
          for (var i = 0; i < 5; i++) 'after-$i',
        ]);
        await reopened.close();
      });
    }

    test('if the rollback truncate also fails, the log moves to a new segment '
        'and recovery accepts the damaged tail', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      for (var i = 0; i < 3; i++) {
        await wal.append(WalEntry.put(_b('ok-$i'), _b('v$i')));
      }
      var failTruncate = true;
      IoFaults.hook = (op, path, length) {
        if (op == IoOp.walWrite) return 7;
        if (op == IoOp.walTruncate && failTruncate) return 0;
        return null;
      };
      await expectLater(
        wal.append(WalEntry.put(_b('lost'), _b('x'))),
        throwsA(isA<FileSystemException>()),
      );
      // Truncate keeps failing, so the next append must rotate.
      IoFaults.hook = (op, path, length) {
        if (op == IoOp.walTruncate && failTruncate) return 0;
        return null;
      };
      await wal.append(WalEntry.put(_b('after'), _b('w')));
      failTruncate = false;
      IoFaults.hook = null;
      await wal.close();

      final files = await _walFiles(dir);
      expect(files.length, 2, reason: 'rotated away from the torn file');
      // Simulated crash: the first file still carries the torn frame.
      final reopened = await WriteAheadLog.open(directory: dir.path);
      final keys = await reopened
          .replay()
          .map((e) => utf8.decode(e.key!))
          .toList();
      expect(keys, ['ok-0', 'ok-1', 'ok-2', 'after']);
      await reopened.close();
    });

    test('engine: failed batch throws, is invisible, and reopen keeps every '
        'acknowledged write', () async {
      final engine = await LsmStorageEngine.open(directory: dir.path);
      final acked = <String>[];
      for (var i = 0; i < 20; i++) {
        await engine.put(_b('k$i'), _b('v$i'));
        acked.add('k$i');
      }
      IoFaults.hook = (op, path, length) => op == IoOp.walWrite ? 3 : null;
      await expectLater(
        engine.writeBatch([
          WriteOp.put(_b('bad-1'), _b('x')),
          WriteOp.put(_b('bad-2'), _b('y')),
        ]),
        throwsA(isA<FileSystemException>()),
      );
      IoFaults.hook = null;
      expect(await engine.get(_b('bad-1')), isNull);
      for (var i = 0; i < 20; i++) {
        await engine.put(_b('after$i'), _b('w$i'));
        acked.add('after$i');
      }
      // Crash: no close.
      final recovered = await LsmStorageEngine.open(directory: dir.path);
      for (final k in acked) {
        expect(await recovered.get(_b(k)), isNotNull, reason: k);
      }
      expect(await recovered.get(_b('bad-1')), isNull);
      expect(await recovered.get(_b('bad-2')), isNull);
      await recovered.close();
    });
  });

  group('WAL segment shorter than its header', () {
    for (final size in [0, 3]) {
      test('newest segment of $size bytes is treated as empty', () async {
        final engine = await LsmStorageEngine.open(
          directory: dir.path,
          syncMode: SyncMode.os,
        );
        await engine.put(_b('a'), _b('1'));
        await engine.flush();
        await engine.put(_b('b'), _b('2'));
        await engine.close();
        final walDir = Directory(p.join(dir.path, 'wal'));
        final files = await _walFiles(walDir);
        // Crash between creating the next segment and writing its header.
        final next = File(
          p.join(walDir.path, 'wal-${'9'.padLeft(20, '0')}.wal'),
        );
        await next.writeAsBytes(List.filled(size, 0x52));
        expect(files.length, 1);

        final reopened = await LsmStorageEngine.open(directory: dir.path);
        expect(await reopened.get(_b('a')), _b('1'));
        expect(await reopened.get(_b('b')), _b('2'));
        await reopened.put(_b('c'), _b('3'));
        await reopened.close();
        final again = await LsmStorageEngine.open(directory: dir.path);
        expect(await again.get(_b('c')), _b('3'));
        await again.close();
      });
    }
  });

  group('WAL torn tail vs. corruption', () {
    test('corrupt length field followed by valid records throws and keeps '
        'the file intact', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      for (var i = 0; i < 10; i++) {
        await wal.append(WalEntry.put(_b('key$i'), _b('value$i')));
      }
      await wal.close();
      final file = (await _walFiles(dir)).single;
      final bytes = await file.readAsBytes();
      final third = _frameOffsets(bytes)[2];
      // A length that runs past EOF used to look like a torn tail.
      bytes[third + 4] = 0xff;
      bytes[third + 5] = 0x7f;
      await file.writeAsBytes(bytes, flush: true);

      await expectLater(
        WriteAheadLog.open(directory: dir.path),
        throwsA(isA<CorruptionException>()),
      );
      expect(await file.length(), bytes.length, reason: 'evidence kept');
    });

    test('genuine torn tail is truncated and preserved aside', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      for (var i = 0; i < 4; i++) {
        await wal.append(WalEntry.put(_b('key$i'), _b('value$i')));
      }
      await wal.close();
      final file = (await _walFiles(dir)).single;
      final intact = await file.length();
      final frame = RecordFrame.of(WalEntry.put(_b('torn'), _b('x')), 5);
      await file.writeAsBytes(
        frame.sublist(0, frame.length - 3),
        mode: FileMode.append,
        flush: true,
      );

      final reopened = await WriteAheadLog.open(directory: dir.path);
      expect((await reopened.replay().toList()).length, 4);
      expect(await file.length(), intact);
      final aside = File('${file.path}.corrupt-tail');
      expect(await aside.exists(), isTrue);
      expect(await aside.length(), frame.length - 3);
      await reopened.close();
    });

    test('a batch torn out of order (later pages survived) is dropped as a '
        'unit', () async {
      final engine = await LsmStorageEngine.open(directory: dir.path);
      for (var i = 0; i < 5; i++) {
        await engine.put(_b('single$i'), _b('v$i'));
      }
      await engine.writeBatch([
        for (var i = 0; i < 4; i++) WriteOp.put(_b('batch$i'), _b('b$i')),
      ]);
      // Crash without close, then damage the batch's first put while its
      // later frames (same transaction) survive.
      final file = (await _walFiles(Directory(p.join(dir.path, 'wal')))).last;
      final bytes = await file.readAsBytes();
      final frames = _frameOffsets(bytes);
      // Frames: 5 singles, txBegin, 4 puts, txCommit.
      expect(frames.length, 11);
      bytes.fillRange(frames[6], frames[7], 0);
      await file.writeAsBytes(bytes, flush: true);

      final recovered = await LsmStorageEngine.open(directory: dir.path);
      for (var i = 0; i < 5; i++) {
        expect(await recovered.get(_b('single$i')), _b('v$i'));
      }
      for (var i = 0; i < 4; i++) {
        expect(await recovered.get(_b('batch$i')), isNull);
      }
      await recovered.close();
    });
  });

  group('WAL replay streaming', () {
    test('records larger than a read chunk replay intact', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      final big = Uint8List(700 * 1024);
      for (var i = 0; i < big.length; i++) {
        big[i] = i % 251;
      }
      await wal.append(WalEntry.put(_b('small'), _b('s')));
      await wal.append(WalEntry.put(_b('big'), big));
      await wal.append(WalEntry.put(_b('tail'), _b('t')));
      await wal.close();
      final reopened = await WriteAheadLog.open(directory: dir.path);
      final entries = await reopened.replay().toList();
      expect(entries.map((e) => utf8.decode(e.key!)), ['small', 'big', 'tail']);
      expect(entries[1].value, big);
      await reopened.close();
    });
  });
}

/// Encodes [entry] with [sequence] exactly as the log frames it.
abstract final class RecordFrame {
  static Uint8List of(WalEntry entry, int sequence) {
    final stamped = WalEntry(
      type: entry.type,
      sequenceNumber: sequence,
      key: entry.key,
      value: entry.value,
      transactionId: entry.transactionId,
      timestampMs: entry.timestampMs,
    );
    final payload = stamped.encode();
    final crc = _crc(payload);
    return Uint8List.fromList([
      crc & 0xff,
      (crc >> 8) & 0xff,
      (crc >> 16) & 0xff,
      (crc >> 24) & 0xff,
      payload.length,
      ...payload,
    ]);
  }

  static int _crc(Uint8List bytes) {
    var crc = 0xFFFFFFFF;
    for (final b in bytes) {
      crc ^= b;
      for (var k = 0; k < 8; k++) {
        crc = (crc & 1) != 0 ? 0xEDB88320 ^ (crc >> 1) : crc >> 1;
      }
    }
    return (crc ^ 0xFFFFFFFF) & 0xFFFFFFFF;
  }
}
