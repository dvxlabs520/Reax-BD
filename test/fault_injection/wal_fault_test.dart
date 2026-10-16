import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
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

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('wal_fault_test_');
  });

  tearDown(() async {
    if (await dir.exists()) await dir.delete(recursive: true);
  });

  group('WAL fault injection', () {
    test('torn last record is truncated; earlier entries survive', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.append(WalEntry.put(_b('a'), _b('1')));
      await wal.append(WalEntry.put(_b('b'), _b('2')));
      await wal.close();

      // Simulate a crash mid-write: append half of a plausible record.
      final file = (await _walFiles(dir)).single;
      final intactLength = await file.length();
      await file.writeAsBytes([
        0xde,
        0xad,
        0xbe,
        0xef,
        0x40,
        0x01,
        0x02,
      ], mode: FileMode.append);

      final reopened = await WriteAheadLog.open(directory: dir.path);
      final entries = await reopened.replay().toList();
      expect(entries.length, 2);
      expect(entries[0].key, _b('a'));
      expect(entries[1].key, _b('b'));
      expect(await file.length(), intactLength, reason: 'tail truncated');

      // The log must still be appendable after truncation.
      await reopened.append(WalEntry.put(_b('c'), _b('3')));
      await reopened.close();
      final again = await WriteAheadLog.open(directory: dir.path);
      expect((await again.replay().toList()).length, 3);
      await again.close();
    });

    test(
      'mid-file CRC corruption with surviving tail throws CorruptionException',
      () async {
        final wal = await WriteAheadLog.open(directory: dir.path);
        for (var i = 0; i < 5; i++) {
          await wal.append(WalEntry.put(_b('key-$i'), _b('value-$i')));
        }
        await wal.close();

        final file = (await _walFiles(dir)).single;
        final bytes = await file.readAsBytes();
        // Corrupt payload bytes of the first record (header is 6 bytes,
        // frame overhead 5); records follow, so a surviving tail exists.
        bytes[15] ^= 0xff;
        await file.writeAsBytes(bytes);

        // The damaged file is the active one, so validation fails at open.
        await expectLater(
          WriteAheadLog.open(directory: dir.path),
          throwsA(isA<CorruptionException>()),
        );
      },
    );

    test(
      'corruption in an older (non-active) file surfaces on replay',
      () async {
        final wal = await WriteAheadLog.open(
          directory: dir.path,
          maxFileBytes: 128,
        );
        for (var i = 0; i < 20; i++) {
          await wal.append(WalEntry.put(_b('key-$i'), Uint8List(64)));
        }
        await wal.close();

        final files = await _walFiles(dir);
        expect(files.length, greaterThan(2));
        final victim = files.first;
        final bytes = await victim.readAsBytes();
        bytes[15] ^= 0xff;
        await victim.writeAsBytes(bytes);

        final reopened = await WriteAheadLog.open(directory: dir.path);
        await expectLater(
          reopened.replay().toList(),
          throwsA(isA<CorruptionException>()),
        );
        await reopened.close();
      },
    );

    test('replaying twice after a torn tail is idempotent', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.append(WalEntry.put(_b('stable'), _b('v')));
      await wal.close();
      final file = (await _walFiles(dir)).single;
      await file.writeAsBytes([1, 2, 3], mode: FileMode.append);

      final reopened = await WriteAheadLog.open(directory: dir.path);
      final first = await reopened.replay().toList();
      final second = await reopened.replay().toList();
      expect(first.length, 1);
      expect(second.length, 1);
      expect(second.single.key, first.single.key);
      expect(second.single.sequenceNumber, first.single.sequenceNumber);
      await reopened.close();
    });

    test(
      'uncommitted transaction at the tail is discarded on replay',
      () async {
        final wal = await WriteAheadLog.open(directory: dir.path);
        await wal.append(WalEntry.put(_b('safe'), _b('v')));
        await wal.append(WalEntry.txBegin(7));
        await wal.append(WalEntry.put(_b('half'), _b('x'), transactionId: 7));
        // Crash before txCommit: abandon without close (full sync mode has
        // already made these records durable).
        final recovered = await WriteAheadLog.open(directory: dir.path);
        final keys = await recovered
            .replay()
            .map((e) => utf8.decode(e.key!))
            .toList();
        expect(keys, ['safe']);
        await recovered.close();
      },
    );
  });
}
