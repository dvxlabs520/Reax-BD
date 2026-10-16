import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/io_faults.dart';
import 'package:reaxdb_dart/src/core/storage/lsm_storage_engine.dart';
import 'package:reaxdb_dart/src/core/storage/manifest.dart';
import 'package:reaxdb_dart/src/core/storage/memtable.dart';
import 'package:reaxdb_dart/src/core/storage/sstable.dart';
import 'package:reaxdb_dart/src/core/util/byte_key.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));
String _s(Uint8List b) => utf8.decode(b);
String _k(String prefix, int i) => '$prefix${i.toString().padLeft(5, '0')}';

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('storage_regression_');
  });

  tearDown(() async {
    IoFaults.hook = null;
    if (await dir.exists()) await dir.delete(recursive: true);
  });

  Directory tablesDir() => Directory(p.join(dir.path, 'tables'));

  Future<List<String>> sstFiles() async => [
    await for (final e in tablesDir().list())
      if (e.path.endsWith('.sst')) p.basename(e.path),
  ];

  /// Waits (up to ~2 s) until the table files on disk are exactly the live
  /// tables, i.e. asynchronous deletion of obsolete tables has finished.
  Future<void> settle(LsmStorageEngine e) async {
    for (var i = 0; i < 200; i++) {
      if ((await sstFiles()).length == e.stats.sstableCount) return;
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }

  Future<LsmStorageEngine> openEngine({
    int memtableSizeBytes = 1 << 30,
    int targetTableBytes = 2 * 1024 * 1024,
    int l0CompactionTrigger = 4,
    int? walFlushThresholdBytes,
    SyncMode syncMode = SyncMode.os,
    void Function(Object, StackTrace)? onMaintenanceError,
  }) => LsmStorageEngine.open(
    directory: dir.path,
    syncMode: syncMode,
    memtableSizeBytes: memtableSizeBytes,
    targetTableBytes: targetTableBytes,
    l0CompactionTrigger: l0CompactionTrigger,
    walFlushThresholdBytes: walFlushThresholdBytes,
    onMaintenanceError: onMaintenanceError,
  );

  group('failed manifest publish never reuses a live table id', () {
    test('failed compaction keeps the old tree; later flushes stay '
        'readable (MANIFEST.tmp blocked)', () async {
      var e = await openEngine();
      for (var i = 0; i < 100; i++) {
        await e.put(_b(_k('a', i)), _b('A$i'));
      }
      await e.flush();
      for (var i = 0; i < 100; i++) {
        await e.put(_b(_k('b', i)), _b('B$i'));
      }
      await e.flush();
      final blocker = Directory(p.join(tablesDir().path, 'MANIFEST.tmp'))
        ..createSync();
      await expectLater(e.compact(), throwsA(isA<FileSystemException>()));
      blocker.deleteSync();
      expect(e.stats.levelTableCounts, [2], reason: 'in-memory view intact');
      for (var i = 0; i < 100; i++) {
        await e.put(_b(_k('c', i)), _b('C$i'));
      }
      await e.flush();
      Future<void> verify(LsmStorageEngine e) async {
        for (final pfx in ['a', 'b', 'c']) {
          for (var i = 0; i < 100; i++) {
            final v = await e.get(_b(_k(pfx, i)));
            expect(v, isNotNull, reason: _k(pfx, i));
            expect(_s(v!), '${pfx.toUpperCase()}$i');
          }
        }
      }

      await verify(e);
      expect((await sstFiles()).length, e.stats.sstableCount);
      await e.close();
      e = await openEngine();
      await verify(e);
      await e.compact();
      await verify(e);
      await e.close();
    });

    test('failed flush publish deletes its table and keeps the memtable', () {
      return () async {
        final e = await openEngine();
        for (var i = 0; i < 50; i++) {
          await e.put(_b(_k('k', i)), _b('v$i'));
        }
        IoFaults.hook = (op, _, _) => op == IoOp.manifestWrite ? 0 : null;
        await expectLater(e.flush(), throwsA(isA<FileSystemException>()));
        IoFaults.hook = null;
        expect(await sstFiles(), isEmpty, reason: 'unpublished table removed');
        expect(
          await File(p.join(tablesDir().path, 'MANIFEST.tmp')).exists(),
          isFalse,
        );
        expect(e.stats.memtableEntries, 50);
        expect(_s((await e.get(_b(_k('k', 7))))!), 'v7');
        await e.flush();
        expect((await sstFiles()).length, 1);
        await e.close();
        final reopened = await openEngine();
        for (var i = 0; i < 50; i++) {
          expect(_s((await reopened.get(_b(_k('k', i))))!), 'v$i');
        }
        await reopened.close();
      }();
    });
  });

  group('maintenance errors after a durable write', () {
    test('auto-flush failure does not fail the write; it is reported and '
        'retried', () async {
      final errors = <Object>[];
      final e = await openEngine(
        memtableSizeBytes: 4 * 1024,
        onMaintenanceError: (err, _) => errors.add(err),
      );
      IoFaults.hook = (op, _, _) => op == IoOp.sstableWrite ? 0 : null;
      for (var i = 0; i < 200; i++) {
        // Must not throw: every write is durable in the WAL.
        await e.put(_b(_k('k', i)), Uint8List(100));
      }
      expect(errors, isNotEmpty);
      expect(e.lastMaintenanceError, isA<FileSystemException>());
      expect(e.stats.sstableCount, 0);
      expect(await e.get(_b(_k('k', 150))), isNotNull);
      await expectLater(e.flush(), throwsA(isA<FileSystemException>()));
      IoFaults.hook = null;
      await e.flush();
      expect(e.lastMaintenanceError, isNull);
      expect(e.stats.memtableEntries, 0);
      await e.close();
      final reopened = await openEngine();
      for (var i = 0; i < 200; i++) {
        expect(await reopened.get(_b(_k('k', i))), isNotNull);
      }
      await reopened.close();
    });

    test('close() still releases everything when the final flush fails', () {
      return () async {
        final e = await openEngine();
        await e.put(_b('x'), _b('1'));
        IoFaults.hook = (op, _, _) => op == IoOp.sstableWrite ? 0 : null;
        await e.close();
        IoFaults.hook = null;
        expect(e.lastMaintenanceError, isA<FileSystemException>());
        final reopened = await openEngine();
        expect(await reopened.get(_b('x')), _b('1'));
        await reopened.close();
      }();
    });
  });

  group('WAL growth is bounded by walFlushThresholdBytes', () {
    Future<int> walBytes() async {
      var total = 0;
      await for (final f in Directory(p.join(dir.path, 'wal')).list()) {
        if (f is File) total += await f.length();
      }
      return total;
    }

    test('explicit threshold', () async {
      final e = await openEngine(
        walFlushThresholdBytes: 256 * 1024,
        syncMode: SyncMode.none,
      );
      final value = Uint8List(1024);
      for (var i = 0; i < 5000; i++) {
        await e.put(_b('counter'), value);
      }
      expect(e.stats.sstableCount, greaterThan(0));
      expect(e.walBytesSinceCheckpoint, lessThan(300 * 1024));
      expect(await walBytes(), lessThan(600 * 1024));
      await e.close();
    });

    test('default threshold (overwriting one key, 4 MiB memtable)', () async {
      final e = await LsmStorageEngine.open(
        directory: dir.path,
        syncMode: SyncMode.none,
      );
      final value = Uint8List(1024);
      for (var i = 0; i < 30000; i++) {
        await e.put(_b('counter'), value);
      }
      expect(await walBytes(), lessThan(18 * 1024 * 1024));
      await e.close();
    });
  });

  group('obsolete tables are released as soon as nothing reads them', () {
    test('overwrite rounds do not accumulate files', () async {
      final e = await openEngine(
        memtableSizeBytes: 64 * 1024,
        syncMode: SyncMode.none,
      );
      final value = Uint8List(200);
      for (var round = 0; round < 4; round++) {
        for (var i = 0; i < 5000; i++) {
          await e.put(_b('k${i % 1000}'), value);
        }
      }
      await settle(e);
      expect((await sstFiles()).length, e.stats.sstableCount);
      await e.close();
    });

    test(
      'a scan in progress keeps its tables alive and sees its snapshot',
      () async {
        final e = await openEngine();
        for (var i = 0; i < 100; i++) {
          await e.put(_b(_k('k', i)), _b('v1'));
        }
        await e.flush();
        for (var i = 0; i < 100; i++) {
          await e.put(_b(_k('k', i)), _b('v2'));
        }
        await e.flush();
        final before = await sstFiles();
        expect(before.length, 2);

        final it = StreamIterator(e.scan());
        expect(await it.moveNext(), isTrue);
        await e.compact(); // Replaces both tables.
        for (var i = 0; i < 100; i++) {
          await e.put(_b(_k('k', i)), _b('v3'));
        }
        // Give a (wrong) eager deletion the chance to happen.
        await Future<void>.delayed(const Duration(milliseconds: 50));
        final during = await sstFiles();
        expect(during, containsAll(before), reason: 'pinned by the scan');

        var count = 1;
        expect(_s(it.current.value), 'v2');
        while (await it.moveNext()) {
          expect(_s(it.current.value), 'v2');
          count++;
        }
        expect(count, 100);
        await settle(e);
        final after = await sstFiles();
        expect(after.toSet().intersection(before.toSet()), isEmpty);
        expect(after.length, e.stats.sstableCount);
        await e.close();
      },
    );

    test('idle read handles are capped across tables', () async {
      final tables = <SSTable>[];
      for (var t = 0; t < 100; t++) {
        tables.add(
          await SSTable.create(
            filePath: p.join(dir.path, 't$t.sst'),
            entries: [SSTableEntry(_b('key'), _b('v$t'))],
          ),
        );
      }
      for (final t in tables) {
        expect(await t.get(_b('key')), isNotNull);
      }
      await Future<void>.delayed(const Duration(milliseconds: 20));
      final open = tables.fold(0, (s, t) => s + t.openHandleCount);
      expect(open, lessThanOrEqualTo(64));
      for (final t in tables) {
        await t.close();
      }
    });
  });

  group('cleanup on write failure', () {
    test('SSTable.create leaves no temp or final file behind', () async {
      final path = p.join(dir.path, 'x.sst');
      IoFaults.hook = (op, _, len) => op == IoOp.sstableWrite ? len ~/ 2 : null;
      await expectLater(
        SSTable.create(
          filePath: path,
          entries: [SSTableEntry(_b('k'), _b('v'))],
        ),
        throwsA(isA<FileSystemException>()),
      );
      expect(await File('$path.tmp').exists(), isFalse);
      expect(await File(path).exists(), isFalse);
    });

    test('Manifest.update failure keeps the previous state', () async {
      final m = await Manifest.open(dir.path);
      await m.update(
        const ManifestState(
          nextTableId: 5,
          levels: [
            [1, 2],
          ],
        ),
      );
      for (final op in [IoOp.manifestWrite, IoOp.manifestRename]) {
        IoFaults.hook = (o, _, _) => o == op ? 0 : null;
        await expectLater(
          m.update(const ManifestState(nextTableId: 9, levels: [])),
          throwsA(isA<FileSystemException>()),
        );
        IoFaults.hook = null;
        expect(m.state.nextTableId, 5);
        expect(await File(p.join(dir.path, 'MANIFEST.tmp')).exists(), isFalse);
      }
      final reopened = await Manifest.open(dir.path);
      expect(reopened.state.nextTableId, 5);
      expect(reopened.state.levels, [
        [1, 2],
      ]);
    });
  });

  group('lazy, bounded scans', () {
    Future<SSTable> bigTable(String name) => SSTable.create(
      filePath: p.join(dir.path, name),
      entries: [
        for (var i = 0; i < 20000; i++)
          SSTableEntry(_b(_k('k', i)), Uint8List(40)),
      ],
    );

    Future<void> corruptAt(String name, double fraction) async {
      final f = File(p.join(dir.path, name));
      final bytes = await f.readAsBytes();
      final pos = (bytes.length * fraction).toInt();
      bytes[pos] ^= 0xff;
      await f.writeAsBytes(bytes, flush: true);
    }

    test('SSTable.range reads only what the consumer takes', () async {
      final t = await bigTable('lazy.sst');
      await t.close();
      await corruptAt('lazy.sst', 0.8); // A data record near the end.
      final reopened = await SSTable.open(p.join(dir.path, 'lazy.sst'));
      final first = await reopened.range().take(10).toList();
      expect(first.map((e) => _s(e.key)), [
        for (var i = 0; i < 10; i++) _k('k', i),
      ]);
      final mid = await reopened
          .range(startKey: _b(_k('k', 5000)))
          .take(3)
          .toList();
      expect(_s(mid.first.key), _k('k', 5000));
      await expectLater(
        reopened.range().toList(),
        throwsA(isA<CorruptionException>()),
      );
      await reopened.close();
    });

    test('reverse SSTable.range is lazy too', () async {
      final t = await bigTable('rlazy.sst');
      await t.close();
      await corruptAt('rlazy.sst', 0.05);
      final reopened = await SSTable.open(p.join(dir.path, 'rlazy.sst'));
      final last = await reopened.range(reverse: true).take(5).toList();
      expect(_s(last.first.key), _k('k', 19999));
      await reopened.close();
    });

    test('engine scan with a limit stops early', () async {
      final e = await openEngine();
      for (var i = 0; i < 20000; i++) {
        await e.put(_b(_k('k', i)), Uint8List(40));
      }
      await e.close();
      final sst = (await sstFiles()).single;
      final f = File(p.join(tablesDir().path, sst));
      final bytes = await f.readAsBytes();
      bytes[(bytes.length * 0.8).toInt()] ^= 0xff;
      await f.writeAsBytes(bytes, flush: true);
      final reopened = await openEngine();
      final got = await reopened.scan(limit: 5).toList();
      expect(got.length, 5);
      expect(await reopened.scan().take(5).toList(), hasLength(5));
      expect(await reopened.scan(limit: 0).toList(), isEmpty);
      await reopened.close();
    });

    test('SSTable point and range lookups match a brute-force model', () async {
      final rnd = Random(7);
      final keys = <String>{};
      while (keys.length < 1500) {
        keys.add(_k('k', rnd.nextInt(100000)));
      }
      final sorted = keys.toList()..sort();
      final t = await SSTable.create(
        filePath: p.join(dir.path, 'model.sst'),
        entries: [
          for (final k in sorted)
            k.hashCode.isEven
                ? SSTableEntry(_b(k), _b('v$k'))
                : SSTableEntry.tombstone(_b(k)),
        ],
      );
      for (final k in sorted) {
        final e = await t.get(_b(k));
        expect(e, isNotNull);
        expect(e!.isTombstone, !k.hashCode.isEven);
      }
      for (var i = 0; i < 500; i++) {
        final k = _k('k', rnd.nextInt(100000));
        if (keys.contains(k)) continue;
        expect(await t.get(_b(k)), isNull);
      }
      for (var i = 0; i < 200; i++) {
        final a = _k('k', rnd.nextInt(100000));
        final z = _k('k', rnd.nextInt(100000));
        final reverse = rnd.nextBool();
        var expected = sorted
            .where((k) => k.compareTo(a) >= 0 && k.compareTo(z) < 0)
            .toList();
        if (reverse) expected = expected.reversed.toList();
        final got = await t
            .range(
              startKey: _b(a),
              endKey: _b(z),
              reverse: reverse,
              limitHint: rnd.nextInt(30),
            )
            .map((e) => _s(e.key))
            .toList();
        expect(got, expected, reason: '[$a, $z) reverse=$reverse');
      }
      await t.close();
    });

    test('L1+ lookups across many tables (binary search)', () async {
      final e = await openEngine(targetTableBytes: 2048);
      for (var i = 0; i < 3000; i += 2) {
        await e.put(_b(_k('k', i)), _b('v$i'));
      }
      await e.compact();
      expect(e.stats.levelTableCounts.length, greaterThan(1));
      expect(e.stats.levelTableCounts[1], greaterThan(10));
      for (var i = 0; i < 3000; i++) {
        final v = await e.get(_b(_k('k', i)));
        if (i.isEven) {
          expect(_s(v!), 'v$i');
        } else {
          expect(v, isNull);
        }
      }
      expect(await e.get(_b('a')), isNull);
      expect(await e.get(_b('z')), isNull);
      await e.close();
    });
  });

  group('memtable snapshots without up-front copies', () {
    test('range starts by search and honors bounds', () {
      final t = MemTable(maxSizeBytes: 1 << 20);
      for (var i = 0; i < 100; i += 2) {
        t.put(_b(_k('k', i)), _b('v'));
      }
      List<String> keys(Iterable<MapEntry<ByteKey, MemTableValue>> it) => [
        for (final e in it) e.key.asString,
      ];
      expect(
        keys(
          t.range(
            ByteKey.fromString(_k('k', 51)),
            ByteKey.fromString(_k('k', 58)),
          ),
        ),
        [_k('k', 52), _k('k', 54), _k('k', 56)],
      );
      expect(keys(t.range(null, ByteKey.fromString(_k('k', 3)))), [
        _k('k', 0),
        _k('k', 2),
      ]);
      expect(keys(t.range(ByteKey.fromString(_k('k', 97)), null)), [
        _k('k', 98),
      ]);
    });

    test('cursor keeps its snapshot across concurrent writes', () {
      final t = MemTable(maxSizeBytes: 1 << 20);
      for (var i = 0; i < 10; i++) {
        t.put(_b(_k('k', i)), _b('old'));
      }
      for (final reverse in [false, true]) {
        final c = t.openCursor(
          ByteKey.fromString(_k('k', 2)),
          ByteKey.fromString(_k('k', 8)),
          reverse: reverse,
        );
        final seen = <String>[c.next()!.key.asString];
        t.put(_b(_k('k', 5)), _b('new'));
        t.delete(_b(_k('k', 6)));
        t.put(_b(_k('k', 45)), _b('inserted'));
        while (true) {
          final e = c.next();
          if (e == null) break;
          expect(_s(e.value.value!), 'old');
          seen.add(e.key.asString);
        }
        final expected = [for (var i = 2; i < 8; i++) _k('k', i)];
        expect(seen, reverse ? expected.reversed.toList() : expected);
        for (var i = 0; i < 10; i++) {
          t.put(_b(_k('k', i)), _b('old'));
        }
        t.delete(_b(_k('k', 45)));
      }
    });

    test('engine scan is a snapshot even while writes interleave', () async {
      final e = await openEngine();
      for (var i = 0; i < 200; i++) {
        await e.put(_b(_k('k', i)), _b('a$i'));
      }
      final it = StreamIterator(e.scan());
      final seen = <String>[];
      var n = 0;
      while (await it.moveNext()) {
        seen.add('${_s(it.current.key)}=${_s(it.current.value)}');
        if (n++ == 50) {
          for (var i = 0; i < 200; i += 3) {
            await e.put(_b(_k('k', i)), _b('b'));
          }
          await e.delete(_b(_k('k', 120)));
          await e.put(_b(_k('k', 1005)), _b('new'));
        }
      }
      expect(seen, [for (var i = 0; i < 200; i++) '${_k('k', i)}=a$i']);
      await e.close();
    });
  });
}
