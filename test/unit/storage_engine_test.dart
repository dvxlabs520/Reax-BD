import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/lsm_storage_engine.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('engine_test_');
  });

  tearDown(() async {
    if (await dir.exists()) await dir.delete(recursive: true);
  });

  Future<LsmStorageEngine> openEngine({
    int memtableSizeBytes = 4 * 1024 * 1024,
  }) => LsmStorageEngine.open(
    directory: dir.path,
    syncMode: SyncMode.os,
    memtableSizeBytes: memtableSizeBytes,
  );

  group('LsmStorageEngine basics', () {
    test('put/get/delete round-trip', () async {
      final engine = await openEngine();
      await engine.put(_b('k'), _b('v'));
      expect(await engine.get(_b('k')), _b('v'));
      await engine.delete(_b('k'));
      expect(await engine.get(_b('k')), isNull);
      await engine.close();
    });

    test('empty values are data, not tombstones', () async {
      final engine = await openEngine();
      await engine.put(_b('empty'), Uint8List(0));
      expect(await engine.get(_b('empty')), isEmpty);
      await engine.flush();
      expect(await engine.get(_b('empty')), isEmpty);
      await engine.close();

      final reopened = await openEngine();
      expect(await reopened.get(_b('empty')), isEmpty);
      await reopened.close();
    });

    test('deleted keys stay deleted across flush, compact, reopen', () async {
      final engine = await openEngine();
      await engine.put(_b('victim'), _b('data'));
      await engine.flush();
      await engine.delete(_b('victim'));
      await engine.flush();
      expect(await engine.get(_b('victim')), isNull);
      await engine.compact();
      expect(await engine.get(_b('victim')), isNull);
      await engine.close();

      final reopened = await openEngine();
      expect(
        await reopened.get(_b('victim')),
        isNull,
        reason: 'deleted key must not resurrect after reopen',
      );
      await reopened.close();
    });

    test('newest value wins after multiple flushes and compaction', () async {
      final engine = await openEngine();
      await engine.put(_b('k'), _b('v1'));
      await engine.flush();
      await engine.put(_b('k'), _b('v2'));
      await engine.flush();
      await engine.put(_b('k'), _b('v3'));
      expect(await engine.get(_b('k')), _b('v3'));
      await engine.compact();
      expect(await engine.get(_b('k')), _b('v3'));
      await engine.close();
    });

    test('writeBatch is applied together', () async {
      final engine = await openEngine();
      await engine.put(_b('old'), _b('x'));
      await engine.writeBatch([
        WriteOp.put(_b('a'), _b('1')),
        WriteOp.put(_b('b'), _b('2')),
        WriteOp.delete(_b('old')),
      ]);
      expect(await engine.get(_b('a')), _b('1'));
      expect(await engine.get(_b('b')), _b('2'));
      expect(await engine.get(_b('old')), isNull);
      await engine.close();
    });

    test('data survives clean close and reopen via WAL replay', () async {
      final engine = await openEngine();
      await engine.put(_b('persist'), _b('me'));
      await engine.close();
      final reopened = await openEngine();
      expect(await reopened.get(_b('persist')), _b('me'));
      await reopened.close();
    });

    test('memtable overflow flushes to disk automatically', () async {
      final engine = await openEngine(memtableSizeBytes: 8 * 1024);
      for (var i = 0; i < 200; i++) {
        await engine.put(_b('key-$i'), Uint8List(100));
      }
      expect(engine.stats.sstableCount, greaterThan(0));
      for (var i = 0; i < 200; i += 37) {
        expect(await engine.get(_b('key-$i')), Uint8List(100));
      }
      await engine.close();
    });

    test('operations after close throw DatabaseClosedException', () async {
      final engine = await openEngine();
      await engine.close();
      expect(
        () => engine.put(_b('k'), _b('v')),
        throwsA(isA<DatabaseClosedException>()),
      );
      expect(
        () => engine.get(_b('k')),
        throwsA(isA<DatabaseClosedException>()),
      );
      // Double close is safe.
      await engine.close();
    });

    test('100 concurrent puts and gets settle consistently', () async {
      final engine = await openEngine();
      final puts = [
        for (var i = 0; i < 100; i++) engine.put(_b('c-$i'), _b('value-$i')),
      ];
      await Future.wait(puts);
      final gets = await Future.wait([
        for (var i = 0; i < 100; i++) engine.get(_b('c-$i')),
      ]);
      for (var i = 0; i < 100; i++) {
        expect(gets[i], _b('value-$i'));
      }
      await engine.close();
    });
  });

  group('LsmStorageEngine scan', () {
    test('scans in key order across memtable and sstables', () async {
      final engine = await openEngine();
      await engine.put(_b('d'), _b('4'));
      await engine.put(_b('b'), _b('2'));
      await engine.flush();
      await engine.put(_b('a'), _b('1'));
      await engine.put(_b('c'), _b('3'));
      final keys = await engine
          .scan()
          .map((kv) => utf8.decode(kv.key))
          .toList();
      expect(keys, ['a', 'b', 'c', 'd']);
      await engine.close();
    });

    test('newest version wins in scan', () async {
      final engine = await openEngine();
      await engine.put(_b('k'), _b('old'));
      await engine.flush();
      await engine.put(_b('k'), _b('new'));
      final values = await engine
          .scan()
          .map((kv) => utf8.decode(kv.value))
          .toList();
      expect(values, ['new']);
      await engine.close();
    });

    test('tombstones hide flushed values in scan', () async {
      final engine = await openEngine();
      await engine.put(_b('a'), _b('1'));
      await engine.put(_b('b'), _b('2'));
      await engine.flush();
      await engine.delete(_b('a'));
      final keys = await engine
          .scan()
          .map((kv) => utf8.decode(kv.key))
          .toList();
      expect(keys, ['b']);
      await engine.close();
    });

    test('range bounds are inclusive start, exclusive end', () async {
      final engine = await openEngine();
      for (final k in ['a', 'b', 'c', 'd', 'e']) {
        await engine.put(_b(k), _b('v$k'));
      }
      await engine.flush();
      final keys = await engine
          .scan(startKey: _b('b'), endKey: _b('e'))
          .map((kv) => utf8.decode(kv.key))
          .toList();
      expect(keys, ['b', 'c', 'd']);
      await engine.close();
    });

    test('reverse scan yields descending keys with same bounds', () async {
      final engine = await openEngine();
      for (final k in ['a', 'b', 'c', 'd', 'e']) {
        await engine.put(_b(k), _b('v$k'));
      }
      await engine.flush();
      await engine.put(_b('bb'), _b('mem'));
      final keys = await engine
          .scan(startKey: _b('b'), endKey: _b('e'), reverse: true)
          .map((kv) => utf8.decode(kv.key))
          .toList();
      expect(keys, ['d', 'c', 'bb', 'b']);
      await engine.close();
    });

    test('limit caps results in both directions', () async {
      final engine = await openEngine();
      for (final k in ['a', 'b', 'c', 'd']) {
        await engine.put(_b(k), _b('v$k'));
      }
      final forward = await engine
          .scan(limit: 2)
          .map((kv) => utf8.decode(kv.key))
          .toList();
      expect(forward, ['a', 'b']);
      final backward = await engine
          .scan(limit: 2, reverse: true)
          .map((kv) => utf8.decode(kv.key))
          .toList();
      expect(backward, ['d', 'c']);
      await engine.close();
    });

    test('scan merges across multiple levels after compaction', () async {
      final engine = await openEngine(memtableSizeBytes: 2 * 1024);
      for (var i = 0; i < 100; i++) {
        await engine.put(
          _b('key-${i.toString().padLeft(3, '0')}'),
          Uint8List(64),
        );
      }
      await engine.compact();
      await engine.put(_b('key-050'), _b('updated'));
      final results = await engine.scan().toList();
      expect(results.length, 100);
      final updated = results.firstWhere(
        (kv) => utf8.decode(kv.key) == 'key-050',
      );
      expect(utf8.decode(updated.value), 'updated');
      await engine.close();
    });
  });

  group('LsmStorageEngine stats', () {
    test('reports memtable and sstable counters', () async {
      final engine = await openEngine();
      await engine.put(_b('k'), _b('v'));
      expect(engine.stats.memtableEntries, 1);
      expect(engine.stats.lastSequenceNumber, greaterThan(0));
      await engine.flush();
      expect(engine.stats.memtableEntries, 0);
      expect(engine.stats.sstableCount, 1);
      expect(engine.stats.sstableSizeBytes, greaterThan(0));
      await engine.close();
    });
  });
}
