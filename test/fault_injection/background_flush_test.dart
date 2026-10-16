import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/storage/io_faults.dart';
import 'package:reaxdb_dart/src/core/storage/lsm_storage_engine.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));
String _s(Uint8List b) => utf8.decode(b);
String _k(int i) => 'k${i.toString().padLeft(6, '0')}';

/// Flushes and compactions run after the write that triggers them. These
/// tests cover the sealed memtables they leave behind and the crash points
/// of the background flush.
void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('background_flush_');
  });

  tearDown(() async {
    IoFaults.hook = null;
    await deleteTempDirectory(dir);
  });

  Future<LsmStorageEngine> openEngine({
    int memtableSizeBytes = 4 * 1024,
    void Function(Object, StackTrace)? onMaintenanceError,
  }) => LsmStorageEngine.open(
    directory: dir.path,
    memtableSizeBytes: memtableSizeBytes,
    onMaintenanceError: onMaintenanceError,
  );

  /// Waits until background maintenance has finished everything.
  Future<void> settle(LsmStorageEngine e) async {
    for (
      var i = 0;
      i < 1000 && (e.isMaintaining || e.stats.immutableMemtableCount > 0);
      i++
    ) {
      await Future<void>.delayed(const Duration(milliseconds: 2));
    }
    expect(e.isMaintaining, isFalse);
    expect(e.stats.immutableMemtableCount, 0);
  }

  /// Writes keys until at least one memtable has been sealed.
  Future<int> writeUntilSealed(LsmStorageEngine e, {int from = 0}) async {
    var i = from;
    while (e.stats.immutableMemtableCount == 0) {
      await e.put(_b(_k(i)), Uint8List(200));
      i++;
    }
    return i;
  }

  test('a write that seals the memtable does not wait for its flush', () async {
    final e = await openEngine();
    await writeUntilSealed(e);
    // The sealed memtable is still waiting: the write returned first.
    expect(e.stats.immutableMemtableCount, greaterThan(0));
    await settle(e);
    expect(e.stats.sstableCount, greaterThan(0));
    await e.close();
  });

  test('reads and scans see sealed memtables, tombstones included', () async {
    final e = await openEngine();
    await e.put(_b('gone'), _b('old'));
    await e.put(_b('kept'), _b('old'));
    await e.flush(); // Both now live in an SSTable.

    await e.delete(_b('gone'));
    await e.put(_b('kept'), _b('new'));
    await writeUntilSealed(e, from: 0);

    // Sealed, not flushed yet: the tombstone and the new value must win.
    expect(await e.get(_b('gone')), isNull);
    expect(_s((await e.get(_b('kept')))!), 'new');
    final keys = <String>[
      await for (final KeyValue kv in e.scan()) utf8.decode(kv.key),
    ];
    expect(keys, isNot(contains('gone')));
    expect(keys, contains('kept'));
    final sorted = List<String>.of(keys)..sort();
    expect(keys, sorted);

    await settle(e);
    expect(await e.get(_b('gone')), isNull);
    expect(_s((await e.get(_b('kept')))!), 'new');
    await e.close();
  });

  test('a scan running while a flush finishes stays complete', () async {
    final e = await openEngine();
    final written = await writeUntilSealed(e);
    final seen = <String>[];
    await for (final KeyValue kv in e.scan()) {
      seen.add(utf8.decode(kv.key));
      if (seen.length == 3) await settle(e);
    }
    expect(seen, <String>[for (var i = 0; i < written; i++) _k(i)]);
    await e.close();
  });

  test('close waits for the background flush', () async {
    var e = await openEngine();
    final written = await writeUntilSealed(e);
    await e.close();
    expect(e.stats.immutableMemtableCount, 0);
    e = await openEngine();
    expect(e.stats.sstableCount, greaterThan(0));
    for (var i = 0; i < written; i++) {
      expect(await e.get(_b(_k(i))), isNotNull, reason: _k(i));
    }
    await e.close();
  });

  test('the WAL keeps counting writes made after a seal', () async {
    final e = await openEngine();
    await writeUntilSealed(e);
    await settle(e);
    // The checkpoint covered only the sealed memtable.
    expect(e.walBytesSinceCheckpoint, 0);
    await e.put(_b('after'), Uint8List(100));
    final afterOne = e.walBytesSinceCheckpoint;
    expect(afterOne, greaterThan(100));
    await writeUntilSealed(e, from: 10000);
    await settle(e);
    expect(e.walBytesSinceCheckpoint, lessThan(afterOne + 4 * 1024));
    await e.close();
  });

  test('forEachOldestFirst rebuilds exactly what a scan returns', () async {
    // No fsync: 6,000 durable writes take more than the timeout on Windows,
    // and this test is about what is read back, not durability.
    final e = await LsmStorageEngine.open(
      directory: dir.path,
      syncMode: SyncMode.none,
      memtableSizeBytes: 8 * 1024,
      l0CompactionTrigger: 2,
      baseLevelTargetBytes: 32 * 1024,
      targetTableBytes: 8 * 1024,
    );
    for (var round = 0; round < 4; round++) {
      for (var i = 0; i < 1500; i++) {
        if ((i + round) % 5 == 0) {
          await e.delete(_b(_k(i)));
        } else {
          await e.put(_b(_k(i)), _b('r$round-$i'));
        }
      }
    }
    // Leave some data in memtables too, sealed and active.
    for (var i = 0; i < 40; i++) {
      await e.put(_b(_k(i)), _b('last-$i'));
    }
    expect(
      e.stats.levelTableCounts.where((int c) => c > 0).length,
      greaterThan(1),
    );

    final rebuilt = <String, String>{};
    await e.forEachOldestFirst((Uint8List key, Uint8List? value) {
      if (value == null) {
        rebuilt.remove(utf8.decode(key));
      } else {
        rebuilt[utf8.decode(key)] = utf8.decode(value);
      }
    });
    final scanned = <String, String>{
      await for (final KeyValue kv in e.scan())
        utf8.decode(kv.key): utf8.decode(kv.value),
    };
    expect(rebuilt, scanned);
    await e.close();
  });

  group('a crash at each step of the flush loses nothing', () {
    Future<void> crashDuring(IoOp failing) async {
      final errors = <Object>[];
      final e = await openEngine(
        onMaintenanceError: (err, _) => errors.add(err),
      );
      IoFaults.hook = (op, _, _) => op == failing ? 0 : null;
      final written = await writeUntilSealed(e);
      // Let the background flush run into the failure.
      for (var i = 0; i < 200 && errors.isEmpty; i++) {
        await Future<void>.delayed(const Duration(milliseconds: 2));
      }
      expect(errors, isNotEmpty, reason: 'the injected failure was reported');
      IoFaults.hook = null;
      // Abandon the engine without closing it: a process kill.

      final recovered = await openEngine();
      for (var i = 0; i < written; i++) {
        expect(await recovered.get(_b(_k(i))), isNotNull, reason: _k(i));
      }
      var count = 0;
      await for (final _ in recovered.scan()) {
        count++;
      }
      expect(count, written, reason: 'no key lost or duplicated');
      await recovered.close();
    }

    test('writing the SSTable fails', () => crashDuring(IoOp.sstableWrite));
    test(
      'publishing the manifest fails',
      () => crashDuring(IoOp.manifestWrite),
    );
    test('the checkpoint after the manifest fails', () {
      return crashDuring(IoOp.walCreate);
    });
  });

  test('a failed background flush is retried by a later write', () async {
    final errors = <Object>[];
    final e = await openEngine(onMaintenanceError: (err, _) => errors.add(err));
    IoFaults.hook = (op, _, _) => op == IoOp.sstableWrite ? 0 : null;
    await writeUntilSealed(e);
    for (var i = 0; i < 200 && errors.isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 2));
    }
    expect(e.lastMaintenanceError, isNotNull);
    IoFaults.hook = null;
    // After the backoff, the next write starts the flush again.
    await Future<void>.delayed(const Duration(milliseconds: 120));
    await e.put(_b('trigger'), _b('1'));
    await settle(e);
    expect(e.lastMaintenanceError, isNull);
    expect(e.stats.sstableCount, greaterThan(0));
    await e.close();
  });

  test(
    'SyncMode.none still flushes and checkpoints in the background',
    () async {
      final e = await LsmStorageEngine.open(
        directory: dir.path,
        memtableSizeBytes: 4 * 1024,
        syncMode: SyncMode.none,
      );
      final written = await writeUntilSealed(e);
      await settle(e);
      await e.close();
      final reopened = await openEngine();
      for (var i = 0; i < written; i++) {
        expect(await reopened.get(_b(_k(i))), isNotNull);
      }
      await reopened.close();
    },
  );
}
