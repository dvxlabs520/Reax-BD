import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/storage/lsm_storage_engine.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

/// Engine-level crash recovery: the engine is abandoned without close
/// (simulating a process kill) and reopened over the same directory.
void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('crash_recovery_test_');
  });

  // The tests abandon engines on purpose, and Windows refuses to delete
  // files that are still open.
  tearDown(() => deleteTempDirectory(dir));

  Future<LsmStorageEngine> openEngine({SyncMode syncMode = SyncMode.full}) =>
      LsmStorageEngine.open(directory: dir.path, syncMode: syncMode);

  group('Crash recovery', () {
    test('acknowledged writes survive an abandoned engine', () async {
      final engine = await openEngine();
      await engine.put(_b('a'), _b('1'));
      await engine.put(_b('b'), _b('2'));
      await engine.delete(_b('a'));
      // Abandon without close: only the WAL has this data.

      final recovered = await openEngine();
      expect(await recovered.get(_b('a')), isNull);
      expect(await recovered.get(_b('b')), _b('2'));
      await recovered.close();
    });

    test('recovery preserves data already flushed to sstables', () async {
      final engine = await openEngine();
      await engine.put(_b('flushed'), _b('on-disk'));
      await engine.flush();
      await engine.put(_b('walled'), _b('in-wal'));
      // Abandon without close.

      final recovered = await openEngine();
      expect(await recovered.get(_b('flushed')), _b('on-disk'));
      expect(await recovered.get(_b('walled')), _b('in-wal'));
      await recovered.close();
    });

    test('a batch is all-or-nothing across recovery', () async {
      final engine = await openEngine();
      await engine.writeBatch([
        WriteOp.put(_b('t1'), _b('1')),
        WriteOp.put(_b('t2'), _b('2')),
        WriteOp.delete(_b('t3')),
      ], transactionId: 99);
      // Abandon without close.

      final recovered = await openEngine();
      expect(await recovered.get(_b('t1')), _b('1'));
      expect(await recovered.get(_b('t2')), _b('2'));
      await recovered.close();
    });

    test(
      'repeated crash/reopen cycles do not duplicate or lose data',
      () async {
        for (var round = 0; round < 3; round++) {
          final engine = await openEngine();
          await engine.put(_b('round-$round'), _b('value-$round'));
          // Abandon without close every round.
          for (var i = 0; i <= round; i++) {
            expect(await engine.get(_b('round-$i')), _b('value-$i'));
          }
        }
        final recovered = await openEngine();
        final all = await recovered.scan().toList();
        expect(all.length, 3);
        await recovered.close();
      },
    );
  });
}
