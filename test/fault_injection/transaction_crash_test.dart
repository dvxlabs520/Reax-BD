import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:reaxdb_dart/src/core/transactions/transaction_manager.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

Uint8List bytes(String s) => Uint8List.fromList(utf8.encode(s));

/// Engine that "crashes" (throws) inside writeBatch, applying nothing, and
/// counts every per-key mutation so the test can prove the transaction
/// manager never bypasses the atomic batch path.
class CrashOnBatchEngine implements StorageEngine {
  final Map<String, Uint8List> data = <String, Uint8List>{};
  bool crashOnNextBatch = false;
  int putCalls = 0;
  int deleteCalls = 0;
  int batchCalls = 0;

  String _k(Uint8List key) => base64Encode(key);

  @override
  Future<void> put(Uint8List key, Uint8List value) async {
    putCalls++;
    data[_k(key)] = value;
  }

  @override
  Future<Uint8List?> get(Uint8List key) async => data[_k(key)];

  @override
  Future<void> delete(Uint8List key) async {
    deleteCalls++;
    data.remove(_k(key));
  }

  @override
  Future<void> writeBatch(List<WriteOp> ops, {int? transactionId}) async {
    batchCalls++;
    if (crashOnNextBatch) {
      crashOnNextBatch = false;
      // Simulated crash: the batch is atomic, so NOTHING is applied.
      throw const CorruptionException('simulated crash during writeBatch');
    }
    for (final WriteOp op in ops) {
      if (op.isDelete) {
        data.remove(_k(op.key));
      } else {
        data[_k(op.key)] = op.value!;
      }
    }
  }

  @override
  Stream<KeyValue> scan({
    Uint8List? startKey,
    Uint8List? endKey,
    int? limit,
    bool reverse = false,
  }) => const Stream<KeyValue>.empty();

  @override
  Future<void> flush() async {}

  @override
  Future<void> compact() async {}

  @override
  Future<void> close() async {}

  @override
  StorageStats get stats => const StorageStats(
    memtableEntries: 0,
    memtableSizeBytes: 0,
    immutableMemtableCount: 0,
    sstableCount: 0,
    sstableSizeBytes: 0,
    levelTableCounts: <int>[],
    lastSequenceNumber: 0,
  );
}

void main() {
  group('transaction crash mid-commit (WAL level)', () {
    late Directory dir;

    setUp(() async {
      dir = await Directory.systemTemp.createTemp('reaxdb_tx_crash_');
    });

    tearDown(() async {
      if (await dir.exists()) {
        await dir.delete(recursive: true);
      }
    });

    test('a transaction whose commit record never hit disk is discarded '
        'entirely on replay (all-or-nothing)', () async {
      final WriteAheadLog wal = await WriteAheadLog.open(
        directory: dir.path,
        syncMode: SyncMode.full,
      );

      // Committed transaction 1: must survive in full.
      await wal.append(WalEntry.txBegin(1));
      await wal.append(
        WalEntry.put(bytes('c1'), bytes('v1'), transactionId: 1),
      );
      await wal.append(
        WalEntry.put(bytes('c2'), bytes('v2'), transactionId: 1),
      );
      await wal.append(WalEntry.txCommit(1));

      // Transaction 2 crashes mid-commit: begin + a SUBSET of its writes are
      // durable, but the commit record never made it.
      await wal.append(WalEntry.txBegin(2));
      await wal.append(
        WalEntry.put(bytes('u1'), bytes('x1'), transactionId: 2),
      );
      await wal.append(
        WalEntry.put(bytes('u2'), bytes('x2'), transactionId: 2),
      );
      // CRASH: abandon the log without close/commit.

      final WriteAheadLog reopened = await WriteAheadLog.open(
        directory: dir.path,
        syncMode: SyncMode.full,
      );
      final List<WalEntry> replayed = await reopened.replay().toList();

      final Set<String> keys = replayed
          .map((WalEntry e) => utf8.decode(e.key!))
          .toSet();
      expect(
        keys,
        containsAll(<String>['c1', 'c2']),
        reason: 'the committed transaction must survive in full',
      );
      expect(
        keys,
        isNot(contains('u1')),
        reason: 'uncommitted transaction writes must be discarded',
      );
      expect(keys, isNot(contains('u2')));

      // Never a partial write set: either both of tx2's keys or neither.
      final bool anyOfTx2 = keys.contains('u1') || keys.contains('u2');
      final bool allOfTx2 = keys.contains('u1') && keys.contains('u2');
      expect(anyOfTx2, allOfTx2, reason: 'commit must be all-or-nothing');

      await reopened.close();
    });

    test('replay after crash is idempotent (same result twice)', () async {
      final WriteAheadLog wal = await WriteAheadLog.open(
        directory: dir.path,
        syncMode: SyncMode.full,
      );
      await wal.append(WalEntry.txBegin(7));
      await wal.append(WalEntry.put(bytes('a'), bytes('1'), transactionId: 7));
      await wal.append(WalEntry.txCommit(7));
      await wal.append(WalEntry.txBegin(8));
      await wal.append(WalEntry.put(bytes('b'), bytes('2'), transactionId: 8));
      // CRASH before commit of tx 8.

      final WriteAheadLog reopened = await WriteAheadLog.open(
        directory: dir.path,
        syncMode: SyncMode.full,
      );
      List<String> keysOf(List<WalEntry> entries) =>
          entries.map((WalEntry e) => utf8.decode(e.key!)).toList();
      final List<String> first = keysOf(await reopened.replay().toList());
      final List<String> second = keysOf(await reopened.replay().toList());
      expect(first, <String>['a']);
      expect(second, first);
      await reopened.close();
    });
  });

  group('transaction crash mid-commit (manager level)', () {
    test('a commit that fails inside writeBatch applies NOTHING and never '
        'falls back to per-key writes', () async {
      final CrashOnBatchEngine engine = CrashOnBatchEngine();
      final TransactionManager manager = TransactionManager(
        storageEngine: engine,
      );

      // Seed committed state.
      await manager.runTransaction<void>((Transaction tx) async {
        await tx.put('balance:a', bytes('100'));
        await tx.put('balance:b', bytes('0'));
      });

      // Multi-key transfer that crashes during the batch.
      engine.crashOnNextBatch = true;
      final Transaction tx = manager.begin();
      await tx.put('balance:a', bytes('40'));
      await tx.put('balance:b', bytes('60'));
      await expectLater(
        manager.commit(tx),
        throwsA(isA<CorruptionException>()),
      );

      // All-or-nothing: both keys keep their pre-transaction values.
      expect(engine.data[base64Encode(utf8.encode('balance:a'))], bytes('100'));
      expect(engine.data[base64Encode(utf8.encode('balance:b'))], bytes('0'));
      expect(
        engine.putCalls,
        0,
        reason: 'transactional commit must never issue per-key puts',
      );
      expect(engine.deleteCalls, 0);
      expect(manager.lockManager.lockedKeyCount, 0);
      expect(manager.lockManager.waiterCount, 0);
      await manager.close();
    });
  });
}
