import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:reaxdb_dart/src/core/transactions/transaction_manager.dart';
import 'package:test/test.dart';

/// In-memory storage engine used to test the transaction layer in isolation.
class FakeStorageEngine implements StorageEngine {
  final Map<String, Uint8List> _data = <String, Uint8List>{};

  int putCalls = 0;
  int deleteCalls = 0;
  int writeBatchCalls = 0;
  final List<int?> batchTransactionIds = <int?>[];
  Object? failNextWriteBatch;

  String _mapKey(Uint8List key) => base64Encode(key);

  /// Direct write bypassing the transaction layer (simulates an external
  /// committed writer).
  void putDirect(String key, List<int> value) {
    _data[base64Encode(utf8.encode(key))] = Uint8List.fromList(value);
  }

  Uint8List? getDirect(String key) => _data[base64Encode(utf8.encode(key))];

  @override
  Future<void> put(Uint8List key, Uint8List value) async {
    putCalls++;
    _data[_mapKey(key)] = value;
  }

  @override
  Future<Uint8List?> get(Uint8List key) async => _data[_mapKey(key)];

  @override
  Future<void> delete(Uint8List key) async {
    deleteCalls++;
    _data.remove(_mapKey(key));
  }

  @override
  Future<void> writeBatch(List<WriteOp> ops, {int? transactionId}) async {
    writeBatchCalls++;
    batchTransactionIds.add(transactionId);
    final Object? failure = failNextWriteBatch;
    if (failure != null) {
      failNextWriteBatch = null;
      throw failure;
    }
    for (final WriteOp op in ops) {
      if (op.isDelete) {
        _data.remove(_mapKey(op.key));
      } else {
        _data[_mapKey(op.key)] = op.value!;
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

Uint8List bytes(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  late FakeStorageEngine engine;
  late TransactionManager manager;

  setUp(() {
    engine = FakeStorageEngine();
    manager = TransactionManager(storageEngine: engine);
  });

  tearDown(() async {
    await manager.close();
  });

  void expectNoLocksHeld() {
    expect(manager.lockManager.lockedKeyCount, 0);
    expect(manager.lockManager.waiterCount, 0);
  }

  group('basic lifecycle', () {
    test('tx.get then tx.put on the same key does not self-deadlock', () async {
      // The historical bug: shared -> exclusive upgrade on the same key hung
      // for the full lock timeout. This must finish immediately.
      for (final IsolationLevel level in <IsolationLevel>[
        IsolationLevel.readCommitted,
        IsolationLevel.repeatableRead,
        IsolationLevel.serializable,
      ]) {
        engine.putDirect('upgrade-key', <int>[1]);
        final Transaction tx = manager.begin(isolationLevel: level);
        final Stopwatch watch = Stopwatch()..start();
        await tx.get('upgrade-key');
        await tx.put('upgrade-key', bytes('new'));
        await manager.commit(tx);
        watch.stop();
        expect(
          watch.elapsed,
          lessThan(const Duration(seconds: 2)),
          reason: 'upgrade must not wait for the lock timeout ($level)',
        );
        expect(engine.getDirect('upgrade-key'), bytes('new'));
      }
      expectNoLocksHeld();
    });

    test(
      'read-your-own-writes: get observes buffered put and delete',
      () async {
        engine.putDirect('k', <int>[9]);
        final Transaction tx = manager.begin();
        await tx.put('k', bytes('mine'));
        expect(await tx.get('k'), bytes('mine'));
        await tx.delete('k');
        expect(await tx.get('k'), isNull);
        await manager.commit(tx);
        expect(engine.getDirect('k'), isNull);
      },
    );

    test(
      'commit applies the write set as ONE writeBatch with the tx id',
      () async {
        final Transaction tx = manager.begin();
        await tx.put('a', bytes('1'));
        await tx.put('b', bytes('2'));
        await tx.delete('c');
        final CommitResult result = await manager.commit(tx);
        expect(engine.writeBatchCalls, 1);
        expect(engine.batchTransactionIds.single, tx.id);
        expect(
          engine.putCalls,
          0,
          reason: 'transactional writes must never use per-key put',
        );
        expect(
          engine.deleteCalls,
          0,
          reason: 'transactional writes must never use per-key delete',
        );
        expect(result.operations, hasLength(3));
      },
    );

    test(
      'commit result reports key, old value and new value per operation',
      () async {
        engine.putDirect('existing', <int>[1, 2]);
        engine.putDirect('doomed', <int>[3]);
        final Transaction tx = manager.begin();
        await tx.put('existing', bytes('updated'));
        await tx.put('fresh', bytes('created'));
        await tx.delete('doomed');
        final CommitResult result = await manager.commit(tx);

        final Map<String, AppliedOperation> byKey = <String, AppliedOperation>{
          for (final AppliedOperation op in result.operations) op.key: op,
        };
        expect(byKey['existing']!.type, AppliedOperationType.put);
        expect(byKey['existing']!.oldValue, Uint8List.fromList(<int>[1, 2]));
        expect(byKey['existing']!.newValue, bytes('updated'));
        expect(byKey['fresh']!.oldValue, isNull);
        expect(byKey['fresh']!.newValue, bytes('created'));
        expect(byKey['doomed']!.type, AppliedOperationType.delete);
        expect(byKey['doomed']!.oldValue, Uint8List.fromList(<int>[3]));
        expect(byKey['doomed']!.newValue, isNull);
      },
    );

    test(
      'operations on a finished transaction throw a typed exception',
      () async {
        final Transaction tx = manager.begin();
        await tx.put('x', bytes('1'));
        await manager.commit(tx);
        expect(() => tx.get('x'), throwsA(isA<TransactionConflictException>()));
        expect(
          () => tx.put('x', bytes('2')),
          throwsA(isA<TransactionConflictException>()),
        );
        expect(
          () => manager.commit(tx),
          throwsA(isA<TransactionConflictException>()),
        );
      },
    );
  });

  group('isolation', () {
    test(
      'repeatableRead returns the first-read value on repeat reads',
      () async {
        engine.putDirect('rr', <int>[1]);
        final Transaction tx = manager.begin(
          isolationLevel: IsolationLevel.repeatableRead,
        );
        expect(await tx.get('rr'), Uint8List.fromList(<int>[1]));
        engine.putDirect('rr', <int>[2]); // external change
        expect(await tx.get('rr'), Uint8List.fromList(<int>[1]));
        await manager.abort(tx);
        expectNoLocksHeld();
      },
    );

    test(
      'serializable: reading an ABSENT key and committing after an external '
      'insert of that key fails validation (phantom on point read)',
      () async {
        final Transaction tx = manager.begin(
          isolationLevel: IsolationLevel.serializable,
        );
        expect(await tx.get('ghost'), isNull);
        // External (non-transactional) writer inserts the key.
        engine.putDirect('ghost', <int>[42]);
        await tx.put('other', bytes('v'));
        await expectLater(
          manager.commit(tx),
          throwsA(isA<TransactionConflictException>()),
        );
        expect(tx.state, TransactionState.aborted);
        // The aborted write set must not have been applied.
        expect(engine.getDirect('other'), isNull);
        expectNoLocksHeld();
      },
    );

    test(
      'optimistic: two conflicting transactions -> conflict, not corruption',
      () async {
        engine.putDirect('acct', <int>[100]);
        final Transaction a = manager.begin(
          isolationLevel: IsolationLevel.optimistic,
        );
        final Transaction b = manager.begin(
          isolationLevel: IsolationLevel.optimistic,
        );
        await a.get('acct');
        await b.get('acct');
        await a.put('acct', bytes('from-a'));
        await b.put('acct', bytes('from-b'));

        await manager.commit(a); // first committer wins
        await expectLater(
          manager.commit(b),
          throwsA(isA<TransactionConflictException>()),
        );
        expect(
          engine.getDirect('acct'),
          bytes('from-a'),
          reason: 'loser must not have clobbered the winner',
        );
        expectNoLocksHeld();
      },
    );

    test(
      'optimistic read-only dependency conflicts when a read key changes',
      () async {
        engine.putDirect('r', <int>[1]);
        final Transaction a = manager.begin(
          isolationLevel: IsolationLevel.optimistic,
        );
        await a.get('r');
        await a.put('out', bytes('derived'));
        // A pessimistic transaction commits a change to the read key.
        final Transaction b = manager.begin();
        await b.put('r', bytes('changed'));
        await manager.commit(b);
        await expectLater(
          manager.commit(a),
          throwsA(isA<TransactionConflictException>()),
        );
      },
    );
  });

  group('deadlock and timeout', () {
    test(
      'two-transaction deadlock is detected quickly, one victim aborts',
      () async {
        final Transaction t1 = manager.begin();
        final Transaction t2 = manager.begin();
        await t1.put('A', bytes('t1'));
        await t2.put('B', bytes('t2'));

        final Stopwatch watch = Stopwatch()..start();
        final Future<Object?> f1 = t1
            .put('B', bytes('t1'))
            .then<Object?>((_) => null, onError: (Object e) => e);
        final Future<Object?> f2 = t2
            .put('A', bytes('t2'))
            .then<Object?>((_) => null, onError: (Object e) => e);
        final List<Object?> outcomes = await Future.wait(<Future<Object?>>[
          f1,
          f2,
        ]);
        watch.stop();

        final List<Object?> errors = outcomes
            .where((Object? o) => o != null)
            .toList();
        expect(
          errors,
          hasLength(1),
          reason: 'exactly one transaction is the deadlock victim',
        );
        expect(errors.single, isA<DeadlockException>());
        expect(
          watch.elapsed,
          lessThan(const Duration(seconds: 3)),
          reason: 'detection must not rely on the lock timeout',
        );

        // The survivor can commit; the victim was auto-aborted.
        final Transaction survivor = outcomes[0] == null ? t1 : t2;
        final Transaction victim = outcomes[0] == null ? t2 : t1;
        expect(victim.state, TransactionState.aborted);
        await manager.commit(survivor);
        expectNoLocksHeld();
      },
    );

    test(
      'shared-to-exclusive upgrade deadlock between two readers resolves',
      () async {
        engine.putDirect('hot', <int>[0]);
        final Transaction t1 = manager.begin(
          isolationLevel: IsolationLevel.repeatableRead,
        );
        final Transaction t2 = manager.begin(
          isolationLevel: IsolationLevel.repeatableRead,
        );
        await t1.get('hot');
        await t2.get('hot');

        final Future<Object?> u1 = t1
            .put('hot', bytes('1'))
            .then<Object?>((_) => null, onError: (Object e) => e);
        final Future<Object?> u2 = t2
            .put('hot', bytes('2'))
            .then<Object?>((_) => null, onError: (Object e) => e);
        final List<Object?> outcomes = await Future.wait(<Future<Object?>>[
          u1,
          u2,
        ]).timeout(const Duration(seconds: 5));

        expect(outcomes.whereType<DeadlockException>(), hasLength(1));
        final Transaction survivor = outcomes[0] == null ? t1 : t2;
        await manager.commit(survivor);
        expectNoLocksHeld();
      },
    );

    test(
      'lock wait times out with TransactionTimeoutException as a backstop',
      () async {
        final TransactionManager fastManager = TransactionManager(
          storageEngine: engine,
          lockTimeout: const Duration(milliseconds: 200),
        );
        final Transaction holder = fastManager.begin();
        await holder.put('locked', bytes('h'));
        final Transaction waiter = fastManager.begin();
        // No cycle here (holder is not waiting), so this is a pure timeout.
        await expectLater(
          waiter.put('locked', bytes('w')),
          throwsA(isA<TransactionTimeoutException>()),
        );
        expect(waiter.state, TransactionState.aborted);
        await fastManager.commit(holder);
        expect(fastManager.lockManager.lockedKeyCount, 0);
        expect(fastManager.lockManager.waiterCount, 0);
        await fastManager.close();
      },
    );
  });

  group('lock release on every path', () {
    test('locks fully released after commit', () async {
      final Transaction tx = manager.begin(
        isolationLevel: IsolationLevel.serializable,
      );
      await tx.get('a');
      await tx.put('b', bytes('1'));
      await manager.commit(tx);
      expectNoLocksHeld();
    });

    test('locks fully released after abort', () async {
      final Transaction tx = manager.begin(
        isolationLevel: IsolationLevel.serializable,
      );
      await tx.get('a');
      await tx.put('b', bytes('1'));
      await manager.abort(tx);
      expectNoLocksHeld();
    });

    test(
      'locks fully released after a storage engine error during commit',
      () async {
        final Transaction tx = manager.begin();
        await tx.put('k', bytes('v'));
        engine.failNextWriteBatch = const CorruptionException('injected');
        await expectLater(
          manager.commit(tx),
          throwsA(isA<CorruptionException>()),
        );
        expect(tx.state, TransactionState.aborted);
        expectNoLocksHeld();
        // A later transaction can lock and commit the same key.
        final Transaction next = manager.begin();
        await next.put('k', bytes('v2'));
        await manager.commit(next);
        expect(engine.getDirect('k'), bytes('v2'));
      },
    );

    test('locks fully released after a lock timeout', () async {
      final TransactionManager fastManager = TransactionManager(
        storageEngine: engine,
        lockTimeout: const Duration(milliseconds: 100),
      );
      final Transaction holder = fastManager.begin();
      await holder.put('t', bytes('h'));
      final Transaction waiter = fastManager.begin();
      await waiter.put('other', bytes('w'));
      await expectLater(
        waiter.put('t', bytes('w')),
        throwsA(isA<TransactionTimeoutException>()),
      );
      // The waiter's already-held lock on "other" was released by auto-abort.
      final Transaction third = fastManager.begin();
      await third.put('other', bytes('3'));
      await fastManager.commit(third);
      await fastManager.commit(holder);
      expect(fastManager.lockManager.lockedKeyCount, 0);
      expect(fastManager.lockManager.waiterCount, 0);
      await fastManager.close();
    });
  });

  group('runTransaction retry helper', () {
    test('retries on conflict and succeeds within the bound', () async {
      int attempts = 0;
      final String result = await manager.runTransaction<String>(
        (Transaction tx) async {
          attempts++;
          if (attempts < 3) {
            throw const TransactionConflictException('synthetic conflict');
          }
          await tx.put('retry', bytes('done'));
          return 'ok';
        },
        maxAttempts: 3,
        initialBackoff: const Duration(milliseconds: 1),
      );
      expect(result, 'ok');
      expect(attempts, 3);
      expect(engine.getDirect('retry'), bytes('done'));
      expectNoLocksHeld();
    });

    test('gives up after maxAttempts and rethrows the conflict', () async {
      int attempts = 0;
      await expectLater(
        manager.runTransaction<void>(
          (Transaction tx) async {
            attempts++;
            throw const TransactionConflictException('always conflicts');
          },
          maxAttempts: 3,
          initialBackoff: const Duration(milliseconds: 1),
        ),
        throwsA(isA<TransactionConflictException>()),
      );
      expect(attempts, 3);
    });

    test('does NOT retry non-conflict errors', () async {
      int attempts = 0;
      await expectLater(
        manager.runTransaction<void>((Transaction tx) async {
          attempts++;
          throw const SerializationException('bad payload');
        }, maxAttempts: 5),
        throwsA(isA<SerializationException>()),
      );
      expect(attempts, 1);
      expectNoLocksHeld();
    });

    test('onCommit receives the commit result', () async {
      CommitResult? seen;
      await manager.runTransaction<void>((Transaction tx) async {
        await tx.put('cb', bytes('v'));
      }, onCommit: (CommitResult r) => seen = r);
      expect(seen, isNotNull);
      expect(seen!.operations.single.key, 'cb');
    });
  });

  group('concurrency', () {
    test('concurrent transactions on disjoint keys all commit', () async {
      final List<Future<void>> futures = <Future<void>>[
        for (int i = 0; i < 20; i++)
          manager.runTransaction<void>((Transaction tx) async {
            await tx.put('key-$i', bytes('value-$i'));
            expect(await tx.get('key-$i'), bytes('value-$i'));
          }),
      ];
      await Future.wait(futures);
      for (int i = 0; i < 20; i++) {
        expect(engine.getDirect('key-$i'), bytes('value-$i'));
      }
      expect(manager.stats.committedTransactions, 20);
      expectNoLocksHeld();
    });

    test(
      'keys containing underscores and colons never cross-notify locks',
      () async {
        // Historical bug: waiter keys were built as "<key>_<txId>" and matched
        // by prefix, so "a" and "a_1" interfered. Verify independence.
        final Transaction t1 = manager.begin();
        final Transaction t2 = manager.begin();
        await t1.put('a', bytes('1'));
        await t2.put('a_1', bytes('2'));
        await t2.put('users:1', bytes('3'));
        await manager.commit(t1);
        await manager.commit(t2);
        expect(engine.getDirect('a'), bytes('1'));
        expect(engine.getDirect('a_1'), bytes('2'));
        expectNoLocksHeld();
      },
    );
  });
}
