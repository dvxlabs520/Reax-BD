/// Concurrency stress tests for the ReaxDB facade.
///
/// These exercise the paths that only misbehave when several operations are
/// in flight at once: interleaved mutations, reads racing a compaction,
/// transactions contending for the same keys, and change streams under load.
/// Every test is deterministic in what it asserts - where the interleaving is
/// genuinely nondeterministic the assertion is an invariant, not an exact
/// value.
@Timeout(Duration(minutes: 3))
library;

import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:reaxdb_dart/src/core/streams/reactive_stream.dart'
    show ChangeStreamHub;
import 'package:test/test.dart';

/// Polls [condition] until it holds or [within] elapses.
Future<void> waitUntil(
  bool Function() condition, {
  Duration within = const Duration(seconds: 10),
}) async {
  final DateTime deadline = DateTime.now().add(within);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) return;
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
}

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_stress_conc_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  Future<ReaxDB> openDb({
    int memtableSizeBytes = 4 * 1024 * 1024,
    int cacheMaxEntries = 10000,
    SyncMode syncMode = SyncMode.none,
    String name = 'db',
  }) => ReaxDB.open(
    path: '${root.path}/$name',
    syncMode: syncMode,
    memtableSizeBytes: memtableSizeBytes,
    cacheMaxEntries: cacheMaxEntries,
  );

  group('interleaved mutations', () {
    test(
      '200 concurrent put/get/delete chains settle to an exact state',
      () async {
        final ReaxDB db = await openDb();
        const int chains = 200;

        // Each chain is deterministic on its own key, so the final state is
        // exact; the chains themselves run interleaved, which is the point.
        Future<void> chain(int i) async {
          final String key = 'chain:${i.toString().padLeft(4, '0')}';
          await db.put(key, <String, dynamic>{'v': i, 'stage': 'first'});
          final Map<String, dynamic>? first = await db
              .get<Map<String, dynamic>>(key);
          expect(first, isNotNull, reason: 'first write of $key vanished');
          expect(first!['v'], i);

          await db.put(key, <String, dynamic>{'v': i * 2, 'stage': 'second'});
          final Map<String, dynamic>? second = await db
              .get<Map<String, dynamic>>(key);
          expect(second!['v'], i * 2);

          if (i % 3 == 0) {
            await db.delete(key);
            expect(await db.get<Map<String, dynamic>>(key), isNull);
          }
        }

        await Future.wait<void>(<Future<void>>[
          for (int i = 0; i < chains; i++) chain(i),
        ]);

        for (int i = 0; i < chains; i++) {
          final String key = 'chain:${i.toString().padLeft(4, '0')}';
          final Map<String, dynamic>? value = await db
              .get<Map<String, dynamic>>(key);
          if (i % 3 == 0) {
            expect(value, isNull, reason: '$key should have been deleted');
          } else {
            expect(value, isNotNull, reason: '$key should still exist');
            expect(value!['v'], i * 2);
            expect(value['stage'], 'second');
          }
        }

        final List<String> live = await db.keys(prefix: 'chain:').toList();
        expect(live.length, chains - (chains / 3).ceil());
        await db.close();
      },
    );

    test(
      'racing writers on one key never expose a value nobody wrote',
      () async {
        final ReaxDB db = await openDb();
        const String key = 'contended';
        const int writers = 128;

        final List<int> observed = <int>[];
        await Future.wait<void>(<Future<void>>[
          for (int i = 0; i < writers; i++)
            () async {
              await db.put(key, <String, dynamic>{'writer': i});
              final Map<String, dynamic>? read = await db
                  .get<Map<String, dynamic>>(key);
              expect(read, isNotNull, reason: 'a concurrent write erased $key');
              observed.add(read!['writer'] as int);
            }(),
        ]);

        // Every value ever observed must be one somebody actually wrote, and
        // the surviving value must be one of them too.
        for (final int value in observed) {
          expect(value, inInclusiveRange(0, writers - 1));
        }
        final Map<String, dynamic>? finalValue = await db
            .get<Map<String, dynamic>>(key);
        expect(finalValue, isNotNull);
        expect(finalValue!['writer'], inInclusiveRange(0, writers - 1));
        await db.close();
      },
    );

    test('concurrent batch writes are each applied in full', () async {
      final ReaxDB db = await openDb();
      const int batches = 40;
      const int perBatch = 25;

      await Future.wait<void>(<Future<void>>[
        for (int b = 0; b < batches; b++)
          db.putBatch(<String, Object?>{
            for (int i = 0; i < perBatch; i++)
              'batch:$b:$i': <String, dynamic>{'b': b, 'i': i},
          }),
      ]);

      for (int b = 0; b < batches; b++) {
        for (int i = 0; i < perBatch; i++) {
          final Map<String, dynamic>? value = await db
              .get<Map<String, dynamic>>('batch:$b:$i');
          expect(value, isNotNull, reason: 'batch:$b:$i missing');
          expect(value!['b'], b);
          expect(value['i'], i);
        }
      }
      await db.close();
    });
  });

  group('reads racing compaction', () {
    test(
      'point reads during a forced compaction never miss or go stale',
      () async {
        // A small memtable forces many L0 tables, so the compaction below has
        // real work to do while the reads are running. A tiny record cache
        // makes the reads genuine storage reads rather than cache hits, which
        // is the interesting race.
        final ReaxDB db = await openDb(
          memtableSizeBytes: 32 * 1024,
          cacheMaxEntries: 8,
        );
        const int count = 4000;
        for (int i = 0; i < count; i++) {
          await db.put('key:${i.toString().padLeft(6, '0')}', <String, dynamic>{
            'v': i,
            'pad': 'x' * 100,
          });
        }
        expect(db.storageStats.sstableCount, greaterThan(1));

        final Random random = Random(42);
        bool compacting = true;
        final Future<void> compaction = db.compact().whenComplete(() {
          compacting = false;
        });

        int reads = 0;
        Object? failure;
        // The cap keeps a stalled compaction from spinning forever; the
        // explicit yield hands the event loop back so the compaction's file
        // I/O can actually make progress between reads.
        while (compacting && reads < 20000) {
          final int i = random.nextInt(count);
          final String key = 'key:${i.toString().padLeft(6, '0')}';
          try {
            final Map<String, dynamic>? value = await db
                .get<Map<String, dynamic>>(key);
            if (value == null || value['v'] != i) {
              failure ??= 'read of $key returned $value during compaction';
            }
          } catch (error) {
            failure ??= error;
          }
          reads++;
          await Future<void>.delayed(Duration.zero);
        }
        await compaction;

        expect(failure, isNull);
        expect(reads, greaterThan(0), reason: 'the reads never overlapped');
        expect(compacting, isFalse, reason: 'compaction did not finish');

        // Everything is still exactly right once compaction has finished.
        for (int i = 0; i < count; i += 37) {
          final String key = 'key:${i.toString().padLeft(6, '0')}';
          expect((await db.get<Map<String, dynamic>>(key))!['v'], i);
        }
        await db.close();
      },
    );

    test(
      'scans during a forced compaction stay complete and ordered',
      () async {
        final ReaxDB db = await openDb(
          memtableSizeBytes: 32 * 1024,
          cacheMaxEntries: 8,
        );
        const int count = 3000;
        for (int i = 0; i < count; i++) {
          await db.put('s:${i.toString().padLeft(6, '0')}', <String, dynamic>{
            'v': i,
            'pad': 'y' * 100,
          });
        }

        bool compacting = true;
        final Future<void> compaction = db.compact().whenComplete(() {
          compacting = false;
        });

        final List<Object> problems = <Object>[];
        int scans = 0;
        while (compacting && scans < 2000) {
          try {
            final List<ReaxEntry<Map<String, dynamic>>> entries = await db
                .scan<Map<String, dynamic>>(
                  startKey: 's:000100',
                  endKey: 's:000200',
                )
                .toList();
            if (entries.length != 100) {
              problems.add('scan returned ${entries.length} entries, want 100');
            }
            for (int i = 0; i < entries.length - 1; i++) {
              if (entries[i].key.compareTo(entries[i + 1].key) >= 0) {
                problems.add('scan out of order at ${entries[i].key}');
              }
            }
          } catch (error) {
            problems.add(error);
          }
          scans++;
          await Future<void>.delayed(Duration.zero);
        }
        await compaction;

        expect(problems, isEmpty);
        expect(scans, greaterThan(0), reason: 'the scans never overlapped');
        expect(compacting, isFalse, reason: 'compaction did not finish');
        await db.close();
      },
    );
  });

  group('contending transactions', () {
    test('overlapping transfers either commit or fail with a typed exception, '
        'and the total is preserved', () async {
      final ReaxDB db = await openDb();
      const int accounts = 6;
      const int startingBalance = 100;
      const int attempts = 60;

      for (int a = 0; a < accounts; a++) {
        await db.put('acct:$a', <String, dynamic>{'balance': startingBalance});
      }

      Future<void> transfer(int i) => db.transaction<void>(
        (ReaxTransaction tx) async {
          // A ring of transfers: adjacent accounts, so different orderings
          // really do contend and can deadlock if locking is wrong.
          final String from = 'acct:${i % accounts}';
          final String to = 'acct:${(i + 1) % accounts}';
          final Map<String, dynamic> source = (await tx
              .get<Map<String, dynamic>>(from))!;
          final Map<String, dynamic> target = (await tx
              .get<Map<String, dynamic>>(to))!;
          final int balance = source['balance'] as int;
          if (balance < 1) return;
          await tx.put(from, <String, dynamic>{'balance': balance - 1});
          await tx.put(to, <String, dynamic>{
            'balance': (target['balance'] as int) + 1,
          });
        },
        isolationLevel: IsolationLevel.serializable,
        maxAttempts: 6,
      );

      final List<Object?> outcomes =
          await Future.wait<Object?>(<Future<Object?>>[
            for (int i = 0; i < attempts; i++)
              transfer(i).then<Object?>((_) => null, onError: (Object e) => e),
          ]);

      final List<Object> failures = outcomes.whereType<Object>().toList();
      for (final Object failure in failures) {
        expect(
          failure,
          anyOf(
            isA<TransactionConflictException>(),
            isA<DeadlockException>(),
            isA<TransactionTimeoutException>(),
          ),
          reason: 'a contending transaction failed with an untyped error',
        );
      }
      expect(
        failures.length,
        lessThan(attempts),
        reason: 'no transaction made progress at all',
      );

      int total = 0;
      for (int a = 0; a < accounts; a++) {
        final Map<String, dynamic> account = (await db
            .get<Map<String, dynamic>>('acct:$a'))!;
        final int balance = account['balance'] as int;
        expect(balance, greaterThanOrEqualTo(0));
        total += balance;
      }
      expect(
        total,
        accounts * startingBalance,
        reason:
            'the committed transactions are not equivalent to any serial '
            'order: money was created or destroyed',
      );
      await db.close();
    });

    test(
      'serializable read-then-write of one key never deadlocks under load',
      () async {
        // Every transaction reads the same key (shared lock) and then writes it
        // (exclusive lock). That upgrade is the classic self-deadlock shape, and
        // 50 of them run at once here.
        final ReaxDB db = await openDb();
        await db.put('counter', <String, dynamic>{'n': 0});

        final List<Object?> outcomes = await Future.wait<Object?>(
          <Future<Object?>>[
            for (int i = 0; i < 50; i++)
              db
                  .transaction<void>(
                    (ReaxTransaction tx) async {
                      final Map<String, dynamic> current = (await tx
                          .get<Map<String, dynamic>>('counter'))!;
                      await tx.put('counter', <String, dynamic>{
                        'n': (current['n'] as int) + 1,
                      });
                    },
                    isolationLevel: IsolationLevel.serializable,
                    maxAttempts: 6,
                  )
                  .then<Object?>((_) => null, onError: (Object e) => e),
          ],
        );

        final int committed = outcomes.where((Object? o) => o == null).length;
        for (final Object failure in outcomes.whereType<Object>()) {
          expect(
            failure,
            anyOf(
              isA<TransactionConflictException>(),
              isA<DeadlockException>(),
              isA<TransactionTimeoutException>(),
            ),
          );
        }
        expect(committed, greaterThan(0));
        final Map<String, dynamic> counter = (await db
            .get<Map<String, dynamic>>('counter'))!;
        expect(
          counter['n'],
          committed,
          reason:
              'under serializable isolation the counter must equal the number '
              'of committed increments: no update may be lost',
        );
        await db.close();
      },
    );

    test('read-committed increments may be lost but never corrupt', () async {
      // Documented isolation semantics, pinned so a change is deliberate:
      // IsolationLevel.readCommitted reads without a lock, so concurrent
      // read-modify-write transactions CAN lose updates. What must never
      // happen is a torn value, an untyped error or a hang.
      final ReaxDB db = await openDb();
      await db.put('rc', <String, dynamic>{'n': 0});

      final List<Object?> outcomes = await Future.wait<Object?>(
        <Future<Object?>>[
          for (int i = 0; i < 50; i++)
            db
                .transaction<void>((ReaxTransaction tx) async {
                  final Map<String, dynamic> current = (await tx
                      .get<Map<String, dynamic>>('rc'))!;
                  await tx.put('rc', <String, dynamic>{
                    'n': (current['n'] as int) + 1,
                  });
                }, isolationLevel: IsolationLevel.readCommitted)
                .then<Object?>((_) => null, onError: (Object e) => e),
        ],
      );

      for (final Object failure in outcomes.whereType<Object>()) {
        expect(failure, isA<ReaxDbException>());
      }
      final int committed = outcomes.where((Object? o) => o == null).length;
      final Map<String, dynamic> counter = (await db.get<Map<String, dynamic>>(
        'rc',
      ))!;
      expect(counter['n'], greaterThanOrEqualTo(1));
      expect(counter['n'], lessThanOrEqualTo(committed));
      await db.close();
    });

    test('compareAndSwap under contention succeeds exactly once', () async {
      final ReaxDB db = await openDb();
      await db.put('cas', 'initial');

      final List<bool> results = await Future.wait<bool>(<Future<bool>>[
        for (int i = 0; i < 40; i++)
          db
              .compareAndSwap<String>('cas', 'initial', 'winner-$i')
              .then<bool>((bool ok) => ok, onError: (Object _) => false),
      ]);

      expect(
        results.where((bool ok) => ok).length,
        1,
        reason: 'exactly one CAS may observe the initial value',
      );
      expect(await db.get<String>('cas'), startsWith('winner-'));
      await db.close();
    });
  });

  group('change streams under load', () {
    test('a single-writer sequence delivers every event, in order', () async {
      final ReaxDB db = await openDb();
      const int writes = 500;
      final List<String> received = <String>[];
      final StreamSubscription<DatabaseChangeEvent> subscription = db
          .watchPrefix('evt:')
          .listen((DatabaseChangeEvent e) => received.add(e.key));

      await Future<void>.delayed(Duration.zero);
      for (int i = 0; i < writes; i++) {
        await db.put('evt:${i.toString().padLeft(4, '0')}', i);
      }
      await waitUntil(() => received.length >= writes);

      expect(received.length, writes, reason: 'events were dropped');
      expect(received, <String>[
        for (int i = 0; i < writes; i++) 'evt:${i.toString().padLeft(4, '0')}',
      ]);
      await subscription.cancel();
      await db.close();
    });

    test('deletes and puts are distinguishable in the event stream', () async {
      final ReaxDB db = await openDb();
      final List<DatabaseChangeEvent> received = <DatabaseChangeEvent>[];
      final StreamSubscription<DatabaseChangeEvent> subscription = db
          .watchPrefix('mix:')
          .listen(received.add);
      await Future<void>.delayed(Duration.zero);

      for (int i = 0; i < 200; i++) {
        await db.put('mix:$i', i);
        if (i.isEven) await db.delete('mix:$i');
      }
      await waitUntil(() => received.length >= 300);

      expect(received.length, 300);
      expect(
        received
            .where((DatabaseChangeEvent e) => e.type == ChangeType.delete)
            .length,
        100,
      );
      await subscription.cancel();
      await db.close();
    });

    test(
      'cancelled watchers stop receiving and do not disturb writes',
      () async {
        final ReaxDB db = await openDb();
        final List<int> counts = List<int>.filled(100, 0);
        final List<StreamSubscription<DatabaseChangeEvent>> subscriptions =
            <StreamSubscription<DatabaseChangeEvent>>[
              for (int i = 0; i < 100; i++)
                db.watchPrefix('w$i:').listen((DatabaseChangeEvent _) {
                  counts[i]++;
                }),
            ];
        await Future<void>.delayed(Duration.zero);

        for (int i = 0; i < 100; i++) {
          await db.put('w$i:live', i);
        }
        await waitUntil(() => counts.every((int c) => c == 1));
        expect(counts, everyElement(1));

        for (final StreamSubscription<DatabaseChangeEvent> subscription
            in subscriptions) {
          await subscription.cancel();
        }
        await Future<void>.delayed(Duration.zero);

        for (int i = 0; i < 100; i++) {
          await db.put('w$i:after', i);
        }
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(
          counts,
          everyElement(1),
          reason: 'a cancelled watcher still received events',
        );
        expect(await db.get<int>('w7:after'), 7);
        await db.close();
      },
    );

    test(
      'the hub releases every controller after a churn of subscribers',
      () async {
        // The facade's hub is private, so the leak assertion runs against the
        // same component directly, under the same subscribe/publish/cancel
        // churn the database puts it through.
        final ChangeStreamHub hub = ChangeStreamHub();
        for (int round = 0; round < 5; round++) {
          final List<StreamSubscription<DatabaseChangeEvent>> subscriptions =
              <StreamSubscription<DatabaseChangeEvent>>[
                for (int i = 0; i < 100; i++)
                  hub.watch('round$round:$i:*').listen((_) {}),
              ];
          await Future<void>.delayed(Duration.zero);
          expect(hub.activePatternCount, 100);
          for (int i = 0; i < 100; i++) {
            hub.publish(
              DatabaseChangeEvent(
                type: ChangeType.put,
                key: 'round$round:$i:k',
                value: i,
                timestamp: DateTime.now(),
              ),
            );
          }
          for (final StreamSubscription<DatabaseChangeEvent> subscription
              in subscriptions) {
            await subscription.cancel();
          }
          await Future<void>.delayed(Duration.zero);
          expect(
            hub.activePatternCount,
            0,
            reason: 'controllers leaked after round $round',
          );
        }
        await hub.close();
      },
    );
  });
}
