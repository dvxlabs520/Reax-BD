import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:reaxdb_dart/src/reaxdb.dart' show debugKillBackgroundEngine;
import 'package:test/test.dart';

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_background_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (await root.exists()) await root.delete(recursive: true);
  });

  String at(String name) => p.join(root.path, name);

  Future<ReaxDB> open(
    String name, {
    bool memory = false,
    int changeStreamBufferSize = 10000,
    List<LogOutput>? logOutputs,
    EncryptionConfig encryption = const EncryptionConfig.none(),
  }) => ReaxDB.open(
    path: at(name),
    background: true,
    keepInMemory: memory,
    syncMode: SyncMode.none,
    changeStreamBufferSize: changeStreamBufferSize,
    logOutputs: logOutputs,
    encryption: encryption,
  );

  group('reads and writes', () {
    test('behave as in a local database', () async {
      final ReaxDB db = await open('basic');
      await db.put('user:1', <String, dynamic>{'name': 'Ada'});
      await db.putBatch(<String, Object?>{'n:1': 1, 'n:2': 2, 'n:3': 3});
      expect(await db.get<Map<String, dynamic>>('user:1'), <String, dynamic>{
        'name': 'Ada',
      });
      expect(await db.exists('n:2'), isTrue);
      expect(await db.getBatch<int>(<String>['n:1', 'missing']), <String, int?>{
        'n:1': 1,
        'missing': null,
      });
      expect(await db.keys(prefix: 'n:').toList(), <String>[
        'n:1',
        'n:2',
        'n:3',
      ]);
      expect(
        (await db.range<int>('n:2', 'n:9')).map((ReaxEntry<int> e) => e.value),
        <int>[2, 3],
      );
      await db.deleteBatch(<String>['n:1', 'n:2']);
      expect(await db.keys(prefix: 'n:').toList(), <String>['n:3']);
      expect(
        () => db.get<String>('n:3'),
        throwsA(isA<SerializationException>()),
      );
    });

    test('long scans arrive in order, and stopping early is clean', () async {
      final ReaxDB db = await open('scan');
      await db.putBatch(<String, Object?>{
        for (int i = 0; i < 1500; i++) 'k:${i.toString().padLeft(5, '0')}': i,
      });
      final List<int> all = await db
          .scanPrefix<int>('k:')
          .map((ReaxEntry<int> e) => e.value)
          .toList();
      expect(all, List<int>.generate(1500, (int i) => i));
      expect(
        await db.scan<int>(reverse: true).take(3).map((e) => e.value).toList(),
        <int>[1499, 1498, 1497],
      );
      expect(await db.keys(limit: 2).toList(), <String>['k:00000', 'k:00001']);
      // The engine still serves calls after the cancelled scans.
      expect(await db.get<int>('k:00007'), 7);
    });

    test('invalid keys and values fail as in a local database', () async {
      final ReaxDB db = await open('invalid');
      expect(() => db.put('', 1), throwsA(isA<InvalidKeyException>()));
      expect(() => db.get<int>(''), throwsA(isA<InvalidKeyException>()));
      expect(
        () => db.put('k', Object()),
        throwsA(isA<SerializationException>()),
      );
      final ReceivePort port = ReceivePort();
      addTearDown(port.close);
      expect(
        () => db.put('k', <String, Object?>{'port': port}),
        throwsA(isA<SerializationException>()),
      );
      expect(await db.exists('k'), isFalse);
    });

    test('statistics follow the calls', () async {
      final ReaxDB db = await open('stats');
      final int before = db.storageStats.lastSequenceNumber;
      await db.put('a', 1);
      expect(db.storageStats.lastSequenceNumber, greaterThan(before));
      await db.get<int>('a');
      await db.get<int>('a');
      expect(db.cacheStats.hits, greaterThan(0));
      expect(db.statistics()['database'], containsPair('path', db.path));
    });
  });

  group('keepInMemory', () {
    test('getSync sees every write once it returns', () async {
      final ReaxDB db = await open('memory', memory: true);
      expect(db.keepsInMemory, isTrue);
      await db.put('a', 1);
      expect(db.getSync<int>('a'), 1);

      await db.transaction<void>((ReaxTransaction tx) async {
        await tx.put('b', 2);
        await tx.delete('a');
      });
      expect(db.getSync<int>('a'), isNull);
      expect(db.getSync<int>('b'), 2);

      await db.put('c', <String, dynamic>{'n': 1});
      expect(await db.query('c').count(), 0);
      await db.query('x').delete();
      await db.put('t', 'gone', expiresAt: DateTime.now());
      expect(db.getSync<String>('t'), isNull);
      expect(await db.purgeExpired(), 0);
      expect(await db.keys().toList(), <String>['b', 'c']);
    });

    test('the values loaded at open come from disk', () async {
      ReaxDB db = await open('reload');
      await db.putBatch(<String, Object?>{
        for (int i = 0; i < 300; i++) 'k$i': i,
      });
      await db.close();
      db = await open('reload', memory: true);
      expect(db.getSync<int>('k299'), 299);
      expect(await db.keys().length, 300);
    });

    test('an encrypted database decrypts in the engine', () async {
      final EncryptionConfig key = EncryptionConfig.aes256(
        key: Uint8List.fromList(List<int>.generate(32, (int i) => i)),
      );
      ReaxDB db = await open('enc', memory: true, encryption: key);
      await db.put('secret', 'value');
      await db.close();
      db = await open('enc', memory: true, encryption: key);
      expect(db.getSync<String>('secret'), 'value');
      expect(db.encryptionInfo['type'], 'aes256');
    });
  });

  group('change streams', () {
    test(
      'an event matching several patterns arrives once per listener',
      () async {
        final ReaxDB db = await open('events');
        final List<String> all = <String>[];
        final List<String> users = <String>[];
        final List<String> one = <String>[];
        final List<StreamSubscription<DatabaseChangeEvent>> subs =
            <StreamSubscription<DatabaseChangeEvent>>[
              db.watch('*').listen((DatabaseChangeEvent e) => all.add(e.key)),
              db
                  .watchPrefix('user:')
                  .listen((DatabaseChangeEvent e) => users.add(e.key)),
              db
                  .watchKey('user:1')
                  .listen((DatabaseChangeEvent e) => one.add(e.key)),
            ];
        await db.put('user:1', 1);
        // Delivered before the write returned.
        expect(all, <String>['user:1']);
        await db.put('other', 2);
        await db.delete('user:1');
        expect(all, <String>['user:1', 'other', 'user:1']);
        expect(users, <String>['user:1', 'user:1']);
        expect(one, <String>['user:1', 'user:1']);
        for (final StreamSubscription<DatabaseChangeEvent> s in subs) {
          await s.cancel();
        }
        await db.put('user:2', 3);
        expect(all, hasLength(3));
      },
    );

    test('a paused subscription overflows like a local one', () async {
      final ReaxDB db = await open('overflow', changeStreamBufferSize: 5);
      final Completer<Object> failed = Completer<Object>();
      final StreamSubscription<DatabaseChangeEvent> sub = db
          .watch('*')
          .listen((_) {}, onError: failed.complete);
      sub.pause();
      for (int i = 0; i < 7; i++) {
        await db.put('k$i', i);
      }
      sub.resume();
      expect(await failed.future, isA<ChangeStreamOverflowException>());
      await sub.cancel();
    });

    test('closing the database ends the streams', () async {
      final ReaxDB db = await open('close-streams');
      final Future<List<DatabaseChangeEvent>> events = db.watch('*').toList();
      await db.put('a', 1);
      await db.close();
      expect(await events, hasLength(1));
    });
  });

  group('transactions', () {
    test('the body runs here and its result never crosses', () async {
      final ReaxDB db = await open('tx');
      final Object token = Object();
      final Object result = await db.transaction<Object>((
        ReaxTransaction tx,
      ) async {
        await tx.put('a', 1);
        expect(await tx.get<int>('a'), 1);
        return token;
      });
      expect(result, same(token));
      expect(await db.get<int>('a'), 1);
    });

    test('the body error is rethrown as the original object', () async {
      final ReaxDB db = await open('tx-error');
      final StateError error = StateError('boom');
      await expectLater(
        db.transaction<void>((ReaxTransaction tx) async {
          await tx.put('a', 1);
          throw error;
        }),
        throwsA(same(error)),
      );
      expect(await db.exists('a'), isFalse);
    });

    test('a conflict thrown by the body is retried like locally', () async {
      final ReaxDB db = await open('tx-retry');
      int attempts = 0;
      await expectLater(
        db.transaction<void>((ReaxTransaction tx) async {
          attempts++;
          throw const TransactionConflictException('try again');
        }, maxAttempts: 3),
        throwsA(isA<TransactionConflictException>()),
      );
      expect(attempts, 3);
    });

    test('concurrent serializable increments are not lost', () async {
      final ReaxDB db = await open('tx-counter');
      await db.put('counter', 0);
      Future<void> increment() => db.transaction<void>(
        (ReaxTransaction tx) async {
          final int value = await tx.get<int>('counter') ?? 0;
          await tx.put('counter', value + 1);
        },
        isolationLevel: IsolationLevel.serializable,
        maxAttempts: 20,
      );
      await Future.wait(<Future<void>>[
        for (int i = 0; i < 10; i++) increment(),
      ]);
      expect(await db.get<int>('counter'), 10);
      expect(await db.compareAndSwap<int>('counter', 10, 11), isTrue);
      expect(await db.compareAndSwap<int>('counter', 10, 12), isFalse);
    });
  });

  group('queries and indexes', () {
    test('run in the engine with its indexes', () async {
      final ReaxDB db = await open('query');
      await db.createIndex('users', <String>['age']);
      expect(db.hasIndex('users', 'age'), isTrue);
      expect(db.listIndexes('users'), hasLength(1));
      await db.putBatch(<String, Object?>{
        for (int i = 0; i < 20; i++)
          'users:$i': <String, dynamic>{'age': i % 5, 'name': 'u$i'},
      });
      expect(await db.query('users').whereEquals('age', 3).count(), 4);
      expect(
        await db
            .query('users')
            .whereLessThan('age', 2)
            .orderBy('name')
            .limit(2)
            .find(),
        hasLength(2),
      );
      expect(await db.query('users').distinct('age'), hasLength(5));
      expect(
        await db.query('users').whereEquals('age', 0).update(<String, dynamic>{
          'flag': true,
        }),
        4,
      );
      expect(await db.where('users', 'flag', true), hasLength(4));
      expect(await db.query('users').whereEquals('age', 4).delete(), 4);
      expect(await db.query('users').count(), 16);
      final dynamic stats = await db
          .query('users')
          .aggregate((AggregationBuilder a) => a.count().sum('age'))
          .executeAggregation();
      expect(stats, isNotNull);
      await db.dropIndex('users', <String>['age']);
      expect(db.hasIndex('users', 'age'), isFalse);
    });
  });

  group('lifecycle', () {
    test('a second open of the same path fails', () async {
      await open('locked');
      expect(() => open('locked'), throwsA(isA<DatabaseLockedException>()));
      expect(
        () => ReaxDB.open(path: at('locked')),
        throwsA(isA<DatabaseLockedException>()),
      );
    });

    test('errors at open reach the caller', () async {
      final EncryptionConfig key = EncryptionConfig.aes256(
        key: Uint8List.fromList(List<int>.generate(32, (int i) => i)),
      );
      final ReaxDB db = await open('wrong-key', encryption: key);
      await db.put('a', 1);
      await db.close();
      await expectLater(
        () => open(
          'wrong-key',
          encryption: EncryptionConfig.aes256(
            key: Uint8List.fromList(List<int>.generate(32, (int i) => 31 - i)),
          ),
        ),
        throwsA(isA<EncryptionException>()),
      );
      // The failed open left nothing behind.
      expect((await open('wrong-key', encryption: key)).isClosed, isFalse);
    });

    test('close releases the lock for a local open', () async {
      final ReaxDB db = await open('reopen');
      await db.put('a', 1);
      await db.close();
      expect(db.isClosed, isTrue);
      expect(() => db.put('b', 2), throwsA(isA<DatabaseClosedException>()));
      final ReaxDB local = await ReaxDB.open(path: at('reopen'));
      expect(await local.get<int>('a'), 1);
    });

    test('an engine that dies fails the calls in flight', () async {
      final ReaxDB db = await open('killed');
      await db.put('a', 1);
      debugKillBackgroundEngine(db);
      await expectLater(db.put('b', 2), throwsA(isA<StorageException>()));
      expect(() => db.get<int>('a'), throwsA(isA<StorageException>()));
      await db.close();
      expect(db.isClosed, isTrue);
    });

    test('onUpgrade runs here with the database', () async {
      final ReaxDB first = await ReaxDB.open(path: at('upgrade'));
      await first.put('v', 1);
      await first.close();
      final List<String> calls = <String>[];
      final ReaxDB db = await ReaxDB.open(
        path: at('upgrade'),
        background: true,
        keepInMemory: true,
        schemaVersion: 2,
        onUpgrade: (int from, int to, ReaxDB db) async {
          calls.add('$from->$to');
          await db.put('v', (db.getSync<int>('v') ?? 0) + 1);
        },
      );
      expect(calls, <String>['1->2']);
      expect(db.schemaVersion, 2);
      expect(db.getSync<int>('v'), 2);
    });

    test('a failing onUpgrade fails the open with its error', () async {
      final StateError error = StateError('no');
      await (await ReaxDB.open(path: at('upgrade-fail'))).close();
      await expectLater(
        ReaxDB.open(
          path: at('upgrade-fail'),
          background: true,
          schemaVersion: 2,
          onUpgrade: (int from, int to, ReaxDB db) => throw error,
        ),
        throwsA(same(error)),
      );
      final ReaxDB db = await open('upgrade-fail');
      expect(db.schemaVersion, 1);
    });

    test('engine log records reach the outputs given here', () async {
      final MemoryLogOutput logs = MemoryLogOutput();
      final ReaxDB db = await open('logs', logOutputs: <LogOutput>[logs]);
      await db.close();
      final List<String> messages = <String>[
        for (final LogEntry e in logs.logs) e.message,
      ];
      // Nothing is logged at the default warning level, as locally.
      expect(messages, isEmpty);

      final MemoryLogOutput info = MemoryLogOutput();
      await (await ReaxDB.open(
        path: at('logs'),
        background: true,
        logLevel: LogLevel.info,
        logOutputs: <LogOutput>[info],
      )).close();
      expect(
        <String>[for (final LogEntry e in info.logs) e.message],
        <String>[
          '[logs] Database opened at ${at('logs')}',
          '[logs] Database closed',
        ],
      );
    });

    test('rekey hands over to a new background instance', () async {
      final ReaxDB db = await open('rekey', memory: true);
      await db.put('a', 1);
      final ReaxDB rekeyed = await db.changePassphrase(
        'new passphrase',
        iterations: 1000,
      );
      expect(db.isClosed, isTrue);
      expect(rekeyed.getSync<int>('a'), 1);
      expect(rekeyed.encryptionInfo['type'], 'aes256');
      await rekeyed.put('b', 2);
      expect(await rekeyed.keys().toList(), <String>['a', 'b']);
    });

    test('a database left open does not keep the process alive', () async {
      final ProcessResult result = await Process.run(
        Platform.resolvedExecutable,
        <String>['run', 'test/support/background_left_open.dart', at('left')],
      );
      expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
      expect('${result.stdout}', contains('done'));
      final ReaxDB db = await ReaxDB.open(path: at('left'));
      expect(await db.get<int>('a'), 1);
    }, timeout: const Timeout(Duration(minutes: 2)));
  });
}
