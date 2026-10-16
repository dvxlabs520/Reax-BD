import 'dart:io';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

void main() {
  late Directory root;
  late ReaxDB db;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_tx_');
    db = await ReaxDB.open(path: '${root.path}/db');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  test('a committed transaction applies every write', () async {
    await db.transaction<void>((ReaxTransaction tx) async {
      await tx.put('account:1', <String, dynamic>{'balance': 100});
      await tx.put('account:2', <String, dynamic>{'balance': 200});
    });

    expect((await db.get<Map<String, dynamic>>('account:1'))!['balance'], 100);
    expect((await db.get<Map<String, dynamic>>('account:2'))!['balance'], 200);
  });

  test('a failing transaction applies nothing', () async {
    await db.put('account:1', <String, dynamic>{'balance': 100});
    await expectLater(
      db.transaction<void>((ReaxTransaction tx) async {
        await tx.put('account:1', <String, dynamic>{'balance': 0});
        await tx.put('account:2', <String, dynamic>{'balance': 999});
        throw StateError('rollback');
      }),
      throwsA(isA<StateError>()),
    );

    expect((await db.get<Map<String, dynamic>>('account:1'))!['balance'], 100);
    expect(await db.get<Map<String, dynamic>>('account:2'), isNull);
  });

  test('reads observe the transaction own writes', () async {
    final int result = await db.transaction<int>((ReaxTransaction tx) async {
      await tx.put('counter', 5);
      return (await tx.get<int>('counter'))! + 1;
    });
    expect(result, 6);
    expect(await db.get<int>('counter'), 5);
  });

  test('transactional deletes are applied', () async {
    await db.put('gone', 'value');
    await db.transaction<void>((ReaxTransaction tx) => tx.delete('gone'));
    expect(await db.get<String>('gone'), isNull);
  });

  test('a transaction returns a typed value', () async {
    final String name = await db.transaction<String>((
      ReaxTransaction tx,
    ) async {
      await tx.put('user:1', <String, dynamic>{'name': 'Ada'});
      final Map<String, dynamic>? user = await tx.get<Map<String, dynamic>>(
        'user:1',
      );
      return user!['name'] as String;
    });
    expect(name, 'Ada');
  });

  test('committed writes update the cache and emit change events', () async {
    final List<DatabaseChangeEvent> seen = <DatabaseChangeEvent>[];
    final subscription = db.watch('tx:*').listen(seen.add);
    await Future<void>.delayed(Duration.zero);

    await db.transaction<void>((ReaxTransaction tx) async {
      await tx.put('tx:a', 1);
      await tx.put('tx:b', 2);
    });
    await Future<void>.delayed(Duration.zero);
    await subscription.cancel();

    expect(
      seen.map((DatabaseChangeEvent e) => e.key).toList()..sort(),
      <String>['tx:a', 'tx:b'],
    );
    expect(
      seen.every((DatabaseChangeEvent e) => e.type == ChangeType.put),
      isTrue,
    );
    expect(await db.get<int>('tx:a'), 1);
  });

  test('committed writes maintain secondary indexes', () async {
    await db.createIndex('people', <String>['city']);
    await db.transaction<void>((ReaxTransaction tx) async {
      await tx.put('people:1', <String, dynamic>{
        'name': 'Ada',
        'city': 'Oslo',
      });
    });
    expect(
      await db.query('people').whereEquals('city', 'Oslo').find(),
      hasLength(1),
    );

    await db.transaction<void>((ReaxTransaction tx) => tx.delete('people:1'));
    expect(
      await db.query('people').whereEquals('city', 'Oslo').find(),
      isEmpty,
    );
  });

  test('transactional writes honor TTL', () async {
    await db.transaction<void>(
      (ReaxTransaction tx) =>
          // Long enough for a durable commit and a read on a slow CI runner.
          tx.put('temp', 'v', ttl: const Duration(seconds: 1)),
    );
    expect(await db.get<String>('temp'), 'v');
    await Future<void>.delayed(const Duration(milliseconds: 1200));
    expect(await db.get<String>('temp'), isNull);
  });

  test('serializable transactions detect a conflicting write', () async {
    await db.put('shared', 1);
    await expectLater(
      db.transaction<void>(
        (ReaxTransaction tx) async {
          await tx.get<int>('shared');
          // A concurrent committed write invalidates the read set.
          await db.put('shared', 2);
          await tx.put('other', 3);
        },
        isolationLevel: IsolationLevel.serializable,
        maxAttempts: 1,
      ),
      throwsA(isA<TransactionConflictException>()),
    );
    expect(await db.get<int>('other'), isNull);
  });

  test('concurrent transactions on distinct keys all commit', () async {
    await Future.wait(<Future<void>>[
      for (int i = 0; i < 20; i++)
        db.transaction<void>((ReaxTransaction tx) => tx.put('c:$i', i)),
    ]);
    for (int i = 0; i < 20; i++) {
      expect(await db.get<int>('c:$i'), i);
    }
  });
}
