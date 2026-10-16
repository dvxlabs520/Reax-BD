import 'package:reaxdb_dart/src/core/indexing/index_manager.dart';
import 'package:reaxdb_dart/src/core/query/aggregation.dart';
import 'package:reaxdb_dart/src/core/query/query_builder.dart';
import 'package:test/test.dart';

import '../support/query_test_harness.dart';

void main() {
  late IndexManager indexManager;
  late PipelineDocumentStore store;

  setUp(() {
    final harness = buildHarness();
    indexManager = harness.indexManager;
    store = harness.store;
  });

  QueryBuilder query(String collection) => QueryBuilder(
    collection: collection,
    store: store,
    indexManager: indexManager,
  );

  group('collection scanning', () {
    test('finds documents with string ids and ids above 1000', () async {
      // The old scan probed only 'collection:1'..'collection:1000'.
      await store.putDocument('users', 'uuid-abc', {'name': 'ana'});
      await store.putDocument('users', '5000', {'name': 'bob'});
      await store.putDocument('users', '7', {'name': 'eve'});

      final results = await query('users').find();
      expect(results, hasLength(3));

      expect(await query('users').count(), equals(3));
      expect(
        await query('users').whereEquals('name', 'ana').find(),
        hasLength(1),
      );
    });

    test('update and delete reach documents regardless of id shape', () async {
      await store.putDocument('users', 'uuid-abc', {'age': 1});
      await store.putDocument('users', '9999', {'age': 1});

      final int updated = await query('users').update({'age': 2});
      expect(updated, equals(2));
      expect((await store.getDocument('users', 'uuid-abc'))!['age'], equals(2));

      final int deleted = await query('users').delete();
      expect(deleted, equals(2));
      expect(await query('users').count(), equals(0));
    });

    test('update on a document without an id field updates in place', () async {
      // The old update() rebuilt the key from doc['id'] ?? doc['_id'] and
      // otherwise generated a timestamp key, creating a NEW document.
      await store.putDocument('users', 'k1', {'name': 'ana', 'age': 30});

      final int updated = await query(
        'users',
      ).whereEquals('name', 'ana').update({'age': 31});
      expect(updated, equals(1));

      expect(
        await query('users').count(),
        equals(1),
        reason: 'update must not create a duplicate document',
      );
      expect((await store.getDocument('users', 'k1'))!['age'], equals(31));
    });
  });

  group('builder behavior', () {
    test('findOne does not mutate the builder', () async {
      for (int i = 0; i < 5; i++) {
        await store.putDocument('n', '$i', {'v': i});
      }
      final QueryBuilder q = query('n');
      final first = await q.findOne();
      expect(first, isNotNull);
      expect(
        await q.find(),
        hasLength(5),
        reason: 'findOne must not permanently apply limit(1)',
      );
    });

    test('limit and offset window results deterministically', () async {
      for (int i = 0; i < 10; i++) {
        await store.putDocument('n', 'k$i', {'v': i});
      }
      final results = await query('n').orderBy('v').offset(3).limit(4).find();
      expect(results.map((doc) => doc['v']).toList(), equals([3, 4, 5, 6]));
    });

    test(
      'scan terminates early when limit is reached without orderBy',
      () async {
        for (int i = 0; i < 50; i++) {
          await store.putDocument('n', 'k$i', {'v': i});
        }
        final results = await query('n').limit(2).find();
        expect(results, hasLength(2));
      },
    );
  });

  group('type comparisons', () {
    test('mixed int and double order numerically', () async {
      await store.putDocument('m', 'a', {'v': 10});
      await store.putDocument('m', 'b', {'v': 9.5});
      await store.putDocument('m', 'c', {'v': -3});

      final results = await query('m').orderBy('v').find();
      expect(results.map((doc) => doc['v']).toList(), equals([-3, 9.5, 10]));
    });

    test('numbers and numeric strings never compare via toString', () async {
      // Old behavior: "10" < "9" applied even to numbers vs strings.
      await store.putDocument('m', 'a', {'v': 10});
      await store.putDocument('m', 'b', {'v': '9'});

      final results = await query('m').whereGreaterThan('v', 9).find();
      // 10 > 9 numerically; the string "9" is a different type and sorts
      // above all numbers, so it also matches the total order.
      expect(results, hasLength(2));

      final onlyNumbers = await query('m').whereBetween('v', 0, 1000).find();
      expect(onlyNumbers.map((doc) => doc['v']).toList(), equals([10]));
    });

    test('booleans and nulls have defined positions', () async {
      await store.putDocument('m', 'a', {'v': true});
      await store.putDocument('m', 'b', {'v': false});
      await store.putDocument('m', 'c', {'v': null});
      await store.putDocument('m', 'd', {'v': 1});

      final results = await query('m').orderBy('v').find();
      expect(
        results.map((doc) => doc['v']).toList(),
        equals([null, false, true, 1]),
      );
    });

    test('orderBy supports nullsLast', () async {
      await store.putDocument('m', 'a', {'v': null});
      await store.putDocument('m', 'b', {'v': 2});
      await store.putDocument('m', 'c', {'v': 1});

      final results = await query('m').orderBy('v', nullsLast: true).find();
      expect(results.map((doc) => doc['v']).toList(), equals([1, 2, null]));

      final descending = await query(
        'm',
      ).orderBy('v', descending: true, nullsLast: true).find();
      expect(descending.map((doc) => doc['v']).toList(), equals([2, 1, null]));
    });

    test('whereEquals(5) matches 5.0 on the scan path too', () async {
      await store.putDocument('m', 'a', {'v': 5.0});
      expect(await query('m').whereEquals('v', 5).find(), hasLength(1));
    });

    test('whereIn matches with numeric unification', () async {
      await store.putDocument('m', 'a', {'v': 2.0});
      await store.putDocument('m', 'b', {'v': 3});
      expect(await query('m').whereIn('v', [2, 4]).find(), hasLength(1));
    });
  });

  group('index integration', () {
    test('indexed and scanned execution return identical results', () async {
      for (int i = 0; i < 20; i++) {
        await store.putDocument('u', 'k$i', {'age': i, 'even': i.isEven});
      }
      final unindexed = await query(
        'u',
      ).whereGreaterThan('age', 10).orderBy('age').find();

      await indexManager.createIndex('u', 'age');
      final indexed = await query(
        'u',
      ).whereGreaterThan('age', 10).orderBy('age').find();

      expect(
        indexed.map((doc) => doc['age']).toList(),
        equals(unindexed.map((doc) => doc['age']).toList()),
      );
    });

    test('an index on another field never hides documents', () async {
      // The old planner treated any index's candidate set as authoritative
      // and an empty index returned [] for everything.
      await indexManager.createIndex('u', 'age');
      await store.putDocument('u', '1', {'name': 'ana'});
      expect(await query('u').whereEquals('name', 'ana').find(), hasLength(1));
    });

    test('nested field queries agree between index and scan', () async {
      await store.putDocument('u', '1', {
        'address': {'city': 'lima'},
      });
      final scanned = await query(
        'u',
      ).whereEquals('address.city', 'lima').find();
      await indexManager.createIndex('u', 'address.city');
      final indexed = await query(
        'u',
      ).whereEquals('address.city', 'lima').find();
      expect(indexed, equals(scanned));
      expect(indexed, hasLength(1));
    });
  });

  group('aggregation', () {
    setUp(() async {
      await store.putDocument('s', '1', {'cat': 'a', 'v': 10});
      await store.putDocument('s', '2', {'cat': 'a', 'v': 2.5});
      await store.putDocument('s', '3', {'cat': 'b', 'v': 7});
    });

    test('sum, avg, min, max over mixed int/double', () async {
      final results =
          await query('s')
                  .aggregate(
                    (AggregationBuilder a) =>
                        a.sum('v').avg('v').min('v').max('v').count(),
                  )
                  .executeAggregation()
              as Map<String, AggregationResult>;

      expect(results['sum_v']!.value, equals(19.5));
      expect(results['avg_v']!.value, equals(6.5));
      expect(results['min_v']!.value, equals(2.5));
      expect(results['max_v']!.value, equals(10));
      expect(results['count']!.value, equals(3));
    });

    test('min and max tolerate mixed types without throwing', () async {
      // The old implementation cast to Comparable and threw on int vs
      // double vs String mixes.
      await store.putDocument('s', '4', {'v': 'zz'});
      final results =
          await query('s')
                  .aggregate((AggregationBuilder a) => a.min('v').max('v'))
                  .executeAggregation()
              as Map<String, AggregationResult>;
      expect(results['min_v']!.value, equals(2.5));
      expect(results['max_v']!.value, equals('zz'));
    });

    test('groupBy aggregates per group', () async {
      final results =
          await query('s')
                  .aggregate(
                    (AggregationBuilder a) => a.groupBy('cat').sum('v'),
                  )
                  .executeAggregation()
              as List<GroupByResult>;
      expect(results, hasLength(2));
      final GroupByResult groupA = results.firstWhere(
        (GroupByResult g) => g.groupKey == 'a',
      );
      expect(groupA.aggregations['sum_v']!.value, equals(12.5));
    });

    test('distinct unifies 1 and 1.0', () async {
      await store.putDocument('s', '5', {'v': 7.0});
      final distinct = await query('s').distinct('v');
      expect(distinct, hasLength(3)); // 10, 2.5, 7 (7.0 == 7)
    });
  });

  group('joins and search', () {
    test('join attaches matching foreign documents', () async {
      await store.putDocument('orders', 'o1', {'userId': 'u1', 'total': 5});
      await store.putDocument('orders', 'o2', {'userId': 'u2', 'total': 9});
      await store.putDocument('users', 'a', {'id': 'u1', 'name': 'ana'});
      await store.putDocument('users', 'b', {'id': 'u9', 'name': 'zoe'});

      final results = await query(
        'orders',
      ).join('users', 'userId', 'id').orderBy('total').find();

      expect(results, hasLength(2));
      final joined = results.first['_joined_users'] as List;
      expect((joined.single as Map)['name'], equals('ana'));
      expect(results.last.containsKey('_joined_users'), isFalse);
    });

    test('text search filters case-insensitively', () async {
      await store.putDocument('docs', '1', {'body': 'Hello World'});
      await store.putDocument('docs', '2', {'body': 'other'});
      expect(await query('docs').search('world').find(), hasLength(1));
      expect(
        await query('docs').search('WORLD', field: 'body').find(),
        hasLength(1),
      );
    });
  });
}
