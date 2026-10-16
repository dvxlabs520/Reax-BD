import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/indexing/index_manager.dart';
import 'package:reaxdb_dart/src/core/indexing/secondary_index.dart';
import 'package:reaxdb_dart/src/core/query/query_builder.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';
import 'package:test/test.dart';

import '../support/query_test_harness.dart';

void main() {
  group('IndexValueCodec ordering', () {
    int cmp(dynamic a, dynamic b) => compareBytes(
      IndexValueCodec.encodeValue(a),
      IndexValueCodec.encodeValue(b),
    );

    test('negative integers sort below positive integers', () {
      // Raw big-endian int64 made negatives sort ABOVE positives.
      expect(cmp(-5, 3), lessThan(0));
      expect(cmp(-1, 0), lessThan(0));
      expect(cmp(-1000000, -1), lessThan(0));
    });

    test('negative doubles keep numeric order among themselves', () {
      // Raw float64 bits reversed the order of negative doubles.
      expect(cmp(-2.5, -1.5), lessThan(0));
      expect(cmp(-0.1, 0.1), lessThan(0));
    });

    test('int and double with equal value encode identically', () {
      // Tags 2 and 3 used to give int and double disjoint key spaces.
      expect(
        IndexValueCodec.encodeValue(5),
        equals(IndexValueCodec.encodeValue(5.0)),
      );
      expect(cmp(4, 4.5), lessThan(0));
      expect(cmp(4.5, 5), lessThan(0));
    });

    test('strings use utf8 bytes, not UTF-16 code units', () {
      final Uint8List encoded = IndexValueCodec.encodeValue('eé');
      // 'e' is 1 byte, e-acute is 2 utf8 bytes, plus tag and terminator.
      expect(encoded.length, equals(1 + 1 + 2 + 1));
    });

    test('cross-type total order: null < bool < num < String < List < Map', () {
      final List<dynamic> ordered = [
        null,
        false,
        true,
        -7,
        3.14,
        'a',
        [1],
        {'k': 1},
      ];
      for (int i = 0; i < ordered.length - 1; i++) {
        expect(
          cmp(ordered[i], ordered[i + 1]),
          lessThan(0),
          reason: '${ordered[i]} should sort before ${ordered[i + 1]}',
        );
      }
    });

    test('numeric strings compare as strings, numbers as numbers', () {
      expect(cmp('10', '9'), lessThan(0)); // byte order for strings
      expect(cmp(10, 9), greaterThan(0)); // numeric order for numbers
    });

    test('lists compare element-wise and maps structurally', () {
      expect(cmp([1, 2], [1, 3]), lessThan(0));
      expect(cmp([1], [1, 0]), lessThan(0));
      expect(cmp({'a': 1}, {'a': 2}), lessThan(0));
      expect(cmp({'b': 1, 'a': 2}, {'a': 2, 'b': 1}), equals(0));
    });

    test('compareValues agrees with encoding order', () {
      expect(compareValues(-5, 3), lessThan(0));
      expect(compareValues(5, 5.0), equals(0));
      expect(valuesEqual([1, 2], [1, 2]), isTrue);
      expect(valuesEqual(null, null), isTrue);
    });

    test('skipValue walks every encoding shape', () {
      for (final dynamic value in [
        null,
        true,
        false,
        42,
        -3.5,
        'text',
        [1, 'two', null],
        {
          'k': [1, 2],
          'j': 'v',
        },
      ]) {
        final Uint8List encoded = IndexValueCodec.encodeValue(value);
        expect(
          IndexValueCodec.skipValue(encoded, 0),
          equals(encoded.length),
          reason: 'skipValue must consume exactly the encoding of $value',
        );
      }
    });

    test('document id round-trips through entry keys', () {
      final IndexDefinition definition = IndexDefinition(
        collection: 'users',
        fields: const ['age'],
      );
      for (final String docId in ['1', 'abc-def', 'user:42', 'a b']) {
        final Uint8List key = IndexKeyCodec.entryKey(definition, [30], docId);
        expect(IndexKeyCodec.docIdFromEntryKey(definition, key), equals(docId));
      }
    });

    test('prefixUpperBound produces the smallest greater key', () {
      expect(
        IndexKeyCodec.prefixUpperBound(Uint8List.fromList([1, 2, 3])),
        equals(Uint8List.fromList([1, 2, 4])),
      );
      expect(
        IndexKeyCodec.prefixUpperBound(Uint8List.fromList([1, 0xFF])),
        equals(Uint8List.fromList([2])),
      );
      expect(
        IndexKeyCodec.prefixUpperBound(Uint8List.fromList([0xFF, 0xFF])),
        isNull,
      );
    });
  });

  group('IndexManager', () {
    late InMemoryStorageEngine engine;
    late IndexManager indexManager;
    late PipelineDocumentStore store;

    setUp(() {
      final harness = buildHarness();
      engine = harness.engine;
      indexManager = harness.indexManager;
      store = harness.store;
    });

    QueryCondition eq(String field, dynamic value) => QueryCondition(
      field: field,
      operator: QueryOperator.equals,
      value: value,
    );

    test('createIndex backfills existing documents', () async {
      // The old _rebuildIndex was a stub: the planner trusted an EMPTY
      // index and every query on pre-existing data returned [].
      await store.putDocument('users', '1', {'name': 'ana', 'age': 30});
      await store.putDocument('users', '2', {'name': 'bob', 'age': 25});
      await store.putDocument('users', 'uuid-3', {'name': 'eve', 'age': 30});

      await indexManager.createIndex('users', 'age');

      final Set<String>? ids = await indexManager.candidateIds('users', [
        eq('age', 30),
      ]);
      expect(ids, equals({'1', 'uuid-3'}));
    });

    test(
      'candidateIds returns null when no index can serve the query',
      () async {
        await indexManager.createIndex('users', 'age');
        expect(
          await indexManager.candidateIds('users', [eq('name', 'ana')]),
          isNull,
        );
        expect(await indexManager.candidateIds('users', []), isNull);
        expect(
          await indexManager.candidateIds('other', [eq('age', 1)]),
          isNull,
        );
      },
    );

    test(
      'candidateIds returns an empty set only as a definitive answer',
      () async {
        await indexManager.createIndex('users', 'age');
        final Set<String>? ids = await indexManager.candidateIds('users', [
          eq('age', 99),
        ]);
        expect(ids, isNotNull);
        expect(ids, isEmpty);
      },
    );

    test('updating an indexed field removes the old posting', () async {
      // put() used to call only onDocumentInsert, so old postings stayed
      // forever and the index grew without bound.
      await indexManager.createIndex('users', 'age');
      await store.putDocument('users', '1', {'age': 30});
      await store.putDocument('users', '1', {'age': 31});

      expect(
        await indexManager.candidateIds('users', [eq('age', 30)]),
        isEmpty,
      );
      expect(
        await indexManager.candidateIds('users', [eq('age', 31)]),
        equals({'1'}),
      );
    });

    test('deleting a document removes its postings', () async {
      await indexManager.createIndex('users', 'age');
      await store.putDocument('users', '1', {'age': 30});
      await store.deleteDocument('users', '1');
      expect(
        await indexManager.candidateIds('users', [eq('age', 30)]),
        isEmpty,
      );
    });

    test(
      'buildIndexOps returns ops for atomic batching with the document',
      () async {
        await indexManager.createIndex('users', 'age');
        final List<WriteOp> insertOps = await indexManager.buildIndexOps(
          collection: 'users',
          docId: '1',
          oldDoc: null,
          newDoc: {'age': 30},
        );
        expect(insertOps, hasLength(1));
        expect(insertOps.single.isDelete, isFalse);

        final List<WriteOp> updateOps = await indexManager.buildIndexOps(
          collection: 'users',
          docId: '1',
          oldDoc: {'age': 30},
          newDoc: {'age': 31},
        );
        expect(updateOps.where((WriteOp o) => o.isDelete), hasLength(1));
        expect(updateOps.where((WriteOp o) => !o.isDelete), hasLength(1));

        final List<WriteOp> noChange = await indexManager.buildIndexOps(
          collection: 'users',
          docId: '1',
          oldDoc: {'age': 30, 'name': 'a'},
          newDoc: {'age': 30, 'name': 'b'},
        );
        expect(noChange, isEmpty, reason: 'unchanged indexed value: no ops');
      },
    );

    test('null and missing indexed values are queryable', () async {
      await indexManager.createIndex('users', 'nickname');
      await store.putDocument('users', '1', {'nickname': 'ace'});
      await store.putDocument('users', '2', {'nickname': null});
      await store.putDocument('users', '3', {'name': 'no nickname field'});

      expect(
        await indexManager.candidateIds('users', [eq('nickname', null)]),
        equals({'2', '3'}),
      );
    });

    test('in-place mutation of an indexed list is reindexed', () async {
      // updateEntry compared old != new with ==, so a mutated list (same
      // identity) was never reindexed.
      await indexManager.createIndex('users', 'tags');
      final List<String> tags = ['a'];
      await store.putDocument('users', '1', {'tags': tags});
      tags.add('b');
      await store.putDocument('users', '1', {'tags': tags});

      expect(
        await indexManager.candidateIds('users', [
          eq('tags', ['a']),
        ]),
        isEmpty,
      );
      expect(
        await indexManager.candidateIds('users', [
          eq('tags', ['a', 'b']),
        ]),
        equals({'1'}),
      );
    });

    test('whereEquals(field, 5) matches documents storing 5.0', () async {
      await indexManager.createIndex('m', 'score');
      await store.putDocument('m', '1', {'score': 5.0});
      await store.putDocument('m', '2', {'score': 5});
      expect(
        await indexManager.candidateIds('m', [eq('score', 5)]),
        equals({'1', '2'}),
      );
    });

    test('range conditions with negative numbers', () async {
      await indexManager.createIndex('t', 'v');
      await store.putDocument('t', 'a', {'v': -10});
      await store.putDocument('t', 'b', {'v': -1});
      await store.putDocument('t', 'c', {'v': 0});
      await store.putDocument('t', 'd', {'v': 7});

      final Set<String>? ids = await indexManager.candidateIds('t', [
        const QueryCondition(
          field: 'v',
          operator: QueryOperator.greaterThan,
          value: -5,
        ),
      ]);
      expect(ids, equals({'b', 'c', 'd'}));

      final Set<String>? between = await indexManager.candidateIds('t', [
        const QueryCondition(
          field: 'v',
          operator: QueryOperator.between,
          value: [-10, 0],
        ),
      ]);
      expect(between, equals({'a', 'b', 'c'}));
    });

    test('nested dotted fields are fully indexed', () async {
      // entry.key.split('.')[1] used to truncate nested field names.
      await indexManager.createIndex('users', 'address.city');
      await store.putDocument('users', '1', {
        'address': {'city': 'lima'},
      });
      await store.putDocument('users', '2', {
        'address': {'city': 'quito'},
      });
      expect(
        await indexManager.candidateIds('users', [eq('address.city', 'lima')]),
        equals({'1'}),
      );
    });

    test('compound index serves equality plus range', () async {
      await indexManager.createCompoundIndex('users', ['city', 'age']);
      await store.putDocument('users', '1', {'city': 'lima', 'age': 20});
      await store.putDocument('users', '2', {'city': 'lima', 'age': 40});
      await store.putDocument('users', '3', {'city': 'quito', 'age': 40});

      final Set<String>? ids = await indexManager.candidateIds('users', [
        eq('city', 'lima'),
        const QueryCondition(
          field: 'age',
          operator: QueryOperator.greaterThan,
          value: 25,
        ),
      ]);
      expect(ids, equals({'2'}));

      expect(
        await indexManager.candidateIds('users', [eq('city', 'lima')]),
        equals({'1', '2'}),
      );
    });

    test(
      'collections with underscores survive persistence round-trip',
      () async {
        // loadIndexes used to split '${collection}_$field' on '_', so
        // user_profiles came back as collection "user".
        await store.putDocument('user_profiles', 'p1', {'level': 3});
        await indexManager.createIndex('user_profiles', 'level');

        final IndexManager reloaded = IndexManager(
          storage: engine,
          decodeDocument: (Uint8List _, Uint8List bytes) =>
              decodeJsonDocument(bytes),
        );
        await reloaded.loadIndexes();

        expect(
          reloaded.listIndexes('user_profiles').single.fields,
          equals(['level']),
        );
        expect(
          await reloaded.candidateIds('user_profiles', [eq('level', 3)]),
          equals({'p1'}),
        );
      },
    );

    test('dropIndex removes postings and metadata', () async {
      await store.putDocument('users', '1', {'age': 30});
      await indexManager.createIndex('users', 'age');
      await indexManager.dropIndex('users', ['age']);

      expect(await indexManager.candidateIds('users', [eq('age', 30)]), isNull);
      final IndexManager reloaded = IndexManager(
        storage: engine,
        decodeDocument: (Uint8List _, Uint8List bytes) =>
            decodeJsonDocument(bytes),
      );
      await reloaded.loadIndexes();
      expect(reloaded.listIndexes(), isEmpty);
    });

    test('duplicate createIndex throws QueryException', () async {
      await indexManager.createIndex('users', 'age');
      expect(
        () => indexManager.createIndex('users', 'age'),
        throwsA(isA<QueryException>()),
      );
    });

    test('index entries never collide with document keys', () async {
      await indexManager.createIndex('users', 'age');
      await store.putDocument('users', '1', {'age': 30});
      // Scanning the document prefix must yield exactly the one document.
      final List<String> ids = await store.scanDocumentIds('users').toList();
      expect(ids, equals(['1']));
    });
  });
}
