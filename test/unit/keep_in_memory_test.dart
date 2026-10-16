import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:reaxdb_dart/src/core/cache/memory_store.dart';
import 'package:test/test.dart';

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_memory_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (await root.exists()) await root.delete(recursive: true);
  });

  Future<ReaxDB> open(String name, {bool memory = true}) => ReaxDB.open(
    path: p.join(root.path, name),
    keepInMemory: memory,
    syncMode: SyncMode.none,
  );

  group('reads and writes', () {
    test('put, get, getSync, exists and delete', () async {
      final ReaxDB db = await open('basic');
      expect(db.keepsInMemory, isTrue);
      await db.put('user:1', {'name': 'Ada', 'age': 36});
      await db.put('n', 7);

      expect(await db.get<Map<String, dynamic>>('user:1'), {
        'name': 'Ada',
        'age': 36,
      });
      expect(db.getSync<int>('n'), 7);
      expect(db.getSync<String>('missing'), isNull);
      expect(await db.exists('user:1'), isTrue);

      await db.delete('user:1');
      expect(db.getSync<Map<String, dynamic>>('user:1'), isNull);
      expect(await db.exists('user:1'), isFalse);
    });

    test('getSync needs keepInMemory', () async {
      final ReaxDB db = await open('disk', memory: false);
      expect(db.keepsInMemory, isFalse);
      expect(
        () => db.getSync<int>('k'),
        throwsA(isA<UnsupportedApiException>()),
      );
    });

    test('invalid keys are rejected like in disk mode', () async {
      final ReaxDB db = await open('keys');
      expect(() => db.getSync<int>(''), throwsA(isA<InvalidKeyException>()));
      expect(
        () => db.getSync<int>('\u0000internal'),
        throwsA(isA<InvalidKeyException>()),
      );
      await expectLater(db.get<int>(''), throwsA(isA<InvalidKeyException>()));
    });

    test('values keep the types a read from disk returns', () async {
      final ReaxDB memory = await open('types_memory');
      final ReaxDB disk = await open('types_disk', memory: false);
      final Map<String, Object?> values = <String, Object?>{
        'bytes': <int>[1, 2, 3],
        'typed': Uint8List.fromList(<int>[4, 5]),
        'map': <String, Object>{'a': 1, 'b': 1.5},
        'list': <Object>[1, 'two', true],
        'double': 2.0,
        'text': 'hello',
        'flag': false,
      };
      for (final MapEntry<String, Object?> e in values.entries) {
        await memory.put(e.key, e.value);
        await disk.put(e.key, e.value);
      }
      for (final String key in values.keys) {
        final Object? fromMemory = await memory.get<Object>(key);
        final Object? fromDisk = await disk.get<Object>(key);
        expect(fromMemory.runtimeType, fromDisk.runtimeType, reason: key);
        expect(fromMemory, fromDisk, reason: key);
      }
    });
  });

  group('copies', () {
    test('changing a value read never changes the stored one', () async {
      final ReaxDB db = await open('copies');
      await db.put('doc:1', {
        'tags': <String>['a'],
        'inner': {'n': 1},
      });

      final Map<String, dynamic> read = (await db.get<Map<String, dynamic>>(
        'doc:1',
      ))!;
      read['tags'].add('changed');
      (read['inner'] as Map<String, dynamic>)['n'] = 99;
      db.getSync<Map<String, dynamic>>('doc:1')!['extra'] = true;
      await for (final ReaxEntry<Map<String, dynamic>> e
          in db.scanPrefix<Map<String, dynamic>>('doc:')) {
        e.value['scanned'] = true;
      }
      final List<Map<String, dynamic>> found = await db.query('doc').find();
      found.single['queried'] = true;

      expect(await db.get<Map<String, dynamic>>('doc:1'), {
        'tags': <String>['a'],
        'inner': {'n': 1},
      });
    });

    test(
      'changing the written object or an event value changes nothing',
      () async {
        final ReaxDB db = await open('aliasing');
        final List<DatabaseChangeEvent> events = <DatabaseChangeEvent>[];
        final StreamSubscription<DatabaseChangeEvent> sub = db
            .watch('*')
            .listen(events.add);

        final Map<String, dynamic> original = <String, dynamic>{'n': 1};
        await db.put('a', original);
        original['n'] = 2;

        await db.transaction((ReaxTransaction tx) async {
          await tx.put('b', <String, dynamic>{'n': 1});
        });
        await Future<void>.delayed(Duration.zero);
        for (final DatabaseChangeEvent event in events) {
          (event.value as Map<String, dynamic>)['n'] = 42;
        }
        await sub.cancel();

        expect(db.getSync<Map<String, dynamic>>('a'), {'n': 1});
        expect(db.getSync<Map<String, dynamic>>('b'), {'n': 1});
      },
    );
  });

  group('persistence', () {
    test('everything written is loaded again on open', () async {
      ReaxDB db = await open('persist');
      await db.putBatch(<String, Object?>{
        for (int i = 0; i < 300; i++) 'k:$i': {'i': i},
      });
      await db.delete('k:7');
      await db.close();

      db = await open('persist');
      expect(db.getSync<Map<String, dynamic>>('k:299'), {'i': 299});
      expect(db.getSync<Map<String, dynamic>>('k:7'), isNull);
      expect((await db.info()).entryCount, 299);
      await db.close();

      // The files are ordinary: disk mode reads the same data.
      db = await open('persist', memory: false);
      expect(await db.get<Map<String, dynamic>>('k:299'), {'i': 299});
    });

    test('an encrypted database works and reloads', () async {
      final EncryptionConfig key = EncryptionConfig.aes256(
        key: Uint8List.fromList(List<int>.generate(32, (int i) => i)),
      );
      final String path = p.join(root.path, 'encrypted');
      ReaxDB db = await ReaxDB.open(
        path: path,
        encryption: key,
        keepInMemory: true,
      );
      await db.put('secret', 'value');
      await db.close();
      db = await ReaxDB.open(path: path, encryption: key, keepInMemory: true);
      expect(db.getSync<String>('secret'), 'value');
    });
  });

  group('expiry', () {
    test('expired entries read as absent and are reclaimed', () async {
      final ReaxDB db = await open('ttl');
      await db.put('soon', 'v', ttl: const Duration(milliseconds: 1));
      await db.put('later', 'v', ttl: const Duration(hours: 1));
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(db.getSync<String>('soon'), isNull);
      expect(await db.exists('soon'), isFalse);
      expect(await db.keys().toList(), <String>['later']);
      expect(
        await db.scan<String>().map((ReaxEntry<String> e) => e.key).toList(),
        <String>['later'],
      );
      expect(db.getSync<String>('later'), 'v');
    });

    test('rewriting an expired document drops its old index posting', () async {
      final ReaxDB db = await open('ttl_index');
      await db.createIndex('s', <String>['n']);
      await db.put('s:1', {'n': 7}, ttl: const Duration(milliseconds: 1));
      await db.close();

      // Reopened while expired: the entry must still be in memory so the
      // pipeline can remove its posting when it is rewritten.
      await Future<void>.delayed(const Duration(milliseconds: 20));
      final ReaxDB reopened = await open('ttl_index');
      await reopened.put('s:1', {'n': 8});
      expect(await reopened.query('s').whereEquals('n', 7).count(), 0);
      expect(await reopened.query('s').whereEquals('n', 8).count(), 1);
    });
  });

  group('order', () {
    test('scans follow the storage key order, beyond U+FFFF too', () async {
      final ReaxDB memory = await open('order_memory');
      final ReaxDB disk = await open('order_disk', memory: false);
      final List<String> keys = <String>[
        'a',
        'b',
        'z',
        'ä',
        '',
        '�',
        '😀',
        '𝄞',
        'a😀',
        'a￿',
        'ab',
      ];
      for (final String key in keys) {
        await memory.put(key, key);
        await disk.put(key, key);
      }
      expect(await memory.keys().toList(), await disk.keys().toList());
      expect(
        await memory.keys(reverse: true).toList(),
        await disk.keys(reverse: true).toList(),
      );
      expect(
        await memory.keys(prefix: 'a').toList(),
        await disk.keys(prefix: 'a').toList(),
      );
    });

    test('the memory store keeps keys in order through many changes', () {
      final MemoryStore store = MemoryStore();
      final Set<String> reference = <String>{};
      final Random random = Random(11);
      final DateTime now = DateTime.now();
      String randomKey() => 'k${random.nextInt(6000)}';
      List<String> expected(
        String? start,
        String? end,
        bool reverse,
        int? limit,
      ) {
        final List<String> sorted = reference.toList()
          ..sort(MemoryStore.compareKeys);
        Iterable<String> inRange = sorted.where(
          (String k) =>
              (start == null || MemoryStore.compareKeys(k, start) >= 0) &&
              (end == null || MemoryStore.compareKeys(k, end) < 0),
        );
        if (reverse) inRange = inRange.toList().reversed;
        return (limit == null ? inRange : inRange.take(limit)).toList();
      }

      for (int round = 0; round < 40; round++) {
        // Enough inserts to split chunks, and removals to empty some.
        for (int i = 0; i < 400; i++) {
          final String key = randomKey();
          if (random.nextInt(3) == 0) {
            store.remove(key);
            reference.remove(key);
          } else {
            store.put(key, i, null);
            reference.add(key);
          }
        }
        expect(store.length, reference.length);
        for (int q = 0; q < 10; q++) {
          final String? start = random.nextBool() ? randomKey() : null;
          final String? end = random.nextBool() ? randomKey() : null;
          final bool reverse = random.nextBool();
          final int? limit = random.nextBool() ? random.nextInt(50) : null;
          expect(
            store
                .snapshot(start, end, now: now, reverse: reverse, limit: limit)
                .map((MapEntry<String, MemoryEntry> e) => e.key)
                .toList(),
            expected(start, end, reverse, limit),
            reason: 'start=$start end=$end reverse=$reverse limit=$limit',
          );
        }
      }
    });

    test('compareKeys matches UTF-8 byte order', () {
      final Random random = Random(3);
      String randomKey() => String.fromCharCodes(<int>[
        for (int i = 0; i < 1 + random.nextInt(4); i++)
          <int>[
            0x41 + random.nextInt(3),
            0xE000 + random.nextInt(3),
            0xFFFD,
            0x1F600 + random.nextInt(3),
          ][random.nextInt(4)],
      ]);
      int bytesCompare(String a, String b) {
        final List<int> x = utf8.encode(a);
        final List<int> y = utf8.encode(b);
        for (int i = 0; i < min(x.length, y.length); i++) {
          if (x[i] != y[i]) return x[i] - y[i];
        }
        return x.length - y.length;
      }

      for (int i = 0; i < 2000; i++) {
        final String a = randomKey();
        final String b = randomKey();
        expect(
          MemoryStore.compareKeys(a, b).sign,
          bytesCompare(a, b).sign,
          reason: '$a vs $b',
        );
      }
    });

    test('a scan is a snapshot, as in disk mode', () async {
      Future<List<String>> run(ReaxDB db) async {
        await db.putBatch(<String, Object?>{
          for (int i = 0; i < 10; i++) 'k:$i': i,
        });
        final List<String> seen = <String>[];
        await for (final ReaxEntry<int> e in db.scanPrefix<int>('k:')) {
          seen.add('${e.key}=${e.value}');
          if (e.key == 'k:2') {
            await db.delete('k:5');
            await db.put('k:8a', 1);
            await db.put('k:9', 99);
          }
        }
        return seen;
      }

      final List<String> memory = await run(await open('snap_memory'));
      final List<String> disk = await run(
        await open('snap_disk', memory: false),
      );
      expect(memory, disk);
      expect(memory, contains('k:5=5'));
      expect(memory, isNot(contains('k:8a=1')));
      expect(memory.last, 'k:9=9');
    });
  });

  test('random operations read the same in both modes', () async {
    final ReaxDB memory = await open('diff_memory');
    final ReaxDB disk = await open('diff_disk', memory: false);
    final List<ReaxDB> both = <ReaxDB>[memory, disk];
    for (final ReaxDB db in both) {
      await db.createIndex('users', <String>['city']);
      await db.createIndex('users', <String>['city', 'age']);
    }

    final Random random = Random(20261016);
    const List<String> cities = <String>['London', 'Paris', 'Lima', 'Oslo'];
    const List<String> collections = <String>['users', 'orders', 'misc'];
    String key() => '${collections[random.nextInt(3)]}:${random.nextInt(60)}';
    Map<String, dynamic> doc() => <String, dynamic>{
      'city': cities[random.nextInt(cities.length)],
      'age': random.nextInt(80),
      'tags': <String>[for (int i = 0; i < random.nextInt(3); i++) 't$i'],
    };

    for (int step = 0; step < 1500; step++) {
      final int op = random.nextInt(10);
      final String k = key();
      final Map<String, dynamic> d = doc();
      final List<String> many = <String>[for (int i = 0; i < 5; i++) key()];
      final Map<String, Object?> batch = <String, Object?>{
        for (final String b in many) b: doc(),
      };
      final String city = cities[random.nextInt(cities.length)];
      for (final ReaxDB db in both) {
        switch (op) {
          case 0 || 1 || 2:
            await db.put(k, d);
          case 3:
            await db.delete(k);
          case 4:
            await db.putBatch(batch);
          case 5:
            await db.deleteBatch(many.take(2).toList());
          case 6:
            await db.transaction((ReaxTransaction tx) async {
              final Object? stored = await tx.get<Object>(k);
              final Map<String, dynamic>? current =
                  stored is Map<String, dynamic> ? stored : null;
              await tx.put(k, <String, dynamic>{
                ...?current,
                'touched': (current?['touched'] as int? ?? 0) + 1,
              });
            }, isolationLevel: IsolationLevel.serializable);
          case 7:
            await db.query('users').whereEquals('city', city).update(
              <String, dynamic>{'flag': step},
            );
          case 8:
            await db.query('orders').whereLessThan('age', 10).delete();
          default:
            await db.put(k, 'scalar $step');
        }
      }
    }

    Future<List<Object?>> snapshot(ReaxDB db) async => <Object?>[
      await db.scan<Object?>().map((ReaxEntry<Object?> e) {
        return <Object?>[e.key, e.value];
      }).toList(),
      await db.keys(prefix: 'users:', reverse: true).toList(),
      await db
          .scanPrefix<Object?>('orders:', limit: 7, reverse: true)
          .map((ReaxEntry<Object?> e) => e.key)
          .toList(),
      (await db.range<Object?>(
        'misc:1',
        'misc:5',
      )).map((ReaxEntry<Object?> e) => e.key).toList(),
      for (final String city in cities) ...<Object?>[
        await db.query('users').whereEquals('city', city).orderBy('age').find(),
        await db
            .query('users')
            .whereEquals('city', city)
            .whereGreaterThan('age', 30)
            .count(),
      ],
      await db.query('orders').orderBy('age', descending: true).limit(5).find(),
      await db.query('misc').count(),
      (await db.info()).entryCount,
      for (int i = 0; i < 60; i++) await db.get<Object?>('users:$i'),
    ];

    expect(await snapshot(memory), await snapshot(disk));
  });
}
