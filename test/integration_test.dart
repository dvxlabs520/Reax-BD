import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

class Product {
  Product({required this.sku, required this.name, required this.price});

  factory Product.fromJson(Map<String, dynamic> json) => Product(
    sku: json['sku'] as String,
    name: json['name'] as String,
    price: (json['price'] as num).toDouble(),
  );

  final String sku;
  final String name;
  final double price;

  Map<String, dynamic> toJson() => <String, dynamic>{
    'sku': sku,
    'name': name,
    'price': price,
  };
}

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_integration_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  String at(String name) => '${root.path}/$name';

  group('backup and restore', () {
    test('exportTo and importFrom round-trip data and indexes', () async {
      final ReaxDB source = await ReaxDB.open(path: at('source'));
      await source.putBatch(<String, Object?>{
        'people:1': <String, dynamic>{'name': 'Ada', 'city': 'London'},
        'people:2': <String, dynamic>{'name': 'Bob', 'city': 'Oslo'},
        'config': <String, dynamic>{'theme': 'dark'},
        'counter': 41,
      });
      await source.createIndex('people', <String>['city']);
      final String archive = at('backup.rxdb');
      final int exported = await source.exportTo(archive);
      expect(exported, 4);
      await source.close();

      final ReaxDB restored = await ReaxDB.importFrom(
        archivePath: archive,
        path: at('restored'),
      );
      expect(await restored.get<int>('counter'), 41);
      expect(
        (await restored.get<Map<String, dynamic>>('config'))!['theme'],
        'dark',
      );
      expect(restored.hasIndex('people', 'city'), isTrue);
      expect(
        await restored.query('people').whereEquals('city', 'Oslo').find(),
        hasLength(1),
      );
      await restored.close();
    });

    test(
      'a backup can be restored into a differently encrypted database',
      () async {
        final ReaxDB source = await ReaxDB.open(path: at('plain'));
        await source.put('secret', 'value');
        final String archive = at('plain.rxdb');
        await source.exportTo(archive);
        await source.close();

        final ReaxDB encrypted = await ReaxDB.importFrom(
          archivePath: archive,
          path: at('encrypted'),
          encryption: EncryptionConfig.aes256(
            key: Uint8List.fromList(List<int>.filled(32, 3)),
          ),
        );
        expect(await encrypted.get<String>('secret'), 'value');
        expect(encrypted.encryptionInfo['type'], 'aes256');
        await encrypted.close();
      },
    );

    test('TTL metadata survives export and import', () async {
      final ReaxDB source = await ReaxDB.open(path: at('ttl'));
      await source.put('temp', 'v', ttl: const Duration(hours: 1));
      final String archive = at('ttl.rxdb');
      await source.exportTo(archive);
      await source.close();

      final ReaxDB restored = await ReaxDB.importFrom(
        archivePath: archive,
        path: at('ttl-restored'),
      );
      final List<ReaxEntry<String>> entries = await restored
          .scan<String>()
          .toList();
      expect(entries.single.expiresAt, isNotNull);
      await restored.close();
    });

    test('importing into a non-empty directory needs overwrite', () async {
      final ReaxDB source = await ReaxDB.open(path: at('src2'));
      await source.put('a', 1);
      final String archive = at('src2.rxdb');
      await source.exportTo(archive);
      await source.close();

      final ReaxDB occupied = await ReaxDB.open(path: at('target'));
      await occupied.put('b', 2);
      await occupied.close();

      expect(
        () => ReaxDB.importFrom(archivePath: archive, path: at('target')),
        throwsA(isA<DatabaseLockedException>()),
      );

      final ReaxDB replaced = await ReaxDB.importFrom(
        archivePath: archive,
        path: at('target'),
        overwrite: true,
      );
      expect(await replaced.get<int>('a'), 1);
      expect(await replaced.get<int>('b'), isNull);
      await replaced.close();
    });
  });

  group('schema migrations', () {
    test('onUpgrade runs once and the version is recorded', () async {
      ReaxDB db = await ReaxDB.open(path: at('schema'), schemaVersion: 1);
      await db.put('user:1', <String, dynamic>{'name': 'Ada'});
      await db.close();

      final List<String> calls = <String>[];
      db = await ReaxDB.open(
        path: at('schema'),
        schemaVersion: 2,
        onUpgrade: (int from, int to, ReaxDB target) async {
          calls.add('$from->$to');
          final Map<String, dynamic>? user = await target
              .get<Map<String, dynamic>>('user:1');
          await target.put('user:1', <String, dynamic>{
            ...user!,
            'migrated': true,
          });
        },
      );
      expect(calls, <String>['1->2']);
      expect(db.schemaVersion, 2);
      expect(
        (await db.get<Map<String, dynamic>>('user:1'))!['migrated'],
        isTrue,
      );
      await db.close();

      // Reopening at the same version does not re-run the migration.
      db = await ReaxDB.open(
        path: at('schema'),
        schemaVersion: 2,
        onUpgrade: (int from, int to, ReaxDB target) async {
          calls.add('$from->$to');
        },
      );
      expect(calls, <String>['1->2']);
      await db.close();
    });
  });

  group('instance registry and locking', () {
    test('a second open in the same isolate is rejected', () async {
      final ReaxDB db = await ReaxDB.open(path: at('locked'));
      expect(
        () => ReaxDB.open(path: at('locked')),
        throwsA(
          isA<DatabaseLockedException>().having(
            (DatabaseLockedException e) => e.path,
            'path',
            isNotNull,
          ),
        ),
      );
      await db.close();
    });

    test('another process cannot open a locked database', () async {
      final ReaxDB db = await ReaxDB.open(path: at('cross'));
      final String script = at('probe.dart');
      await File(script).writeAsString('''
import 'dart:io';
Future<void> main() async {
  final RandomAccessFile handle =
      await File('${at('cross')}/LOCK').open(mode: FileMode.write);
  try {
    await handle.lock(FileLock.exclusive);
    stdout.write('acquired');
  } on FileSystemException {
    stdout.write('locked');
  }
  await handle.close();
}
''');
      final ProcessResult result = await Process.run(
        Platform.resolvedExecutable,
        <String>['run', script],
      );
      expect(result.stdout, contains('locked'));
      await db.close();
    });

    test('the lock is released on close and the path reopens', () async {
      ReaxDB db = await ReaxDB.open(path: at('reopen'));
      await db.put('a', 1);
      await db.close();
      db = await ReaxDB.open(path: at('reopen'));
      expect(await db.get<int>('a'), 1);
      await db.close();
    });
  });

  group('sync modes', () {
    test('a database can be opened in each sync mode', () async {
      for (final SyncMode mode in SyncMode.values) {
        final ReaxDB db = await ReaxDB.open(
          path: at('sync-${mode.name}'),
          syncMode: mode,
        );
        expect(db.syncMode, mode);
        await db.put('a', 1);
        await db.close();

        final ReaxDB reopened = await ReaxDB.open(
          path: at('sync-${mode.name}'),
          syncMode: mode,
        );
        expect(await reopened.get<int>('a'), 1);
        await reopened.close();
      }
    });

    test('a per-write override survives a reopen', () async {
      ReaxDB db = await ReaxDB.open(
        path: at('override'),
        syncMode: SyncMode.none,
      );
      await db.put('durable', 'value', sync: SyncMode.full);
      await db.close();

      db = await ReaxDB.open(path: at('override'), syncMode: SyncMode.none);
      expect(await db.get<String>('durable'), 'value');
      await db.close();
    });
  });

  group('end-to-end workflow', () {
    test(
      'typed collections, indexes, queries and streams work together',
      () async {
        final ReaxDB db = await ReaxDB.open(path: at('shop'));
        final ReaxCollection<Product> products = db.collection<Product>(
          'products',
          fromJson: Product.fromJson,
          toJson: (Product p) => p.toJson(),
        );
        await products.createIndex(<String>['price']);

        final List<CollectionChange<Product>> changes =
            <CollectionChange<Product>>[];
        final subscription = products.watch().listen(changes.add);
        await Future<void>.delayed(Duration.zero);

        await products.putAll(<String, Product>{
          'a': Product(sku: 'a', name: 'Anvil', price: 30),
          'b': Product(sku: 'b', name: 'Bolt', price: 5),
          'c': Product(sku: 'c', name: 'Cog', price: 12),
        });

        final List<Product> cheap = await products.find(
          (QueryBuilder q) => q.whereLessThan('price', 20).orderBy('price'),
        );
        expect(cheap.map((Product p) => p.sku), <String>['b', 'c']);

        final Map<String, AggregationResult> totals =
            await db
                    .query('products')
                    .aggregate(
                      (AggregationBuilder b) => b
                        ..sum('price')
                        ..count(),
                    )
                    .executeAggregation()
                as Map<String, AggregationResult>;
        expect(totals['sum_price']!.value, 47);
        expect(totals['count']!.value, 3);

        await Future<void>.delayed(Duration.zero);
        await subscription.cancel();
        expect(changes, hasLength(3));

        await db.transaction<void>((ReaxTransaction tx) async {
          await tx.put('products:d', <String, dynamic>{
            'sku': 'd',
            'name': 'Drum',
            'price': 8.0,
          });
        });
        expect(await products.count(), 4);

        final DatabaseInfo info = await db.info();
        expect(info.entryCount, 4);
        expect(info.indexCount, 1);
        await db.close();
      },
    );

    test('a heavier mixed workload stays consistent', () async {
      final ReaxDB db = await ReaxDB.open(path: at('mixed'));
      await db.createIndex('doc', <String>['bucket']);

      for (int i = 0; i < 300; i++) {
        await db.put('doc:${i.toString().padLeft(4, '0')}', <String, dynamic>{
          'i': i,
          'bucket': i % 5,
        });
      }
      await db.flush();

      for (int i = 0; i < 300; i += 3) {
        await db.delete('doc:${i.toString().padLeft(4, '0')}');
      }
      await db.compact();

      expect(await db.query('doc').count(), 200);
      final List<Map<String, dynamic>> bucketZero = await db
          .query('doc')
          .whereEquals('bucket', 0)
          .find();
      for (final Map<String, dynamic> doc in bucketZero) {
        expect((doc['i'] as int) % 5, 0);
        expect((doc['i'] as int) % 3, isNot(0));
      }
      await db.close();
    });
  });
}
