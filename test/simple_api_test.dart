import 'dart:io';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

void main() {
  late Directory root;
  late SimpleReaxDB db;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_simple_');
    db = await ReaxDB.simple('myapp', path: '${root.path}/myapp');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  group('basic operations', () {
    test('put, get and delete', () async {
      await db.put('user:1', <String, dynamic>{'name': 'Ada', 'age': 36});
      final Object? user = await db.get('user:1');
      expect(user, isA<Map<String, dynamic>>());
      expect((user as Map<String, dynamic>)['name'], 'Ada');

      expect(await db.exists('user:1'), isTrue);
      await db.delete('user:1');
      expect(await db.get('user:1'), isNull);
      expect(await db.exists('user:1'), isFalse);
    });

    test('stores primitives as well as documents', () async {
      await db.put('count', 7);
      await db.put('ratio', 0.5);
      await db.put('flag', true);
      await db.put('label', 'text');
      await db.put('items', <dynamic>[1, 2, 3]);

      expect(await db.get('count'), 7);
      expect(await db.get('ratio'), 0.5);
      expect(await db.get('flag'), true);
      expect(await db.get('label'), 'text');
      expect(await db.get('items'), <dynamic>[1, 2, 3]);
    });

    test('putAll and deleteAll', () async {
      await db.putAll(<String, Object?>{
        'user:1': <String, dynamic>{'name': 'Ada'},
        'user:2': <String, dynamic>{'name': 'Bob'},
        'user:3': <String, dynamic>{'name': 'Cy'},
      });
      expect(await db.count('user:*'), 3);
      await db.deleteAll(<String>['user:1', 'user:2']);
      expect(await db.count('user:*'), 1);
    });

    test('TTL entries expire', () async {
      // Long enough for a durable put and a read on a slow CI runner.
      await db.put('session', 'token', ttl: const Duration(seconds: 1));
      expect(await db.get('session'), 'token');
      await Future<void>.delayed(const Duration(milliseconds: 1200));
      expect(await db.get('session'), isNull);
    });
  });

  group('iteration without a key registry', () {
    setUp(() async {
      await db.putAll(<String, Object?>{
        'user:1': <String, dynamic>{'name': 'Ada'},
        'user:2': <String, dynamic>{'name': 'Bob'},
        'settings:theme': 'dark',
        'counter': 1,
      });
    });

    test('query returns keys matching a prefix pattern', () async {
      expect((await db.query('user:*'))..sort(), <String>['user:1', 'user:2']);
      expect(await db.query('settings:*'), <String>['settings:theme']);
    });

    test('query with * returns every key', () async {
      expect(await db.count(), 4);
      expect((await db.query('*')).length, 4);
    });

    test('query with an exact key', () async {
      expect(await db.query('counter'), <String>['counter']);
      expect(await db.query('missing'), isEmpty);
    });

    test('getAll returns key/value pairs', () async {
      final Map<String, Object?> users = await db.getAll('user:*');
      expect(users.keys.toList()..sort(), <String>['user:1', 'user:2']);
      expect((users['user:1']! as Map<String, dynamic>)['name'], 'Ada');
    });

    test(
      'keys written by another instance are visible after a reopen',
      () async {
        await db.close();
        final SimpleReaxDB reopened = await ReaxDB.simple(
          'myapp',
          path: '${root.path}/myapp',
        );
        expect(await reopened.count(), 4);
        expect((await reopened.query('user:*')).length, 2);
        await reopened.close();
      },
    );

    test('clear removes everything', () async {
      await db.clear();
      expect(await db.count(), 0);
      expect(await db.get('counter'), isNull);
    });
  });

  group('watching', () {
    test('watch emits change events', () async {
      final List<DatabaseChangeEvent> seen = <DatabaseChangeEvent>[];
      final subscription = db.watch('user:*').listen(seen.add);
      await Future<void>.delayed(Duration.zero);
      await db.put('user:1', <String, dynamic>{'name': 'Ada'});
      await db.put('other', 1);
      await Future<void>.delayed(Duration.zero);
      await subscription.cancel();
      expect(seen, hasLength(1));
      expect(seen.single.key, 'user:1');
    });
  });

  group('security', () {
    test('a simple database is not encrypted by default', () async {
      expect(db.advanced.encryptionInfo['enabled'], isFalse);
    });

    test('encryption requires real key material, never the name', () async {
      final SimpleReaxDB encrypted = await ReaxDB.simple(
        'secure',
        path: '${root.path}/secure',
        encryption: EncryptionConfig.aes256FromPassphrase(
          passphrase: 'a strong passphrase',
          iterations: 1000,
        ),
      );
      await encrypted.put('secret', 'value');
      expect(encrypted.advanced.encryptionInfo['type'], 'aes256');
      expect(encrypted.advanced.encryptionInfo['is_authenticated'], isTrue);
      await encrypted.close();

      // The same database name with no passphrase cannot read the data.
      expect(
        () => ReaxDB.simple('secure', path: '${root.path}/secure'),
        throwsA(isA<EncryptionException>()),
      );
    });
  });

  group('advanced escape hatch', () {
    test('advanced exposes the full API', () async {
      await db.advanced.createIndex('user', <String>['name']);
      await db.putAll(<String, Object?>{
        'user:1': <String, dynamic>{'name': 'Ada'},
        'user:2': <String, dynamic>{'name': 'Bob'},
      });
      final List<Map<String, dynamic>> found = await db.advanced
          .query('user')
          .whereEquals('name', 'Ada')
          .find();
      expect(found, hasLength(1));

      await db.advanced.transaction<void>(
        (ReaxTransaction tx) =>
            tx.put('user:3', <String, dynamic>{'name': 'Cy'}),
      );
      expect(await db.count('user:*'), 3);
    });

    test('info reports the real path and entry count', () async {
      await db.put('a', 1);
      final DatabaseInfo info = await db.info();
      expect(info.entryCount, 1);
      expect(Directory(info.path).existsSync(), isTrue);
    });
  });
}
