// Tests for the ReaxDB calls the demo screens are built from.
//
// They run against real files in a temporary directory, so they check the
// same code paths the app uses on a device.

import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:reaxdb_example/screens/collection_demo_screen.dart';
import 'package:reaxdb_example/services/database_service.dart';

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_example_test');
    DatabaseService.documentsDirectory = () async => root;
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (await root.exists()) await root.delete(recursive: true);
  });

  test('opens a database under the example root', () async {
    final ReaxDB db = await DatabaseService.open('overview');
    addTearDown(db.close);

    expect(db.path, contains(DatabaseService.rootDirectoryName));
    expect(db.path, endsWith('overview'));
    expect(db.syncMode, SyncMode.full);
    expect(db.schemaVersion, 1);
  });

  test('the small API stores and queries by pattern', () async {
    final SimpleReaxDB db = await DatabaseService.openSimple('basics');
    addTearDown(db.close);

    await db.putAll(<String, Object?>{
      'user:1': <String, dynamic>{'name': 'Ada'},
      'user:2': <String, dynamic>{'name': 'Grace'},
      'counter': 42,
    });

    expect(await db.get('counter'), 42);
    expect(await db.query('user:*'), <String>['user:1', 'user:2']);
    expect(await db.count(), 3);

    await db.delete('user:1');
    expect(await db.exists('user:1'), isFalse);
  });

  test('a typed collection round-trips through a compound index', () async {
    final ReaxDB db = await DatabaseService.open('collections');
    addTearDown(db.close);

    final ReaxCollection<Employee> employees = db.collection<Employee>(
      'employees',
      fromJson: Employee.fromJson,
      toJson: (Employee e) => e.toJson(),
    );
    await employees.createIndex(<String>['department', 'salary']);

    await employees.putAll(<String, Employee>{
      '1': Employee(
        id: '1',
        name: 'Ada',
        department: 'Engineering',
        salary: 120000,
        hiredAt: DateTime(2024, 1, 1),
      ),
      '2': Employee(
        id: '2',
        name: 'Grace',
        department: 'Engineering',
        salary: 80000,
        hiredAt: DateTime(2024, 2, 1),
      ),
      '3': Employee(
        id: '3',
        name: 'Radia',
        department: 'Design',
        salary: 95000,
        hiredAt: DateTime(2024, 3, 1),
      ),
    });

    final Employee? ada = await employees.get('1');
    expect(ada?.name, 'Ada');
    expect(ada?.hiredAt, DateTime(2024, 1, 1));

    final List<Employee> paid = await employees.find(
      (QueryBuilder query) => query
          .whereEquals('department', 'Engineering')
          .whereGreaterThanOrEqual('salary', 90000)
          .orderBy('salary', descending: true),
    );
    expect(paid.map((Employee e) => e.name), <String>['Ada']);
    expect(await employees.count(), 3);
    expect(db.listIndexes('employees'), hasLength(1));
  });

  test('scans are ordered, bounded and reversible', () async {
    final ReaxDB db = await DatabaseService.open('iteration');
    addTearDown(db.close);

    await db.putBatch(<String, Object?>{
      'city:berlin': <String, dynamic>{'population': 3850809},
      'city:london': <String, dynamic>{'population': 8866180},
      'city:oslo': <String, dynamic>{'population': 709037},
      'country:pt': <String, dynamic>{'name': 'Portugal'},
    });

    final List<String> cities = await db.keys(prefix: 'city:').toList();
    expect(cities, <String>['city:berlin', 'city:london', 'city:oslo']);

    final List<ReaxEntry<Map<String, dynamic>>> page = await db
        .range<Map<String, dynamic>>('city:l', 'city:p');
    expect(page.map((ReaxEntry<Map<String, dynamic>> e) => e.key), <String>[
      'city:london',
      'city:oslo',
    ]);

    final List<ReaxEntry<Map<String, dynamic>>> newest =
        await db
            .scanPrefix<Map<String, dynamic>>('city:', reverse: true, limit: 1)
            .toList();
    expect(newest.single.key, 'city:oslo');
  });

  test('an expired entry reads as absent and can be purged', () async {
    final ReaxDB db = await DatabaseService.open('ttl');
    addTearDown(db.close);

    await db.put('session:live', 'token', ttl: const Duration(minutes: 5));
    await db.put('session:gone', 'token', ttl: const Duration(milliseconds: 1));
    await Future<void>.delayed(const Duration(milliseconds: 20));

    expect(await db.get<String>('session:gone'), isNull);
    expect(await db.exists('session:gone'), isFalse);
    expect(await db.get<String>('session:live'), 'token');

    final List<ReaxEntry<String>> live =
        await db.scanPrefix<String>('session:').toList();
    expect(live.single.key, 'session:live');
    expect(live.single.expiresAt, isNotNull);

    expect(await db.purgeExpired(), 0);
  });

  test('a transaction commits as a unit and rolls back on a throw', () async {
    final ReaxDB db = await DatabaseService.open('transactions');
    addTearDown(db.close);

    await db.putBatch(<String, Object?>{
      'account:checking': <String, dynamic>{'balance': 500},
      'account:savings': <String, dynamic>{'balance': 1500},
    });

    Future<void> transfer(int amount) =>
        db.transaction((ReaxTransaction tx) async {
          final Map<String, dynamic> from =
              (await tx.get<Map<String, dynamic>>('account:checking'))!;
          final Map<String, dynamic> to =
              (await tx.get<Map<String, dynamic>>('account:savings'))!;
          if ((from['balance'] as int) < amount) {
            throw const InsufficientFundsForTest();
          }
          await tx.put('account:checking', <String, dynamic>{
            ...from,
            'balance': (from['balance'] as int) - amount,
          });
          await tx.put('account:savings', <String, dynamic>{
            ...to,
            'balance': (to['balance'] as int) + amount,
          });
        }, isolationLevel: IsolationLevel.serializable);

    await transfer(50);
    expect(
      (await db.get<Map<String, dynamic>>('account:checking'))!['balance'],
      450,
    );

    await expectLater(
      transfer(10000),
      throwsA(isA<InsufficientFundsForTest>()),
    );
    expect(
      (await db.get<Map<String, dynamic>>('account:checking'))!['balance'],
      450,
      reason: 'a transaction that throws must write nothing',
    );

    expect(await db.compareAndSwap<int>('counter', null, 1), isTrue);
    expect(await db.compareAndSwap<int>('counter', null, 2), isFalse);
  });

  test(
    'an archive restores into a database with different encryption',
    () async {
      final ReaxDB source = await DatabaseService.open('backup_source');
      await source.createIndex('note', <String>['tag']);
      await source.putBatch(<String, Object?>{
        'note:1': <String, dynamic>{'title': 'First', 'tag': 'work'},
        'note:2': <String, dynamic>{'title': 'Second', 'tag': 'home'},
      });

      final String archive = await DatabaseService.filePath('backup.rxdb');
      expect(await source.exportTo(archive), 2);
      await source.close();

      final Uint8List key = Uint8List(32)..fillRange(0, 32, 7);
      final ReaxDB restored = await ReaxDB.importFrom(
        archivePath: archive,
        path: await DatabaseService.pathFor('backup_restored'),
        encryption: EncryptionConfig.aes256(key: key),
        overwrite: true,
      );
      addTearDown(restored.close);

      final DatabaseInfo info = await restored.info();
      expect(info.entryCount, 2);
      expect(info.encryptionType, 'aes256');
      expect(await restored.where('note', 'tag', 'work'), hasLength(1));
    },
  );

  test('a damaged archive is rejected before anything is written', () async {
    final ReaxDB source = await DatabaseService.open('backup_source');
    await source.put('note:1', <String, dynamic>{'title': 'First'});
    final String archive = await DatabaseService.filePath('backup.rxdb');
    await source.exportTo(archive);
    await source.close();

    final Uint8List bytes = await File(archive).readAsBytes();
    bytes[bytes.length ~/ 2] ^= 0x01;
    final String damaged = await DatabaseService.filePath('damaged.rxdb');
    await File(damaged).writeAsBytes(bytes, flush: true);

    await expectLater(
      ReaxDB.importFrom(
        archivePath: damaged,
        path: await DatabaseService.pathFor('backup_damaged'),
        overwrite: true,
      ),
      throwsA(isA<CorruptionException>()),
    );
  });

  test('a schema upgrade runs onUpgrade exactly once', () async {
    final ReaxDB v1 = await DatabaseService.open('migration');
    await v1.put('profile:1', <String, dynamic>{'name': 'Ada'});
    await v1.close();

    int calls = 0;
    final ReaxDB v2 = await DatabaseService.open(
      'migration',
      schemaVersion: 2,
      onUpgrade: (int from, int to, ReaxDB db) async {
        calls++;
        expect(from, 1);
        expect(to, 2);
        await for (final ReaxEntry<Map<String, dynamic>> entry in db
            .scanPrefix<Map<String, dynamic>>('profile:')) {
          await db.put(entry.key, <String, dynamic>{
            ...entry.value,
            'schema': to,
          });
        }
      },
    );
    expect(calls, 1);
    expect(v2.schemaVersion, 2);
    expect((await v2.get<Map<String, dynamic>>('profile:1'))!['schema'], 2);
    await v2.close();

    // Reopening at the same version does not run the callback again.
    final ReaxDB again = await DatabaseService.open(
      'migration',
      schemaVersion: 2,
      onUpgrade: (_, _, _) async => calls++,
    );
    addTearDown(again.close);
    expect(calls, 1);
  });

  test('opening the same path twice reports the lock', () async {
    final ReaxDB db = await DatabaseService.open('locked');
    addTearDown(db.close);

    await expectLater(
      DatabaseService.open('locked'),
      throwsA(isA<DatabaseLockedException>()),
    );
  });

  test('reserved keys are rejected', () async {
    final ReaxDB db = await DatabaseService.open('keys');
    addTearDown(db.close);

    await expectLater(db.put('', 'x'), throwsA(isA<InvalidKeyException>()));
  });
}

/// Stands in for the demo's own insufficient-funds error.
class InsufficientFundsForTest implements Exception {
  const InsufficientFundsForTest();
}
