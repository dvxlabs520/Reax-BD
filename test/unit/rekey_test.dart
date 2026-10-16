import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

Uint8List key(int seed) =>
    Uint8List.fromList(List<int>.generate(32, (int i) => (i * 7 + seed) % 256));

List<String> rawBytesOf(String dir) => <String>[
  for (final FileSystemEntity e in Directory(dir).listSync(recursive: true))
    if (e is File) latin1.decode(e.readAsBytesSync()),
];

void main() {
  late Directory root;
  late String path;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_rekey_');
    path = p.join(root.path, 'db');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    await deleteTempDirectory(root);
  });

  Future<void> fill(ReaxDB db) async {
    await db.createIndex('users', <String>['city']);
    await db.put('users:1', {'name': 'Ada', 'city': 'London'});
    await db.put('users:2', {'name': 'Alan', 'city': 'Paris'});
    await db.put('session', 'token', ttl: const Duration(hours: 1));
    await db.put('plain-marker', 'find-me-in-the-files');
  }

  Future<void> expectContent(ReaxDB db) async {
    expect(await db.get<Map<String, dynamic>>('users:1'), {
      'name': 'Ada',
      'city': 'London',
    });
    expect(await db.get<String>('session'), 'token');
    expect(db.hasIndex('users', 'city'), isTrue);
    final List<Map<String, dynamic>> paris = await db
        .query('users')
        .whereEquals('city', 'Paris')
        .find();
    expect(paris.single['name'], 'Alan');
  }

  test('encrypts a plain database and keeps everything', () async {
    final ReaxDB db = await ReaxDB.open(path: path, schemaVersion: 3);
    await fill(db);

    final ReaxDB rekeyed = await db.rekey(EncryptionConfig.aes256(key: key(1)));
    expect(db.isClosed, isTrue);
    await expectLater(
      db.get<String>('session'),
      throwsA(isA<DatabaseClosedException>()),
    );
    await expectContent(rekeyed);
    expect(rekeyed.schemaVersion, 3);
    expect(rekeyed.encryptionInfo['type'], 'aes256');
    await rekeyed.close();

    // The value is no longer readable in the files, and nothing was left
    // next to the database.
    expect(
      rawBytesOf(path).any((String s) => s.contains('find-me-in-the-files')),
      isFalse,
    );
    expect(
      Directory(
        root.path,
      ).listSync().map((FileSystemEntity e) => p.basename(e.path)),
      <String>['db'],
    );

    await expectLater(
      ReaxDB.open(path: path, schemaVersion: 3),
      throwsA(isA<ReaxDbException>()),
    );
    final ReaxDB reopened = await ReaxDB.open(
      path: path,
      schemaVersion: 3,
      encryption: EncryptionConfig.aes256(key: key(1)),
    );
    await expectContent(reopened);
  });

  test('rotates a key, and removes encryption again', () async {
    ReaxDB db = await ReaxDB.open(
      path: path,
      encryption: EncryptionConfig.aes256(key: key(1)),
    );
    await fill(db);
    db = await db.rekey(EncryptionConfig.aes256(key: key(2)));
    await expectContent(db);
    await db.close();

    await expectLater(
      ReaxDB.open(
        path: path,
        encryption: EncryptionConfig.aes256(key: key(1)),
      ),
      throwsA(isA<EncryptionException>()),
    );
    db = await ReaxDB.open(
      path: path,
      encryption: EncryptionConfig.aes256(key: key(2)),
    );
    db = await db.rekey(const EncryptionConfig.none());
    await expectContent(db);
    await db.close();
    expect(
      rawBytesOf(path).any((String s) => s.contains('find-me-in-the-files')),
      isTrue,
    );
  });

  test('changePassphrase', () async {
    ReaxDB db = await ReaxDB.open(
      path: path,
      encryption: EncryptionConfig.aes256FromPassphrase(
        passphrase: 'old secret',
        iterations: 1000,
      ),
    );
    await fill(db);
    db = await db.changePassphrase('new secret', iterations: 1000);
    await expectContent(db);
    await db.close();

    await expectLater(
      ReaxDB.open(
        path: path,
        encryption: EncryptionConfig.aes256FromPassphrase(
          passphrase: 'old secret',
          iterations: 1000,
        ),
      ),
      throwsA(isA<EncryptionException>()),
    );
    db = await ReaxDB.open(
      path: path,
      encryption: EncryptionConfig.aes256FromPassphrase(
        passphrase: 'new secret',
        iterations: 1000,
      ),
    );
    await expectContent(db);
  });

  test('the database opens again with the same options', () async {
    final ReaxDB db = await ReaxDB.open(path: path, keepInMemory: true);
    await fill(db);
    final ReaxDB rekeyed = await db.rekey(EncryptionConfig.aes256(key: key(5)));
    expect(rekeyed.keepsInMemory, isTrue);
    expect(rekeyed.getSync<String>('session'), 'token');
  });

  group('a restore interrupted while swapping directories', () {
    Future<void> makeDatabase(String at, String value) async {
      final ReaxDB db = await ReaxDB.open(path: at);
      await db.put('k', value);
      await db.close();
    }

    test('is finished when the restored copy is complete', () async {
      await makeDatabase(path, 'old');
      final String staging = '$path.importing-1';
      await makeDatabase(staging, 'new');
      File(p.join(staging, 'RESTORE_COMPLETE')).writeAsStringSync('');
      // The crash: the original moved away, the copy not yet moved in.
      Directory(path).renameSync('$path.replaced-1');

      final ReaxDB db = await ReaxDB.open(path: path);
      expect(await db.get<String>('k'), 'new');
      expect(Directory('$path.replaced-1').existsSync(), isFalse);
      expect(File(p.join(path, 'RESTORE_COMPLETE')).existsSync(), isFalse);
    });

    test('is undone when the restored copy is incomplete', () async {
      await makeDatabase(path, 'old');
      final String staging = '$path.importing-1';
      await makeDatabase(staging, 'half');
      Directory(path).renameSync('$path.replaced-1');

      final ReaxDB db = await ReaxDB.open(path: path);
      expect(await db.get<String>('k'), 'old');
      // An incomplete staging copy may belong to a restore still running.
      expect(Directory(staging).existsSync(), isTrue);
    });

    test('leftovers of a finished swap are cleaned up', () async {
      await makeDatabase(path, 'current');
      await makeDatabase('$path.replaced-1', 'superseded');
      final ReaxDB db = await ReaxDB.open(path: path);
      expect(await db.get<String>('k'), 'current');
      expect(Directory('$path.replaced-1').existsSync(), isFalse);
    });

    test('a database with no leftovers is untouched', () async {
      await makeDatabase(path, 'only');
      final ReaxDB db = await ReaxDB.open(path: path);
      expect(await db.get<String>('k'), 'only');
    });
  });
}
