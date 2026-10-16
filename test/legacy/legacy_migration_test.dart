/// End-to-end tests for [LegacyMigration] against databases written by the
/// real ReaxDB 1.4.1 engine.
///
/// The ground truth of every "did the data survive" assertion is what 1.x
/// itself reads back from the same files (recorded by [LegacyFixtures]), not
/// what this test thinks 1.x should have stored.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:reaxdb_dart/src/legacy/legacy.dart';
import 'package:test/test.dart';

import 'legacy_fixtures.dart';

void main() {
  late LegacyFixtures fixtures;
  late Directory work;

  setUpAll(() async {
    fixtures = await LegacyFixtures.build();
  });

  tearDownAll(() async {
    await fixtures.dispose();
  });

  setUp(() async {
    work = await Directory.systemTemp.createTemp('reaxdb_migration_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (await work.exists()) await work.delete(recursive: true);
  });

  String target(String name) => p.join(work.path, 'migrated-$name');

  /// Reads every key of [expected] out of the migrated database and compares
  /// it with what 1.x itself returned.
  Future<void> expectMatchesLegacy(
    String databasePath,
    Map<String, Object?> expected, {
    EncryptionConfig encryption = const EncryptionConfig.none(),
    Set<String> ignoreKeys = const <String>{},
  }) async {
    final ReaxDB db = await ReaxDB.open(
      path: databasePath,
      encryption: encryption,
    );
    try {
      for (final MapEntry<String, Object?> entry in expected.entries) {
        if (ignoreKeys.contains(entry.key)) continue;
        final Object? actual = await db.get<Object?>(entry.key);
        expect(
          LegacyFixtures.canonical(actual),
          jsonEncode(entry.value),
          reason: 'key "${entry.key}" does not match the 1.x readback',
        );
      }
      final Set<String> live = await db.keys().toSet();
      expect(
        live.difference(expected.keys.toSet()).difference(ignoreKeys),
        isEmpty,
        reason: 'the migrated database has keys 1.x did not serve',
      );
      expect(
        expected.keys.toSet().difference(live).difference(ignoreKeys),
        isEmpty,
        reason: 'the migrated database is missing keys 1.x served',
      );
    } finally {
      await db.close();
    }
  }

  group('detect', () {
    test('recognises every 1.x fixture', () async {
      for (final String scenario in fixtures.scenarios) {
        final LegacyDatabaseInfo? info = await LegacyMigration.detect(
          fixtures.pathOf(scenario),
        );
        expect(info, isNotNull, reason: scenario);
        expect(info!.detectedVersion, '1.x');
        expect(info.walFileCount, greaterThan(0), reason: scenario);
      }
    });

    test('counts files and live entries', () async {
      final LegacyDatabaseInfo info = (await LegacyMigration.detect(
        fixtures.pathOf('plain'),
      ))!;
      expect(info.sstableFileCount, greaterThan(0));
      expect(info.estimatedEntryCount, fixtures.expected('plain').length);
      expect(info.looksEncrypted, isFalse);
    });

    test('flags an encrypted 1.x database', () async {
      expect(
        (await LegacyMigration.detect(
          fixtures.pathOf('encrypted_aes'),
        ))!.looksEncrypted,
        isTrue,
      );
      expect(
        (await LegacyMigration.detect(
          fixtures.pathOf('encrypted_xor'),
        ))!.looksEncrypted,
        isTrue,
      );
    });

    test('counts deleted keys out of the live total', () async {
      final LegacyDatabaseInfo info = (await LegacyMigration.detect(
        fixtures.pathOf('deletes'),
      ))!;
      expect(info.estimatedEntryCount, fixtures.expected('deletes').length);
    });

    test('returns null for a directory that is not a 1.x database', () async {
      expect(await LegacyMigration.detect(p.join(work.path, 'nope')), isNull);
      final Directory empty = Directory(p.join(work.path, 'empty'))
        ..createSync();
      expect(await LegacyMigration.detect(empty.path), isNull);
      await File(p.join(empty.path, 'random.txt')).writeAsString('hi');
      expect(await LegacyMigration.detect(empty.path), isNull);
    });

    test('returns null for a 2.x database', () async {
      final String path = p.join(work.path, 'v2');
      final ReaxDB db = await ReaxDB.open(path: path);
      await db.put('a', 1);
      await db.close();
      expect(await LegacyMigration.detect(path), isNull);
    });

    test('returns null for a migrated database', () async {
      final String source = await fixtures.copyOf('plain', work);
      final String destination = target('plain');
      await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: destination,
      );
      expect(await LegacyMigration.detect(destination), isNull);
    });
  });

  group('migrate', () {
    test('reproduces the plain 1.x key/value set exactly', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('plain'),
      );
      expect(report.entriesSkipped, 0);
      expect(report.entriesMigrated, fixtures.expected('plain').length);
      await expectMatchesLegacy(target('plain'), fixtures.expected('plain'));
    });

    test('preserves value types, including bytes and doubles', () async {
      final String source = await fixtures.copyOf('plain', work);
      await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('types'),
      );
      final ReaxDB db = await ReaxDB.open(path: target('types'));
      addTearDown(db.close);
      expect(await db.get<String>('greeting'), 'hello world');
      expect(await db.get<int>('count'), 42);
      expect(await db.get<int>('negative'), -7);
      expect(await db.get<double>('ratio'), 3.5);
      expect(await db.get<bool>('flag'), isTrue);
      expect(await db.get<bool>('off'), isFalse);
      expect(await db.get<Uint8List>('blob'), <int>[0, 1, 2, 250, 255]);
      expect(await db.get<List<dynamic>>('list'), <String>['x', 'y', 'z']);
      expect(await db.get<String>('empty:string'), '');
      expect(await db.get<Map<String, dynamic>>('user:1'), <String, Object?>{
        'name': 'Alice',
        'age': 30,
        'tags': <String>['a', 'b'],
      });
      expect(await db.get<String>('unicode'), 'café über naïve');
    });

    test('warns about keys 1.x stored as truncated UTF-16 units', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('accent'),
      );
      expect(
        report.warnings.where((String w) => w.contains('above 0x7F')),
        isNotEmpty,
      );
      final ReaxDB db = await ReaxDB.open(path: target('accent'));
      addTearDown(db.close);
      expect(await db.get<Map<String, dynamic>>('café:1'), <String, Object?>{
        'accented': 'key',
      });
    });

    test('honours 1.x deletes and reports the tombstones', () async {
      final String source = await fixtures.copyOf('deletes', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('deletes'),
      );
      expect(report.tombstonesApplied, greaterThan(0));
      expect(report.entriesMigrated, fixtures.expected('deletes').length);
      await expectMatchesLegacy(
        target('deletes'),
        fixtures.expected('deletes'),
      );
      final ReaxDB db = await ReaxDB.open(path: target('deletes'));
      addTearDown(db.close);
      expect(await db.get<Object?>('item:0'), isNull);
      expect(await db.get<Map<String, dynamic>>('item:5'), <String, Object?>{
        'index': 5,
        'resurrected': true,
      });
    });

    test('migrates keys longer than 32 bytes', () async {
      final String source = await fixtures.copyOf('long_keys', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('long'),
      );
      expect(report.entriesMigrated, fixtures.expected('long_keys').length);
      expect(report.entriesSkipped, 0);
      await expectMatchesLegacy(target('long'), fixtures.expected('long_keys'));
    });

    test('recovers a key the 1.x memtable key-cache collision hid', () async {
      // 1.x cached the string form of keys over 32 bytes under an 8-bit XOR
      // fold of their bytes, so two colliding keys shared a slot and only one
      // survived in memory. The WAL still holds both, so migration recovers
      // more than 1.x could serve; the anomaly must be reported.
      final Map<String, Object?> legacyReadback = fixtures.expected(
        'colliding_long_keys',
      );
      expect(legacyReadback, hasLength(1));

      final String source = await fixtures.copyOf('colliding_long_keys', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('collide'),
      );
      expect(report.entriesMigrated, 2);
      expect(
        report.warnings.where((String w) => w.contains('key-cache fold')),
        isNotEmpty,
      );

      final ReaxDB db = await ReaxDB.open(path: target('collide'));
      addTearDown(db.close);
      const String base = 'collision-demo-key-padding-padding-';
      expect(await db.get<Map<String, dynamic>>('${base}ab'), <String, Object?>{
        'which': 'a',
      });
      expect(await db.get<Map<String, dynamic>>('${base}ba'), <String, Object?>{
        'which': 'b',
      });
    });

    test('migrates a database that flushed and compacted', () async {
      final String source = await fixtures.copyOf('bulk', work);
      int lastMigrated = 0;
      int lastTotal = 0;
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('bulk'),
        onProgress: (int migrated, int total) {
          expect(migrated, greaterThanOrEqualTo(lastMigrated));
          lastMigrated = migrated;
          lastTotal = total;
        },
      );
      expect(report.entriesSkipped, 0);
      expect(report.entriesMigrated, 6000);
      expect(lastMigrated, 6000);
      expect(lastTotal, 6000);
      await expectMatchesLegacy(target('bulk'), fixtures.expected('bulk'));
    });

    test('the newest value wins across LSM levels', () async {
      final String source = await fixtures.copyOf('bulk', work);
      await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('bulk-newest'),
      );
      final ReaxDB db = await ReaxDB.open(path: target('bulk-newest'));
      addTearDown(db.close);
      // These keys were written once and then overwritten after a flush.
      for (int i = 0; i < 200; i += 37) {
        expect(
          await db.get<String>('bulk:${i.toString().padLeft(5, '0')}'),
          'overwritten-$i',
        );
      }
    });

    test('migrates an empty 1.x database', () async {
      final String source = await fixtures.copyOf('empty', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('empty'),
      );
      expect(report.entriesMigrated, 0);
      expect(report.entriesSkipped, 0);
      final ReaxDB db = await ReaxDB.open(path: target('empty'));
      addTearDown(db.close);
      expect(await db.keys().toList(), isEmpty);
    });
  });

  group('simple API databases', () {
    test('drops the 1.x key registry and says so', () async {
      final String source = await fixtures.copyOf('simple_api', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('simple'),
      );
      expect(
        report.warnings.where(
          (String w) => w.contains('__reaxdb_simple_keys__'),
        ),
        isNotEmpty,
      );
      final ReaxDB db = await ReaxDB.open(path: target('simple'));
      addTearDown(db.close);
      expect(await db.get<Object?>('__reaxdb_simple_keys__'), isNull);
      expect(
        await db.keys().toList(),
        isNot(contains('__reaxdb_simple_keys__')),
      );
    });

    test('migrates the real data of a simple 1.x database', () async {
      final String source = await fixtures.copyOf('simple_api', work);
      await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('simple-data'),
      );
      await expectMatchesLegacy(
        target('simple-data'),
        fixtures.expected('simple_api'),
        ignoreKeys: <String>{'__reaxdb_simple_keys__'},
      );
    });
  });

  group('secondary indexes', () {
    test('1.x index definitions are recreated and backfilled', () async {
      final String source = await fixtures.copyOf('indexed', work);
      expect(
        Directory(p.join(source, 'indexes', 'users_age')).existsSync(),
        isTrue,
        reason: 'the fixture must contain a 1.x index directory',
      );

      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('indexed'),
      );
      expect(report.entriesSkipped, 0);
      await expectMatchesLegacy(
        target('indexed'),
        fixtures.expected('indexed'),
      );

      final ReaxDB db = await ReaxDB.open(path: target('indexed'));
      addTearDown(db.close);
      // The 2.x index is rebuilt from the migrated documents, so it answers
      // queries without any 1.x index content being trusted.
      final List<Map<String, dynamic>> found = await db.where(
        'users',
        'age',
        22,
      );
      expect(found, hasLength(1));
      expect(found.single['name'], 'user2');
    });
  });

  group('encryption', () {
    test('migrates an AES-256 1.x database with the original key', () async {
      final String source = await fixtures.copyOf('encrypted_aes', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('aes'),
        legacyEncryptionKey: 'super-secret-legacy-key-2025',
      );
      expect(report.entriesSkipped, 0);
      await expectMatchesLegacy(
        target('aes'),
        fixtures.expected('encrypted_aes'),
      );
    });

    test('migrates an xor 1.x database with the original key', () async {
      final String source = await fixtures.copyOf('encrypted_xor', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('xor'),
        legacyEncryptionKey: 'legacy-xor-key-42',
      );
      expect(report.entriesSkipped, 0);
      await expectMatchesLegacy(
        target('xor'),
        fixtures.expected('encrypted_xor'),
      );
    });

    test('re-encrypts with the 2.x scheme, not the 1.x derivation', () async {
      final String source = await fixtures.copyOf('encrypted_aes', work);
      final EncryptionConfig encryption = EncryptionConfig.aes256FromPassphrase(
        passphrase: 'a-new-and-completely-different-passphrase',
      );
      await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('reencrypted'),
        legacyEncryptionKey: 'super-secret-legacy-key-2025',
        encryption: encryption,
      );
      await expectMatchesLegacy(
        target('reencrypted'),
        fixtures.expected('encrypted_aes'),
        encryption: encryption,
      );

      // The migrated database uses a random per-database PBKDF2 salt, so the
      // 1.x key cannot open it and no 1.x-derived key material is on disk.
      final Uint8List legacyKey = LegacyDecryptor.deriveAes256Key(
        'super-secret-legacy-key-2025',
      );
      for (final FileSystemEntity entity in Directory(
        target('reencrypted'),
      ).listSync(recursive: true)) {
        if (entity is! File) continue;
        expect(
          _contains(entity.readAsBytesSync(), legacyKey),
          isFalse,
          reason: '${entity.path} contains the 1.x derived key',
        );
      }
    });

    test('a wrong legacy key fails with EncryptionException', () async {
      final String source = await fixtures.copyOf('encrypted_aes', work);
      await expectLater(
        LegacyMigration.migrate(
          sourcePath: source,
          targetPath: target('wrong-key'),
          legacyEncryptionKey: 'definitely-not-the-key',
        ),
        throwsA(
          isA<EncryptionException>().having(
            (EncryptionException e) => e.message,
            'message',
            contains('does not decrypt'),
          ),
        ),
      );
    });

    test('a wrong xor key fails with EncryptionException', () async {
      final String source = await fixtures.copyOf('encrypted_xor', work);
      await expectLater(
        LegacyMigration.migrate(
          sourcePath: source,
          targetPath: target('wrong-xor'),
          legacyEncryptionKey: 'nope-nope-nope',
        ),
        throwsA(isA<EncryptionException>()),
      );
    });

    test('a missing legacy key fails with EncryptionException', () async {
      final String source = await fixtures.copyOf('encrypted_aes', work);
      await expectLater(
        LegacyMigration.migrate(
          sourcePath: source,
          targetPath: target('no-key'),
        ),
        throwsA(
          isA<EncryptionException>().having(
            (EncryptionException e) => e.message,
            'message',
            contains('legacyEncryptionKey'),
          ),
        ),
      );
    });

    test('an unnecessary legacy key is reported, not obeyed', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('unneeded-key'),
        legacyEncryptionKey: 'not-actually-used',
      );
      expect(
        report.warnings.where((String w) => w.contains('not encrypted')),
        isNotEmpty,
      );
      await expectMatchesLegacy(
        target('unneeded-key'),
        fixtures.expected('plain'),
      );
    });
  });

  group('the backup', () {
    test('is created next to the source and keeps the 1.x files', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('backup'),
      );
      expect(report.backupPath, '$source.1x-backup-1');
      expect(Directory(report.backupPath).existsSync(), isTrue);
      expect(
        Directory(p.join(report.backupPath, 'wal')).listSync(),
        isNotEmpty,
      );
      expect(
        Directory(p.join(report.backupPath, 'lsm')).listSync(),
        isNotEmpty,
      );
      // The source itself is untouched when migrating to another directory.
      expect(await LegacyMigration.detect(source), isNotNull);
    });

    test(
      'in-place migration moves the originals aside and never deletes them',
      () async {
        final String source = await fixtures.copyOf('plain', work);
        final List<String> originalFiles = _relativeFiles(source);
        final MigrationReport report = await LegacyMigration.migrate(
          sourcePath: source,
          targetPath: source,
        );
        expect(report.backupPath, '$source.1x-backup-1');
        expect(_relativeFiles(report.backupPath), originalFiles);
        expect(await LegacyMigration.detect(report.backupPath), isNotNull);
        // The source path now holds a 2.x database.
        expect(await LegacyMigration.detect(source), isNull);
        await expectMatchesLegacy(source, fixtures.expected('plain'));
      },
    );

    test('a second migration reserves a new backup directory', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport first = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('one'),
      );
      final MigrationReport second = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('two'),
      );
      expect(first.backupPath, endsWith('-1'));
      expect(second.backupPath, endsWith('-2'));
      expect(Directory(first.backupPath).existsSync(), isTrue);
      expect(Directory(second.backupPath).existsSync(), isTrue);
    });

    test('migrating the backup again gives the same result', () async {
      final String source = await fixtures.copyOf('deletes', work);
      final MigrationReport first = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: source,
      );
      final MigrationReport second = await LegacyMigration.migrate(
        sourcePath: first.backupPath,
        targetPath: target('again'),
      );
      expect(second.entriesMigrated, first.entriesMigrated);
      expect(second.entriesSkipped, first.entriesSkipped);
      expect(second.tombstonesApplied, first.tombstonesApplied);
      await expectMatchesLegacy(target('again'), fixtures.expected('deletes'));
      await expectMatchesLegacy(source, fixtures.expected('deletes'));
    });
  });

  group('damaged sources', () {
    test('a truncated SSTable yields a partial report, not a crash', () async {
      final String source = await fixtures.copyOf('bulk', work);
      final File biggest = _largest(p.join(source, 'lsm'), '.sst');
      final Uint8List bytes = await biggest.readAsBytes();
      await biggest.writeAsBytes(
        Uint8List.sublistView(bytes, 0, bytes.length ~/ 3),
      );

      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('trunc-sst'),
      );
      expect(report.warnings, isNotEmpty);
      expect(
        report.warnings.where((String w) => w.contains('ends in the middle')),
        isNotEmpty,
      );
      // The write-ahead log still holds every record, so nothing is lost.
      expect(report.entriesMigrated, 6000);
      await expectMatchesLegacy(target('trunc-sst'), fixtures.expected('bulk'));
    });

    test('a truncated WAL yields a partial report, not a crash', () async {
      final String source = await fixtures.copyOf('plain', work);
      final File biggest = _largest(p.join(source, 'wal'), '.wal');
      final Uint8List bytes = await biggest.readAsBytes();
      await biggest.writeAsBytes(
        Uint8List.sublistView(bytes, 0, bytes.length ~/ 2),
      );

      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('trunc-wal'),
      );
      expect(report.warnings, isNotEmpty);
      expect(report.entriesSkipped, greaterThan(0));
      expect(report.entriesMigrated, greaterThan(0));
      // Whatever survived must still be readable and correct.
      final ReaxDB db = await ReaxDB.open(path: target('trunc-wal'));
      addTearDown(db.close);
      final Map<String, Object?> expected = fixtures.expected('plain');
      await for (final String key in db.keys()) {
        expect(expected, contains(key));
        expect(
          LegacyFixtures.canonical(await db.get<Object?>(key)),
          jsonEncode(expected[key]),
        );
      }
    });

    test('a WAL file of pure garbage is reported, not fatal', () async {
      final String source = await fixtures.copyOf('plain', work);
      await File(
        p.join(source, 'wal', 'wal_9999999999999999.wal'),
      ).writeAsBytes(
        Uint8List.fromList(List<int>.generate(512, (int i) => (i * 7) & 0xFF)),
      );
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: target('garbage-wal'),
      );
      expect(report.warnings, isNotEmpty);
      expect(report.entriesMigrated, fixtures.expected('plain').length);
    });
  });

  group('argument handling', () {
    test('a non-1.x source throws SchemaVersionException', () async {
      final Directory notADatabase = Directory(p.join(work.path, 'plainold'))
        ..createSync();
      await File(p.join(notADatabase.path, 'notes.txt')).writeAsString('hi');
      await expectLater(
        LegacyMigration.migrate(
          sourcePath: notADatabase.path,
          targetPath: target('nope'),
        ),
        throwsA(isA<SchemaVersionException>()),
      );
    });

    test('a non-empty target throws DatabaseLockedException', () async {
      final String source = await fixtures.copyOf('plain', work);
      final Directory occupied = Directory(target('occupied'))..createSync();
      await File(p.join(occupied.path, 'something')).writeAsString('x');
      await expectLater(
        LegacyMigration.migrate(sourcePath: source, targetPath: occupied.path),
        throwsA(isA<DatabaseLockedException>()),
      );
      // Nothing was migrated, and the source is still intact.
      expect(await LegacyMigration.detect(source), isNotNull);
    });
  });
}

bool _contains(Uint8List haystack, Uint8List needle) {
  if (needle.isEmpty || haystack.length < needle.length) return false;
  for (int i = 0; i <= haystack.length - needle.length; i++) {
    bool match = true;
    for (int j = 0; j < needle.length; j++) {
      if (haystack[i + j] != needle[j]) {
        match = false;
        break;
      }
    }
    if (match) return true;
  }
  return false;
}

List<String> _relativeFiles(String root) {
  final List<String> files =
      Directory(root)
          .listSync(recursive: true)
          .whereType<File>()
          .map((File f) => p.relative(f.path, from: root))
          .toList()
        ..sort();
  return files;
}

File _largest(String directory, String extension) {
  final List<File> files =
      Directory(directory)
          .listSync()
          .whereType<File>()
          .where((File f) => f.path.endsWith(extension))
          .toList()
        ..sort((File a, File b) => b.lengthSync().compareTo(a.lengthSync()));
  if (files.isEmpty) {
    throw StateError('no $extension file in $directory');
  }
  return files.first;
}
