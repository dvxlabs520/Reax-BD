/// Tests for the ReaxDB 1.x compatibility surface of 2.0.
///
/// Two things are under test: the deprecated shims (they must compile, do the
/// documented thing, and never silently restore 1.x's insecure behaviour) and
/// the open-time reaction to a 1.x database directory.
@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

/// Writes a directory in the ReaxDB 1.x on-disk layout.
///
/// The bytes follow the 1.x writers exactly: `<dir>/lsm/level_0_<ts>.sst`
/// holds `[keyLen u32 LE][key][valueLen u32 LE][value]` records followed by
/// `[indexLen u32 LE][index JSON]`, keys are `String.codeUnits` and values
/// carry the 1.x type marker (0 = string). `wal/` and `btree/` exist because
/// the 1.x engine always created them.
Future<void> writeLegacyDatabase(
  String directory,
  Map<String, String> entries,
) async {
  final Directory lsm = Directory(p.join(directory, 'lsm'));
  await lsm.create(recursive: true);
  await Directory(p.join(directory, 'wal')).create(recursive: true);
  await Directory(p.join(directory, 'btree')).create(recursive: true);

  final BytesBuilder body = BytesBuilder();
  final Map<String, int> index = <String, int>{};
  int offset = 0;

  final List<String> keys = entries.keys.toList()..sort();
  for (final String key in keys) {
    final List<int> keyBytes = key.codeUnits;
    final Uint8List value = _legacyString(entries[key]!);
    index[key] = offset;

    body.add(_u32(keyBytes.length));
    body.add(keyBytes);
    offset += 4 + keyBytes.length;
    body.add(_u32(value.length));
    body.add(value);
    offset += 4 + value.length;
  }

  final Uint8List indexBytes = Uint8List.fromList(
    utf8.encode(jsonEncode(index)),
  );
  body.add(_u32(indexBytes.length));
  body.add(indexBytes);

  final String name = 'level_0_${DateTime.now().millisecondsSinceEpoch}.sst';
  await File(
    p.join(lsm.path, name),
  ).writeAsBytes(body.takeBytes(), flush: true);
}

Uint8List _u32(int value) =>
    (ByteData(4)..setUint32(0, value, Endian.little)).buffer.asUint8List();

/// The 1.x encoding of a string value: marker 0, length, UTF-8 bytes.
Uint8List _legacyString(String value) {
  final List<int> bytes = utf8.encode(value);
  final BytesBuilder out = BytesBuilder()
    ..add(<int>[0])
    ..add(_u32(bytes.length))
    ..add(bytes);
  return out.takeBytes();
}

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_compat_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  String pathFor(String name) => p.join(root.path, name);

  group('deprecated 1.x open shape', () {
    test('openLegacy(name, path:) opens and stores data', () async {
      // ignore: deprecated_member_use
      final ReaxDB db = await ReaxDB.openLegacy(
        'myapp',
        path: pathFor('legacy_open'),
      );
      addTearDown(db.close);

      await db.put('user:1', <String, dynamic>{'name': 'Ada'});
      expect(await db.get<Map<String, dynamic>>('user:1'), <String, dynamic>{
        'name': 'Ada',
      });
      expect(db.path, endsWith('legacy_open'));
    });

    test('openLegacy(name) alone uses the name as the directory', () async {
      final String name = pathFor('named_db');
      // ignore: deprecated_member_use
      final ReaxDB db = await ReaxDB.openLegacy(name);
      addTearDown(db.close);
      expect(Directory(name).existsSync(), isTrue);
    });

    test('DatabaseConfig maps onto the 2.0 options', () async {
      // ignore: deprecated_member_use
      final DatabaseConfig config = DatabaseConfig.defaultConfig();
      // ignore: deprecated_member_use
      final ReaxDB db = await ReaxDB.openLegacy(
        'cfg',
        path: pathFor('cfg'),
        config: config,
      );
      addTearDown(db.close);
      expect(db.syncMode, SyncMode.full);
    });

    test('syncWrites: false maps to SyncMode.none', () async {
      // ignore: deprecated_member_use
      const DatabaseConfig config = DatabaseConfig(
        memtableSizeMB: 1,
        pageSize: 4096,
        l1CacheSize: 10,
        l2CacheSize: 20,
        l3CacheSize: 30,
        compressionEnabled: true,
        syncWrites: false,
        maxImmutableMemtables: 4,
        cacheSize: 50,
        enableCache: true,
      );
      final ReaxDB db = await ReaxDB.open(
        path: pathFor('async_writes'),
        // ignore: deprecated_member_use
        config: config,
      );
      addTearDown(db.close);
      expect(db.syncMode, SyncMode.none);
    });

    test('encryptionKey derives a real AES-256 key, not the 1.x one', () async {
      final String path = pathFor('keyed');
      final ReaxDB db = await ReaxDB.open(
        path: path,
        // ignore: deprecated_member_use
        encryptionKey: 'correct horse battery staple',
      );
      await db.put('secret', 'value');
      expect(db.encryptionInfo['type'], 'aes256');
      await db.close();

      // The salt is random and stored in the header, so the same passphrase
      // must still open the database.
      final ReaxDB reopened = await ReaxDB.open(
        path: path,
        // ignore: deprecated_member_use
        encryptionKey: 'correct horse battery staple',
      );
      addTearDown(reopened.close);
      expect(await reopened.get<String>('secret'), 'value');
    });

    test('a wrong encryptionKey fails loudly', () async {
      final String path = pathFor('keyed_wrong');
      final ReaxDB db = await ReaxDB.open(
        path: path,
        // ignore: deprecated_member_use
        encryptionKey: 'right',
      );
      await db.put('secret', 'value');
      await db.close();

      await expectLater(
        ReaxDB.open(
          path: path,
          // ignore: deprecated_member_use
          encryptionKey: 'wrong',
        ),
        throwsA(isA<EncryptionException>()),
      );
    });

    test('xor config plus a key gives obfuscation, not AES', () async {
      final ReaxDB db = await ReaxDB.open(
        path: pathFor('xor'),
        // ignore: deprecated_member_use
        config: DatabaseConfig.withXorEncryption(),
        // ignore: deprecated_member_use
        encryptionKey: 'not-a-secret',
      );
      addTearDown(db.close);
      expect(db.encryptionInfo['type'], 'obfuscation');
    });

    test('an encrypted config without a key throws', () async {
      await expectLater(
        ReaxDB.open(
          path: pathFor('no_key'),
          // ignore: deprecated_member_use
          config: DatabaseConfig.withAes256Encryption(),
        ),
        throwsA(isA<EncryptionException>()),
      );
    });

    test('mixing 2.0 encryption: with the 1.x parameters throws', () async {
      await expectLater(
        ReaxDB.open(
          path: pathFor('mixed'),
          encryption: EncryptionConfig.aes256FromPassphrase(passphrase: 'a'),
          // ignore: deprecated_member_use
          encryptionKey: 'b',
        ),
        throwsA(isA<UnsupportedApiException>()),
      );
    });
  });

  group('deprecated simple API', () {
    test('quickStart opens a simple database', () async {
      // ignore: deprecated_member_use
      final SimpleReaxDB db = await ReaxDB.quickStart(pathFor('quick'));
      addTearDown(db.close);
      await db.put('k', 'v');
      expect(await db.get('k'), 'v');
    });

    test('encrypted: false is a no-op', () async {
      final SimpleReaxDB db = await ReaxDB.simple(
        pathFor('plain_simple'),
        // ignore: deprecated_member_use
        encrypted: false,
      );
      addTearDown(db.close);
      expect((await db.info()).encryptionType, 'none');
    });

    test('encrypted: true throws instead of deriving a key from the name', () {
      expect(
        () => ReaxDB.simple(
          pathFor('name_keyed'),
          // ignore: deprecated_member_use
          encrypted: true,
        ),
        throwsA(
          isA<EncryptionException>().having(
            (EncryptionException e) => e.message,
            'message',
            allOf(contains('aes256FromPassphrase'), contains('guessable')),
          ),
        ),
      );
    });
  });

  group('deprecated diagnostics return real data', () {
    late ReaxDB db;

    setUp(() async {
      db = await ReaxDB.open(path: pathFor('diagnostics'));
      await db.put('a', 1);
      await db.put('b', 2);
      await db.get<int>('a');
      await db.get<int>('a');
    });

    tearDown(() => db.close());

    test('getDatabaseInfo reports the real path and entry count', () async {
      // ignore: deprecated_member_use
      final DatabaseInfo info = await db.getDatabaseInfo();
      expect(info.path, db.path);
      expect(info.entryCount, 2);
      expect(info.encryptionType, 'none');
    });

    test('getStatistics matches statistics()', () async {
      // ignore: deprecated_member_use
      final Map<String, dynamic> stats = await db.getStatistics();
      expect(stats.keys, containsAll(<String>['database', 'cache', 'storage']));
      expect((stats['database'] as Map<String, dynamic>)['path'], db.path);
    });

    test('getPerformanceStats reports measured counters only', () async {
      // ignore: deprecated_member_use
      final Map<String, dynamic> stats = db.getPerformanceStats();
      final Map<String, dynamic> cache = stats['cache'] as Map<String, dynamic>;
      expect(cache['hits'], greaterThan(0));
      expect(cache['hitRatio'], isA<double>());
      expect(stats.containsKey('optimization'), isFalse);
      expect(
        (stats['transactions'] as Map<String, dynamic>)['committed'],
        isA<int>(),
      );
    });

    test('getEncryptionInfo matches encryptionInfo', () {
      // ignore: deprecated_member_use
      expect(db.getEncryptionInfo(), db.encryptionInfo);
    });

    test('changeStream and stream still deliver events', () async {
      // ignore: deprecated_member_use
      final Future<DatabaseChangeEvent> first = db.changeStream.first;
      // ignore: deprecated_member_use
      final Future<DatabaseChangeEvent> scoped = db.stream('user:*').first;
      await db.put('user:1', 'x');
      expect((await first).key, 'user:1');
      expect((await scoped).key, 'user:1');
    });
  });

  test('EncryptionType.xor is an alias of obfuscation', () {
    // ignore: deprecated_member_use
    expect(EncryptionType.xor, EncryptionType.obfuscation);
  });

  group('opening a 1.x directory', () {
    late String legacyPath;

    setUp(() async {
      legacyPath = pathFor('legacy_db');
      await writeLegacyDatabase(legacyPath, <String, String>{
        'user:1': 'Ada',
        'user:2': 'Grace',
      });
    });

    test('detect (the default) refuses and explains how to migrate', () async {
      await expectLater(
        ReaxDB.open(path: legacyPath),
        throwsA(
          isA<SchemaVersionException>().having(
            (SchemaVersionException e) => e.message,
            'message',
            allOf(
              contains('ReaxDB 1'),
              contains('migrateFrom1x'),
              contains('LegacyMigrationMode.automatic'),
              contains('LegacyMigrationMode.off'),
              contains(legacyPath),
            ),
          ),
        ),
      );
      // Nothing was rewritten.
      expect(Directory(p.join(legacyPath, 'lsm')).listSync(), isNotEmpty);
    });

    test('off ignores the 1.x files and opens an empty database', () async {
      final ReaxDB db = await ReaxDB.open(
        path: legacyPath,
        legacyMigration: LegacyMigrationMode.off,
      );
      addTearDown(db.close);
      expect(await db.get<String>('user:1'), isNull);
      expect(await db.keys().toList(), isEmpty);
      expect(Directory(p.join(legacyPath, 'lsm')).existsSync(), isTrue);
    });

    test('detect finds nothing in a fresh 2.0 database', () async {
      final String path = pathFor('fresh');
      final ReaxDB db = await ReaxDB.open(path: path);
      await db.put('k', 'v');
      await db.close();

      expect(await ReaxDB.inspect1x(path), isNull);
      final ReaxDB reopened = await ReaxDB.open(path: path);
      addTearDown(reopened.close);
      expect(await reopened.get<String>('k'), 'v');
    });

    test('inspect1x describes the legacy directory', () async {
      final LegacyDatabaseInfo? info = await ReaxDB.inspect1x(legacyPath);
      expect(info, isNotNull);
      expect(info!.sstableFileCount, 1);
      expect(info.looksEncrypted, isFalse);
    });

    test('automatic migrates the data and keeps a backup', () async {
      final ReaxDB db = await ReaxDB.open(
        path: legacyPath,
        legacyMigration: LegacyMigrationMode.automatic,
      );
      addTearDown(db.close);

      expect(await db.get<String>('user:1'), 'Ada');
      expect(await db.get<String>('user:2'), 'Grace');
      expect(
        Directory(
          root.path,
        ).listSync().map((FileSystemEntity e) => p.basename(e.path)),
        contains(startsWith('legacy_db.1x-backup')),
      );
    });

    test('migrateFrom1x reports what it moved', () async {
      final MigrationReport report = await ReaxDB.migrateFrom1x(
        sourcePath: legacyPath,
        targetPath: pathFor('migrated'),
      );
      expect(report.entriesMigrated, 2);
      expect(report.backupPath, isNotEmpty);

      final ReaxDB db = await ReaxDB.open(path: pathFor('migrated'));
      addTearDown(db.close);
      expect(await db.get<String>('user:2'), 'Grace');
    });
  });
}
