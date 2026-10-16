/// Crash-safety and privacy tests for [LegacyMigration]: a failed migration
/// must leave the 1.x source untouched and nothing half-built behind, every
/// interrupted state must be resolved by [LegacyMigration.recoverInterrupted],
/// and migration warnings must never contain raw keys.
library;

import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
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
    work = await Directory.systemTemp.createTemp('reaxdb_migration_safety_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (await work.exists()) await work.delete(recursive: true);
  });

  String staging(String path) => '$path${LegacyMigration.stagingSuffix}';

  /// Every sibling of [path] in its parent directory, by name.
  List<String> siblings(String path) => Directory(
    p.dirname(path),
  ).listSync().map((FileSystemEntity e) => p.basename(e.path)).toList()..sort();

  /// Writes the completion marker the way a finished staging build has it.
  Future<void> markComplete(String directory) => File(
    p.join(directory, LegacyMigration.completionMarkerName),
  ).writeAsString('{}');

  /// Produces a complete 2.x migration of [scenario] at [path] through the
  /// public API, as a stand-in for a finished staging directory.
  Future<void> migratedCopy(String scenario, String path) async {
    final String source = await fixtures.copyOf(
      scenario,
      Directory(p.join(work.path, 'scratch-${p.basename(path)}'))
        ..createSync(recursive: true),
    );
    await LegacyMigration.migrate(sourcePath: source, targetPath: path);
  }

  Future<void> expectMatchesLegacy(String path, String scenario) async {
    final Map<String, Object?> expected = fixtures.expected(scenario);
    final ReaxDB db = await ReaxDB.open(path: path);
    try {
      expect(await db.keys().toSet(), expected.keys.toSet());
      for (final MapEntry<String, Object?> entry in expected.entries) {
        expect(
          LegacyFixtures.canonical(await db.get<Object?>(entry.key)),
          jsonEncode(entry.value),
        );
      }
    } finally {
      await db.close();
    }
  }

  group('a failed migration leaves the source untouched', () {
    test('wrong legacy key, in place', () async {
      final String source = await fixtures.copyOf('encrypted_aes', work);
      final Map<String, String> before = _snapshot(source);
      final List<String> siblingsBefore = siblings(source);

      await expectLater(
        LegacyMigration.migrate(
          sourcePath: source,
          targetPath: source,
          legacyEncryptionKey: 'definitely-not-the-key',
        ),
        throwsA(isA<EncryptionException>()),
      );

      expect(_snapshot(source), before, reason: 'source bytes changed');
      expect(siblings(source), siblingsBefore, reason: 'no backup, no staging');
      expect(await LegacyMigration.detect(source), isNotNull);
    });

    test('wrong legacy key, into another directory', () async {
      final String source = await fixtures.copyOf('encrypted_xor', work);
      final Map<String, String> before = _snapshot(source);
      final String target = p.join(work.path, 'target');

      await expectLater(
        LegacyMigration.migrate(
          sourcePath: source,
          targetPath: target,
          legacyEncryptionKey: 'nope-nope-nope',
        ),
        throwsA(isA<EncryptionException>()),
      );

      expect(_snapshot(source), before);
      expect(Directory(target).existsSync(), isFalse);
      expect(Directory(staging(target)).existsSync(), isFalse);
      expect(Directory('$source.1x-backup-1').existsSync(), isFalse);
    });

    test('automatic open with a wrong key keeps refusing to open', () async {
      // The audit repro: before the fix, the failed automatic migration left
      // an empty 2.x database at the path and the next default open showed
      // it with no warning.
      final String source = await fixtures.copyOf('encrypted_aes', work);
      final Map<String, String> before = _snapshot(source);
      await expectLater(
        ReaxDB.open(
          path: source,
          legacyMigration: LegacyMigrationMode.automatic,
          legacyEncryptionKey: 'WRONG-KEY',
        ),
        throwsA(isA<EncryptionException>()),
      );
      expect(_snapshot(source), before);
      await expectLater(
        ReaxDB.open(path: source),
        throwsA(isA<SchemaVersionException>()),
      );
    });

    test('a successful in-place migration leaves no staging behind', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: source,
      );
      expect(siblings(source), <String>[
        p.basename(source),
        p.basename(report.backupPath),
      ]);
      expect(
        File(p.join(source, LegacyMigration.completionMarkerName)).existsSync(),
        isFalse,
      );
      await expectMatchesLegacy(source, 'plain');
    });

    test('a leftover unfinished staging directory is replaced', () async {
      final String source = await fixtures.copyOf('plain', work);
      final String target = p.join(work.path, 'target');
      Directory(staging(target)).createSync();
      File(p.join(staging(target), 'half-written.sst')).writeAsStringSync('x');

      await LegacyMigration.migrate(sourcePath: source, targetPath: target);
      expect(Directory(staging(target)).existsSync(), isFalse);
      await expectMatchesLegacy(target, 'plain');
    });
  });

  group('recoverInterrupted', () {
    test('is a no-op when nothing exists', () async {
      final String path = p.join(work.path, 'fresh');
      await LegacyMigration.recoverInterrupted(path);
      expect(work.listSync(), isEmpty);
    });

    test('discards an unfinished staging directory', () async {
      final String path = await fixtures.copyOf('plain', work);
      final Map<String, String> before = _snapshot(path);
      Directory(staging(path)).createSync();
      File(p.join(staging(path), 'partial')).writeAsStringSync('partial');

      await LegacyMigration.recoverInterrupted(path);

      expect(Directory(staging(path)).existsSync(), isFalse);
      expect(_snapshot(path), before);
    });

    test('finishes a crash between the two renames', () async {
      // State after "source -> backup" and before "staging -> target".
      final String path = p.join(work.path, 'db');
      await migratedCopy('plain', path);
      await Directory(path).rename(staging(path));
      await markComplete(staging(path));

      await LegacyMigration.recoverInterrupted(path);

      expect(Directory(staging(path)).existsSync(), isFalse);
      expect(
        File(p.join(path, LegacyMigration.completionMarkerName)).existsSync(),
        isFalse,
      );
      await expectMatchesLegacy(path, 'plain');
    });

    test('promotes over an empty directory at the target', () async {
      final String path = p.join(work.path, 'db');
      await migratedCopy('deletes', path);
      await Directory(path).rename(staging(path));
      await markComplete(staging(path));
      Directory(path).createSync();

      await LegacyMigration.recoverInterrupted(path);

      expect(Directory(staging(path)).existsSync(), isFalse);
      await expectMatchesLegacy(path, 'deletes');
    });

    test('keeps the 1.x source when it was never moved aside', () async {
      // State after the marker was written and before "source -> backup".
      final String path = await fixtures.copyOf('plain', work);
      final Map<String, String> before = _snapshot(path);
      await migratedCopy('plain', staging(path));
      await markComplete(staging(path));

      await LegacyMigration.recoverInterrupted(path);

      expect(Directory(staging(path)).existsSync(), isFalse);
      expect(_snapshot(path), before);
      // The migration can simply run again.
      await LegacyMigration.migrate(sourcePath: path, targetPath: path);
      await expectMatchesLegacy(path, 'plain');
    });

    test('refuses to choose when both sides may hold data', () async {
      final String path = p.join(work.path, 'db');
      final ReaxDB other = await ReaxDB.open(path: path);
      await other.put('written-after-the-crash', 1);
      await other.close();
      await migratedCopy('plain', staging(path));
      await markComplete(staging(path));
      final Map<String, String> target = _snapshot(path);
      final Map<String, String> staged = _snapshot(staging(path));

      await expectLater(
        LegacyMigration.recoverInterrupted(path),
        throwsA(
          isA<DatabaseLockedException>().having(
            (DatabaseLockedException e) => e.message,
            'message',
            contains(LegacyMigration.stagingSuffix),
          ),
        ),
      );
      expect(_snapshot(path), target);
      expect(_snapshot(staging(path)), staged);
    });

    test('removes a marker left by a crash after the final rename', () async {
      final String path = p.join(work.path, 'db');
      await migratedCopy('plain', path);
      await markComplete(path);

      await LegacyMigration.recoverInterrupted(path);

      expect(
        File(p.join(path, LegacyMigration.completionMarkerName)).existsSync(),
        isFalse,
      );
      await expectMatchesLegacy(path, 'plain');
    });

    test('resolves relative and absolute spellings alike', () async {
      final String path = p.join(work.path, 'db');
      await migratedCopy('plain', path);
      await Directory(path).rename(staging(path));
      await markComplete(staging(path));

      await LegacyMigration.recoverInterrupted(
        '${p.relative(path, from: Directory.current.path)}/',
      );

      expect(Directory(staging(path)).existsSync(), isFalse);
      expect(Directory(path).existsSync(), isTrue);
    });
  });

  group('warnings carry no key contents', () {
    /// Keys distinctive enough that finding them in prose means a leak.
    Iterable<String> distinctiveKeys(String scenario) => fixtures
        .expected(scenario)
        .keys
        .where((String k) => k.contains(':') || k.contains('-'));

    for (final String scenario in <String>[
      'plain',
      'bulk',
      'colliding_long_keys',
      'deletes',
    ]) {
      test(scenario, () async {
        final String source = await fixtures.copyOf(scenario, work);
        final MigrationReport report = await LegacyMigration.migrate(
          sourcePath: source,
          targetPath: p.join(work.path, 'migrated'),
        );
        final List<String> keys = distinctiveKeys(scenario).toList();
        expect(keys, isNotEmpty);
        for (final String warning in report.warnings) {
          for (final String key in keys) {
            expect(warning, isNot(contains(key)), reason: warning);
          }
        }
        if (scenario == 'bulk') {
          // The cross-level warning names keys by fingerprint only.
          expect(
            report.warnings.where(
              (String w) => w.contains('exists in LSM level'),
            ),
            everyElement(matches(RegExp(r'^Key key#[0-9a-f]{8}\(len=\d+\)'))),
          );
        }
      });
    }

    test('the truncated-UTF-16 warning uses a fingerprint', () async {
      final String source = await fixtures.copyOf('plain', work);
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: p.join(work.path, 'migrated'),
      );
      final String warning = report.warnings.singleWhere(
        (String w) => w.contains('above 0x7F'),
      );
      expect(warning, contains('key#'));
      expect(warning, isNot(contains('caf')));
    });
  });

  group('progress', () {
    test('the final call reports migrated == total despite skips', () async {
      final String source = await fixtures.copyOf('plain', work);
      final File wal = Directory(p.join(source, 'wal'))
          .listSync()
          .whereType<File>()
          .where((File f) => f.path.endsWith('.wal'))
          .reduce((File a, File b) => a.lengthSync() >= b.lengthSync() ? a : b);
      final List<int> bytes = wal.readAsBytesSync();
      // Corrupt one value byte late in the file so a record fails to decode
      // without truncating the log.
      bytes[bytes.length - 3] ^= 0xFF;
      wal.writeAsBytesSync(bytes);

      int lastMigrated = -1;
      int lastTotal = -1;
      final MigrationReport report = await LegacyMigration.migrate(
        sourcePath: source,
        targetPath: p.join(work.path, 'migrated'),
        onProgress: (int migrated, int total) {
          expect(migrated, lessThanOrEqualTo(total));
          lastMigrated = migrated;
          lastTotal = total;
        },
      );
      expect(report.entriesSkipped, greaterThan(0));
      expect(lastMigrated, report.entriesMigrated);
      expect(lastTotal, report.entriesMigrated);
    });
  });
}

/// Relative path -> SHA-256 of every file under [root].
Map<String, String> _snapshot(String root) => <String, String>{
  for (final File file in Directory(
    root,
  ).listSync(recursive: true).whereType<File>())
    p.relative(file.path, from: root): sha256
        .convert(file.readAsBytesSync())
        .toString(),
};
