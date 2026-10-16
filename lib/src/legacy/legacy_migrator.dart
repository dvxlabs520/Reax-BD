/// Detection and migration of ReaxDB 1.x databases into ReaxDB 2.x.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../core/encryption/encryption_config.dart';
import '../core/errors/exceptions.dart';
import '../core/logging/redaction.dart';
import '../core/util/byte_key.dart';
import '../reaxdb.dart';
import 'legacy_encryption.dart';
import 'legacy_sstable_reader.dart';
import 'legacy_value_codec.dart';
import 'legacy_wal_reader.dart';

/// Describes a database directory written by ReaxDB 1.x.
final class LegacyDatabaseInfo {
  /// Creates a description of a 1.x database directory.
  const LegacyDatabaseInfo({
    required this.path,
    required this.detectedVersion,
    required this.sstableFileCount,
    required this.walFileCount,
    required this.looksEncrypted,
    required this.estimatedEntryCount,
  });

  /// Directory that holds the 1.x database.
  final String path;

  /// The format family recognised. ReaxDB 1.x wrote no version marker at all,
  /// so every 1.x database is reported as `'1.x'`.
  final String detectedVersion;

  /// Number of `.sst` files under `<path>/lsm`.
  final int sstableFileCount;

  /// Number of `.wal` files under `<path>/wal`.
  final int walFileCount;

  /// Whether the stored values look encrypted.
  ///
  /// This is a heuristic, and it has to be: ReaxDB 1.x recorded nothing about
  /// encryption on disk. Every plaintext 1.x value starts with a type marker
  /// followed by a length that must match the record size exactly, so the
  /// heuristic samples up to 32 live values and reports `true` when none of
  /// them satisfies that envelope. An empty database reports `false`.
  final bool looksEncrypted;

  /// Number of live (non-deleted) keys found by merging the SSTable levels
  /// and the write-ahead log.
  ///
  /// This is exact for a healthy database and a lower bound for a damaged
  /// one; it is called an estimate because unreadable records are excluded.
  final int estimatedEntryCount;

  @override
  String toString() =>
      'LegacyDatabaseInfo(path: $path, version: $detectedVersion, '
      'sstables: $sstableFileCount, wal: $walFileCount, '
      'encrypted: $looksEncrypted, entries: $estimatedEntryCount)';
}

/// The outcome of migrating a ReaxDB 1.x database.
final class MigrationReport {
  /// Creates a migration report.
  const MigrationReport({
    required this.entriesMigrated,
    required this.entriesSkipped,
    required this.tombstonesApplied,
    required this.warnings,
    required this.elapsed,
    required this.backupPath,
  });

  /// Number of key/value pairs written into the new database.
  final int entriesMigrated;

  /// Number of records that could not be migrated.
  ///
  /// Every skipped record is also described in [warnings]. Nothing is ever
  /// dropped silently; a record is counted here when it is unreadable, when
  /// its value fails to decrypt or decode, or when its key is not usable as a
  /// 2.x key.
  final int entriesSkipped;

  /// Number of keys whose newest 1.x record was a tombstone, and which were
  /// therefore intentionally not migrated.
  final int tombstonesApplied;

  /// Anomalies observed while reading the 1.x database.
  final List<String> warnings;

  /// Wall clock duration of the migration.
  final Duration elapsed;

  /// Directory holding the untouched original 1.x files.
  ///
  /// The source is always preserved and never deleted.
  final String backupPath;

  /// Whether anything at all needed reporting.
  bool get isClean => entriesSkipped == 0 && warnings.isEmpty;

  @override
  String toString() =>
      'MigrationReport(migrated: $entriesMigrated, skipped: $entriesSkipped, '
      'tombstones: $tombstonesApplied, warnings: ${warnings.length}, '
      'elapsed: $elapsed, backup: $backupPath)';
}

/// Reads ReaxDB 1.x databases and migrates them into ReaxDB 2.x.
///
/// ## What is migrated
///
/// The 1.x durable state lives in two places inside a database directory:
/// `lsm/level_<n>_<timestamp>.sst` and `wal/wal_<timestamp>.wal`. (`btree/`
/// held only a two-field metadata stub — the 1.x B+ tree was never actually
/// persisted — and `indexes/` held secondary index definitions, which are
/// recreated on the migrated database so they are rebuilt from the real data.)
///
/// Records are merged with the exact precedence 1.x used when serving a read:
///
/// 1. SSTables, weakest first: higher LSM levels before lower ones, and
///    inside a level older files before newer ones (1.x searched level 0
///    first and, within a level, the newest table first).
/// 2. The write-ahead log, ascending by sequence number. 1.x replayed the
///    whole WAL into the memtable on open and checked the memtable before any
///    SSTable, so a WAL record always wins over a stored one — including a
///    WAL tombstone.
///
/// ## What cannot be repaired
///
/// Migration recovers what is on disk. It cannot undo damage 1.x already did:
///
/// * 1.x dropped tombstones when flushing a memtable to an SSTable
///   (`MemTable.entries` filtered null values out), so a key deleted before a
///   flush can reappear once its WAL file is gone. If the WAL still records
///   the delete, the delete is honoured and counted in
///   [MigrationReport.tombstonesApplied].
/// * 1.x compaction merged tables in level order rather than in recency
///   order, so a stale value could survive a compaction. Where the same key
///   is found in more than one level, a warning is reported.
/// * The 1.x memtable folded keys longer than 32 bytes into an 8-bit XOR hash
///   before caching their string form, so two long keys with the same fold
///   overwrote each other in memory. Colliding long keys are reported as a
///   warning.
///
/// ## Crash safety
///
/// Migration never leaves a partial 2.x database where an application will
/// open it. Everything that can fail on the input — detection, reading, and
/// resolving the 1.x cipher with the legacy key — runs before anything on
/// disk changes. The 2.x database is then built in the sibling staging
/// directory `<target>.reaxdb-migrating` ([stagingSuffix]), closed, and
/// stamped with the completion marker [completionMarkerName]. Only then is
/// the source moved to its backup (in-place migration) and the staging
/// directory renamed onto the target; the marker is removed last.
///
/// A crash at any point leaves one of a few well-defined states, which
/// [recoverInterrupted] resolves. `ReaxDB.open` calls it before touching the
/// database directory.
///
/// Migration logs and warnings never contain key or value contents: keys
/// are reported through `Redaction.keyBytes`.
abstract final class LegacyMigration {
  /// Name of the LSM subdirectory in a 1.x database.
  static const String lsmDirectoryName = 'lsm';

  /// Name of the B+ tree metadata subdirectory in a 1.x database.
  static const String btreeDirectoryName = 'btree';

  /// Name of the secondary index subdirectory in a 1.x database.
  static const String indexesDirectoryName = 'indexes';

  /// The bookkeeping key `SimpleReaxDB` maintained in ReaxDB 1.x.
  ///
  /// 1.x rewrote the full list of known keys under this key on every put and
  /// delete. It is an artefact of the 1.x simple API, not user data, and 2.x
  /// enumerates keys from the storage engine instead, so migration drops it
  /// (and says so in [MigrationReport.warnings]).
  static const String simpleApiRegistryKey = '__reaxdb_simple_keys__';

  /// Number of live values sampled when probing for the 1.x cipher.
  static const int cipherProbeSampleSize = 64;

  /// Maximum number of warnings collected before they are summarised.
  static const int maxWarnings = 100;

  /// Number of entries written to the new database per batch.
  static const int writeBatchSize = 256;

  /// Suffix of the sibling directory a migration builds the 2.x database in
  /// before it is renamed onto the target.
  static const String stagingSuffix = '.reaxdb-migrating';

  /// Name of the file written inside the staging directory once the 2.x
  /// database there is complete and closed.
  ///
  /// Its presence is what makes a staging directory safe to promote; a
  /// staging directory without it is an unfinished build and is discarded.
  static const String completionMarkerName = '.reaxdb-migration-complete';

  /// Staging directories of migrations running in this isolate, which
  /// [recoverInterrupted] must leave alone.
  static final Set<String> _activeStaging = <String>{};

  /// The staging directory a migration into [targetPath] uses.
  static String stagingPathFor(String targetPath) =>
      '${_absolute(targetPath)}$stagingSuffix';

  static String _absolute(String path) => p.normalize(p.absolute(path));

  /// Returns a description of the 1.x database at [path], or null when [path]
  /// is not a ReaxDB 1.x database.
  ///
  /// Null is returned for a directory that does not exist, is empty, already
  /// holds a 2.x database (it has a `reaxdb.meta` header), or has none of the
  /// 1.x subdirectories.
  static Future<LegacyDatabaseInfo?> detect(String path) async =>
      (await _inspect(path))?.info;

  /// Reads the 1.x database at [path] once, for both [detect] and [migrate].
  static Future<_Inspection?> _inspect(String path) async {
    final Directory directory = Directory(path);
    if (!await directory.exists()) return null;
    if (!await _looksLegacy(path)) return null;

    final List<String> sstableFiles = await _listSstables(path);
    final Directory walDirectory = Directory(
      p.join(path, LegacyWalReader.directoryName),
    );
    final LegacyWalScan wal = await walDirectory.exists()
        ? await LegacyWalReader.read(walDirectory.path)
        : const LegacyWalScan(
            entries: <LegacyWalEntry>[],
            warnings: <String>[],
            fileCount: 0,
            skippedRecords: 0,
            truncatedFiles: <String>[],
          );

    final _MergeResult merged = await _merge(path, sstableFiles, wal);
    final List<Uint8List> sample = <Uint8List>[];
    for (final _MergedRecord record in merged.records.values) {
      if (record.value == null) continue;
      sample.add(record.value!);
      if (sample.length >= 32) break;
    }
    final bool looksEncrypted =
        sample.isNotEmpty &&
        !sample.any((Uint8List v) => LegacyValueCodec.isValid(v));

    return _Inspection(
      LegacyDatabaseInfo(
        path: path,
        detectedVersion: '1.x',
        sstableFileCount: sstableFiles.length,
        walFileCount: wal.fileCount,
        looksEncrypted: looksEncrypted,
        estimatedEntryCount: merged.liveCount,
      ),
      wal.warnings,
      wal.skippedRecords,
      merged,
    );
  }

  /// Whether [path] has the shape of a 1.x database: no 2.x header and at
  /// least one of the 1.x subdirectories. Cheap: reads no records.
  static Future<bool> _looksLegacy(String path) async {
    if (await File(p.join(path, ReaxDB.headerFileName)).exists()) return false;
    return await Directory(p.join(path, lsmDirectoryName)).exists() ||
        await Directory(p.join(path, LegacyWalReader.directoryName)).exists() ||
        await File(p.join(path, btreeDirectoryName, 'btree.meta')).exists();
  }

  /// Reads the 1.x database at [sourcePath] and writes its contents into a
  /// fresh 2.x database at [targetPath].
  ///
  /// [legacyEncryptionKey] is the raw key string that was passed to
  /// `ReaxDB.open(..., encryptionKey: '...')` in 1.x; it is needed only when
  /// the source is encrypted. 1.x stored no marker saying which cipher was
  /// used, so the key is tried against real records as AES-256-GCM and then
  /// as the old `xor` mode, and the mode that decodes more records wins. When
  /// no candidate decodes anything an [EncryptionException] is thrown rather
  /// than importing garbage.
  ///
  /// [encryption] is the configuration of the NEW database. The 1.x key
  /// derivation is never reused to protect migrated data.
  ///
  /// The original files are always preserved: they are moved (when
  /// [targetPath] is [sourcePath]) or copied to `<sourcePath>.1x-backup-<n>`,
  /// where `n` is the first free integer. Nothing is deleted.
  ///
  /// The new database is built in `<targetPath>.reaxdb-migrating` and only
  /// renamed onto [targetPath] once it is complete (see "Crash safety" on
  /// [LegacyMigration]). When this method throws — a wrong or missing legacy
  /// key, an unusable target, an I/O error — the source is left exactly as
  /// it was and nothing remains at [targetPath] or in the staging directory.
  /// The one exception is a failure of the final rename itself, after the
  /// source was moved to its backup; [recoverInterrupted] (run by
  /// `ReaxDB.open`) then completes the migration.
  ///
  /// [onProgress] is called with the number of entries written so far and the
  /// total to write. The total starts at the number of live 1.x keys and
  /// shrinks by one for every record that turns out to be unreadable, so the
  /// final call always reports `migrated == total`.
  ///
  /// Throws [SchemaVersionException] when [sourcePath] is not a 1.x database,
  /// [DatabaseLockedException] when [targetPath] already holds data, and
  /// [EncryptionException] when the source cannot be decrypted.
  static Future<MigrationReport> migrate({
    required String sourcePath,
    required String targetPath,
    String? legacyEncryptionKey,
    EncryptionConfig encryption = const EncryptionConfig.none(),
    void Function(int migrated, int total)? onProgress,
  }) async {
    final Stopwatch stopwatch = Stopwatch()..start();
    final bool inPlace =
        p.canonicalize(sourcePath) == p.canonicalize(targetPath);

    // Resolve whatever an earlier, interrupted migration into this target
    // left behind before deciding anything about the target.
    await recoverInterrupted(targetPath);

    // --- Phase 1: everything that can fail on the input. Nothing on disk
    // changes until all of it has succeeded.
    final _Inspection? inspection = await _inspect(sourcePath);
    if (inspection == null) {
      throw SchemaVersionException(
        'The directory "$sourcePath" is not a ReaxDB 1.x database. A 1.x '
        'database contains an "lsm" and/or "wal" subdirectory and no '
        '"${ReaxDB.headerFileName}" header.',
      );
    }
    if (!inPlace) await _ensureUsableTarget(targetPath);

    final List<String> warnings = <String>[];
    int skipped = 0;
    _collect(warnings, inspection.walWarnings);
    skipped += inspection.walSkippedRecords;
    final _MergeResult merged = inspection.merged;
    _collect(warnings, merged.warnings);
    skipped += merged.skippedRecords;

    final LegacyDecryptor decryptor = _resolveDecryptor(
      merged,
      legacyEncryptionKey,
      warnings,
    );

    final String backupPath = await _reserveBackupPath(sourcePath);
    final String staging = stagingPathFor(targetPath);
    final DatabaseLockedException busy = DatabaseLockedException(
      'Another migration into "$targetPath" is in progress: its staging '
      'directory "$staging" exists.',
      path: targetPath,
    );
    // Claimed synchronously so two migrations in this isolate cannot race.
    if (!_activeStaging.add(staging)) throw busy;
    if (await FileSystemEntity.type(staging, followLinks: false) !=
        FileSystemEntityType.notFound) {
      _activeStaging.remove(staging);
      throw busy;
    }

    // --- Phase 2: build the complete 2.x database in the staging directory.
    final _WriteOutcome outcome;
    bool backupCreated = false;
    try {
      outcome = await _writeStaging(
        staging: staging,
        sourcePath: sourcePath,
        merged: merged,
        decryptor: decryptor,
        encryption: encryption,
        warnings: warnings,
        onProgress: onProgress,
      );
      if (!inPlace) {
        backupCreated = true;
        await _copyDirectory(Directory(sourcePath), Directory(backupPath));
      }
      await _writeCompletionMarker(
        staging,
        sourcePath: sourcePath,
        backupPath: backupPath,
        inPlace: inPlace,
      );

      // --- Phase 3: promote. The staging directory is complete, so from
      // here on recoverInterrupted can finish the job after a crash.
      if (inPlace) {
        await Directory(sourcePath).rename(backupPath);
      } else {
        await _clearEmptyTarget(targetPath);
      }
    } catch (_) {
      // The source is untouched: discard what this call created.
      await _deleteIfPresent(staging);
      if (backupCreated) await _deleteIfPresent(backupPath);
      _activeStaging.remove(staging);
      rethrow;
    }
    try {
      await Directory(staging).rename(_absolute(targetPath));
    } finally {
      _activeStaging.remove(staging);
    }
    await _deleteIfPresent(p.join(targetPath, completionMarkerName));

    stopwatch.stop();
    return MigrationReport(
      entriesMigrated: outcome.migrated,
      entriesSkipped: skipped + outcome.skipped,
      tombstonesApplied: outcome.tombstones,
      warnings: List<String>.unmodifiable(warnings),
      elapsed: stopwatch.elapsed,
      backupPath: backupPath,
    );
  }

  /// Decodes every live merged record and writes it, in batches, into a new
  /// 2.x database at [staging], then recreates the 1.x indexes and closes it.
  ///
  /// Records are decoded one batch at a time and removed from [merged] as
  /// they are written, so the decoded values of the whole database are never
  /// held in memory at once.
  static Future<_WriteOutcome> _writeStaging({
    required String staging,
    required String sourcePath,
    required _MergeResult merged,
    required LegacyDecryptor decryptor,
    required EncryptionConfig encryption,
    required List<String> warnings,
    required void Function(int migrated, int total)? onProgress,
  }) async {
    final Map<ByteKey, _MergedRecord> records = merged.records;
    final ByteKey registryKey = ByteKey(
      Uint8List.fromList(simpleApiRegistryKey.codeUnits),
    );
    final bool registryPresent = records[registryKey]?.value != null;
    int total = merged.liveCount - (registryPresent ? 1 : 0);
    int skipped = 0;
    int tombstones = 0;
    int migrated = 0;

    final ReaxDB target = await ReaxDB.open(
      path: staging,
      encryption: encryption,
    );
    try {
      final Map<String, Object?> pending = <String, Object?>{};
      Future<void> flushPending() async {
        if (pending.isEmpty) return;
        await target.putBatch(Map<String, Object?>.of(pending));
        migrated += pending.length;
        pending.clear();
        onProgress?.call(migrated, total);
      }

      for (final ByteKey byteKey in records.keys.toList(growable: false)) {
        final _MergedRecord record = records.remove(byteKey)!;
        final Uint8List? stored = record.value;
        if (stored == null) {
          tombstones++;
          continue;
        }
        final Uint8List keyBytes = byteKey.bytes;
        if (byteKey == registryKey) continue;

        String? problem;
        Object? value;
        if (keyBytes.isEmpty) {
          problem = 'A record with an empty key cannot be migrated';
        } else if (keyBytes[0] == 0x00) {
          problem =
              'Key ${Redaction.keyBytes(keyBytes)} starts with the reserved '
              'byte 0x00 and cannot be migrated to ReaxDB 2.x; skipped';
        } else {
          try {
            value = LegacyValueCodec.decode(decryptor.decrypt(stored));
          } on EncryptionException catch (error) {
            problem =
                'Value for key ${Redaction.keyBytes(keyBytes)} failed to '
                'decrypt (${error.message}); skipped';
          } on SerializationException catch (error) {
            problem =
                'Value for key ${Redaction.keyBytes(keyBytes)} is not a '
                'readable ReaxDB 1.x value (${error.message}); skipped';
          }
        }
        if (problem != null) {
          skipped++;
          total--;
          _addWarning(warnings, problem);
          continue;
        }
        if (keyBytes.any((int b) => b >= 0x80)) {
          _addWarning(
            warnings,
            'Key ${Redaction.keyBytes(keyBytes)} contains bytes above 0x7F. '
            'ReaxDB 1.x stored keys as UTF-16 code units truncated to 8 bits, '
            'so the original key string may not be recoverable; it was '
            'migrated as read.',
          );
        }
        pending[String.fromCharCodes(keyBytes)] = value;
        if (pending.length >= writeBatchSize) await flushPending();
      }
      await flushPending();
      onProgress?.call(migrated, total);

      if (registryPresent) {
        warnings.add(
          'The ReaxDB 1.x simple API bookkeeping key "$simpleApiRegistryKey" '
          'was found and not migrated: it is a 1.x key registry, not user '
          'data. ReaxDB 2.x enumerates keys from the storage engine, so the '
          'registry is unnecessary.',
        );
      }
      await _recreateIndexes(sourcePath, target, warnings);
      await target.flush();
    } finally {
      await target.close();
    }
    return _WriteOutcome(migrated, skipped, tombstones);
  }

  // -- interrupted migrations ---------------------------------------------

  /// Resolves the on-disk state an interrupted migration into [path] left
  /// behind. Call it before opening or creating the database at [path];
  /// `ReaxDB.open` does.
  ///
  /// Costs two `stat` calls when there is nothing to do. Otherwise, with
  /// `staging` = `<path>.reaxdb-migrating`:
  ///
  /// | staging            | [path]                    | action                 |
  /// |--------------------|---------------------------|------------------------|
  /// | absent             | any                       | remove a stray marker  |
  /// | without marker     | any                       | delete staging         |
  /// | complete (marker)  | missing or empty dir      | rename staging → path  |
  /// | complete (marker)  | still a 1.x database      | delete staging         |
  /// | complete (marker)  | anything else             | throw                  |
  ///
  /// * A staging directory without the completion marker is an unfinished
  ///   build. The source was not touched before the marker was written, so
  ///   discarding it loses nothing and the migration can simply run again.
  /// * A complete staging directory with [path] missing means the crash hit
  ///   between moving the source to its backup and promoting the staging
  ///   directory: the promotion is finished. (An empty directory at [path]
  ///   is treated the same way.)
  /// * A complete staging directory next to a 1.x database at [path] means
  ///   the crash hit before the source was moved aside. The source is intact
  ///   and authoritative — it may even have been written by 1.x since — so
  ///   the staged copy is discarded and the migration runs again on demand.
  /// * In every other case (for example a 2.x database at [path] created by
  ///   an older release that did not recover) both directories may hold
  ///   data and choosing either could lose some, so a
  ///   [DatabaseLockedException] naming both is thrown and nothing is
  ///   changed. Move one of them away to resolve it.
  ///
  /// The marker left inside [path] by a crash right after the final rename
  /// is removed. Staging directories of migrations running in this isolate
  /// are left alone; recovery is not coordinated across processes, so do not
  /// open a database while another process is migrating into it.
  ///
  /// [path] must be spelled the way it was given to [migrate] as the target
  /// (relative paths resolve against the current directory); a target whose
  /// last component is a symbolic link is not supported.
  static Future<void> recoverInterrupted(String path) async {
    final String target = _absolute(path);
    final String staging = stagingPathFor(path);
    final FileSystemEntityType stagingType = await FileSystemEntity.type(
      staging,
      followLinks: false,
    );
    if (stagingType == FileSystemEntityType.notFound) {
      await _deleteIfPresent(p.join(target, completionMarkerName));
      return;
    }
    if (_activeStaging.contains(staging)) return;
    if (stagingType != FileSystemEntityType.directory) {
      throw DatabaseLockedException(
        '"$staging" exists but is not a directory, so it is not a ReaxDB '
        'migration staging directory. Remove or rename it before opening '
        '"$target".',
        path: target,
      );
    }

    if (!await File(p.join(staging, completionMarkerName)).exists()) {
      await Directory(staging).delete(recursive: true);
      return;
    }

    final FileSystemEntityType targetType = await FileSystemEntity.type(target);
    if (targetType == FileSystemEntityType.directory &&
        await _isEmptyDirectory(target)) {
      await Directory(target).delete();
    } else if (targetType == FileSystemEntityType.directory &&
        await _looksLegacy(target)) {
      await Directory(staging).delete(recursive: true);
      return;
    } else if (targetType != FileSystemEntityType.notFound) {
      throw DatabaseLockedException(
        'An interrupted ReaxDB 1.x migration left a complete migrated '
        'database in "$staging", but "$target" also exists and is not the '
        'original 1.x database. ReaxDB will not choose between them because '
        'either may hold data the other lacks. Move "$target" away to keep '
        'the migrated database, or delete "$staging" to keep "$target".',
        path: target,
      );
    }
    await Directory(staging).rename(target);
    await _deleteIfPresent(p.join(target, completionMarkerName));
  }

  static Future<void> _writeCompletionMarker(
    String staging, {
    required String sourcePath,
    required String backupPath,
    required bool inPlace,
  }) async {
    final File marker = File(p.join(staging, completionMarkerName));
    // Diagnostic only: recovery relies on the marker's presence, not on its
    // contents. Paths only; no key or value contents.
    await marker.writeAsString(
      jsonEncode(<String, Object?>{
        'format': 1,
        'source': _absolute(sourcePath),
        'backup': _absolute(backupPath),
        'inPlace': inPlace,
        'completedAt': DateTime.now().toUtc().toIso8601String(),
      }),
      flush: true,
    );
  }

  static Future<bool> _isEmptyDirectory(String path) =>
      Directory(path).list().isEmpty;

  /// Removes [path] when it is an empty directory, so the staging directory
  /// can be renamed onto it on every platform. Throws when it has content.
  static Future<void> _clearEmptyTarget(String path) async {
    final Directory directory = Directory(path);
    if (!await directory.exists()) return;
    if (!await _isEmptyDirectory(path)) {
      throw DatabaseLockedException(
        'The migration target "$path" was written to while the migration was '
        'running; the migrated database was discarded.',
        path: path,
      );
    }
    await directory.delete();
  }

  static Future<void> _deleteIfPresent(String path) async {
    switch (await FileSystemEntity.type(path, followLinks: false)) {
      case FileSystemEntityType.notFound:
        return;
      case FileSystemEntityType.directory:
        await Directory(path).delete(recursive: true);
      case FileSystemEntityType.link:
        await Link(path).delete();
      default:
        await File(path).delete();
    }
  }

  // -- merging ------------------------------------------------------------

  static Future<List<String>> _listSstables(String databasePath) async {
    final Directory directory = Directory(
      p.join(databasePath, lsmDirectoryName),
    );
    if (!await directory.exists()) return <String>[];
    final List<String> files =
        (await directory.list().toList())
            .whereType<File>()
            .map((File f) => f.path)
            .where((String f) => f.endsWith(LegacySSTableReader.extension))
            .toList()
          ..sort();
    return files;
  }

  /// Merges every SSTable level and the WAL with 1.x read precedence.
  ///
  /// Tables are ordered by the level and timestamp in their file names and
  /// read one at a time, so only the winning value of each key and a single
  /// table are in memory at once.
  static Future<_MergeResult> _merge(
    String databasePath,
    List<String> sstableFiles,
    LegacyWalScan wal,
  ) async {
    final List<String> warnings = <String>[];
    int skipped = 0;

    // 1.x searched level 0 first and, inside a level, the newest table first.
    // Applying the weakest source first and letting later writes win
    // reproduces that precedence.
    final List<String> ordered = List<String>.of(sstableFiles)
      ..sort((String a, String b) {
        final ({int level, DateTime createdAt}) x =
            LegacySSTableReader.identify(a);
        final ({int level, DateTime createdAt}) y =
            LegacySSTableReader.identify(b);
        final int byLevel = y.level.compareTo(x.level);
        if (byLevel != 0) return byLevel;
        final int byAge = x.createdAt.compareTo(y.createdAt);
        if (byAge != 0) return byAge;
        return a.compareTo(b);
      });

    final Map<ByteKey, _MergedRecord> records = <ByteKey, _MergedRecord>{};
    final Map<ByteKey, int> seenInLevel = <ByteKey, int>{};
    // Reported after every file-level warning, as when all tables were read
    // up front, so a flood of these cannot crowd out damage reports.
    final List<String> levelWarnings = <String>[];
    int withoutIndex = 0;

    for (final String file in ordered) {
      final LegacySSTable table = await LegacySSTableReader.read(file);
      if (!table.indexTrailerReadable) withoutIndex++;
      // A missing index trailer is summarised once below instead of once per
      // file, because it says the same thing about the whole database.
      _collect(
        warnings,
        table.warnings
            .where((String w) => !w.contains('no readable key index trailer'))
            .toList(),
      );
      skipped += table.skippedRecords;

      for (final LegacySSTableRecord record in table.records) {
        final ByteKey key = ByteKey(record.key);
        final int? previousLevel = seenInLevel[key];
        if (previousLevel != null && previousLevel != table.level) {
          _addWarning(
            levelWarnings,
            'Key ${Redaction.keyBytes(record.key)} exists in LSM level '
            '$previousLevel and level ${table.level}. ReaxDB 1.x compaction '
            'merged levels without a recency order, so one of the copies may '
            'be stale; the copy 1.x would have served was kept.',
          );
        }
        seenInLevel[key] = table.level;
        records[key] = _MergedRecord(record.value, 'lsm level ${table.level}');
      }
    }
    if (withoutIndex > 0) {
      warnings.add(
        '$withoutIndex of ${ordered.length} SSTable file(s) have no readable '
        'key index trailer, so their records could only be recovered by '
        'scanning them and nothing cross-checked the result.',
      );
    }
    _collect(warnings, levelWarnings);

    for (final LegacyWalEntry entry in wal.entries) {
      switch (entry.type) {
        case LegacyWalEntryType.checkpoint:
          continue;
        case LegacyWalEntryType.put:
          records[ByteKey(entry.key)] = _MergedRecord(
            entry.value,
            'wal ${entry.sourceFile}',
          );
        case LegacyWalEntryType.delete:
          records[ByteKey(entry.key)] = _MergedRecord(
            null,
            'wal ${entry.sourceFile}',
          );
      }
    }

    _warnAboutLongKeyCollisions(records.keys, warnings);

    int live = 0;
    int liveFromSstableOnly = 0;
    for (final _MergedRecord record in records.values) {
      if (record.value == null) continue;
      live++;
      if (record.source.startsWith('lsm')) liveFromSstableOnly++;
    }
    if (liveFromSstableOnly > 0) {
      warnings.add(
        '$liveFromSstableOnly live key(s) were recovered only from SSTable '
        'files. ReaxDB 1.4.1 wrote the SSTable key index length before the '
        'index instead of after it, so after a restart 1.x could not read its '
        'own SSTables and would have returned null for these keys. They were '
        'migrated because the bytes are intact.',
      );
    }

    return _MergeResult(records, warnings, skipped, live);
  }

  /// Reports keys that hit the 1.x memtable key-cache collision.
  ///
  /// `MemTable._keyToStringOptimized` cached the string form of keys longer
  /// than 32 bytes under an 8-bit XOR fold of their bytes. Two long keys with
  /// the same fold shared a cache slot, so the second key's value was written
  /// under the first key's string. The bytes on disk are authoritative and
  /// migrated as they are, but the in-memory view 1.x served was wrong.
  static void _warnAboutLongKeyCollisions(
    Iterable<ByteKey> keys,
    List<String> warnings,
  ) {
    final Map<int, String> folds = <int, String>{};
    for (final ByteKey key in keys) {
      if (key.bytes.length <= 32) continue;
      int fold = 0;
      for (final int b in key.bytes) {
        fold ^= b;
      }
      final String redacted = Redaction.keyBytes(key.bytes);
      final String? previous = folds[fold];
      if (previous != null && previous != redacted) {
        _addWarning(
          warnings,
          'Keys $previous and $redacted are longer than 32 bytes and '
          'share the ReaxDB 1.x memtable key-cache fold ($fold). 1.x could '
          'serve one under the other; the bytes on disk were migrated '
          'unchanged.',
        );
      } else {
        folds[fold] = redacted;
      }
    }
  }

  // -- encryption ---------------------------------------------------------

  static LegacyDecryptor _resolveDecryptor(
    _MergeResult merged,
    String? legacyEncryptionKey,
    List<String> warnings,
  ) {
    final List<Uint8List> sample = <Uint8List>[];
    for (final _MergedRecord record in merged.records.values) {
      if (record.value == null) continue;
      sample.add(record.value!);
      if (sample.length >= cipherProbeSampleSize) break;
    }
    if (sample.isEmpty) return LegacyDecryptor.none();

    final List<LegacyDecryptor> candidates = <LegacyDecryptor>[
      if (legacyEncryptionKey != null &&
          legacyEncryptionKey.isNotEmpty) ...<LegacyDecryptor>[
        LegacyDecryptor.aes256(legacyEncryptionKey),
        LegacyDecryptor.xor(legacyEncryptionKey),
      ],
      LegacyDecryptor.none(),
    ];

    LegacyDecryptor? best;
    int bestHits = -1;
    for (final LegacyDecryptor candidate in candidates) {
      int hits = 0;
      for (final Uint8List value in sample) {
        try {
          if (LegacyValueCodec.isValid(candidate.decrypt(value))) hits++;
        } on EncryptionException {
          // A candidate that cannot even decrypt scores nothing here.
        }
      }
      if (hits > bestHits) {
        bestHits = hits;
        best = candidate;
      }
      if (hits == sample.length) break;
    }

    if (bestHits <= 0) {
      if (legacyEncryptionKey == null || legacyEncryptionKey.isEmpty) {
        throw EncryptionException(
          'None of the ${sample.length} sampled records is a readable ReaxDB '
          '1.x value, so the source database is encrypted. Pass the original '
          'key as legacyEncryptionKey.',
        );
      }
      throw EncryptionException(
        'The supplied legacy encryption key does not decrypt this ReaxDB 1.x '
        'database: none of the ${sample.length} sampled records could be '
        'read as AES-256-GCM or as the 1.x xor mode.',
      );
    }

    if (best!.mode == LegacyEncryptionMode.none &&
        legacyEncryptionKey != null &&
        legacyEncryptionKey.isNotEmpty) {
      warnings.add(
        'The source database is not encrypted; legacyEncryptionKey was '
        'ignored.',
      );
    }
    if (bestHits < sample.length) {
      warnings.add(
        'Only $bestHits of ${sample.length} sampled records decoded with the '
        '${best.mode.name} cipher; the rest of the database may be damaged '
        'and unreadable records are reported individually.',
      );
    }
    return best;
  }

  // -- indexes ------------------------------------------------------------

  /// Recreates the 1.x secondary index definitions on the migrated database.
  ///
  /// 1.x stored one directory per index named `<collection>_<field>` under
  /// `indexes/`, and derived the collection from the text before the first
  /// underscore. The 2.x index is built from the migrated data, so no 1.x
  /// index content is trusted.
  static Future<void> _recreateIndexes(
    String sourcePath,
    ReaxDB target,
    List<String> warnings,
  ) async {
    final Directory directory = Directory(
      p.join(sourcePath, indexesDirectoryName),
    );
    if (!await directory.exists()) return;
    for (final FileSystemEntity entity in await directory.list().toList()) {
      if (entity is! Directory) continue;
      final String name = p.basename(entity.path);
      final int separator = name.indexOf('_');
      if (separator <= 0 || separator == name.length - 1) {
        _addWarning(
          warnings,
          'Index directory "$name" does not follow the 1.x '
          '<collection>_<field> naming; no index was recreated for it',
        );
        continue;
      }
      final String collection = name.substring(0, separator);
      final String field = name.substring(separator + 1);
      try {
        await target.createIndex(collection, <String>[field]);
      } on ReaxDbException catch (error) {
        _addWarning(
          warnings,
          'Could not recreate the 1.x index on $collection.$field '
          '(${error.message}); create it manually with createIndex()',
        );
      }
    }
  }

  // -- backup -------------------------------------------------------------

  static Future<String> _reserveBackupPath(String sourcePath) async {
    final String base = '${p.normalize(sourcePath)}.1x-backup';
    for (int n = 1; n < 10000; n++) {
      final String candidate = '$base-$n';
      if (!await Directory(candidate).exists() &&
          !await File(candidate).exists()) {
        return candidate;
      }
    }
    throw DatabaseLockedException(
      'Could not reserve a backup directory next to "$sourcePath": 9999 '
      'candidates named "$base-<n>" already exist',
      path: sourcePath,
    );
  }

  static Future<void> _ensureUsableTarget(String targetPath) async {
    final Directory directory = Directory(targetPath);
    if (!await directory.exists()) return;
    final List<FileSystemEntity> contents = await directory.list().toList();
    if (contents.isEmpty) return;
    throw DatabaseLockedException(
      'The migration target "$targetPath" is not empty. Migrate into a fresh '
      'directory, or pass the source path as the target to migrate in place '
      '(the originals are moved to a backup directory first).',
      path: targetPath,
    );
  }

  static Future<void> _copyDirectory(
    Directory source,
    Directory destination,
  ) async {
    await destination.create(recursive: true);
    for (final FileSystemEntity entity in await source.list().toList()) {
      final String name = p.basename(entity.path);
      if (entity is Directory) {
        await _copyDirectory(entity, Directory(p.join(destination.path, name)));
      } else if (entity is File) {
        await entity.copy(p.join(destination.path, name));
      }
    }
  }

  // -- warnings -----------------------------------------------------------

  static void _collect(List<String> into, List<String> from) {
    for (final String warning in from) {
      _addWarning(into, warning);
    }
  }

  static void _addWarning(List<String> warnings, String warning) {
    if (warnings.length < maxWarnings) {
      warnings.add(warning);
    } else if (warnings.length == maxWarnings) {
      warnings.add(
        'More than $maxWarnings anomalies were found; further warnings were '
        'suppressed.',
      );
    }
  }
}

final class _MergedRecord {
  const _MergedRecord(this.value, this.source);

  /// Raw stored bytes, or null when the newest record is a tombstone.
  final Uint8List? value;

  /// Where the winning record came from, for diagnostics.
  final String source;
}

final class _Inspection {
  const _Inspection(
    this.info,
    this.walWarnings,
    this.walSkippedRecords,
    this.merged,
  );

  final LegacyDatabaseInfo info;
  final List<String> walWarnings;
  final int walSkippedRecords;
  final _MergeResult merged;
}

final class _WriteOutcome {
  const _WriteOutcome(this.migrated, this.skipped, this.tombstones);

  final int migrated;
  final int skipped;
  final int tombstones;
}

final class _MergeResult {
  const _MergeResult(
    this.records,
    this.warnings,
    this.skippedRecords,
    this.liveCount,
  );

  final Map<ByteKey, _MergedRecord> records;
  final List<String> warnings;
  final int skippedRecords;
  final int liveCount;
}
