/// Crash-atomic manifest recording LSM level membership.
///
/// The manifest is the single source of truth for which SSTable files belong
/// to which level. It is replaced atomically (write temp, fsync, rename), so
/// a crash mid-update leaves the previous manifest intact. SSTable files on
/// disk that are not referenced by the manifest are garbage from an
/// interrupted flush or compaction and are deleted on open.
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../errors/exceptions.dart';
import '../platform/native_io.dart';
import '../util/record_codec.dart';
import '../util/varint.dart';
import 'io_faults.dart';

/// Immutable snapshot of LSM tree membership.
final class ManifestState {
  /// Creates a manifest state.
  const ManifestState({required this.nextTableId, required this.levels});

  /// Empty state for a fresh database.
  const ManifestState.initial() : this(nextTableId: 1, levels: const []);

  /// Next unused SSTable id.
  final int nextTableId;

  /// SSTable ids per level, index 0 = L0. L0 ids are ordered oldest to
  /// newest; deeper levels are ordered by key range.
  final List<List<int>> levels;

  /// Returns a copy with the given replacements.
  ManifestState copyWith({int? nextTableId, List<List<int>>? levels}) {
    return ManifestState(
      nextTableId: nextTableId ?? this.nextTableId,
      levels: levels ?? this.levels,
    );
  }

  /// Every table id referenced by any level.
  Set<int> get allTableIds => {for (final l in levels) ...l};
}

/// Durable, crash-atomic store for [ManifestState].
final class Manifest {
  Manifest._(this._directory, this._state);

  /// File-format magic: "RXM1" little-endian.
  static const int magic = 0x314D5852;

  /// File-format version.
  static const int formatVersion = 1;

  static const String _fileName = 'MANIFEST';

  final String _directory;
  ManifestState _state;

  /// The current state.
  ManifestState get state => _state;

  /// Canonical file name for the SSTable with [id].
  static String tableFileName(int id) =>
      'sst-${id.toString().padLeft(12, '0')}.sst';

  /// Opens the manifest in [directory], creating an initial one if missing.
  ///
  /// A leftover `MANIFEST.tmp` from an interrupted update is discarded.
  /// Throws [CorruptionException] if an existing manifest fails validation.
  static Future<Manifest> open(String directory) async {
    await Directory(directory).create(recursive: true);
    final tmp = File(p.join(directory, '$_fileName.tmp'));
    if (await tmp.exists()) await tmp.delete();
    final file = File(p.join(directory, _fileName));
    if (!await file.exists()) {
      final m = Manifest._(directory, const ManifestState.initial());
      await m.update(m._state);
      return m;
    }
    final bytes = await file.readAsBytes();
    final start = RecordCodec.readFileHeader(
      bytes,
      magic,
      formatVersion,
      path: file.path,
    );
    final reader = RecordReader(bytes, start);
    final r = reader.next();
    if (r.status != RecordReadStatus.ok || reader.offset != bytes.length) {
      throw CorruptionException(
        'Manifest record failed validation',
        path: file.path,
        offset: r.recordOffset,
      );
    }
    return Manifest._(directory, _decode(r.payload!, file.path));
  }

  static ManifestState _decode(Uint8List payload, String path) {
    try {
      var pos = 0;
      final (nextId, n0) = Varint.read(payload, pos);
      pos += n0;
      final (levelCount, n1) = Varint.read(payload, pos);
      pos += n1;
      final levels = <List<int>>[];
      for (var l = 0; l < levelCount; l++) {
        final (tableCount, a) = Varint.read(payload, pos);
        pos += a;
        final ids = <int>[];
        for (var t = 0; t < tableCount; t++) {
          final (id, b) = Varint.read(payload, pos);
          pos += b;
          ids.add(id);
        }
        levels.add(ids);
      }
      if (pos != payload.length) {
        throw CorruptionException(
          'Manifest payload has trailing bytes',
          path: path,
        );
      }
      return ManifestState(nextTableId: nextId, levels: levels);
    } on CorruptionException {
      rethrow;
    } catch (e) {
      throw CorruptionException('Malformed manifest', path: path, cause: e);
    }
  }

  /// Atomically replaces the on-disk manifest with [newState].
  ///
  /// The new content is fully written and fsynced to a temporary file before
  /// being renamed over the old manifest. On failure the temporary file is
  /// removed, the previous manifest stays in force and [state] is unchanged.
  Future<void> update(ManifestState newState) async {
    final payload = BytesBuilder(copy: false);
    Varint.write(payload, newState.nextTableId);
    Varint.write(payload, newState.levels.length);
    for (final level in newState.levels) {
      Varint.write(payload, level.length);
      for (final id in level) {
        Varint.write(payload, id);
      }
    }
    final buf = BytesBuilder(copy: false);
    RecordCodec.writeFileHeader(buf, magic, formatVersion);
    RecordCodec.writeRecord(buf, payload.takeBytes());

    final tmpPath = p.join(_directory, '$_fileName.tmp');
    try {
      final raf = await File(tmpPath).open(mode: FileMode.write);
      try {
        await IoFaults.write(raf, buf.takeBytes(), IoOp.manifestWrite, tmpPath);
        await raf.flush();
      } finally {
        await raf.close();
      }
      IoFaults.check(IoOp.manifestRename, tmpPath);
      // The rename is atomic; fsyncing the directory makes it durable before
      // the caller checkpoints the WAL (no-op where unavailable, see
      // [syncDirectory]).
      await File(tmpPath).rename(p.join(_directory, _fileName));
      syncDirectory(_directory);
    } catch (_) {
      await _deleteQuietly(tmpPath);
      rethrow;
    }
    _state = newState;
  }

  static Future<void> _deleteQuietly(String path) async {
    try {
      final f = File(path);
      if (await f.exists()) await f.delete();
    } catch (_) {
      // Best effort: a leftover temp file is discarded on the next open.
    }
  }
}
