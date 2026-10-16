/// Leveled LSM tree over immutable SSTables.
///
/// Level 0 holds memtable flushes whose key ranges may overlap; newer L0
/// tables shadow older ones. Levels 1 and deeper hold non-overlapping tables
/// ordered by key range. Compaction merges a level's tables with the
/// overlapping tables of the next level; tombstones are dropped only when
/// writing into the bottom level.
///
/// All membership changes go through the crash-atomic [Manifest] and follow
/// one protocol: new tables are written under fresh ids, the new membership
/// is built as a copy, the manifest is published, and only then does the
/// in-memory view switch over. A failed publish deletes the new files and
/// leaves both the manifest and the in-memory view untouched. Table ids come
/// from an in-memory counter that never goes backwards, even when a publish
/// fails, so an id is never reused while a file with that id may exist.
///
/// Readers pin the current *version* (an immutable membership snapshot) for
/// the duration of a lookup or scan. A table that a compaction removed from
/// the manifest is closed and its file deleted as soon as no pinned version
/// contains it; leftovers from a crash are removed as orphans on the next
/// open.
///
/// Compaction policy: L0 compacts when it has [LsmTree.l0CompactionTrigger]
/// tables; level N (N >= 1) compacts when its total bytes exceed
/// `baseLevelTargetBytes * levelSizeMultiplier^(N-1)`. Merged output is split
/// into tables of about [LsmTree.targetTableBytes].
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../errors/exceptions.dart';
import '../util/byte_key.dart';
import 'manifest.dart';
import 'sstable.dart';

final class _TableHandle {
  _TableHandle(this.id, this.table);

  final int id;
  final SSTable table;

  /// Number of versions that contain this table.
  int refs = 0;
}

/// Immutable membership snapshot. `levels[0]` is L0 ordered oldest to
/// newest; deeper levels are ordered by ascending minimum key and never
/// overlap internally. The lists are never mutated once installed.
final class _Version {
  _Version(this.levels);

  final List<List<_TableHandle>> levels;

  /// Pins held by the tree (while current) and by in-flight readers.
  int refs = 0;

  Iterable<_TableHandle> get tables sync* {
    for (final level in levels) {
      yield* level;
    }
  }
}

/// Leveled collection of SSTables with crash-atomic membership.
final class LsmTree {
  LsmTree._(
    this._directory,
    this._manifest,
    List<List<_TableHandle>> levels,
    this._nextTableId, {
    required this.maxLevels,
    required this.l0CompactionTrigger,
    required this.baseLevelTargetBytes,
    required this.levelSizeMultiplier,
    required this.targetTableBytes,
  }) : _current = _Version(levels) {
    _current.refs = 1;
    for (final h in _current.tables) {
      h.refs++;
    }
  }

  final String _directory;
  final Manifest _manifest;

  _Version _current;

  /// Next table id to hand out. Only ever increases; see the library docs.
  int _nextTableId;

  /// Deepest allowed level index + 1.
  final int maxLevels;

  /// Number of L0 tables that triggers compaction into L1.
  final int l0CompactionTrigger;

  /// Size target for L1; deeper levels multiply by [levelSizeMultiplier].
  final int baseLevelTargetBytes;

  /// Growth factor between consecutive level size targets.
  final int levelSizeMultiplier;

  /// Approximate maximum size of a single compacted table.
  final int targetTableBytes;

  /// Tables removed from the manifest but still pinned by a reader.
  final Set<_TableHandle> _obsolete = Set.identity();

  /// Deletions of obsolete tables that have not finished yet.
  final Set<Future<void>> _pendingDeletes = {};

  /// Serializes membership changes (flushes and compactions).
  Future<void> _mutationLock = Future<void>.value();

  bool _closed = false;

  /// Opens the tree stored in [directory].
  ///
  /// Tables referenced by the manifest are opened and validated; `.sst` and
  /// `.sst.tmp` files not referenced by the manifest (leftovers of an
  /// interrupted flush or compaction) are deleted.
  static Future<LsmTree> open({
    required String directory,
    int maxLevels = 4,
    int l0CompactionTrigger = 4,
    int baseLevelTargetBytes = 8 * 1024 * 1024,
    int levelSizeMultiplier = 10,
    int targetTableBytes = 2 * 1024 * 1024,
  }) async {
    await Directory(directory).create(recursive: true);
    final manifest = await Manifest.open(directory);
    final referenced = manifest.state.allTableIds;
    var nextId = manifest.state.nextTableId;
    for (final id in referenced) {
      if (id >= nextId) nextId = id + 1;
    }

    // Remove orphans before opening anything. Their ids still count towards
    // the id counter, so a new table never reuses a name seen on disk.
    await for (final e in Directory(directory).list()) {
      if (e is! File) continue;
      final name = p.basename(e.path);
      if (name.endsWith('.sst.tmp')) {
        final id = _tableIdOf(name.substring(0, name.length - 4));
        if (id != null && id >= nextId) nextId = id + 1;
        await e.delete();
      } else if (name.endsWith('.sst')) {
        final id = _tableIdOf(name);
        if (id != null && id >= nextId) nextId = id + 1;
        if (id == null || !referenced.contains(id)) await e.delete();
      }
    }

    final levels = <List<_TableHandle>>[];
    try {
      for (final levelIds in manifest.state.levels) {
        final handles = <_TableHandle>[];
        levels.add(handles);
        for (final id in levelIds) {
          final table = await SSTable.open(
            p.join(directory, Manifest.tableFileName(id)),
          );
          handles.add(_TableHandle(id, table));
        }
      }
    } catch (_) {
      for (final level in levels) {
        for (final h in level) {
          await h.table.close();
        }
      }
      rethrow;
    }
    return LsmTree._(
      directory,
      manifest,
      levels,
      nextId,
      maxLevels: maxLevels,
      l0CompactionTrigger: l0CompactionTrigger,
      baseLevelTargetBytes: baseLevelTargetBytes,
      levelSizeMultiplier: levelSizeMultiplier,
      targetTableBytes: targetTableBytes,
    );
  }

  static int? _tableIdOf(String name) {
    if (!name.startsWith('sst-') || !name.endsWith('.sst')) return null;
    return int.tryParse(name.substring(4, name.length - 4));
  }

  void _ensureOpen() {
    if (_closed) throw const DatabaseClosedException('LSM tree is closed');
  }

  /// Number of tables per level.
  List<int> get levelTableCounts => [for (final l in _current.levels) l.length];

  /// Total number of tables.
  int get tableCount =>
      _current.levels.fold(0, (sum, level) => sum + level.length);

  /// Total bytes across all live tables.
  int get totalSizeBytes => _current.levels.fold(
    0,
    (sum, level) => sum + level.fold(0, (s, h) => s + h.table.fileSizeBytes),
  );

  /// Tables already removed from the manifest whose files are kept only
  /// because an in-progress read still uses them.
  int get obsoleteTableCount => _obsolete.length;

  /// Next table id that will be assigned.
  int get nextTableId => _nextTableId;

  // -- versions -----------------------------------------------------------

  _Version _pin() {
    final v = _current;
    v.refs++;
    return v;
  }

  void _unpin(_Version v) {
    if (--v.refs > 0) return;
    for (final h in v.tables) {
      if (--h.refs == 0) _retire(h);
    }
  }

  /// Closes and deletes a table that no version references any more.
  ///
  /// The manifest that dropped the table was renamed into place and its
  /// directory fsynced before this runs, so a crash cannot bring back a
  /// manifest naming the deleted file.
  void _retire(_TableHandle h) {
    if (_closed) return; // close() handles whatever is left.
    late final Future<void> done;
    done = () async {
      try {
        await h.table.close();
        final f = File(h.table.path);
        if (await f.exists()) await f.delete();
        _obsolete.remove(h);
      } catch (_) {
        // Best effort: an undeleted file is an orphan removed on next open.
      } finally {
        _pendingDeletes.remove(done);
      }
    }();
    _pendingDeletes.add(done);
  }

  /// Makes [levels] the current version. Call only after the manifest that
  /// describes [levels] has been published.
  void _install(List<List<_TableHandle>> levels) {
    final next = _Version(levels)..refs = 1;
    for (final h in next.tables) {
      h.refs++;
    }
    final kept = Set<_TableHandle>.identity()..addAll(next.tables);
    final old = _current;
    for (final h in old.tables) {
      if (!kept.contains(h)) _obsolete.add(h);
    }
    _current = next;
    _unpin(old);
  }

  Future<T> _mutating<T>(Future<T> Function() action) {
    final result = _mutationLock.then((_) {
      _ensureOpen();
      return action();
    });
    _mutationLock = result.then((_) {}, onError: (_) {});
    return result;
  }

  static List<List<_TableHandle>> _copyLevels(
    List<List<_TableHandle>> levels,
  ) => [for (final l in levels) List.of(l)];

  static void _trimEmptyLevels(List<List<_TableHandle>> levels) {
    while (levels.isNotEmpty && levels.last.isEmpty) {
      levels.removeLast();
    }
  }

  Future<void> _publish(List<List<_TableHandle>> levels) async {
    await _manifest.update(
      ManifestState(
        nextTableId: _nextTableId,
        levels: [
          for (final level in levels) [for (final h in level) h.id],
        ],
      ),
    );
  }

  String _pathFor(int id) => p.join(_directory, Manifest.tableFileName(id));

  /// Writes a new table under a fresh id. If anything fails, no file with
  /// that id is left behind.
  Future<_TableHandle> _createTable(List<SSTableEntry> entries) async {
    final id = _nextTableId++;
    final path = _pathFor(id);
    try {
      return _TableHandle(
        id,
        await SSTable.create(filePath: path, entries: entries),
      );
    } catch (_) {
      await _deleteQuietly(path);
      rethrow;
    }
  }

  /// Closes and deletes tables that were written but never published.
  static Future<void> _discard(List<_TableHandle> handles) async {
    for (final h in handles) {
      try {
        await h.table.close();
      } catch (_) {
        // Nothing else to release.
      }
      await _deleteQuietly(h.table.path);
    }
  }

  static Future<void> _deleteQuietly(String path) async {
    try {
      final f = File(path);
      if (await f.exists()) await f.delete();
    } catch (_) {
      // Unreferenced files are removed as orphans on the next open.
    }
  }

  // -- writes -------------------------------------------------------------

  /// Writes [entries] (sorted ascending, unique, tombstones included) as a
  /// new L0 table and registers it in the manifest.
  ///
  /// If the table cannot be written or the manifest cannot be published,
  /// the new file is removed, the tree is unchanged and the error is
  /// rethrown.
  Future<void> addFlushedTable(List<SSTableEntry> entries) {
    _ensureOpen();
    if (entries.isEmpty) return Future<void>.value();
    return _mutating(() async {
      final h = await _createTable(entries);
      final levels = _copyLevels(_current.levels);
      if (levels.isEmpty) levels.add([]);
      levels[0].add(h);
      try {
        await _publish(levels);
      } catch (_) {
        await _discard([h]);
        rethrow;
      }
      _install(levels);
    });
  }

  // -- reads --------------------------------------------------------------

  /// Looks up [key] across all levels, newest version first.
  ///
  /// Returns null if the key is in no table; a tombstone entry if the newest
  /// version is a deletion.
  Future<SSTableEntry?> get(Uint8List key) async {
    _ensureOpen();
    final v = _pin();
    try {
      final levels = v.levels;
      if (levels.isNotEmpty) {
        final l0 = levels[0];
        for (var i = l0.length - 1; i >= 0; i--) {
          final e = await l0[i].table.get(key);
          if (e != null) return e;
        }
      }
      for (var l = 1; l < levels.length; l++) {
        final h = _tableCovering(levels[l], key);
        if (h == null) continue;
        final e = await h.table.get(key);
        if (e != null) return e;
      }
      return null;
    } finally {
      _unpin(v);
    }
  }

  /// The table of a non-overlapping level whose key range contains [key].
  static _TableHandle? _tableCovering(List<_TableHandle> level, Uint8List key) {
    // First table whose maxKey >= key.
    var lo = 0;
    var hi = level.length;
    while (lo < hi) {
      final mid = (lo + hi) >> 1;
      if (ByteKey.compareBytes(level[mid].table.maxKey, key) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo == level.length) return null;
    final h = level[lo];
    return ByteKey.compareBytes(key, h.table.minKey) < 0 ? null : h;
  }

  /// Merged, ordered iteration across every level.
  ///
  /// Yields at most one entry per key (newest wins) and includes tombstones
  /// so the caller can mask memtable-shadowed and deleted keys. [startKey]
  /// inclusive, [endKey] exclusive, [reverse] flips direction. The tables
  /// are captured now and kept alive until the stream finishes or its
  /// subscription is cancelled (a stream that is never listened to keeps
  /// them until [close]). [limitHint] sizes the first read of each table
  /// for scans that will stop early.
  Stream<SSTableEntry> scan({
    Uint8List? startKey,
    Uint8List? endKey,
    bool reverse = false,
    int? limitHint,
  }) {
    _ensureOpen();
    final v = _pin();
    bool overlaps(SSTable t) =>
        (startKey == null || ByteKey.compareBytes(t.maxKey, startKey) >= 0) &&
        (endKey == null || ByteKey.compareBytes(t.minKey, endKey) < 0);
    Stream<SSTableEntry> rangeOf(SSTable t) => t.range(
      startKey: startKey,
      endKey: endKey,
      reverse: reverse,
      limitHint: limitHint,
    );

    final sources = <Stream<SSTableEntry>>[];
    // Lower source index = higher priority (newer data).
    final levels = v.levels;
    if (levels.isNotEmpty) {
      for (final h in levels[0].reversed) {
        if (overlaps(h.table)) sources.add(rangeOf(h.table));
      }
    }
    for (var l = 1; l < levels.length; l++) {
      final ordered = reverse ? levels[l].reversed : levels[l];
      final tables = [
        for (final h in ordered)
          if (overlaps(h.table)) h.table,
      ];
      if (tables.isNotEmpty) sources.add(_concat(tables, rangeOf));
    }
    return _pinned(mergeEntryStreams(sources, reverse: reverse), v);
  }

  /// Calls [visit] with every entry of every table, OLDEST data first: the
  /// deepest level first and L0 last, in flush order. A later call for a
  /// key supersedes an earlier one; a null value is a tombstone.
  ///
  /// The current set of tables is pinned when this is called, and each
  /// table is read with one I/O, so loading a database costs one await per
  /// table instead of one per entry.
  Future<void> forEachOldestFirst(
    void Function(Uint8List key, Uint8List? value) visit,
  ) async {
    _ensureOpen();
    final v = _pin();
    try {
      final levels = v.levels;
      final ordered = <_TableHandle>[
        for (var l = levels.length - 1; l >= 1; l--) ...levels[l],
        if (levels.isNotEmpty) ...levels[0],
      ];
      for (final h in ordered) {
        for (final e in await h.table.readAll()) {
          visit(e.key, e.value);
        }
      }
    } finally {
      _unpin(v);
    }
  }

  Stream<SSTableEntry> _pinned(Stream<SSTableEntry> s, _Version v) async* {
    try {
      yield* s;
    } finally {
      _unpin(v);
    }
  }

  static Stream<SSTableEntry> _concat(
    List<SSTable> tables,
    Stream<SSTableEntry> Function(SSTable) rangeOf,
  ) async* {
    for (final t in tables) {
      yield* rangeOf(t);
    }
  }

  /// K-way merge of ordered entry streams.
  ///
  /// Earlier streams in [sources] take priority when keys collide (their
  /// entry wins and later streams' duplicates are skipped). Streams must be
  /// ordered ascending, or descending when [reverse] is true.
  static Stream<SSTableEntry> mergeEntryStreams(
    List<Stream<SSTableEntry>> sources, {
    bool reverse = false,
  }) async* {
    final iterators = [for (final s in sources) StreamIterator(s)];
    final current = List<SSTableEntry?>.filled(iterators.length, null);
    try {
      for (var i = 0; i < iterators.length; i++) {
        if (await iterators[i].moveNext()) current[i] = iterators[i].current;
      }
      while (true) {
        Uint8List? best;
        for (final e in current) {
          if (e == null) continue;
          if (best == null) {
            best = e.key;
            continue;
          }
          final cmp = ByteKey.compareBytes(e.key, best);
          if (reverse ? cmp > 0 : cmp < 0) best = e.key;
        }
        if (best == null) return;
        SSTableEntry? winner;
        for (var i = 0; i < current.length; i++) {
          final e = current[i];
          if (e == null || ByteKey.compareBytes(e.key, best) != 0) continue;
          winner ??= e; // First (highest-priority) source wins.
          current[i] = await iterators[i].moveNext()
              ? iterators[i].current
              : null;
        }
        yield winner!;
      }
    } finally {
      for (final it in iterators) {
        await it.cancel();
      }
    }
  }

  // -- compaction ---------------------------------------------------------

  int _levelTargetBytes(int level) {
    var target = baseLevelTargetBytes;
    for (var i = 1; i < level; i++) {
      target *= levelSizeMultiplier;
    }
    return target;
  }

  static int _levelBytes(List<_TableHandle> level) =>
      level.fold(0, (s, h) => s + h.table.fileSizeBytes);

  /// Index of the shallowest level that needs compaction, or null.
  int? get _compactionCandidate {
    final levels = _current.levels;
    if (levels.isNotEmpty && levels[0].length >= l0CompactionTrigger) {
      return 0;
    }
    for (var l = 1; l < levels.length && l < maxLevels - 1; l++) {
      if (_levelBytes(levels[l]) > _levelTargetBytes(l)) return l;
    }
    return null;
  }

  /// Whether [compact] currently has work to do.
  bool get needsCompaction => _compactionCandidate != null;

  /// Runs compaction rounds until no level exceeds its target.
  ///
  /// When [force] is true, L0 is compacted into L1 even below the trigger
  /// threshold before the normal policy loop runs. A failed round leaves the
  /// tree exactly as it was before that round and rethrows.
  Future<void> compact({bool force = false}) {
    _ensureOpen();
    return _mutating(() async {
      final levels = _current.levels;
      if (force && levels.isNotEmpty && levels[0].isNotEmpty) {
        await _compactLevel(0);
      }
      while (true) {
        final level = _compactionCandidate;
        if (level == null) return;
        await _compactLevel(level);
      }
    });
  }

  Future<void> _compactLevel(int sourceLevel) async {
    final v = _pin();
    final created = <_TableHandle>[];
    var published = false;
    try {
      final levels = v.levels;
      final targetLevel = sourceLevel + 1;
      final targetTables = targetLevel < levels.length
          ? levels[targetLevel]
          : <_TableHandle>[];

      // Source tables: all of L0, or one table from deeper levels.
      final List<_TableHandle> sourceTables = sourceLevel == 0
          ? List.of(levels[0])
          : [levels[sourceLevel].first];

      Uint8List? minKey;
      Uint8List? maxKey;
      for (final h in sourceTables) {
        if (minKey == null ||
            ByteKey.compareBytes(h.table.minKey, minKey) < 0) {
          minKey = h.table.minKey;
        }
        if (maxKey == null ||
            ByteKey.compareBytes(h.table.maxKey, maxKey) > 0) {
          maxKey = h.table.maxKey;
        }
      }

      final overlapping = <_TableHandle>[
        for (final h in targetTables)
          if (ByteKey.compareBytes(h.table.maxKey, minKey!) >= 0 &&
              ByteKey.compareBytes(h.table.minKey, maxKey!) <= 0)
            h,
      ];

      // Newest first: L0 newest table has priority, then older, then target.
      final sources = <Stream<SSTableEntry>>[
        for (final h in sourceTables.reversed) h.table.scanAll(),
        for (final h in overlapping) h.table.scanAll(),
      ];
      final dropTombstones = targetLevel >= maxLevels - 1;

      var pending = <SSTableEntry>[];
      var pendingBytes = 0;

      Future<void> flushPending() async {
        if (pending.isEmpty) return;
        created.add(await _createTable(pending));
        pending = <SSTableEntry>[];
        pendingBytes = 0;
      }

      await for (final e in mergeEntryStreams(sources)) {
        if (dropTombstones && e.isTombstone) continue;
        pending.add(e);
        pendingBytes += e.key.length + (e.value?.length ?? 0) + 32;
        if (pendingBytes >= targetTableBytes) await flushPending();
      }
      await flushPending();

      // Build the new membership as a copy; the current version stays
      // untouched until the manifest describing the copy is durable.
      final removed = Set<_TableHandle>.identity()
        ..addAll(sourceTables)
        ..addAll(overlapping);
      final next = _copyLevels(levels);
      while (next.length <= targetLevel) {
        next.add([]);
      }
      next[sourceLevel].removeWhere(removed.contains);
      next[targetLevel]
        ..removeWhere(removed.contains)
        ..addAll(created)
        ..sort((a, b) => ByteKey.compareBytes(a.table.minKey, b.table.minKey));
      _trimEmptyLevels(next);
      await _publish(next);
      published = true;
      _install(next);
    } finally {
      if (!published) await _discard(created);
      _unpin(v);
    }
  }

  /// Closes every table and deletes files superseded by compaction.
  Future<void> close() async {
    if (_closed) return;
    // Let an in-flight flush or compaction finish first.
    await _mutationLock;
    _closed = true;
    await Future.wait(List.of(_pendingDeletes));
    for (final h in _current.tables) {
      await h.table.close();
    }
    for (final h in List.of(_obsolete)) {
      await h.table.close();
      await _deleteQuietly(h.table.path);
    }
    _obsolete.clear();
  }
}
