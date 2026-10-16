/// LSM-only implementation of [StorageEngine].
///
/// Every mutation flows through one pipeline: the write-ahead log first
/// (durable per the configured [SyncMode] before the call returns), then the
/// memtable. A full memtable is *sealed*: it becomes read-only, a fresh one
/// takes the writes, and the sealed one is flushed to an L0 SSTable in the
/// background, after which the WAL is checkpointed up to the last entry it
/// holds. An acknowledged write is therefore always recoverable from either
/// the WAL or an SSTable. A seal is also triggered once the WAL has grown by
/// `walFlushThresholdBytes` since the last one, which bounds the log (and
/// replay time) when the same keys are overwritten repeatedly.
///
/// A batch is acknowledged as soon as its WAL append succeeds. Flushes and
/// compactions are *maintenance*: they run after the write that triggered
/// them has returned, never inside it. If one fails, the data stays in the
/// sealed memtable and the WAL, the error is reported through
/// `onMaintenanceError` / [LsmStorageEngine.lastMaintenanceError], and the
/// work is retried on a later write (with exponential backoff) or an
/// explicit flush. At most [LsmStorageEngine.maxSealedMemtables] sealed
/// memtables wait for their flush; a write that would exceed that waits,
/// and fails with [StorageException] while maintenance keeps failing, so
/// memory stays bounded when the disk is full.
library;

import 'dart:async';
import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../errors/exceptions.dart';
import '../util/byte_key.dart';
import '../wal/write_ahead_log.dart';
import 'lsm_tree.dart';
import 'memtable.dart';
import 'sstable.dart';
import 'storage_engine.dart';

/// Durable ordered key/value engine backed by a WAL and a leveled LSM tree.
final class LsmStorageEngine implements StorageEngine {
  LsmStorageEngine._(
    this._directory,
    this._wal,
    this._lsm,
    this._memtableSizeBytes,
    this._walFlushThresholdBytes,
    this._onMaintenanceError,
  ) : _memtable = MemTable(maxSizeBytes: _memtableSizeBytes);

  /// Namespace for internally generated batch transaction ids, chosen so it
  /// cannot collide with caller-supplied ids from the transaction manager.
  static const int _internalTxBase = 0x4000000000000000;

  final String _directory;
  final WriteAheadLog _wal;
  final LsmTree _lsm;
  final int _memtableSizeBytes;
  final int _walFlushThresholdBytes;
  final void Function(Object error, StackTrace stack)? _onMaintenanceError;

  MemTable _memtable;
  bool _closed = false;
  int _internalTxCounter = 0;

  /// How many sealed memtables may wait for their flush before writes wait.
  static const int maxSealedMemtables = 2;

  /// Sealed, read-only memtables waiting to be flushed, oldest first.
  final List<_SealedMemtable> _sealed = <_SealedMemtable>[];

  /// WAL bytes appended into the active memtable since it was created.
  int _activeWalBytes = 0;

  /// The background maintenance run in progress, or null.
  Future<void>? _maintenance;

  /// A checkpoint still owed after its SSTable was published, because the
  /// checkpoint itself failed. Retried before anything else.
  _SealedMemtable? _pendingCheckpoint;

  Object? _lastMaintenanceError;
  int _maintenanceBackoffMs = 0;
  final Stopwatch _sinceMaintenanceFailure = Stopwatch();

  static const int _minBackoffMs = 50;
  static const int _maxBackoffMs = 5000;

  /// Serializes every mutation, flush, compaction, and close.
  Future<void> _writeLock = Future<void>.value();

  /// Opens (or creates) an engine rooted at [directory].
  ///
  /// Layout: `directory/wal/` holds WAL segments, `directory/tables/` holds
  /// the manifest and SSTables. Committed WAL entries newer than the last
  /// checkpoint are replayed into the memtable before the engine is
  /// returned.
  ///
  /// [walFlushThresholdBytes] forces a flush once the WAL has grown by that
  /// many bytes since the last checkpoint (default: the larger of four
  /// memtables and 16 MiB). [onMaintenanceError] is called with every error
  /// raised by an automatic flush or compaction that runs after a write was
  /// already acknowledged; such errors never fail the write.
  static Future<LsmStorageEngine> open({
    required String directory,
    SyncMode syncMode = SyncMode.full,
    int memtableSizeBytes = 4 * 1024 * 1024,
    int walMaxFileBytes = 16 * 1024 * 1024,
    int maxLevels = 4,
    // Eight L0 tables and a 64 MiB L1 keep a database of typical mobile size
    // in two levels: a 100,000-document bulk load rewrites its data about
    // half as often as with four tables and 8 MiB, and its slowest batch
    // drops from about 250 ms to under 20 ms. Point reads cost the same,
    // thanks to the bloom filters.
    int l0CompactionTrigger = 8,
    int baseLevelTargetBytes = 64 * 1024 * 1024,
    int levelSizeMultiplier = 10,
    int targetTableBytes = 2 * 1024 * 1024,
    int? walFlushThresholdBytes,
    void Function(Object error, StackTrace stack)? onMaintenanceError,
  }) async {
    await Directory(directory).create(recursive: true);
    final lsm = await LsmTree.open(
      directory: p.join(directory, 'tables'),
      maxLevels: maxLevels,
      l0CompactionTrigger: l0CompactionTrigger,
      baseLevelTargetBytes: baseLevelTargetBytes,
      levelSizeMultiplier: levelSizeMultiplier,
      targetTableBytes: targetTableBytes,
    );
    final wal = await WriteAheadLog.open(
      directory: p.join(directory, 'wal'),
      syncMode: syncMode,
      maxFileBytes: walMaxFileBytes,
    );
    final engine = LsmStorageEngine._(
      directory,
      wal,
      lsm,
      memtableSizeBytes,
      walFlushThresholdBytes ??
          math.max(4 * memtableSizeBytes, 16 * 1024 * 1024),
      onMaintenanceError,
    );
    await for (final entry in wal.replay()) {
      if (entry.type == WalEntryType.put) {
        engine._memtable.put(entry.key!, entry.value!);
      } else {
        engine._memtable.delete(entry.key!);
      }
    }
    // Replayed entries count towards the next seal like fresh writes.
    engine._activeWalBytes = wal.bytesSinceCheckpoint;
    if (engine._flushDue) {
      engine._seal();
      try {
        await engine._drain();
        engine._maintenanceSucceeded();
      } catch (e, st) {
        engine._maintenanceFailed(e, st);
      }
    }
    return engine;
  }

  /// The most recent maintenance (automatic flush/compaction) error that a
  /// later successful flush has not cleared yet, or null.
  Object? get lastMaintenanceError => _lastMaintenanceError;

  /// Whether a background flush or compaction is running.
  bool get isMaintaining => _maintenance != null;

  /// Approximate WAL bytes written since the last checkpoint.
  int get walBytesSinceCheckpoint => _wal.bytesSinceCheckpoint;

  /// Root directory of this engine.
  String get directory => _directory;

  void _ensureOpen() {
    if (_closed) {
      throw const DatabaseClosedException('Storage engine is closed');
    }
  }

  Future<T> _synchronized<T>(Future<T> Function() action) {
    final result = _writeLock.then((_) => action());
    _writeLock = result.then((_) {}, onError: (_) {});
    return result;
  }

  @override
  Future<void> put(Uint8List key, Uint8List value) =>
      writeBatch([WriteOp.put(key, value)]);

  @override
  Future<void> delete(Uint8List key) => writeBatch([WriteOp.delete(key)]);

  @override
  Future<void> writeBatch(List<WriteOp> ops, {int? transactionId}) {
    if (ops.isEmpty) return Future<void>.value();
    return _synchronized(() async {
      _ensureOpen();
      await _waitForRoom();
      final entries = <WalEntry>[];
      int? txId = transactionId;
      final needsTxFraming = ops.length > 1 || txId != null;
      if (needsTxFraming) {
        txId ??= _internalTxBase + _internalTxCounter++;
        entries.add(WalEntry.txBegin(txId));
      }
      for (final op in ops) {
        entries.add(
          op.isDelete
              ? WalEntry.delete(op.key, transactionId: txId)
              : WalEntry.put(op.key, op.value!, transactionId: txId),
        );
      }
      if (needsTxFraming) {
        entries.add(WalEntry.txCommit(txId!));
      }
      // Durability point: appendAll returns only after the batch is durable
      // per the WAL's sync mode. If it throws, nothing below runs and the
      // batch is not visible.
      _activeWalBytes += await _wal.appendAll(entries);
      for (final op in ops) {
        if (op.isDelete) {
          _memtable.delete(op.key);
        } else {
          _memtable.put(op.key, op.value!);
        }
      }
      // From here on the batch is acknowledged; maintenance cannot fail it,
      // and it runs after this write returns.
      if (_flushDue) _seal();
      _startMaintenance();
    });
  }

  bool get _flushDue =>
      _memtable.isFull || _activeWalBytes >= _walFlushThresholdBytes;

  bool get _inBackoff =>
      _maintenanceBackoffMs > 0 &&
      _sinceMaintenanceFailure.elapsedMilliseconds < _maintenanceBackoffMs;

  /// Makes the active memtable read-only and starts a fresh one. Runs while
  /// holding the write lock, between two batches, so the WAL sequence taken
  /// here covers exactly the entries in the sealed memtable.
  void _seal() {
    if (_memtable.isEmpty) return;
    _sealed.add(
      _SealedMemtable(_memtable, _wal.lastSequenceNumber, _activeWalBytes),
    );
    _memtable = MemTable(maxSizeBytes: _memtableSizeBytes);
    _activeWalBytes = 0;
  }

  /// Starts background maintenance if there is work and none is running.
  void _startMaintenance() {
    if (_maintenance != null || _closed || _inBackoff) return;
    if (_sealed.isEmpty &&
        _pendingCheckpoint == null &&
        !_lsm.needsCompaction) {
      return;
    }
    late final Future<void> run;
    run = () async {
      try {
        await _drain();
        _maintenanceSucceeded();
      } catch (e, st) {
        // Never left unhandled: reported, then retried by a later write.
        _maintenanceFailed(e, st);
      } finally {
        if (identical(_maintenance, run)) _maintenance = null;
      }
    }();
    _maintenance = run;
  }

  /// Waits, before a write, until a sealed memtable fits.
  ///
  /// Throws [StorageException] when maintenance keeps failing (for example
  /// on a full disk), instead of waiting forever or letting memory grow.
  Future<void> _waitForRoom() async {
    while (_sealed.length >= maxSealedMemtables) {
      final Future<void>? running = _maintenance;
      if (running != null) {
        await running;
        continue;
      }
      final Object? error = _lastMaintenanceError;
      if (error != null && _inBackoff) {
        throw StorageException(
          'Writes are paused: buffered data could not be written to disk '
          '($error). Free some space; writes resume once a flush succeeds.',
          path: _directory,
          cause: error,
        );
      }
      // Backoff elapsed (or no failure yet): try again now.
      _maintenanceBackoffMs = 0;
      _startMaintenance();
      if (_maintenance == null) return;
    }
  }

  /// Flushes every sealed memtable, oldest first, checkpointing the WAL
  /// after each, then compacts if needed. Errors are thrown.
  ///
  /// Order matters for durability: the SSTable and the manifest describing
  /// it are durable before the checkpoint that lets the WAL forget the same
  /// entries, and memtables are flushed in the order they were sealed.
  Future<void> _drain() async {
    final _SealedMemtable? owed = _pendingCheckpoint;
    if (owed != null) {
      await _wal.checkpoint(owed.flushUpTo, coveredBytes: owed.walBytes);
      _pendingCheckpoint = null;
    }
    while (_sealed.isNotEmpty) {
      final _SealedMemtable next = _sealed.first;
      final entries = <SSTableEntry>[
        for (final e in next.table.entries)
          e.value.isTombstone
              ? SSTableEntry.tombstone(e.key.bytes)
              : SSTableEntry(e.key.bytes, e.value.value!),
      ];
      await _lsm.addFlushedTable(entries);
      // Readers find these entries in L0 from here on.
      _sealed.removeAt(0);
      _pendingCheckpoint = next;
      await _wal.checkpoint(next.flushUpTo, coveredBytes: next.walBytes);
      _pendingCheckpoint = null;
    }
    if (_lsm.needsCompaction) {
      await _lsm.compact();
    }
  }

  /// Waits for background maintenance, seals the active memtable and
  /// flushes everything. Holds the write lock. Errors are thrown.
  Future<void> _flushAllLocked() async {
    final Future<void>? running = _maintenance;
    if (running != null) await running;
    _seal();
    await _drain();
  }

  void _maintenanceSucceeded() {
    _lastMaintenanceError = null;
    _maintenanceBackoffMs = 0;
    _sinceMaintenanceFailure
      ..stop()
      ..reset();
  }

  void _maintenanceFailed(Object error, StackTrace stack) {
    _lastMaintenanceError = error;
    _maintenanceBackoffMs = _maintenanceBackoffMs == 0
        ? _minBackoffMs
        : math.min(_maintenanceBackoffMs * 2, _maxBackoffMs);
    _sinceMaintenanceFailure
      ..reset()
      ..start();
    final callback = _onMaintenanceError;
    if (callback == null) return;
    try {
      callback(error, stack);
    } catch (_) {
      // A faulty observer must not break the write path.
    }
  }

  @override
  Future<Uint8List?> get(Uint8List key) async {
    _ensureOpen();
    final slot = _memtable.lookup(key);
    if (slot != null) return slot.value;
    for (int i = _sealed.length - 1; i >= 0; i--) {
      final sealedSlot = _sealed[i].table.lookup(key);
      if (sealedSlot != null) return sealedSlot.value;
    }
    final entry = await _lsm.get(key);
    return entry?.value;
  }

  @override
  Stream<KeyValue> scan({
    Uint8List? startKey,
    Uint8List? endKey,
    int? limit,
    bool reverse = false,
  }) {
    _ensureOpen();
    if (limit != null && limit <= 0) return const Stream<KeyValue>.empty();
    // Both sources are snapshots taken now: the memtable cursor copies the
    // rest of its range only if a write happens while it is open, and the
    // LSM scan pins the current set of immutable tables.
    final ByteKey? start = startKey == null ? null : ByteKey(startKey);
    final ByteKey? end = endKey == null ? null : ByteKey(endKey);
    // Newest first: the active memtable, then sealed ones newest to oldest,
    // then the tables. Sealed memtables never change, so their cursors are
    // stable even if their flush finishes while the scan runs.
    final merged = LsmTree.mergeEntryStreams([
      _memtableEntries(_memtable.openCursor(start, end, reverse: reverse)),
      for (int i = _sealed.length - 1; i >= 0; i--)
        _memtableEntries(
          _sealed[i].table.openCursor(start, end, reverse: reverse),
        ),
      _lsm.scan(
        startKey: startKey,
        endKey: endKey,
        reverse: reverse,
        limitHint: limit,
      ),
    ], reverse: reverse);
    return _liveEntries(merged, limit);
  }

  static Stream<SSTableEntry> _memtableEntries(MemTableCursor cursor) async* {
    try {
      while (true) {
        final e = cursor.next();
        if (e == null) return;
        yield e.value.isTombstone
            ? SSTableEntry.tombstone(e.key.bytes)
            : SSTableEntry(e.key.bytes, e.value.value!);
      }
    } finally {
      cursor.close();
    }
  }

  static Stream<KeyValue> _liveEntries(
    Stream<SSTableEntry> merged,
    int? limit,
  ) async* {
    var yielded = 0;
    await for (final e in merged) {
      if (e.isTombstone) continue;
      yield KeyValue(e.key, e.value!);
      // Stop as soon as the limit is reached so no further I/O is issued.
      if (limit != null && ++yielded >= limit) return;
    }
  }

  /// Escalates the durability of everything written so far to [mode].
  ///
  /// The engine's configured sync mode already applies to every batch; this
  /// exists so a caller can make one individual write stronger (for example
  /// a database opened with [SyncMode.os] that needs a single fsynced write).
  Future<void> syncTo(SyncMode mode) {
    _ensureOpen();
    return _wal.syncTo(mode);
  }

  /// Persists the active memtable and checkpoints the WAL.
  ///
  /// Unlike the automatic flush after a write, errors are thrown here.
  @override
  Future<void> flush() {
    return _synchronized(() async {
      _ensureOpen();
      await _flushAllLocked();
      _maintenanceSucceeded();
    });
  }

  /// Flushes and compacts. Errors are thrown.
  @override
  Future<void> compact() {
    return _synchronized(() async {
      _ensureOpen();
      await _flushAllLocked();
      await _lsm.compact(force: true);
      _maintenanceSucceeded();
    });
  }

  /// Flushes and closes the engine. Safe to call more than once.
  ///
  /// A failed final flush does not prevent closing (the data is still in
  /// the WAL and is replayed on the next open); it is reported like any
  /// other maintenance error. Only a failure to write out buffered WAL
  /// bytes is thrown, after every resource has been released.
  @override
  Future<void> close() {
    return _synchronized(() async {
      if (_closed) return;
      try {
        await _flushAllLocked();
      } catch (e, st) {
        _maintenanceFailed(e, st);
      }
      _closed = true;
      try {
        await _wal.close();
      } finally {
        await _lsm.close();
      }
    });
  }

  @override
  StorageStats get stats => StorageStats(
    memtableEntries: _memtable.entryCount,
    memtableSizeBytes: _memtable.sizeBytes,
    immutableMemtableCount: _sealed.length,
    sstableCount: _lsm.tableCount,
    sstableSizeBytes: _lsm.totalSizeBytes,
    levelTableCounts: _lsm.levelTableCounts,
    lastSequenceNumber: _wal.lastSequenceNumber,
  );
}

/// A read-only memtable waiting for its flush, with the last WAL sequence
/// it holds and the WAL bytes appended into it.
final class _SealedMemtable {
  _SealedMemtable(this.table, this.flushUpTo, this.walBytes);

  final MemTable table;
  final int flushUpTo;
  final int walBytes;
}
