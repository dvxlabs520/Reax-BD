/// LSM-only implementation of [StorageEngine].
///
/// Every mutation flows through one pipeline: the write-ahead log first
/// (durable per the configured [SyncMode] before the call returns), then the
/// memtable. Full memtables are flushed inline to an L0 SSTable and the WAL
/// is checkpointed, so an acknowledged write is always recoverable from
/// either the WAL or an SSTable. A flush is also triggered once the WAL has
/// grown by `walFlushThresholdBytes` since the last checkpoint, which bounds
/// the log (and replay time) when the same keys are overwritten repeatedly.
///
/// A batch is acknowledged as soon as its WAL append succeeds. Flushes and
/// compactions that run afterwards are *maintenance*: if one fails, the
/// batch still reports success, the data stays in the memtable and the WAL,
/// the error is reported through `onMaintenanceError` /
/// [LsmStorageEngine.lastMaintenanceError], and the work is retried on a
/// later write (with exponential backoff) or an explicit flush.
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

  /// WAL sequence a successful flush still has to checkpoint (set when the
  /// SSTable was published but the checkpoint failed).
  int? _pendingCheckpoint;

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
    int l0CompactionTrigger = 4,
    int baseLevelTargetBytes = 8 * 1024 * 1024,
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
    if (engine._flushDue) {
      await engine._maintain();
    }
    return engine;
  }

  /// The most recent maintenance (automatic flush/compaction) error that a
  /// later successful flush has not cleared yet, or null.
  Object? get lastMaintenanceError => _lastMaintenanceError;

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
      await _wal.appendAll(entries);
      for (final op in ops) {
        if (op.isDelete) {
          _memtable.delete(op.key);
        } else {
          _memtable.put(op.key, op.value!);
        }
      }
      // From here on the batch is acknowledged; maintenance cannot fail it.
      if (_flushDue) {
        await _maintain();
      }
    });
  }

  bool get _flushDue =>
      _memtable.isFull || _wal.bytesSinceCheckpoint >= _walFlushThresholdBytes;

  /// Runs a flush (and any compaction it triggers) as maintenance: errors
  /// are recorded and reported instead of thrown, and retries back off.
  Future<void> _maintain() async {
    if (_maintenanceBackoffMs > 0 &&
        _sinceMaintenanceFailure.elapsedMilliseconds < _maintenanceBackoffMs) {
      return;
    }
    try {
      await _flushLocked();
      _maintenanceSucceeded();
    } catch (e, st) {
      _maintenanceFailed(e, st);
    }
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
    final cursor = _memtable.openCursor(
      startKey == null ? null : ByteKey(startKey),
      endKey == null ? null : ByteKey(endKey),
      reverse: reverse,
    );
    final merged = LsmTree.mergeEntryStreams([
      _memtableEntries(cursor),
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

  /// Flushes the active memtable to an L0 table and checkpoints the WAL,
  /// then compacts if needed. Must be called while holding the write lock.
  ///
  /// Each step is retry-safe: if the table cannot be published the memtable
  /// is kept; if the checkpoint fails it is retried by the next call; a
  /// failed compaction leaves the tree unchanged and is retried as well.
  Future<void> _flushLocked() async {
    if (!_memtable.isEmpty) {
      final flushUpTo = _wal.lastSequenceNumber;
      final entries = <SSTableEntry>[
        for (final e in _memtable.entries)
          e.value.isTombstone
              ? SSTableEntry.tombstone(e.key.bytes)
              : SSTableEntry(e.key.bytes, e.value.value!),
      ];
      await _lsm.addFlushedTable(entries);
      _memtable = MemTable(maxSizeBytes: _memtableSizeBytes);
      _pendingCheckpoint = flushUpTo;
    }
    final checkpoint = _pendingCheckpoint;
    if (checkpoint != null) {
      await _wal.checkpoint(checkpoint);
      _pendingCheckpoint = null;
    }
    if (_lsm.needsCompaction) {
      await _lsm.compact();
    }
  }

  /// Persists the active memtable and checkpoints the WAL.
  ///
  /// Unlike the automatic flush after a write, errors are thrown here.
  @override
  Future<void> flush() {
    return _synchronized(() async {
      _ensureOpen();
      await _flushLocked();
      _maintenanceSucceeded();
    });
  }

  /// Flushes and compacts. Errors are thrown.
  @override
  Future<void> compact() {
    return _synchronized(() async {
      _ensureOpen();
      await _flushLocked();
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
        await _flushLocked();
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
    immutableMemtableCount: 0,
    sstableCount: _lsm.tableCount,
    sstableSizeBytes: _lsm.totalSizeBytes,
    levelTableCounts: _lsm.levelTableCounts,
    lastSequenceNumber: _wal.lastSequenceNumber,
  );
}
