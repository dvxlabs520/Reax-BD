/// ACID transactions on top of the storage engine.
///
/// Buffered writes (read-your-own-writes), pessimistic locking with lock
/// upgrade and deadlock detection, an optimistic mode with version
/// validation, and atomic commit through [StorageEngine.writeBatch] so a
/// crash mid-commit can never persist a partial write set.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import '../../domain/entities/transaction_entity.dart';
import '../errors/exceptions.dart';
import '../storage/storage_engine.dart';
import 'lock_manager.dart';

export '../../domain/entities/transaction_entity.dart';
export 'lock_manager.dart' show LockManager, LockType;

/// A buffered write inside a transaction's write set.
class _PendingWrite {
  _PendingWrite.put(Uint8List this.value);
  _PendingWrite.delete() : value = null;

  /// The value to write, or null for a delete.
  final Uint8List? value;

  bool get isDelete => value == null;
}

/// Serializes async critical sections in FIFO order.
class _AsyncMutex {
  Future<void> _tail = Future<void>.value();

  /// Runs [action] after all previously scheduled actions complete.
  Future<T> run<T>(Future<T> Function() action) async {
    final Future<void> previous = _tail;
    final Completer<void> gate = Completer<void>();
    _tail = gate.future;
    await previous;
    try {
      return await action();
    } finally {
      gate.complete();
    }
  }
}

/// A single database transaction.
///
/// Obtain instances from [TransactionManager.begin] (or, preferably,
/// [TransactionManager.runTransaction] which handles commit, abort and
/// retries). Writes are buffered in the transaction and become visible to
/// other transactions only after commit; within the transaction, [get]
/// always observes the transaction's own pending writes.
///
/// Operations on a transaction that is no longer [TransactionState.active]
/// throw [TransactionConflictException]. If a lock acquisition fails
/// (deadlock or timeout) the transaction is aborted automatically and all
/// its locks are released before the exception propagates.
final class Transaction {
  Transaction._({
    required this.id,
    required this.isolationLevel,
    required this._manager,
  });

  /// Monotonically increasing transaction identifier (lower id = older).
  final int id;

  /// Concurrency-control mode this transaction runs under.
  final IsolationLevel isolationLevel;

  final TransactionManager _manager;

  TransactionState _state = TransactionState.active;

  /// Buffered mutations, one per key, in first-write order.
  final Map<String, _PendingWrite> _writeSet = <String, _PendingWrite>{};

  /// Committed values observed by reads (null = key absent), used for
  /// repeatable reads and commit-time validation. Pessimistic levels only.
  final Map<String, Uint8List?> _readSet = <String, Uint8List?>{};

  /// Optimistic mode: version of each key at first read/write access.
  final Map<String, int> _accessVersions = <String, int>{};

  /// Current lifecycle state.
  TransactionState get state => _state;

  /// Number of keys this transaction has pending writes for.
  int get writeSetSize => _writeSet.length;

  /// Number of keys recorded in the read set (pessimistic levels).
  int get readSetSize => _readSet.length;

  void _ensureActive() {
    if (_state != TransactionState.active) {
      throw TransactionConflictException(
        'Transaction $id is ${_state.name} and no longer accepts operations',
        transactionId: id,
      );
    }
  }

  /// Reads [key], observing this transaction's own pending writes first.
  ///
  /// Returns null when the key is absent (or deleted by this transaction).
  Future<Uint8List?> get(String key) async {
    _ensureActive();

    final _PendingWrite? pending = _writeSet[key];
    if (pending != null) {
      return pending.isDelete ? null : pending.value;
    }

    switch (isolationLevel) {
      case IsolationLevel.readCommitted:
        // Lock-free: storage only ever contains committed data.
        return _manager._readCommitted(key);
      case IsolationLevel.repeatableRead:
      case IsolationLevel.serializable:
        if (_readSet.containsKey(key)) return _readSet[key];
        await _acquire(key, LockType.shared);
        final Uint8List? value = await _manager._readCommitted(key);
        _readSet[key] = value;
        return value;
      case IsolationLevel.optimistic:
        // The version is taken BEFORE the read: a commit that lands while the
        // read is in flight must make validation fail, not slip past it.
        _accessVersions.putIfAbsent(key, () => _manager._versionOf(key));
        return _manager._readCommitted(key);
    }
  }

  /// Buffers a write of [key] = [value], applied atomically at commit.
  Future<void> put(String key, Uint8List value) async {
    _ensureActive();
    await _lockForWrite(key);
    _writeSet[key] = _PendingWrite.put(value);
  }

  /// Buffers a deletion of [key], applied atomically at commit.
  Future<void> delete(String key) async {
    _ensureActive();
    await _lockForWrite(key);
    _writeSet[key] = _PendingWrite.delete();
  }

  Future<void> _lockForWrite(String key) async {
    if (isolationLevel == IsolationLevel.optimistic) {
      _accessVersions.putIfAbsent(key, () => _manager._versionOf(key));
      return;
    }
    await _acquire(key, LockType.exclusive);
  }

  /// Acquires a lock, aborting this transaction on deadlock/timeout so its
  /// locks are released on every error path.
  Future<void> _acquire(String key, LockType mode) async {
    try {
      await _manager._lockManager.acquire(key, id, mode);
    } on ReaxDbException {
      await _manager.abort(this);
      rethrow;
    }
  }
}

/// Hooks that let the database facade run a commit inside its own write
/// pipeline.
///
/// Without a pipeline a commit writes the transaction's values in one
/// [StorageEngine.writeBatch] and nothing else. With one, the commit runs
/// inside [exclusive] (so no other write of the facade can interleave with
/// validation and the batch), appends [extraOps] (secondary index
/// maintenance) to the SAME atomic batch, and calls [afterCommit] (cache and
/// change events) before the exclusive section ends.
abstract interface class CommitPipeline {
  /// Runs [action] while no other pipeline write can run.
  Future<T> exclusive<T>(Future<T> Function() action);

  /// Extra operations committed atomically with the transaction's values.
  Future<List<WriteOp>> extraOps(List<AppliedOperation> applied);

  /// Called once the batch is durable, still inside [exclusive].
  ///
  /// Errors thrown here are reported to [onAfterCommitError] and never turn a
  /// durable commit into a failure.
  void afterCommit(CommitResult result);

  /// Receives an error thrown by [afterCommit].
  void onAfterCommitError(Object error, StackTrace stack);
}

/// Creates, commits and aborts [Transaction]s over a [StorageEngine].
///
/// Commit applies the whole write set through a single
/// [StorageEngine.writeBatch] call tagged with the transaction id; the
/// engine's WAL makes the batch all-or-nothing across crashes. The manager
/// never calls `put`/`delete` on the engine for transactional writes.
final class TransactionManager {
  /// Creates a manager over [storageEngine].
  ///
  /// [lockTimeout] bounds how long any single lock wait may block before
  /// failing with [TransactionTimeoutException] (deadlocks are normally
  /// detected immediately and do not wait for this backstop).
  TransactionManager({
    required StorageEngine storageEngine,
    this._defaultIsolationLevel = IsolationLevel.readCommitted,
    Duration lockTimeout = const Duration(seconds: 10),
    this._pipeline,
  }) : _engine = storageEngine,
       _lockManager = LockManager(defaultTimeout: lockTimeout);

  final StorageEngine _engine;
  final CommitPipeline? _pipeline;
  final IsolationLevel _defaultIsolationLevel;
  final LockManager _lockManager;
  final _AsyncMutex _commitMutex = _AsyncMutex();

  final Map<int, Transaction> _active = <int, Transaction>{};

  /// Version stamp per key, bumped on every committed write and on every
  /// non-transactional write reported through [noteExternalWrites]. Used by
  /// optimistic validation; cleared whenever no transaction is active so it
  /// cannot grow without bound.
  final Map<String, int> _keyVersions = <String, int>{};
  int _versionCounter = 0;

  int _nextTransactionId = 0;
  int _committedCount = 0;
  int _abortedCount = 0;
  bool _closed = false;

  /// The lock manager (exposed for tests and diagnostics).
  LockManager get lockManager => _lockManager;

  /// Counters describing this manager's activity.
  TransactionStats get stats => TransactionStats(
    activeTransactions: _active.length,
    committedTransactions: _committedCount,
    abortedTransactions: _abortedCount,
  );

  /// Transactions currently active or preparing.
  List<Transaction> get activeTransactions => _active.values.toList();

  /// Starts a new transaction.
  Transaction begin({IsolationLevel? isolationLevel}) {
    if (_closed) {
      throw const DatabaseClosedException('TransactionManager is closed');
    }
    final Transaction tx = Transaction._(
      id: ++_nextTransactionId,
      isolationLevel: isolationLevel ?? _defaultIsolationLevel,
      manager: this,
    );
    _active[tx.id] = tx;
    return tx;
  }

  /// Commits [tx], applying its whole write set atomically.
  ///
  /// Returns a [CommitResult] listing every applied operation with the
  /// committed value that preceded it, so the caller (the database facade)
  /// can invalidate caches, maintain secondary indexes and emit change
  /// events. Throws [TransactionConflictException] when validation fails
  /// (serializable read-set validation or optimistic version validation);
  /// on any failure the transaction is aborted and its locks are released.
  Future<CommitResult> commit(Transaction tx) async {
    if (tx._state != TransactionState.active) {
      throw TransactionConflictException(
        'Cannot commit transaction ${tx.id}: it is ${tx._state.name}',
        transactionId: tx.id,
      );
    }
    tx._state = TransactionState.preparing;
    try {
      final CommitResult result = await _commitMutex.run(
        () => _exclusive(() async {
          switch (tx.isolationLevel) {
            case IsolationLevel.readCommitted:
              break;
            case IsolationLevel.repeatableRead:
            case IsolationLevel.serializable:
              await _validateReadSet(tx);
            case IsolationLevel.optimistic:
              _validateVersions(tx);
          }

          final List<AppliedOperation> applied = <AppliedOperation>[];
          final List<WriteOp> ops = <WriteOp>[];
          for (final MapEntry<String, _PendingWrite> entry
              in tx._writeSet.entries) {
            final Uint8List keyBytes = _keyBytes(entry.key);
            final Uint8List? oldValue = await _engine.get(keyBytes);
            if (entry.value.isDelete) {
              ops.add(WriteOp.delete(keyBytes));
              applied.add(
                AppliedOperation(
                  type: AppliedOperationType.delete,
                  key: entry.key,
                  oldValue: oldValue,
                  newValue: null,
                ),
              );
            } else {
              final Uint8List value = entry.value.value!;
              ops.add(WriteOp.put(keyBytes, value));
              applied.add(
                AppliedOperation(
                  type: AppliedOperationType.put,
                  key: entry.key,
                  oldValue: oldValue,
                  newValue: value,
                ),
              );
            }
          }

          final CommitPipeline? pipeline = _pipeline;
          if (ops.isNotEmpty) {
            if (pipeline != null) {
              // Index maintenance rides in the same atomic batch, so a crash
              // can never leave a committed document without its postings.
              ops.addAll(await pipeline.extraOps(applied));
            }
            await _engine.writeBatch(ops, transactionId: tx.id);
            for (final String key in tx._writeSet.keys) {
              _keyVersions[key] = ++_versionCounter;
            }
          }

          tx._state = TransactionState.committed;
          _committedCount++;
          final CommitResult committed = CommitResult(
            transactionId: tx.id,
            operations: applied,
          );
          if (pipeline != null && applied.isNotEmpty) {
            try {
              pipeline.afterCommit(committed);
            } catch (error, stack) {
              pipeline.onAfterCommitError(error, stack);
            }
          }
          return committed;
        }),
      );
      return result;
    } catch (_) {
      if (tx._state != TransactionState.committed) {
        tx._state = TransactionState.aborted;
        _abortedCount++;
      }
      rethrow;
    } finally {
      _lockManager.releaseAll(tx.id);
      _active.remove(tx.id);
      _maybeResetVersions();
    }
  }

  /// Aborts [tx], discarding its write set and releasing its locks.
  ///
  /// Idempotent: aborting an already-finished transaction is a no-op.
  Future<void> abort(Transaction tx) async {
    if (tx._state == TransactionState.committed ||
        tx._state == TransactionState.aborted) {
      // Still release defensively in case abort raced a failed operation.
      _lockManager.releaseAll(tx.id);
      _active.remove(tx.id);
      return;
    }
    tx._state = TransactionState.aborted;
    _abortedCount++;
    _lockManager.releaseAll(tx.id);
    _active.remove(tx.id);
    _maybeResetVersions();
  }

  /// Runs [body] in a transaction, committing on success and aborting on
  /// any error.
  ///
  /// Retries only on [TransactionConflictException] and
  /// [DeadlockException], up to [maxAttempts] total attempts, sleeping
  /// `initialBackoff * 2^(attempt-1)` between attempts (exponential
  /// backoff, no jitter). Any other exception aborts and rethrows
  /// immediately. [onCommit] receives the [CommitResult] of the successful
  /// attempt, before this method returns.
  Future<T> runTransaction<T>(
    Future<T> Function(Transaction tx) body, {
    IsolationLevel? isolationLevel,
    int maxAttempts = 3,
    Duration initialBackoff = const Duration(milliseconds: 5),
    void Function(CommitResult result)? onCommit,
  }) async {
    if (maxAttempts < 1) {
      throw TransactionConflictException(
        'maxAttempts must be at least 1 (got $maxAttempts)',
      );
    }
    int attempt = 0;
    while (true) {
      attempt++;
      final Transaction tx = begin(isolationLevel: isolationLevel);
      try {
        final T value = await body(tx);
        final CommitResult result = await commit(tx);
        onCommit?.call(result);
        return value;
      } on ReaxDbException catch (error) {
        await abort(tx);
        final bool retryable =
            error is TransactionConflictException || error is DeadlockException;
        if (!retryable || attempt >= maxAttempts) rethrow;
        await Future<void>.delayed(initialBackoff * (1 << (attempt - 1)));
      } catch (_) {
        await abort(tx);
        rethrow;
      }
    }
  }

  /// Aborts every active transaction and rejects new ones.
  Future<void> close() async {
    _closed = true;
    for (final Transaction tx in _active.values.toList()) {
      await abort(tx);
    }
  }

  /// Records that [keys] were written outside any transaction.
  ///
  /// Optimistic transactions validate against key versions; a plain write
  /// must bump them too, or an optimistic commit would silently overwrite
  /// it (a lost update).
  void noteExternalWrites(Iterable<String> keys) {
    if (_active.isEmpty) return;
    for (final String key in keys) {
      _keyVersions[key] = ++_versionCounter;
    }
  }

  // -- internals ----------------------------------------------------------

  Future<T> _exclusive<T>(Future<T> Function() action) {
    final CommitPipeline? pipeline = _pipeline;
    return pipeline == null ? action() : pipeline.exclusive<T>(action);
  }

  Uint8List _keyBytes(String key) => Uint8List.fromList(utf8.encode(key));

  Future<Uint8List?> _readCommitted(String key) => _engine.get(_keyBytes(key));

  int _versionOf(String key) => _keyVersions[key] ?? 0;

  void _maybeResetVersions() {
    if (_active.isEmpty) {
      _keyVersions.clear();
    }
  }

  /// Re-reads every key in the read set and fails if any committed value
  /// differs from what the transaction observed, including keys that were
  /// read as absent (phantom inserts of a read key).
  Future<void> _validateReadSet(Transaction tx) async {
    for (final MapEntry<String, Uint8List?> entry in tx._readSet.entries) {
      final Uint8List? current = await _readCommitted(entry.key);
      final Uint8List? observed = entry.value;
      final bool same =
          (current == null && observed == null) ||
          (current != null &&
              observed != null &&
              _bytesEqual(current, observed));
      if (!same) {
        throw TransactionConflictException(
          'Read-set validation failed for transaction ${tx.id}: '
          'key "${entry.key}" changed after it was read',
          transactionId: tx.id,
        );
      }
    }
  }

  /// Optimistic validation: every key this transaction read or wrote must
  /// still be at the version it had when first accessed.
  void _validateVersions(Transaction tx) {
    for (final MapEntry<String, int> entry in tx._accessVersions.entries) {
      if (_versionOf(entry.key) != entry.value) {
        throw TransactionConflictException(
          'Optimistic validation failed for transaction ${tx.id}: '
          'key "${entry.key}" was modified concurrently',
          transactionId: tx.id,
        );
      }
    }
  }

  bool _bytesEqual(Uint8List a, Uint8List b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
}
