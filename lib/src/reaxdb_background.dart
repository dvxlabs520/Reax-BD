part of 'reaxdb.dart';

// The engine in a background isolate: `ReaxDB.open(background: true)`.
//
// The engine isolate opens an ordinary ReaxDB and serves it through an
// [_EngineHost]. The caller gets a [_BackgroundReaxDB], a subclass of ReaxDB
// with no engine of its own, which forwards every call as a message
// `[id, op, args...]`. `op` is the tear-off of one of the static `_op*`
// functions below: both isolates belong to the same isolate group, so
// functions and ordinary objects can be sent, and the VM copies them.
//
// Messages from one isolate to one port arrive in order. The engine sends
// what a call causes (mirror updates, change events, log records) before
// that call's reply, so when an awaited call returns, the caller's memory
// mirror and change-stream listeners have already seen its effects, as they
// would in the same isolate.

/// Opens every database with `background: true`, for running the test suite
/// in that mode. Not exported.
bool debugOpenInBackground = false;

/// Kills the engine isolate of [db], opened with `background: true`, as a
/// crash would. For tests. Not exported.
void debugKillBackgroundEngine(ReaxDB db) => (db as _BackgroundReaxDB)
    ._client
    ._isolate
    .kill(priority: Isolate.immediate);

/// An operation run by the engine on behalf of the caller.
typedef _Op = FutureOr<Object?> Function(_EngineHost host, List<Object?> m);

// Messages from the engine that are not replies start with one of these.
const int _evMirror = -1;
const int _evChange = -2;
const int _evLog = -3;
const int _evTxAttempt = -4;
const int _evUpgrade = -5;

/// Message id of a call that expects no reply.
const int _noReply = -1;

/// Entries per chunk of a remote scan, after the first.
const int _scanChunk = 512;

/// Entries in the first chunk of a remote scan: small, so `first` and short
/// scans do not read far ahead.
const int _scanFirstChunk = 64;

/// The open options that cross to the engine isolate: everything but the
/// callbacks, which stay with the caller.
final class _BackgroundOptions {
  _BackgroundOptions({
    required this.path,
    required this.encryption,
    required this.syncMode,
    required this.memtableSizeBytes,
    required this.cacheMaxEntries,
    required this.cacheMaxMemoryBytes,
    required this.cacheDefaultTtl,
    required this.changeStreamBufferSize,
    required this.keepInMemory,
    required this.logLevel,
    required this.loggerName,
    required this.defaultIsolationLevel,
    required this.lockTimeout,
    required this.schemaVersion,
    required this.hasUpgrade,
    required this.legacyMigration,
    required this.legacyEncryptionKey,
    required this.config,
    required this.encryptionKey,
  });

  final String path;
  final EncryptionConfig encryption;
  final SyncMode syncMode;
  final int memtableSizeBytes;
  final int cacheMaxEntries;
  final int cacheMaxMemoryBytes;
  final Duration? cacheDefaultTtl;
  final int changeStreamBufferSize;
  final bool keepInMemory;
  final LogLevel logLevel;
  final String? loggerName;
  final IsolationLevel defaultIsolationLevel;
  final Duration lockTimeout;
  final int schemaVersion;
  final bool hasUpgrade;
  final LegacyMigrationMode legacyMigration;
  final String? legacyEncryptionKey;
  // ignore: deprecated_member_use_from_same_package
  final DatabaseConfig? config;
  final String? encryptionKey;
}

/// Counters the caller reads synchronously, as of the engine's last reply.
final class _EngineStats {
  _EngineStats(this.cache, this.storage, this.transactions);

  final CacheStatistics cache;
  final StorageStats storage;
  final TransactionStats transactions;
}

/// What the caller needs to know about the engine's open database.
final class _EngineState {
  _EngineState({
    required this.path,
    required this.header,
    required this.syncMode,
    required this.encryptionType,
    required this.encryptionInfo,
    required this.indexes,
    required this.stats,
    required this.memory,
  });

  final String path;
  final _DatabaseHeader header;
  final SyncMode syncMode;
  final EncryptionType encryptionType;
  final Map<String, dynamic> encryptionInfo;
  final List<IndexDefinition> indexes;
  final _EngineStats stats;

  /// Every user entry, for `keepInMemory`: the caller keeps this copy.
  final MemoryStore? memory;
}

/// A database whose engine runs in a background isolate.
///
/// Reads that a memory mirror can answer ([get], [getSync], [exists],
/// [getBatch], [scan], [keys]) never leave this isolate; everything else is
/// forwarded to the engine.
final class _BackgroundReaxDB extends ReaxDB {
  _BackgroundReaxDB._(this._client, _EngineState state)
    : super._background(
        path: state.path,
        hub: ChangeStreamHub(
          maxBufferedEvents: _client.options.changeStreamBufferSize,
        ),
        logger: _client.logger,
        syncMode: state.syncMode,
        header: state.header,
        memory: state.memory,
      ) {
    _apply(state);
    _client.owner = this;
  }

  final _EngineClient _client;
  late EncryptionType _encryptionType;
  late Map<String, dynamic> _encryptionInfo;
  late List<IndexDefinition> _indexDefinitions;

  /// The engine's counters as of its last reply.
  late _EngineStats _stats;

  void _apply(_EngineState state) {
    _header = state.header;
    _encryptionType = state.encryptionType;
    _encryptionInfo = state.encryptionInfo;
    _indexDefinitions = state.indexes;
    _stats = state.stats;
  }

  /// Opens the database described by [options] in a new engine isolate.
  static Future<ReaxDB> open(
    _BackgroundOptions options, {
    required List<LogOutput>? logOutputs,
    required FutureOr<void> Function(int from, int to, ReaxDB db)? onUpgrade,
  }) async {
    final String canonical = ReaxDB._canonicalize(options.path);
    if (ReaxDB._instances.containsKey(canonical)) {
      throw DatabaseLockedException(
        'Database at "$canonical" is already open in this isolate. Close the '
        'existing ReaxDB instance before opening it again.',
        path: canonical,
      );
    }
    final _EngineClient client = await _EngineClient.spawn(
      options,
      logOutputs,
      canonical,
    );
    _BackgroundReaxDB? upgraded;
    (Object, StackTrace)? upgradeFailure;
    final Zone zone = Zone.current;
    client.onUpgrade = (int from, int to, _EngineState state) =>
        zone.run(() async {
          final _BackgroundReaxDB db = upgraded = _BackgroundReaxDB._(
            client,
            state,
          );
          ReaxDB._instances[canonical] = db;
          try {
            await onUpgrade!(from, to, db);
            client.notify(_opUpgradeDone, true);
          } catch (error, stack) {
            upgradeFailure = (error, stack);
            client.notify(_opUpgradeDone, false, _sendable(error));
          }
        });
    try {
      final _EngineState state =
          await client.call(_opOpen, options) as _EngineState;
      final _BackgroundReaxDB db;
      if (upgraded != null) {
        db = upgraded!.._apply(state);
      } else {
        db = _BackgroundReaxDB._(client, state);
      }
      ReaxDB._instances[canonical] = db;
      return db;
    } catch (error, stack) {
      client.closing = true;
      // A failed open exits the engine by itself; this covers a request
      // that never reached it.
      try {
        await client.call(_opClose);
      } catch (_) {
        // Already exiting.
      }
      await client.exited;
      // Ends anything onUpgrade started on the instance it was given.
      await upgraded?._detach();
      await client.logger.close();
      final (Object, StackTrace)? failure = upgradeFailure;
      if (failure != null) Error.throwWithStackTrace(failure.$1, failure.$2);
      Error.throwWithStackTrace(error, stack);
    }
  }

  // -- accessors ------------------------------------------------------------

  @override
  Map<String, dynamic> get encryptionInfo => _encryptionInfo;

  @override
  CacheStatistics get cacheStats => _stats.cache;

  @override
  TransactionStats get transactionStats => _stats.transactions;

  @override
  StorageStats get storageStats => _stats.storage;

  @override
  Map<String, dynamic> statistics() {
    _ensureOpen();
    final _EngineStats stats = _stats;
    return _statisticsOf(
      _encryptionType,
      stats.cache,
      stats.storage,
      stats.transactions,
    );
  }

  @override
  // ignore: deprecated_member_use_from_same_package
  Future<Map<String, dynamic>> getStatistics() async {
    _ensureOpen();
    await _client.call(_opStats);
    return statistics();
  }

  @override
  Map<String, dynamic> getPerformanceStats() {
    _ensureOpen();
    final _EngineStats stats = _stats;
    return ReaxDB._performanceStatsOf(
      stats.cache,
      stats.storage,
      stats.transactions,
    );
  }

  // -- reads ----------------------------------------------------------------

  @override
  Future<T?> get<T>(String key) async {
    if (_memory != null) return super.get<T>(key);
    _ensureOpen();
    _checkKeyEncodes(key);
    return ReaxDB._cast<T>(key, await _client.call(_opGet, key));
  }

  @override
  Future<Map<String, T?>> getBatch<T>(List<String> keys) async {
    if (_memory != null) return super.getBatch<T>(keys);
    _ensureOpen();
    final Map<String, Object?> values =
        await _client.call(_opGetBatch, keys) as Map<String, Object?>;
    return <String, T?>{
      for (final MapEntry<String, Object?> e in values.entries)
        e.key: ReaxDB._cast<T>(e.key, e.value),
    };
  }

  @override
  Future<bool> exists(String key) async {
    if (_memory != null) return super.exists(key);
    _ensureOpen();
    _checkKeyEncodes(key);
    return await _client.call(_opExists, key) as bool;
  }

  /// Validates [key] like [ReaxDB.encodeKey], without the round trip.
  static void _checkKeyEncodes(String key) => ReaxDB._checkKey(key);

  @override
  Stream<ReaxEntry<T>> scan<T>({
    String? startKey,
    String? endKey,
    int? limit,
    bool reverse = false,
  }) {
    if (_memory != null) {
      return super.scan<T>(
        startKey: startKey,
        endKey: endKey,
        limit: limit,
        reverse: reverse,
      );
    }
    return _remoteEntries<T>(<Object?>[
      false,
      startKey,
      endKey,
      limit,
      reverse,
    ], limit);
  }

  @override
  Stream<ReaxEntry<T>> scanPrefix<T>(
    String prefix, {
    int? limit,
    bool reverse = false,
  }) {
    if (_memory != null) {
      return super.scanPrefix<T>(prefix, limit: limit, reverse: reverse);
    }
    return _remoteEntries<T>(<Object?>[
      true,
      prefix,
      null,
      limit,
      reverse,
    ], limit);
  }

  Stream<ReaxEntry<T>> _remoteEntries<T>(
    List<Object?> request,
    int? limit,
  ) async* {
    _ensureOpen();
    await for (final List<Object?> chunk in _remoteChunks(
      _opScanOpen,
      request,
      limit,
    )) {
      for (int i = 0; i < chunk.length; i += 3) {
        final String key = chunk[i]! as String;
        yield ReaxEntry<T>(
          key: key,
          value: ReaxDB._cast<T>(key, chunk[i + 1]) as T,
          expiresAt: chunk[i + 2] as DateTime?,
        );
      }
    }
  }

  @override
  Stream<String> keys({String? prefix, int? limit, bool reverse = false}) {
    if (_memory != null) {
      return super.keys(prefix: prefix, limit: limit, reverse: reverse);
    }
    return _remoteKeys(prefix, limit, reverse);
  }

  Stream<String> _remoteKeys(String? prefix, int? limit, bool reverse) async* {
    _ensureOpen();
    await for (final List<Object?> chunk in _remoteChunks(
      _opKeysOpen,
      <Object?>[prefix, limit, reverse],
      limit,
    )) {
      for (final Object? key in chunk) {
        yield key! as String;
      }
    }
  }

  /// Pulls the chunks of a cursor the engine opens with [open], one chunk
  /// at a time, and closes the cursor when the listener stops early.
  Stream<List<Object?>> _remoteChunks(
    _Op open,
    List<Object?> request,
    int? limit,
  ) async* {
    final int first = limit != null && limit < _scanFirstChunk
        ? limit
        : _scanFirstChunk;
    List<Object?> reply =
        await _client.call(open, request, first) as List<Object?>;
    final int cursor = reply[0]! as int;
    bool done = reply[1]! as bool;
    try {
      while (true) {
        yield reply[2]! as List<Object?>;
        if (done) return;
        reply = await _client.call(_opCursorNext, cursor) as List<Object?>;
        done = reply[1]! as bool;
      }
    } finally {
      if (!done) _client.notify(_opCursorCancel, cursor);
    }
  }

  // -- writes ---------------------------------------------------------------

  @override
  Future<void> put(
    String key,
    Object? value, {
    Duration? ttl,
    DateTime? expiresAt,
    SyncMode? sync,
  }) {
    _ensureOpen();
    return _client.call(
      _opPut,
      key,
      value,
      ReaxDB._resolveExpiry(ttl, expiresAt),
      sync,
    );
  }

  @override
  Future<void> putBatch(
    Map<String, Object?> entries, {
    Duration? ttl,
    DateTime? expiresAt,
    SyncMode? sync,
  }) {
    _ensureOpen();
    return _client.call(
      _opPutBatch,
      entries,
      ReaxDB._resolveExpiry(ttl, expiresAt),
      sync,
    );
  }

  @override
  Future<void> delete(String key, {SyncMode? sync}) {
    _ensureOpen();
    return _client.call(_opDelete, key, sync);
  }

  @override
  Future<void> deleteBatch(List<String> keys, {SyncMode? sync}) {
    _ensureOpen();
    return _client.call(_opDeleteBatch, keys, sync);
  }

  @override
  Future<int> _reclaimExpired(List<String> keys) async =>
      await _client.call(_opReclaimExpired, keys) as int;

  @override
  Future<int> purgeExpired() async {
    _ensureOpen();
    return await _client.call(_opPurgeExpired) as int;
  }

  // -- transactions -----------------------------------------------------------

  @override
  Future<T> transaction<T>(
    Future<T> Function(ReaxTransaction tx) body, {
    IsolationLevel? isolationLevel,
    int maxAttempts = 3,
  }) async {
    _ensureOpen();
    final _RemoteTransactionBody<T> remote = _RemoteTransactionBody<T>(
      this,
      body,
    );
    final int id = _client.registerTransaction(remote);
    try {
      await _client.call(_opTransaction, id, isolationLevel, maxAttempts);
    } catch (error, stack) {
      // The body's own error, not the copy the engine rethrew.
      final (Object, StackTrace)? failure = remote.failure;
      if (failure != null) Error.throwWithStackTrace(failure.$1, failure.$2);
      Error.throwWithStackTrace(error, stack);
    } finally {
      _client.unregisterTransaction(id);
    }
    return remote.result as T;
  }

  @override
  Future<bool> compareAndSwap<T>(
    String key,
    T? expectedValue,
    T newValue, {
    Duration? ttl,
    DateTime? expiresAt,
  }) async {
    _ensureOpen();
    return await _client.call(
          _opCompareAndSwap,
          key,
          expectedValue,
          newValue,
          ReaxDB._resolveExpiry(ttl, expiresAt),
        )
        as bool;
  }

  // -- queries and indexes ----------------------------------------------------

  @override
  QueryBuilder query(String collection) {
    _ensureOpen();
    validateCollectionName(collection);
    return QueryBuilder(
      collection: collection,
      store: _documentStore,
      runner: _runQuery,
    );
  }

  Future<Object?> _runQuery(
    QuerySpec spec,
    QueryTerminal terminal,
    Object? argument,
  ) {
    _ensureOpen();
    return _client.call(_opQuery, spec, terminal, argument);
  }

  @override
  Future<void> createIndex(String collection, List<String> fields) async {
    _ensureOpen();
    _indexDefinitions =
        await _client.call(_opCreateIndex, collection, fields)
            as List<IndexDefinition>;
  }

  @override
  Future<void> dropIndex(String collection, List<String> fields) async {
    _ensureOpen();
    _indexDefinitions =
        await _client.call(_opDropIndex, collection, fields)
            as List<IndexDefinition>;
  }

  @override
  bool hasIndex(String collection, String field) {
    _ensureOpen();
    return _indexDefinitions.any(
      (IndexDefinition d) =>
          d.collection == collection && d.fields.first == field,
    );
  }

  @override
  List<IndexDefinition> listIndexes([String? collection]) {
    _ensureOpen();
    return _indexDefinitions
        .where(
          (IndexDefinition d) =>
              collection == null || d.collection == collection,
        )
        .toList();
  }

  // -- change streams ---------------------------------------------------------

  @override
  Stream<DatabaseChangeEvent> watch(String pattern) {
    _ensureOpen();
    // The events come from the engine into this isolate's own hub, which
    // keeps the buffering, pause and overflow behaviour of the local mode.
    // The engine forwards an event only while some pattern here matches it.
    final Stream<DatabaseChangeEvent> local = _hub.watch(pattern);
    return Stream<DatabaseChangeEvent>.multi((
      MultiStreamController<DatabaseChangeEvent> controller,
    ) {
      bool watching = true;
      void release() {
        if (!watching) return;
        watching = false;
        _client.notify(_opWatchOff, pattern);
      }

      _client.notify(_opWatchOn, pattern);
      final StreamSubscription<DatabaseChangeEvent> subscription = local.listen(
        controller.addSync,
        onError: controller.addErrorSync,
        onDone: () {
          release();
          controller.closeSync();
        },
      );
      controller
        ..onPause = subscription.pause
        ..onResume = subscription.resume
        ..onCancel = () {
          release();
          return subscription.cancel();
        };
    });
  }

  // -- maintenance ------------------------------------------------------------

  @override
  Future<void> flush() async {
    _ensureOpen();
    await _client.call(_opFlush);
  }

  @override
  Future<void> compact() async {
    _ensureOpen();
    await _client.call(_opCompact);
  }

  @override
  Future<DatabaseInfo> info() async {
    _ensureOpen();
    return await _client.call(_opInfo) as DatabaseInfo;
  }

  @override
  Future<int> exportTo(
    String archivePath, {
    EncryptionConfig encryption = const EncryptionConfig.none(),
  }) async {
    _ensureOpen();
    return await _client.call(_opExport, archivePath, encryption) as int;
  }

  @override
  Future<ReaxDB> rekey(EncryptionConfig newEncryption) async {
    _ensureOpen();
    _closing = true;
    final _EngineState state;
    try {
      state = await _client.call(_opRekey, newEncryption) as _EngineState;
    } catch (_) {
      // The engine closed its database, as rekey does locally.
      await close();
      rethrow;
    }
    // This instance is closed, as after a local rekey, and a new one takes
    // over the same engine isolate.
    await _detach();
    _client.logger = _client.newLogger();
    final _BackgroundReaxDB next = _BackgroundReaxDB._(_client, state);
    ReaxDB._instances[_path] = next;
    return next;
  }

  // -- lifecycle ----------------------------------------------------------------

  @override
  Future<void> close() {
    if (_closed) return Future<void>.value();
    return _closeFuture ??= _closeRemote();
  }

  Future<void> _closeRemote() async {
    _closing = true;
    Object? error;
    StackTrace? stack;
    _client.closing = true;
    try {
      await _client.call(_opClose);
    } catch (e, s) {
      // When the engine already stopped there is nothing left to close.
      if (!_client._gone) {
        error = e;
        stack = s;
      }
    }
    // The engine isolate exits after its reply to close.
    await _client.exited;
    await _detach();
    if (error != null) Error.throwWithStackTrace(error, stack!);
  }

  /// Releases what this instance holds in this isolate.
  Future<void> _detach() async {
    if (identical(_client.owner, this)) _client.owner = null;
    if (identical(ReaxDB._instances[_path], this)) {
      ReaxDB._instances.remove(_path);
    }
    _memory?.clear();
    try {
      await _hub.close();
    } finally {
      await _logger.close();
      _closed = true;
      _closing = false;
    }
  }

  /// Called when the engine isolate exits without being asked to.
  void _engineGone(Object error) {
    unawaited(
      _logger.error('The background engine of "$_path" stopped: $error'),
    );
    unawaited(_hub.close());
  }

  void _onMirror(List<Object?> keys, List<Object?> envelopes) {
    final MemoryStore? memory = _memory;
    if (memory == null || _closed) return;
    for (int i = 0; i < keys.length; i++) {
      final String key = keys[i]! as String;
      final Uint8List? envelope = envelopes[i] as Uint8List?;
      if (envelope == null) {
        memory.remove(key);
      } else {
        memory.putEncoded(key, envelope, RecordEnvelope.peekExpiry(envelope));
      }
    }
  }

  void _onChange(DatabaseChangeEvent event) {
    if (!_hub.isClosed) _hub.publish(event);
  }
}

/// The caller's half of a transaction whose engine runs in the background.
///
/// The engine runs the transaction and asks for the body once per attempt;
/// the body runs here, against a [ReaxTransaction] that forwards its reads
/// and writes. Its result never leaves this isolate.
final class _RemoteTransactionBody<T> {
  _RemoteTransactionBody(this._db, this._body);

  final _BackgroundReaxDB _db;
  final Future<T> Function(ReaxTransaction tx) _body;

  /// The zone [ReaxDB.transaction] was called in, where a local
  /// transaction would run its body.
  final Zone _zone = Zone.current;

  /// The value of the last attempt whose body completed.
  T? result;

  /// The error of the last attempt, when its body threw.
  (Object, StackTrace)? failure;

  Future<void> run(int handle, int id, IsolationLevel level) =>
      _zone.run(() => _run(handle, id, level));

  Future<void> _run(int handle, int id, IsolationLevel level) async {
    final _EngineClient client = _db._client;
    try {
      result = await _body(
        ReaxTransaction._remote(_db, client, handle, id, level),
      );
      failure = null;
      client.notify(_opTxDone, handle, true);
    } catch (error, stack) {
      failure = (error, stack);
      // A copy, so the engine can decide whether to retry (a conflict) the
      // same way it does for a local transaction.
      client.notify(_opTxDone, handle, false, _sendable(error));
    }
  }
}

/// [error] if it can be sent to another isolate, otherwise null.
Object? _sendable(Object error) {
  try {
    final RawReceivePort probe = RawReceivePort();
    try {
      probe.sendPort.send(error);
    } finally {
      probe.close();
    }
    return error;
  } catch (_) {
    return null;
  }
}

/// The caller's end of the connection to an engine isolate.
final class _EngineClient {
  _EngineClient._(this._port, this.options, this.logOutputs, this.canonical)
    : logger = ReaxLogger(
        level: options.logLevel,
        outputs: logOutputs,
        name: options.loggerName ?? p.basename(canonical),
      );

  final RawReceivePort _port;
  late final SendPort _engine;
  late final Isolate _isolate;
  final _BackgroundOptions options;
  final List<LogOutput>? logOutputs;
  final String canonical;

  /// The caller-side logger; engine log records are written here.
  ReaxLogger logger;

  /// The open database this connection serves, null before open and after
  /// close.
  _BackgroundReaxDB? owner;

  /// Runs `onUpgrade` here while the engine's open waits for it.
  Future<void> Function(int from, int to, _EngineState state)? onUpgrade;

  final Map<int, Completer<Object?>> _pending = <int, Completer<Object?>>{};
  int _nextId = 0;
  final Map<int, _RemoteTransactionBody<Object?>> _transactions =
      <int, _RemoteTransactionBody<Object?>>{};
  int _nextTransaction = 0;
  final Completer<void> _exited = Completer<void>();
  bool _gone = false;

  /// Set when the engine was asked to exit, so its exit is not an error.
  bool closing = false;

  /// Completes when the engine isolate has exited.
  Future<void> get exited {
    if (!_exited.isCompleted) _port.keepIsolateAlive = true;
    return _exited.future;
  }

  ReaxLogger newLogger() => ReaxLogger(
    level: options.logLevel,
    outputs: logOutputs,
    name: options.loggerName ?? p.basename(canonical),
  );

  static Future<_EngineClient> spawn(
    _BackgroundOptions options,
    List<LogOutput>? logOutputs,
    String canonical,
  ) async {
    final RawReceivePort port = RawReceivePort();
    final _EngineClient client = _EngineClient._(
      port,
      options,
      logOutputs,
      canonical,
    );
    final Completer<SendPort> ready = Completer<SendPort>();
    port.handler = (Object? message) {
      if (message is SendPort) {
        ready.complete(message);
      } else if (!ready.isCompleted) {
        ready.completeError(
          StorageException(
            'The background engine for "$canonical" failed to start',
            path: canonical,
          ),
        );
      }
    };
    try {
      client._isolate = await Isolate.spawn(
        _engineMain,
        port.sendPort,
        onExit: port.sendPort,
        errorsAreFatal: false,
        debugName: 'ReaxDB ${p.basename(canonical)}',
      );
      client._engine = await ready.future;
    } catch (_) {
      port.close();
      await client.logger.close();
      rethrow;
    }
    port
      ..handler = client._onMessage
      ..keepIsolateAlive = false;
    return client;
  }

  /// Sends [op] with its arguments and returns the engine's reply.
  Future<Object?> call(_Op op, [Object? a, Object? b, Object? c, Object? d]) {
    if (_gone) return Future<Object?>.error(_goneError());
    final int id = _nextId++;
    try {
      _engine.send(<Object?>[id, op, a, b, c, d]);
    } on ArgumentError catch (error) {
      return Future<Object?>.error(
        SerializationException(
          'A value cannot be sent to the background engine: $error',
          cause: error,
        ),
      );
    }
    final Completer<Object?> completer = Completer<Object?>();
    if (_pending.isEmpty) _port.keepIsolateAlive = true;
    _pending[id] = completer;
    return completer.future;
  }

  /// Sends [op] without waiting for, or receiving, a reply.
  void notify(_Op op, [Object? a, Object? b, Object? c]) {
    if (_gone) return;
    _engine.send(<Object?>[_noReply, op, a, b, c]);
  }

  int registerTransaction(_RemoteTransactionBody<Object?> body) {
    final int id = _nextTransaction++;
    _transactions[id] = body;
    return id;
  }

  void unregisterTransaction(int id) => _transactions.remove(id);

  StorageException _goneError() => StorageException(
    'The background engine of "$canonical" is not running',
    path: canonical,
  );

  void _onMessage(Object? raw) {
    if (raw == null) {
      _onExit();
      return;
    }
    final List<Object?> m = raw as List<Object?>;
    final int tag = m[0]! as int;
    if (tag >= 0) {
      final Completer<Object?> completer = _pending.remove(tag)!;
      if (_pending.isEmpty && !_gone) _port.keepIsolateAlive = false;
      if (m.length == 3) {
        final Object? stats = m[2];
        if (stats != null) owner?._stats = stats as _EngineStats;
        completer.complete(m[1]);
      } else {
        completer.completeError(m[1]!, StackTrace.fromString(m[2]! as String));
      }
      return;
    }
    switch (tag) {
      case _evMirror:
        owner?._onMirror(m[1]! as List<Object?>, m[2]! as List<Object?>);
      case _evChange:
        owner?._onChange(m[1]! as DatabaseChangeEvent);
      case _evLog:
        unawaited(_log(m[1]! as LogLevel, m[2]! as String, m[3]));
      case _evTxAttempt:
        unawaited(
          _transactions[m[1]! as int]?.run(
            m[2]! as int,
            m[3]! as int,
            m[4]! as IsolationLevel,
          ),
        );
      case _evUpgrade:
        unawaited(
          onUpgrade!(m[1]! as int, m[2]! as int, m[3]! as _EngineState),
        );
    }
  }

  Future<void> _log(LogLevel level, String message, Object? metadata) {
    final Map<String, dynamic>? data = metadata as Map<String, dynamic>?;
    return switch (level) {
      LogLevel.error => logger.error(message, metadata: data),
      LogLevel.warning => logger.warning(message, metadata: data),
      LogLevel.info => logger.info(message, metadata: data),
      LogLevel.debug || LogLevel.none => logger.debug(message, metadata: data),
    };
  }

  void _onExit() {
    _gone = true;
    _port.close();
    final List<Completer<Object?>> pending = _pending.values.toList();
    _pending.clear();
    final StorageException error = StorageException(
      'The background engine of "$canonical" '
      '${closing ? 'has exited' : 'stopped unexpectedly'}',
      path: canonical,
    );
    for (final Completer<Object?> completer in pending) {
      completer.completeError(error);
    }
    if (!closing) owner?._engineGone(error);
    _exited.complete();
  }
}

// -- the engine isolate ---------------------------------------------------------

void _engineMain(SendPort caller) {
  final _EngineHost host = _EngineHost(caller);
  runZonedGuarded(host.start, host.uncaught);
}

/// Serves one open database to the isolate that opened it.
final class _EngineHost {
  _EngineHost(this.caller);

  final SendPort caller;
  late final RawReceivePort _port;
  ReaxDB? _db;
  bool _mirror = false;

  /// The call whose reply ends the isolate: close, or a failed open.
  int? _exitWith;

  final _PatternSet _patterns = _PatternSet();
  final Map<int, ReaxTransaction> _transactions = <int, ReaxTransaction>{};
  final Map<int, Completer<void>> _bodies = <int, Completer<void>>{};
  int _nextHandle = 0;
  final Map<int, _Cursor> _cursors = <int, _Cursor>{};
  int _nextCursor = 0;
  Completer<void>? _upgrade;

  ReaxDB get db {
    final ReaxDB? db = _db;
    if (db == null) {
      throw const DatabaseClosedException('The database is not open');
    }
    return db;
  }

  void start() {
    _port = RawReceivePort(_handle, 'ReaxDB engine');
    caller.send(_port.sendPort);
  }

  void uncaught(Object error, StackTrace stack) {
    _send(<Object?>[
      _evLog,
      LogLevel.error,
      'Uncaught error in the background engine: $error',
      <String, dynamic>{'stackTrace': stack.toString()},
    ]);
  }

  void _handle(Object? raw) {
    final List<Object?> m = raw! as List<Object?>;
    final int id = m[0]! as int;
    final _Op op = m[1]! as _Op;
    final FutureOr<Object?> result;
    try {
      result = op(this, m);
    } catch (error, stack) {
      _fail(id, error, stack);
      return;
    }
    if (result is Future<Object?>) {
      result.then(
        (Object? value) => _reply(id, value),
        onError: (Object error, StackTrace stack) => _fail(id, error, stack),
      );
    } else {
      _reply(id, result);
    }
  }

  void _send(List<Object?> message) {
    try {
      caller.send(message);
    } catch (_) {
      // The caller is gone, or a value cannot be sent: nothing to do here.
    }
  }

  void _reply(int id, Object? value) {
    if (id == _noReply) return;
    if (id == _exitWith) _exit(<Object?>[id, value, null]);
    // The counters travel with every reply, so the caller's synchronous
    // stats getters are as current as the last call it awaited.
    final ReaxDB? db = _db;
    try {
      caller.send(<Object?>[
        id,
        value,
        db == null || db._closed ? null : _statsOf(db),
      ]);
    } on ArgumentError catch (error, stack) {
      _fail(id, error, stack);
    }
  }

  void _fail(int id, Object error, StackTrace stack) {
    if (id == _noReply) {
      uncaught(error, stack);
      return;
    }
    final List<Object?> message = <Object?>[
      id,
      _sendable(error) ?? RemoteError('$error', '$stack'),
      '$stack',
      null,
    ];
    if (id == _exitWith) _exit(message);
    caller.send(message);
  }

  Never _exit(List<Object?> lastReply) {
    _port.close();
    Isolate.exit(caller, lastReply);
  }

  /// Starts serving [db]: forwards its mirror updates and change events.
  void attach(ReaxDB db) {
    _db = db;
    if (_mirror) {
      db._onApplied = (List<String> keys, List<Uint8List?> envelopes) =>
          caller.send(<Object?>[_evMirror, keys, envelopes]);
    }
    db._hub.tap = (DatabaseChangeEvent event) {
      if (_patterns.matches(event.key)) _send(<Object?>[_evChange, event]);
    };
  }

  static _EngineStats _statsOf(ReaxDB db) =>
      _EngineStats(db._cache.stats, db._storage.stats, db._transactions.stats);

  Future<_EngineState> describe(ReaxDB db, {bool withMemory = true}) async {
    MemoryStore? memory;
    if (_mirror && withMemory) {
      memory = MemoryStore();
      await db._loadMemoryInto(memory);
    }
    return _EngineState(
      path: db._path,
      header: db._header,
      syncMode: db._syncMode,
      encryptionType: db._encryption.type,
      encryptionInfo: db.encryptionInfo,
      indexes: db.listIndexes(),
      stats: _statsOf(db),
      memory: memory,
    );
  }

  Future<void> _runUpgrade(int from, int to, ReaxDB db) async {
    attach(db);
    final Completer<void> done = _upgrade = Completer<void>();
    caller.send(<Object?>[_evUpgrade, from, to, await describe(db)]);
    try {
      await done.future;
    } finally {
      _upgrade = null;
    }
  }

  ReaxTransaction transaction(Object? handle) =>
      _transactions[handle! as int] ??
      (throw const TransactionConflictException(
        'The transaction is no longer active',
      ));
}

/// The change-stream patterns some subscription in the caller listens to.
final class _PatternSet {
  final Map<String, int> _exact = <String, int>{};
  final Map<String, int> _prefixes = <String, int>{};
  final Map<String, (RegExp, int)> _globs = <String, (RegExp, int)>{};

  bool get isEmpty => _exact.isEmpty && _prefixes.isEmpty && _globs.isEmpty;

  void add(String pattern) {
    final int star = pattern.indexOf('*');
    if (star < 0) {
      _exact[pattern] = (_exact[pattern] ?? 0) + 1;
    } else if (star == pattern.length - 1) {
      final String prefix = pattern.substring(0, star);
      _prefixes[prefix] = (_prefixes[prefix] ?? 0) + 1;
    } else {
      final (RegExp, int)? glob = _globs[pattern];
      _globs[pattern] = (
        glob?.$1 ??
            RegExp('^${pattern.split('*').map(RegExp.escape).join('.*')}\$'),
        (glob?.$2 ?? 0) + 1,
      );
    }
  }

  void remove(String pattern) {
    final int star = pattern.indexOf('*');
    if (star < 0) {
      _release(_exact, pattern);
    } else if (star == pattern.length - 1) {
      _release(_prefixes, pattern.substring(0, star));
    } else {
      final (RegExp, int)? glob = _globs[pattern];
      if (glob == null) return;
      if (glob.$2 <= 1) {
        _globs.remove(pattern);
      } else {
        _globs[pattern] = (glob.$1, glob.$2 - 1);
      }
    }
  }

  static void _release(Map<String, int> counts, String key) {
    final int? count = counts[key];
    if (count == null) return;
    if (count <= 1) {
      counts.remove(key);
    } else {
      counts[key] = count - 1;
    }
  }

  bool matches(String key) {
    if (isEmpty) return false;
    if (_exact.containsKey(key)) return true;
    for (final String prefix in _prefixes.keys) {
      if (key.startsWith(prefix)) return true;
    }
    for (final (RegExp, int) glob in _globs.values) {
      if (glob.$1.hasMatch(key)) return true;
    }
    return false;
  }
}

/// A scan the caller pulls in chunks.
final class _Cursor {
  _Cursor(Stream<Object?> source, this._width)
    : _iterator = StreamIterator<Object?>(source);

  final StreamIterator<Object?> _iterator;

  /// Slots per item in a chunk: 3 for entries, 1 for keys.
  final int _width;

  /// The next [count] items, flattened, and whether the scan is done.
  Future<(List<Object?>, bool)> next(int count) async {
    final List<Object?> chunk = <Object?>[];
    while (chunk.length < count * _width) {
      if (!await _iterator.moveNext()) return (chunk, true);
      final Object? item = _iterator.current;
      if (item is ReaxEntry<Object?>) {
        chunk
          ..add(item.key)
          ..add(item.value)
          ..add(item.expiresAt);
      } else {
        chunk.add(item);
      }
    }
    return (chunk, false);
  }

  Future<void> cancel() => _iterator.cancel();
}

// -- operations -------------------------------------------------------------------
//
// Each runs in the engine isolate; `m` is `[id, op, a, b, c, d]`.

/// A transaction body that failed in the caller with an error that cannot
/// be sent; aborts the transaction without a retry.
final class _RemoteBodyFailed implements Exception {
  const _RemoteBodyFailed();
}

Future<Object?> _opOpen(_EngineHost host, List<Object?> m) async {
  final _BackgroundOptions o = m[2]! as _BackgroundOptions;
  host._mirror = o.keepInMemory;
  bool upgraded = false;
  try {
    final ReaxDB db = await ReaxDB._openLocal(
      path: o.path,
      encryption: o.encryption,
      syncMode: o.syncMode,
      memtableSizeBytes: o.memtableSizeBytes,
      cacheMaxEntries: o.cacheMaxEntries,
      cacheMaxMemoryBytes: o.cacheMaxMemoryBytes,
      cacheDefaultTtl: o.cacheDefaultTtl,
      changeStreamBufferSize: o.changeStreamBufferSize,
      // The caller keeps the values in memory; the engine reads its LSM.
      keepInMemory: false,
      // Everything is forwarded; the caller's logger applies its level.
      logLevel: LogLevel.debug,
      logOutputs: <LogOutput>[_ForwardingLogOutput(host)],
      loggerName: o.loggerName,
      defaultIsolationLevel: o.defaultIsolationLevel,
      lockTimeout: o.lockTimeout,
      schemaVersion: o.schemaVersion,
      onUpgrade: o.hasUpgrade
          ? (int from, int to, ReaxDB db) {
              upgraded = true;
              return host._runUpgrade(from, to, db);
            }
          : null,
      legacyMigration: o.legacyMigration,
      legacyEncryptionKey: o.legacyEncryptionKey,
      // ignore: deprecated_member_use_from_same_package
      config: o.config,
      // ignore: deprecated_member_use_from_same_package
      encryptionKey: o.encryptionKey,
    );
    host.attach(db);
    // After an upgrade the caller already holds the values, kept current.
    return await host.describe(db, withMemory: !upgraded);
  } catch (_) {
    host._exitWith = m[0]! as int;
    rethrow;
  }
}

Object? _opUpgradeDone(_EngineHost host, List<Object?> m) {
  final Completer<void>? upgrade = host._upgrade;
  if (upgrade == null) return null;
  if (m[2]! as bool) {
    upgrade.complete();
  } else {
    upgrade.completeError(m[3] ?? const _RemoteBodyFailed());
  }
  return null;
}

Future<Object?> _opClose(_EngineHost host, List<Object?> m) async {
  host._exitWith = m[0]! as int;
  for (final _Cursor cursor in host._cursors.values) {
    unawaited(cursor.cancel());
  }
  host._cursors.clear();
  await host._db?.close();
  return null;
}

Future<Object?> _opRekey(_EngineHost host, List<Object?> m) async {
  final ReaxDB next = await host.db.rekey(m[2]! as EncryptionConfig);
  host.attach(next);
  return host.describe(next);
}

/// Nothing: the reply brings the counters, as every reply does.
Object? _opStats(_EngineHost host, List<Object?> m) => null;

Object? _opWatchOn(_EngineHost host, List<Object?> m) {
  host._patterns.add(m[2]! as String);
  return null;
}

Object? _opWatchOff(_EngineHost host, List<Object?> m) {
  host._patterns.remove(m[2]! as String);
  return null;
}

Future<Object?> _opGet(_EngineHost host, List<Object?> m) =>
    host.db.get<Object?>(m[2]! as String);

Future<Object?> _opGetBatch(_EngineHost host, List<Object?> m) =>
    host.db.getBatch<Object?>(m[2]! as List<String>);

Future<Object?> _opExists(_EngineHost host, List<Object?> m) =>
    host.db.exists(m[2]! as String);

Future<Object?> _opPut(_EngineHost host, List<Object?> m) => host.db.put(
  m[2]! as String,
  m[3],
  expiresAt: m[4] as DateTime?,
  sync: m[5] as SyncMode?,
);

Future<Object?> _opPutBatch(_EngineHost host, List<Object?> m) =>
    host.db.putBatch(
      m[2]! as Map<String, Object?>,
      expiresAt: m[3] as DateTime?,
      sync: m[4] as SyncMode?,
    );

Future<Object?> _opDelete(_EngineHost host, List<Object?> m) =>
    host.db.delete(m[2]! as String, sync: m[3] as SyncMode?);

Future<Object?> _opDeleteBatch(_EngineHost host, List<Object?> m) =>
    host.db.deleteBatch(m[2]! as List<String>, sync: m[3] as SyncMode?);

Future<Object?> _opReclaimExpired(_EngineHost host, List<Object?> m) {
  final ReaxDB db = host.db;
  db._ensureOpen();
  return db._track(() => db._reclaimExpired(m[2]! as List<String>));
}

Future<Object?> _opPurgeExpired(_EngineHost host, List<Object?> m) =>
    host.db.purgeExpired();

Future<Object?> _opTransaction(_EngineHost host, List<Object?> m) {
  final int call = m[2]! as int;
  return host.db.transaction<Object?>(
    (ReaxTransaction tx) async {
      final int handle = host._nextHandle++;
      final Completer<void> body = Completer<void>();
      host._transactions[handle] = tx;
      host._bodies[handle] = body;
      host.caller.send(<Object?>[
        _evTxAttempt,
        call,
        handle,
        tx.id,
        tx.isolationLevel,
      ]);
      try {
        await body.future;
      } finally {
        host._transactions.remove(handle);
        host._bodies.remove(handle);
      }
      return null;
    },
    isolationLevel: m[3] as IsolationLevel?,
    maxAttempts: m[4]! as int,
  );
}

Object? _opTxDone(_EngineHost host, List<Object?> m) {
  final Completer<void>? body = host._bodies[m[2]! as int];
  if (body == null) return null;
  if (m[3]! as bool) {
    body.complete();
  } else {
    body.completeError(m[4] ?? const _RemoteBodyFailed());
  }
  return null;
}

Future<Object?> _opTxGet(_EngineHost host, List<Object?> m) =>
    host.transaction(m[2]).get<Object?>(m[3]! as String);

Future<Object?> _opTxPut(_EngineHost host, List<Object?> m) => host
    .transaction(m[2])
    .put(m[3]! as String, m[4], expiresAt: m[5] as DateTime?);

Future<Object?> _opTxDelete(_EngineHost host, List<Object?> m) =>
    host.transaction(m[2]).delete(m[3]! as String);

Future<Object?> _opCompareAndSwap(_EngineHost host, List<Object?> m) =>
    host.db.compareAndSwap<Object?>(
      m[2]! as String,
      m[3],
      m[4],
      expiresAt: m[5] as DateTime?,
    );

Future<Object?> _opQuery(_EngineHost host, List<Object?> m) {
  final QuerySpec spec = m[2]! as QuerySpec;
  return runQuerySpec(
    host.db.query(spec.collection),
    spec,
    m[3]! as QueryTerminal,
    m[4],
  );
}

Future<Object?> _opCreateIndex(_EngineHost host, List<Object?> m) async {
  final ReaxDB db = host.db;
  await db.createIndex(m[2]! as String, m[3]! as List<String>);
  return db.listIndexes();
}

Future<Object?> _opDropIndex(_EngineHost host, List<Object?> m) async {
  final ReaxDB db = host.db;
  await db.dropIndex(m[2]! as String, m[3]! as List<String>);
  return db.listIndexes();
}

Future<Object?> _opFlush(_EngineHost host, List<Object?> m) => host.db.flush();

Future<Object?> _opCompact(_EngineHost host, List<Object?> m) =>
    host.db.compact();

Future<Object?> _opInfo(_EngineHost host, List<Object?> m) => host.db.info();

Future<Object?> _opExport(_EngineHost host, List<Object?> m) =>
    host.db.exportTo(m[2]! as String, encryption: m[3]! as EncryptionConfig);

Future<Object?> _opScanOpen(_EngineHost host, List<Object?> m) {
  final List<Object?> r = m[2]! as List<Object?>;
  final ReaxDB db = host.db;
  final Stream<ReaxEntry<Object?>> source = r[0]! as bool
      ? db.scanPrefix<Object?>(
          r[1]! as String,
          limit: r[3] as int?,
          reverse: r[4]! as bool,
        )
      : db.scan<Object?>(
          startKey: r[1] as String?,
          endKey: r[2] as String?,
          limit: r[3] as int?,
          reverse: r[4]! as bool,
        );
  return _openCursor(host, _Cursor(source, 3), m[3]! as int);
}

Future<Object?> _opKeysOpen(_EngineHost host, List<Object?> m) {
  final List<Object?> r = m[2]! as List<Object?>;
  final Stream<String> source = host.db.keys(
    prefix: r[0] as String?,
    limit: r[1] as int?,
    reverse: r[2]! as bool,
  );
  return _openCursor(host, _Cursor(source, 1), m[3]! as int);
}

Future<Object?> _openCursor(_EngineHost host, _Cursor cursor, int first) async {
  final int id = host._nextCursor++;
  host._cursors[id] = cursor;
  return _nextChunk(host, id, cursor, first);
}

Future<Object?> _nextChunk(
  _EngineHost host,
  int id,
  _Cursor cursor,
  int count,
) async {
  try {
    final (List<Object?> chunk, bool done) = await cursor.next(count);
    if (done) host._cursors.remove(id);
    return <Object?>[id, done, chunk];
  } catch (_) {
    host._cursors.remove(id);
    unawaited(cursor.cancel());
    rethrow;
  }
}

Future<Object?> _opCursorNext(_EngineHost host, List<Object?> m) {
  final int id = m[2]! as int;
  final _Cursor? cursor = host._cursors[id];
  if (cursor == null) {
    throw const DatabaseClosedException('The scan was closed');
  }
  return _nextChunk(host, id, cursor, _scanChunk);
}

Object? _opCursorCancel(_EngineHost host, List<Object?> m) {
  final _Cursor? cursor = host._cursors.remove(m[2]! as int);
  if (cursor != null) unawaited(cursor.cancel());
  return null;
}

/// Sends the engine's log records to the caller, whose logger writes them.
final class _ForwardingLogOutput extends LogOutput {
  _ForwardingLogOutput(this._host);

  final _EngineHost _host;

  @override
  Future<void> write(
    LogLevel level,
    String message, {
    Map<String, dynamic>? metadata,
  }) async {
    // The caller's logger adds the same "[name] " prefix again.
    final int end = message.startsWith('[') ? message.indexOf('] ') : -1;
    _host._send(<Object?>[
      _evLog,
      level,
      end < 0 ? message : message.substring(end + 2),
      metadata,
    ]);
  }
}
