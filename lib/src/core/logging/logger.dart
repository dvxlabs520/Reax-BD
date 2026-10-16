import 'log_level.dart';
import 'log_output.dart';

/// ReaxDB logger with per-instance configuration.
///
/// Each database instance owns its own [ReaxLogger], so two databases in the
/// same process can log at different levels to different outputs. A shared
/// process-wide default remains available as [ReaxLogger.root] (also exposed
/// as the top-level [logger]) for code that is not tied to one database.
///
/// ## No-PII policy
///
/// ReaxDB never logs database key contents, value contents, encryption keys,
/// passphrases, or derived key material — at any level, including debug. Use
/// the helpers in `redaction.dart` (`Redaction.key`, `Redaction.value`) when
/// a message needs to reference a key or value.
///
/// ## Ownership
///
/// The outputs attached to a logger are owned by it: [close] closes them
/// (for example flushing and closing a `FileLogOutput` sink). A database
/// closes its own logger when the database is closed. After [close], further
/// log calls are silently dropped.
final class ReaxLogger {
  /// Creates a logger with its own level and outputs.
  ///
  /// If [outputs] is omitted, a [ConsoleLogOutput] is attached when running
  /// in a non-product build, and nothing otherwise. [name], when provided,
  /// is prefixed to every message (useful to distinguish databases).
  ReaxLogger({
    this._level = LogLevel.info,
    List<LogOutput>? outputs,
    this._enabled = true,
    this.name,
  }) : _outputs = outputs != null
           ? List<LogOutput>.of(outputs)
           : <LogOutput>[
               if (const bool.fromEnvironment('dart.vm.product') == false)
                 ConsoleLogOutput(),
             ];

  /// Process-wide default logger.
  static final ReaxLogger root = ReaxLogger();

  /// Optional name prefixed to every message.
  final String? name;

  LogLevel _level;
  bool _enabled;
  bool _closed = false;
  final List<LogOutput> _outputs;

  /// The current level threshold.
  LogLevel get level => _level;

  /// Whether logging is enabled.
  bool get isEnabled => _enabled && !_closed;

  /// Reconfigures this logger instance.
  ///
  /// When [outputs] is provided the previous outputs are replaced but NOT
  /// closed — close them yourself if this logger owned them exclusively.
  void configure({LogLevel? level, List<LogOutput>? outputs, bool? enabled}) {
    if (level != null) _level = level;
    if (outputs != null) {
      _outputs
        ..clear()
        ..addAll(outputs);
    }
    if (enabled != null) _enabled = enabled;
  }

  /// Adds a log output. The logger takes ownership (see [close]).
  void addOutput(LogOutput output) => _outputs.add(output);

  /// Removes a log output without closing it.
  void removeOutput(LogOutput output) => _outputs.remove(output);

  /// Removes all outputs without closing them.
  void clearOutputs() => _outputs.clear();

  /// Sets the level threshold.
  void setLevel(LogLevel level) => _level = level;

  /// Enables or disables logging.
  void setEnabled(bool enabled) => _enabled = enabled;

  /// Logs an error message.
  Future<void> error(
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, dynamic>? metadata,
  }) {
    return _log(
      LogLevel.error,
      message,
      error: error,
      stackTrace: stackTrace,
      metadata: metadata,
    );
  }

  /// Logs a warning message.
  Future<void> warning(String message, {Map<String, dynamic>? metadata}) {
    return _log(LogLevel.warning, message, metadata: metadata);
  }

  /// Logs an info message.
  Future<void> info(String message, {Map<String, dynamic>? metadata}) {
    return _log(LogLevel.info, message, metadata: metadata);
  }

  /// Logs a debug message.
  Future<void> debug(String message, {Map<String, dynamic>? metadata}) {
    return _log(LogLevel.debug, message, metadata: metadata);
  }

  Future<void> _log(
    LogLevel level,
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, dynamic>? metadata,
  }) async {
    if (_closed || !_enabled || !_level.shouldLog(level)) {
      return;
    }
    final String fullMessage = name == null ? message : '[$name] $message';
    final Map<String, dynamic> fullMetadata = <String, dynamic>{
      ...?metadata,
      if (error != null) 'error': error.toString(),
      if (stackTrace != null) 'stackTrace': stackTrace.toString(),
    };
    for (final LogOutput output in _outputs) {
      try {
        await output.write(
          level,
          fullMessage,
          metadata: fullMetadata.isEmpty ? null : fullMetadata,
        );
      } catch (_) {
        // A failing output must not take down the database.
      }
    }
  }

  /// Closes this logger and every attached output.
  ///
  /// Idempotent. Subsequent log calls are dropped.
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    for (final LogOutput output in _outputs) {
      try {
        await output.close();
      } catch (_) {
        // Best effort: closing one output must not skip the others.
      }
    }
    _outputs.clear();
  }
}

/// Process-wide default logger instance (same object as [ReaxLogger.root]).
@Deprecated(
  'Use ReaxLogger.root. This top-level name collides with package:logger. '
  'Removed in 3.0.',
)
final ReaxLogger logger = ReaxLogger.root;
