/// Typed exceptions thrown by ReaxDB.
///
/// Every failure surfaced by the public API is a subtype of [ReaxDbException],
/// so callers can handle database errors without matching on message strings.
library;

/// Base type for every error thrown by ReaxDB.
sealed class ReaxDbException implements Exception {
  /// Creates a database exception with a human readable [message].
  const ReaxDbException(this.message, {this.cause});

  /// Description of what went wrong.
  final String message;

  /// The underlying error, when this exception wraps another failure.
  final Object? cause;

  @override
  String toString() {
    final String name = runtimeType.toString();
    return cause == null
        ? '$name: $message'
        : '$name: $message (cause: $cause)';
  }
}

/// Thrown when an operation is attempted on a database that is closed.
final class DatabaseClosedException extends ReaxDbException {
  /// Creates a closed database exception.
  const DatabaseClosedException([
    super.message = 'The database is closed',
    Object? cause,
  ]) : super(cause: cause);
}

/// Thrown when a database path is already open by another instance or process.
final class DatabaseLockedException extends ReaxDbException {
  /// Creates a locked database exception for [path].
  const DatabaseLockedException(super.message, {this.path, super.cause});

  /// The database directory that is already in use.
  final String? path;
}

/// Thrown when a key is not usable as a database key.
///
/// Keys are UTF-8 byte strings. They must be non-empty and must not begin
/// with byte `0x00`, which is reserved for ReaxDB's internal key space
/// (secondary index postings and index metadata).
final class InvalidKeyException extends ReaxDbException {
  /// Creates an invalid key exception.
  const InvalidKeyException(super.message, {super.cause});
}

/// Thrown when data on disk fails validation, such as a checksum mismatch.
final class CorruptionException extends ReaxDbException {
  /// Creates a corruption exception, optionally locating the damaged bytes.
  const CorruptionException(
    super.message, {
    this.path,
    this.offset,
    super.cause,
  });

  /// The file that contains the damaged data.
  final String? path;

  /// Byte offset within [path] where the damage was detected.
  final int? offset;
}

/// Thrown when encryption or decryption fails.
final class EncryptionException extends ReaxDbException {
  /// Creates an encryption exception.
  const EncryptionException(super.message, {super.cause});
}

/// Thrown when a transaction cannot commit because of a conflicting write.
final class TransactionConflictException extends ReaxDbException {
  /// Creates a conflict exception for the transaction identified by [transactionId].
  const TransactionConflictException(
    super.message, {
    this.transactionId,
    super.cause,
  });

  /// Identifier of the transaction that failed to commit.
  final int? transactionId;
}

/// Thrown when two or more transactions wait on each other's locks.
final class DeadlockException extends ReaxDbException {
  /// Creates a deadlock exception.
  const DeadlockException(super.message, {this.transactionId, super.cause});

  /// Identifier of the transaction that was aborted to break the cycle.
  final int? transactionId;
}

/// Thrown when a transaction exceeds its time limit.
final class TransactionTimeoutException extends ReaxDbException {
  /// Creates a transaction timeout exception.
  const TransactionTimeoutException(
    super.message, {
    this.transactionId,
    super.cause,
  });

  /// Identifier of the transaction that timed out.
  final int? transactionId;
}

/// Thrown when the stored schema version cannot be handled.
final class SchemaVersionException extends ReaxDbException {
  /// Creates a schema version exception describing the mismatch.
  const SchemaVersionException(
    super.message, {
    this.storedVersion,
    this.expectedVersion,
    super.cause,
  });

  /// Version found on disk.
  final int? storedVersion;

  /// Version the application expected.
  final int? expectedVersion;
}

/// Thrown when a value cannot be encoded or decoded.
final class SerializationException extends ReaxDbException {
  /// Creates a serialization exception.
  const SerializationException(super.message, {super.cause});
}

/// Thrown when a ReaxDB 1.x API is called in a way 2.0 cannot honour safely.
///
/// The 2.0 compatibility layer keeps the 1.x symbols alive so old code still
/// compiles, but a few of them described behaviour that was insecure or
/// meaningless. Those throw this exception instead of quietly doing something
/// different; the message always names the 2.0 replacement.
final class UnsupportedApiException extends ReaxDbException {
  /// Creates an unsupported API exception.
  const UnsupportedApiException(super.message, {super.cause});
}

/// Thrown when a query is malformed or cannot be executed.
final class QueryException extends ReaxDbException {
  /// Creates a query exception.
  const QueryException(super.message, {super.cause});
}

/// Thrown when the file system fails an operation ReaxDB needs.
///
/// Examples: the database directory cannot be created (on iOS and Android a
/// relative path such as `'myapp'` resolves against a read-only working
/// directory; pass an absolute path, for example from `path_provider`), the
/// disk is full, or a file cannot be read. [cause] holds the original
/// `FileSystemException`.
final class StorageException extends ReaxDbException {
  /// Creates a storage exception, optionally naming the [path] involved.
  const StorageException(super.message, {this.path, super.cause});

  /// The file or directory the failed operation addressed.
  final String? path;
}

/// Thrown into a change stream whose subscriber fell too far behind.
///
/// Each subscription queues at most a bounded number of undelivered events
/// (see `ReaxDB.open(changeStreamBufferSize: ...)`). Rather than growing
/// without bound, or dropping events silently, the subscription receives
/// this error and is closed. Re-read the state you need and subscribe again.
final class ChangeStreamOverflowException extends ReaxDbException {
  /// Creates an overflow exception for the subscription to [pattern].
  const ChangeStreamOverflowException(
    super.message, {
    this.pattern,
    super.cause,
  });

  /// The watched pattern.
  final String? pattern;
}
