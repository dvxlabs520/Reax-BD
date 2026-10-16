/// ReaxDB - an embedded NoSQL database written in pure Dart.
///
/// This barrel exports the PUBLIC API only. Internal engine types (the LSM
/// storage engine, the write-ahead log, the cache, the encryption engine, the
/// index manager) are deliberately not exported: they are implementation
/// details and are free to change without a breaking release. If you need
/// one of them for an experiment, import it from `src/` and accept that it
/// may move.
///
/// This entry point depends on `dart:io` (the storage engine writes files),
/// so it targets the Dart VM and Flutter on desktop and mobile. It is not
/// usable on the web.
library;

// Database facade and simple API.
export 'src/reaxdb.dart' show LegacyMigrationMode, ReaxDB, ReaxTransaction;
export 'src/simple_api.dart' show SimpleReaxDB;

// Migration of ReaxDB 1.x databases. The legacy readers themselves are
// internal and strictly read-only: nothing in 2.0 can write the 1.x format.
export 'src/legacy/legacy_migrator.dart'
    show LegacyDatabaseInfo, LegacyMigration, MigrationReport;

// Deprecated 1.x compatibility shims, removed in 3.0. See MIGRATION.md.
// ignore: deprecated_member_use_from_same_package
export 'src/reaxdb.dart' show DatabaseConfig;

// Typed collections.
export 'src/collection.dart' show CollectionChange, ReaxCollection;

// Public value types.
export 'src/domain/entities/database_entity.dart'
    show ChangeType, DatabaseChangeEvent, DatabaseInfo, ReaxEntry;

// Errors.
export 'src/core/errors/exceptions.dart';

// Durability configuration.
export 'src/core/wal/write_ahead_log.dart' show SyncMode;

// Transactions.
export 'src/domain/entities/transaction_entity.dart'
    show
        AppliedOperation,
        AppliedOperationType,
        CommitResult,
        IsolationLevel,
        TransactionState,
        TransactionStats;

// Queries, aggregation and index definitions.
export 'src/core/query/query_builder.dart'
    show QueryBuilder, QueryCondition, QueryOperator;
export 'src/core/query/aggregation.dart'
    show
        AggregationBuilder,
        AggregationFunction,
        AggregationResult,
        GroupByResult;
export 'src/core/indexing/secondary_index.dart'
    show IndexDefinition, compareValues, extractFieldValue, valuesEqual;

// Cache and storage statistics (the cache and engine themselves are
// internal).
export 'src/core/cache/multi_level_cache.dart' show CacheStatistics;
export 'src/core/storage/storage_engine.dart' show StorageStats;

// Encryption configuration (the engine itself is internal).
export 'src/core/encryption/encryption_config.dart'
    show
        Aes256KeyConfig,
        Aes256PassphraseConfig,
        EncryptionConfig,
        NoEncryptionConfig,
        ObfuscationConfig;
export 'src/core/encryption/encryption_type.dart'
    show EncryptionType, EncryptionTypeExtension;
export 'src/core/encryption/key_derivation.dart' show KeyDerivation;

// Reactive streams.
export 'src/core/streams/reactive_stream.dart'
    show ReactiveStream, ReactiveStreamExtension;

// Logging.
export 'src/core/logging/log_level.dart';
export 'src/core/logging/log_output.dart';
export 'src/core/logging/logger.dart';
export 'src/core/logging/redaction.dart';
