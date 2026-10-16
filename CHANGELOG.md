# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - 2.5.0

### Added

- `ReaxDB.open(keepInMemory: true)` keeps every value in memory, like a
  Hive box, decoded the first time it is read. `get`, `exists`, `scan`, `scanPrefix`, `keys`, `range`
  and queries are served from memory, without disk access or decoding.
  Writes still go to the write-ahead log first, with the same durability.
  Values read are copies, so changing one never changes the stored value.
  Opening loads every entry, so it takes longer and memory use grows with
  the data.
- `rekey(newEncryption)` and `changePassphrase(newPassphrase)` re-encrypt
  every value and return the database opened with the new configuration.
  No plaintext reaches the disk, a crash leaves the database readable with
  either the old or the new configuration, and values written by 2.0.0 are
  bound to their key afterwards.
- `getSync()` reads a value synchronously from a database opened with
  `keepInMemory: true`, for example in a Flutter `build` method.
- `benchmark/hive_comparison.dart`: ReaxDB and Hive (`hive_ce`) measured on
  the same machine with the same documents.

### Fixed

- A crash between the two renames that `importFrom(overwrite: true)` uses
  to replace a database left the path missing, so the next open created an
  empty database. The next open now finishes the swap when the restore was
  complete, and puts the original back otherwise.

### Changed

- Opening with `keepInMemory: true` reads each SSTable with one I/O instead
  of streaming entry by entry: about 168 ms instead of 306 ms for 100,000
  documents.

- Flushing a full memtable and compacting the LSM tree now happen in the
  background, after the write that triggered them has returned, instead of
  inside it. The WAL is checkpointed only after the new SSTable and its
  manifest are durable, so crash safety is unchanged.
- Writes can now fail with `StorageException` when flushing keeps failing
  (for example on a full disk) and two sealed memtables already wait for
  it. Before, the memtable grew without bound in that situation. Writes
  resume once a flush succeeds. Code that switches exhaustively over
  `ReaxDbException` needs no change.
- New LSM defaults: compaction starts at eight L0 tables instead of four,
  and L1 holds 64 MiB instead of 8 MiB. A 100,000-document bulk load
  rewrites its data about half as often.
- The record cache does every operation in constant time. A `put` that had
  to evict cost about 4 microseconds and now costs well under one, which
  roughly halves the cost of `putBatch`.
- Queries no longer await once per document. Each matching document now
  costs a plain loop iteration when the documents are in memory, and one
  await per chunk on the index path.

## [2.1.1] - 2026-10-16

Documentation only. No code changes.

### Documentation

- The README now gets you started in 30 seconds: install, open, `put` and
  `get` first, then common tasks with short examples and a comparison with
  Hive based on its documentation. The details it used to hold (durability
  contract, key rules, encryption envelope, change stream limits, schema
  versions, errors) moved to ADVANCED.md.

## [2.1.0] - 2026-10-16

A correctness, durability and security release. Every fix below comes with a
regression test that fails on 2.0.0. The on-disk format stays compatible:
2.1 opens 2.0.0 databases and archives as they are.

### Fixed — data loss and wrong results

- A transaction and a plain `put` on the same key could leave the cache and
  the secondary indexes disagreeing with storage, until eviction or restart.
  Transactions now commit inside the write pipeline.
- Index postings of a transaction were written in a second batch after the
  commit, so a crash between the two lost them for good, and a failure there
  reported a durable commit as failed. They now commit in the same atomic
  batch as the documents.
- TTL reclamation (`get` of an expired key, `purgeExpired`) could delete a
  fresh value written at the same moment. It now re-checks each key under the
  pipeline lock.
- A `get` racing a `put` or `delete` of the same key could put the older
  value back in the cache.
- Expired documents left their index postings behind forever.
- `compareAndSwap` always failed for maps and lists (it compared identity).
- Integers beyond 2^53 matched their neighbours in queries, on both the index
  and the scan path. `-0.0` did not equal `0`.
- Optimistic transactions silently overwrote plain writes made while they
  ran.
- Collection names containing `:` silently missed index maintenance; they are
  now rejected.
- `dropIndex` interrupted by a crash could bring back an incomplete index;
  `createIndex` could index a stale version of a document changed during the
  backfill.
- Storage engine: see "Fixed — storage engine" below.

### Fixed — security

- Opening an encrypted database with the wrong key or passphrase succeeded and
  accepted writes, after which every scan failed even with the right key. A
  key check stored in the header now rejects the wrong key at `open`. A 2.0.0
  database gets its key check on the first open by 2.1, after the key has been
  proven against a stored value.
- AES-GCM values were not bound to their key: copying the ciphertext of one
  key onto another decrypted fine. New values use envelope version 2, which
  authenticates the record key as associated data. Version 1 values written
  by 2.0.0 are still read.
- PBKDF2 key derivation (210,000 iterations) ran on the calling isolate and
  froze a Flutter UI for seconds. It now runs in `Isolate.run`.
- Two isolates of one process, or a symlinked or differently cased path,
  could open the same database at once and lose writes. A `flock(2)` on
  `LOCK.flock`, taken through `dart:ffi`, now excludes them; the `dart:io`
  lock on `LOCK` still excludes other processes, including 2.0.0 ones.
  Windows keeps the `dart:io` lock, which already excluded isolates.
- `Redaction.key` was a plain truncated SHA-256, reversible for guessable keys
  such as emails. It is now an HMAC under a random per-isolate key.
- Migration warnings contained raw 1.x keys, and were logged.
- `importFrom(overwrite: true)` deleted any non-empty directory, ReaxDB or not.

### Fixed — 1.x migration

- An in-place migration that failed (for example with the wrong legacy key)
  or crashed midway left an empty or partial database at the path and gave no
  warning. Everything that can fail is now checked before the 1.x files are
  touched, the new database is built in a staging directory and swapped in
  when complete, and `ReaxDB.open` finishes or discards an interrupted
  migration before opening.
- Migration reads one SSTable at a time and writes in batches, instead of
  holding the whole database in memory.

### Fixed — storage engine

- A failed manifest write during compaction (for example a full disk) left
  the next flush reusing the id of a live table: the flushed data was lost on
  restart. Table ids now only go up, and new levels are installed only after
  the manifest naming them is durable.
- A failed WAL append (`ENOSPC`, `EIO`) left a partial record that made the
  database impossible to open, hiding every acknowledged write. The log is
  now cut back to the last complete record (or moved to a new segment), and
  the failed batch throws without consuming sequence numbers.
- Overwriting the same keys never filled the memtable, so the WAL grew
  without bound (105 MB for 100,000 overwrites of one key) and was replayed
  in full on open. A flush now also triggers on WAL size
  (`walFlushThresholdBytes`), and replay streams segments.
- A zero-length or short newest WAL segment (a crash right after creating
  it) made open fail. It is treated as empty; segment headers are always
  fsynced.
- A corrupt record followed by valid later records was treated as a torn
  tail, and those fsynced records were silently truncated away. Open now
  throws `CorruptionException` and leaves the file intact; a genuine torn
  tail is copied to `<segment>.corrupt-tail` before it is truncated.
- A write whose WAL append succeeded threw anyway when the flush that
  followed failed. Background flush and compaction failures no longer fail
  durable writes: they are logged and retried.
- Replaced SSTables and their file handles stayed until `close()` (12.6 MB on
  disk for 0.4 MB of live data, 138 open descriptors after a few rewrite
  rounds; iOS allows 256). Tables are reference-counted and deleted as soon
  as no reader holds them; idle read handles are capped.
- Range scans read and decoded each table to its end before yielding
  anything, ignoring `limit`. They are now lazy and stop when the consumer
  stops; level 1+ lookups binary-search the level; the per-table key index
  keeps every 16th key plus a 4-byte fingerprint per entry instead of a copy
  of every key.
- A failed write no longer leaks the file handle or the temporary file of a
  manifest or SSTable.

### Fixed — reliability

- Raw `FileSystemException`s escaped the API (for example `ReaxDB.simple`
  with a relative path on iOS/Android). They are now `StorageException`s,
  with a hint about absolute paths on mobile.
- A failure inside `close()` left the database half closed, holding its lock
  forever. Every step now runs, the lock is always released, and a second
  `close()` waits for the first.
- The `LOCK` file contained the literal text `$pid`.
- A second `open` issued while the first instance was closing could slip
  through.
- File creations and renames are made durable by fsyncing their directory
  (through `dart:ffi` on Linux, Android, macOS and iOS).

### Changed

- **Backups.** `exportTo` streams the archive (memory no longer grows with the
  database), writes it to a temporary file and renames it into place, and can
  encrypt it: `exportTo(path, encryption: EncryptionConfig.aes256...)`, with
  `importFrom(..., archiveEncryption: ...)`. Archives are format version 2;
  version 1 archives from 2.0 still import. `importFrom` verifies the whole
  file first, restores in batches into a staging directory and moves it into
  place when complete. Plaintext archives are now clearly documented as such.
- **Change streams are bounded.** A paused or slow subscription queues at most
  `changeStreamBufferSize` events (`ReaxDB.open`, default 10,000); one more
  ends it with `ChangeStreamOverflowException`. Before, it buffered without
  limit while the documentation promised backpressure.
- Index-served queries stream their candidates, so `limit` without `orderBy`
  and `findOne` stop the index scan early. `count()` without conditions reads
  keys only.
- Minimum SDK is now Dart 3.12 (Flutter 3.44): ReaxDB supports the current
  and the previous stable Flutter release.
- `StorageStats` is exported. The top-level `logger` is deprecated in favour
  of `ReaxLogger.root` (it collides with `package:logger`).

### Performance

Measured AOT on the same machine against 2.0.0, at 100,000 documents (see
[BENCHMARKS.md](BENCHMARKS.md)): durable sequential writes +17%, random writes
+27%, batches +7%, a 100-entry range scan 15x faster, a full-scan query 7%
faster, opening a closed database 11% faster; cold point reads unchanged; an
indexed query returning 5,000 documents 5% slower.

### Added

- `StorageException` and `ChangeStreamOverflowException`. Both are new
  subtypes of the sealed `ReaxDbException`: an exhaustive `switch` over it
  needs two more cases.
- `ReaxDB.open(changeStreamBufferSize:)`, `exportTo(encryption:)`,
  `importFrom(archiveEncryption:)`.
- Continuous integration on Linux, macOS and Windows with Flutter 3.44 and
  3.47.
- The benchmark harness also runs compiled with `dart compile exe`
  (`reopen_wal_replay` used to require `dart run`); BENCHMARKS.md now reports
  AOT numbers, which is how Flutter release builds run.

### Documentation

- The README no longer says 1.x data cannot be read (it migrates).
- Documented what is not encrypted (keys and indexed field values), the
  `SyncMode.full` limits on Apple platforms, lost updates under the default
  `readCommitted` isolation, cross-type comparisons, and result order without
  `orderBy`.
- The quick start shows an absolute path from `path_provider` for Flutter.

## [2.0.0] - 2026-07-30

A rewrite of the storage engine, transaction manager, query layer, encryption
and public API. Correctness and durability were the goal; several 1.x features
were deleted rather than patched because they could not be made correct.

**1.x data migrates.** This corrects an earlier statement in this changelog
that 1.x data could not be read by 2.0. It can: 2.0 ships read-only readers
for the 1.x SSTable, WAL and encryption formats and migrates their contents
into a 2.0 database. **Encrypted 1.x data is recoverable too** — pass the
original key as `legacyEncryptionKey` — which corrects the earlier claim that
1.x ciphertext was unreadable. Migration never deletes the 1.x files and never
writes the 1.x format. See [MIGRATION.md](MIGRATION.md).

Most 1.x code also still compiles: the old API survives as deprecated shims,
removed in 3.0.

### Added — migrating from 1.x

- `ReaxDB.open(..., legacyMigration: LegacyMigrationMode.detect | automatic |
  off)`. The default, `detect`, throws `SchemaVersionException` explaining
  exactly how to migrate rather than rewriting a user's data; `automatic`
  migrates in place keeping the backup and logs the `MigrationReport`; `off`
  ignores the 1.x files.
- `ReaxDB.migrateFrom1x(...)` and `ReaxDB.inspect1x(path)`, plus the public
  `LegacyMigration`, `LegacyDatabaseInfo` and `MigrationReport` types.
- `legacyEncryptionKey:` on `open` and `migrateFrom1x` for encrypted 1.x
  sources. The 1.x derivation reads only; migrated data is re-encrypted with
  PBKDF2 + a random per-database salt and AES-256-GCM.
- Migration reports what it cannot vouch for: skipped records, duplicate keys
  across levels and other anomalies land in `MigrationReport.warnings`. It
  recovers what 1.x wrote; it cannot repair what 1.x had already lost
  (dropped tombstones, stale values after compaction, the 256-bucket hash
  collision for keys over 32 bytes).

### Breaking changes

Everything below marked "deprecated shim" still compiles in 2.0 and is removed
in 3.0.

- `ReaxDB.open` takes named parameters with a required `path`. Deprecated
  shim: `ReaxDB.openLegacy('name', config:, encryptionKey:, path:)` keeps the
  1.x call shape — Dart does not allow a method to take both an optional
  positional parameter and named ones, so it could not stay on `open`.
  `config:` and `encryptionKey:` are also accepted by `open` itself.
- `DatabaseConfig` deprecated. Real knobs map to `ReaxDB.open`; knobs that did
  nothing (`pageSize`, `compressionEnabled`, `cacheSize`, `enableCache`,
  `maxImmutableMemtables`) are accepted and ignored.
- `encryptionKey: String` deprecated but honoured, with a deliberate
  difference: the key is now derived with PBKDF2-HMAC-SHA256 and a random salt
  stored in the header, not the 1.x 10,001-round SHA-256 with a hardcoded
  package-wide salt. Databases created this way are new databases, not 1.x
  ones; 1.x data must be migrated. A key with no explicit type now means
  AES-256 (1.x stored plaintext in that case).
- `ReaxDB.simple(name, encrypted: true)` throws `EncryptionException` naming
  the replacement. It derived the encryption key from the database name; that
  is not coming back. `encrypted: false` is a no-op.
- `ReaxDB.quickStart` deprecated, delegates to `ReaxDB.simple`.
- `EnhancedTransaction`, `EnhancedTransactionManager`,
  `beginEnhancedTransaction`, `beginReadOnlyTransaction`, `withTransaction`
  and `TransactionType` removed with no shim — that manager never wrote to
  storage, so no shim could be faithful.
- `EncryptionType.xor` renamed to `EncryptionType.obfuscation`; the old name
  survives as a deprecated constant with the same value, so it cannot be used
  as a `switch` pattern. The XOR "AES fallback" is deleted.
- New `UnsupportedApiException` for 1.x calls 2.0 cannot honour safely.
- `DatabaseException` removed with no shim: an alias or superclass would have
  broken the sealed `ReaxDbException` hierarchy that makes exhaustive
  `switch`es over ReaxDB failures possible. Every failure is now a subtype of
  `ReaxDbException`.
- `db.collection(name)` now returns a typed `ReaxCollection<T>`;
  `db.query(name)` returns the `QueryBuilder`.
- `createIndex`/`dropIndex` take a `List<String>` of fields; `listIndexes`
  returns `List<IndexDefinition>`.
- `db.changeStream`, `db.stream`, `db.watchPattern` replaced by
  `db.watch(pattern)`, `db.watchKey`, `db.watchPrefix`, `db.watchCollection`
  and `db.changes`.
- `ChangeType.update` removed; writes are `put`, removals are `delete`.
- `getDatabaseInfo()`, `getStatistics()`, `getPerformanceStats()`,
  `getEncryptionInfo()`, `changeStream`, `stream(pattern)` and
  `watchPattern(pattern)` replaced by `info()`, `statistics()`,
  `encryptionInfo`, `cacheStats`, `storageStats`, `transactionStats` and
  `watch(pattern)`; all seven survive as deprecated shims returning real
  measured data. `getPerformanceStats()` no longer reports the
  `optimization` block of hardcoded `true` values, and `getDatabaseInfo()`
  returns the real path instead of the literal `'database_path'`.
- The barrel no longer exports `HybridStorageEngine`, `MultiLevelCache`,
  `EncryptionEngine`, `IndexManager` or the raw transaction manager. Exporting
  them froze internals as public API.
- Keys are encoded with UTF-8 instead of `String.codeUnits`, and keys starting
  with byte `0x00` are rejected (reserved for the internal index key space).
- `DatabaseEntry`, `StorageConfig` and the old `CacheStats` value types are
  removed; `ReaxEntry<T>`, `DatabaseInfo` and `CacheStatistics` replace them.

### Fixed — durability

- Writes are acknowledged only after the write-ahead log record is durable per
  the effective sync mode. `RandomAccessFile.flush()` (a real fsync) is used
  for `SyncMode.full`; `IOSink.flush()` is not treated as an fsync.
- Batch writes are atomic: a crash mid-batch leaves all or none of it visible.
- WAL replay discards writes of transactions with no commit record, truncates
  a torn tail, and throws `CorruptionException` on a mid-file checksum
  failure. Replay is idempotent.
- Documents and their secondary index postings commit in one batch, so a
  document can never be durable while invisible to indexed queries.

### Fixed — correctness

- The cache stores exactly one representation (the plaintext record envelope).
  1.x cached ciphertext on write and plaintext on read, so reading back a
  freshly written encrypted value double-decrypted it.
- The cache is written after the durability point, not before, so a failed
  write cannot leave a phantom cache entry.
- Change events are published only after the durability point.
- `compareAndSwap` runs its comparison and write inside one serializable
  transaction; it was two unsynchronised operations.
- `scan` and `scanPrefix` were stubs that always returned an empty map. They
  are implemented over the engine's ordered, tombstone-aware merge scan.
- Query `update`/`delete` address documents by the key they were loaded from,
  not by an id reconstructed from document fields.
- `IndexManager.candidateIds` distinguishes "no index can serve this" (null)
  from "an index was consulted and nothing matched" (empty set).
- Indexed and scanned query execution share one documented total value order,
  so they cannot disagree; `5` and `5.0` compare equal.
- `ReactiveStream` operators keep their state per subscription instead of
  sharing timers and counters between listeners.
- The simple API no longer keeps a key registry rewritten on every `put`
  (O(total keys) per write, and keys lost if the registry write was
  interrupted). Iteration uses the storage engine.
- `getDatabaseInfo()` reported a fabricated path and timestamps.
- Cache statistics counted one logical miss three times.

### Fixed — security

- No key is ever derived from the database name.
- PBKDF2-HMAC-SHA256 from `package:pointycastle` with a per-database random
  salt (`Random.secure()`), persisted in the database header with its
  iteration count. Default 210,000 iterations, minimum 1,000.
- AES-256-GCM with a fresh random 96-bit IV per value and a 128-bit
  authentication tag. Tampering and wrong keys are detected and raise
  `EncryptionException`.
- Ciphertext carries a versioned, self-describing envelope; the stored
  algorithm is validated against the configured one.
- ReaxDB never downgrades to a weaker cipher: if the requested cipher is
  unavailable, construction throws.
- No key contents, value contents, passphrases or derived key material are
  logged at any level. `ReaxEntry.toString()` and
  `DatabaseChangeEvent.toString()` are redacted.

### Fixed — error handling

- Nothing is swallowed into `null` or an empty result. Corruption, decode
  failures and unsupported input throw typed exceptions.
- Stubs that returned empty results were deleted rather than shipped.

### Added

- **Typed collections without code generation**: `db.collection<T>(name,
  fromJson:, toJson:)` with `put`, `putAll`, `get`, `delete`, `exists`, `all`,
  `where`, `find`, `count`, `createIndex` and a typed `watch`.
- **Real iteration**: `scan`, `scanPrefix`, `keys` and `range` on the public
  API, backed by the engine's ordered scan.
- **TTL / expiry**: per-key `ttl:` and `expiresAt:` honoured on read (an
  expired entry is absent), reclaimed lazily on read and eagerly with
  `purgeExpired()`. The cache honours the same instant.
- **Compound indexes**: `createIndex(collection, [fieldA, fieldB])`.
- **Backup and restore**: `db.exportTo(path)` and `ReaxDB.importFrom(...)` —
  point-in-time snapshot, magic number, format version, SHA-256 digest
  verified on import, index definitions carried across. Records are stored
  decrypted so a backup can be restored under different encryption.
- **Schema versions and migrations**: `schemaVersion` plus
  `onUpgrade(from, to, db)`, stored in the database header. Downgrades throw
  `SchemaVersionException`.
- **Instance registry and cross-process lock**: a second open of the same path
  throws `DatabaseLockedException`; across processes an OS advisory lock on
  `<path>/LOCK` is used, which the OS releases on process death, so stale
  locks cannot occur.
- **Sync modes**: `SyncMode.none/os/full` per database and a per-write `sync:`
  override that can only strengthen durability.
- `InvalidKeyException` for empty and reserved keys.
- `db.purgeExpired()`, `db.flush()`, `db.compact()`, `ReaxDB.closeAll()`.
- Per-instance loggers with redaction helpers; file logging is exported from
  `package:reaxdb_dart/reaxdb_dart_io.dart` so the main barrel stays free of
  it.
- Fault-injection and concurrency test suites: simulated crash, torn WAL tail,
  mid-file CRC corruption, partial SSTable, crash mid-compaction,
  replay-twice idempotence, crash mid-commit, and a 10k random-key
  write/flush/compact/reopen round trip.

### Removed

- The dual-write "hybrid" storage engine and the B-tree half of it.
- `enhanced_transaction.dart`.
- The XOR "AES fallback".
- The simple API key registry.
- The `21,000+ writes/sec` headline and the third-party device comparisons in
  the documentation. `BENCHMARKS.md` is now generated from a reproducible
  harness that states the machine and methodology.
- Documentation for APIs that never existed (`ReaxDB.open(encryptionType:)`,
  `MemoryLogOutput(maxLines:)`, enhanced-transaction callbacks).

## [1.4.1] - 2025-09-25

### Changed
- Updated GitHub repository URL from `ReaxBD` to `Reax-BD`
- Enhanced README to emphasize open source nature of the project
- Added comprehensive contributing guidelines
- Updated all documentation links to new repository URL

### Added
- Open source badges (MIT License, PRs Welcome, Open Source Love)
- Detailed contribution section with guidelines for community participation
- Contributors section for acknowledging community contributions

## [1.4.0] - 2025-08-20

### Added
- **Simple API**: New `ReaxDB.simple()` and `ReaxDB.quickStart()` methods for easy database setup
  - Start with just 3 lines of code
  - Automatic configuration with optimized defaults
  - No breaking changes - fully backward compatible
  - Simplified methods: `put()`, `get()`, `delete()`, `query()`, `watch()`
  - Access to advanced features via `.advanced` property
- **Improved Persistence**: Fixed key tracking in Simple API for proper data persistence between sessions
- **Integrated Demo App**: Example app now includes 4 comprehensive demos:
  - Performance demo (existing stress tests)
  - Simple CRUD demo
  - Todo app demo
  - Real-time chat demo
- **Real Benchmarks**: Updated performance benchmarks with actual measured data
  - 111,111 ops/sec for reads
  - 14,493 ops/sec for sequential writes
  - 71,429 ops/sec for batch writes
  - Comparison with Hive and Isar based on real data

### Changed
- **Documentation Restructure**:
  - Simplified README from 528 to 205 lines focusing on ease of use
  - Moved advanced features to separate `ADVANCED.md`
  - Added `MIGRATION.md` with guides for migrating from Hive/Isar/SQLite
  - Updated `BENCHMARKS.md` with real performance data
- **Better Developer Experience**:
  - Clear positioning: "The simplest way to store data in Dart & Flutter"
  - Focus on getting started quickly
  - Progressive disclosure of advanced features

### Fixed
- Simple API now properly persists keys metadata between sessions
- Example app navigation and demo integration
- Test coverage for Simple API (28 tests, all passing)

### Developer Experience
- Zero to functional in 3 lines of code
- No configuration required for basic usage
- Smooth learning curve from simple to advanced features
- Better documentation organization for different skill levels

## [1.3.0] - 2025-08-11

### Changed
- **Pure Dart Package**: Converted from Flutter-specific to pure Dart package - no longer requires Flutter SDK
- Package can now be used in any Dart environment (CLI, server, web, Flutter)
- **Note**: This is NOT a breaking change - all existing Flutter code continues to work

### Added
- **Configurable Logging System**: Multi-level logging with console, file, and memory outputs
- **Reactive Streams**: Advanced stream operators including:
  - `debounce()` - Delay events until a pause in emissions
  - `throttle()` - Limit event frequency
  - `buffer()` - Collect multiple events before emitting
  - `take()` and `skip()` - Control number of events
  - `map()` - Transform event data
- **Enhanced Query Builder**:
  - Aggregation functions: COUNT, SUM, AVG, MIN, MAX, DISTINCT
  - GROUP BY operations with multiple aggregations
  - Full-text search capabilities
  - Batch update operations
  - Batch delete operations
  - Improved scanning for large datasets (up to 1000 IDs)
- **Advanced Transaction Features**:
  - Transaction isolation levels (ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)
  - Read-only transactions with write protection
  - Savepoints for partial rollback
  - Nested transactions
  - Automatic retry logic with exponential backoff
  - Transaction timeout support
  - Transaction statistics and monitoring
- **Enhanced Transaction Manager**:
  - `withTransaction()` - Automatic transaction management with retry
  - `beginEnhancedTransaction()` - Create transactions with advanced features
  - `beginReadOnlyTransaction()` - Create read-only transactions
  - Active transaction tracking
  - Bulk transaction closure

### Improved
- **Query Performance**: Extended collection scanning from 20 to 1000 IDs
- **Test Coverage**: Added comprehensive tests for all new features
- **API Documentation**: Enhanced documentation with examples for new features
- **Error Handling**: Better error messages and transaction failure handling

### Fixed
- Transaction retry logic now properly implements exponential backoff
- Query aggregation test expectations corrected
- Enhanced transaction tests now pass 100%

### Acknowledgments
- Special thanks to [@omtodkar](https://github.com/omtodkar) for the pure Dart conversion (PR #4)

## [1.2.3] - 2025-07-20

### Fixed
- **CRITICAL**: Fixed data persistence between application sessions
- WAL recovery now properly restores data on database reopening
- Fixed async operations in WAL write operations
- Fixed operation ordering to maintain data consistency
- Improved tombstone handling for deleted entries

### Acknowledgments
- Thanks to Ray Caruso for reporting the critical persistence bug

## [1.2.2] - 2025-07-15

### Fixed
- Fixed pub.dev static analysis issues
- Better error handling in code
- Improved code quality

### Added
- API documentation for all public methods

## [1.2.1] - 2025-07-15

### Fixed
- Minor fixes and code improvements

## [1.2.0] - 2025-07-11

### Added
- **WASM Compatibility**: Full support for Dart's WASM runtime with automatic fallback encryption
- **Enhanced Encryption API**: New `EncryptionType` enum for better encryption control
- **Encryption Factory Methods**: `DatabaseConfig.withXorEncryption()` and `DatabaseConfig.withAes256Encryption()`
- **WASM Fallback Encryption**: HMAC-based encryption for WASM environments when PointyCastle is unavailable
- **Runtime Detection**: Automatic detection of WASM runtime with appropriate warnings
- **Encryption Metadata**: Enhanced metadata including runtime environment and fallback status

### Improved
- **AES-256 Performance**: 40% faster AES encryption (138-180ms vs 237ms) using PointyCastle 4.0.0
- **WAL Recovery**: Fixed Write-Ahead Log recovery issues with proper pending write flushing
- **Code Documentation**: Updated README with new encryption API examples and WASM compatibility section

### Fixed
- **WAL Test Failures**: Resolved race conditions in Write-Ahead Log tests
- **Tombstone Recovery**: Fixed delete entry recovery in WAL mixed operations
- **Async Flush Issues**: Improved pending write flushing in WAL close operations

### Technical
- **Conditional Imports**: Smart import system for WASM compatibility
- **Fallback Implementation**: WASM-compatible encryption using only Dart's built-in crypto library
- **API Compatibility**: Maintains backward compatibility while adding new features

## [1.1.1] - 2025-06-15

### Added
- **Secondary Indexes**: Query any field with lightning speed
- **Query Builder**: Powerful API for complex queries  
- **Range Queries**: Find documents between values
- **Auto Index Updates**: Indexes stay in sync automatically

### Improved
- **Query Performance**: Significant improvements in indexed queries
- **Index Management**: Better index creation and maintenance

## [1.0.1] - 2025-05-20

### Added
- **4.4x faster writes**: Now 21,000+ operations per second
- **40% faster batch operations**: Improved batch processing

### Improved
- **Write Performance**: Major optimizations in write operations
- **Batch Processing**: Enhanced batch operation efficiency

## [1.0.0] - 2025-05-01

### Added
- Initial release of ReaxDB
- **High Performance**: Zero-copy serialization and multi-level caching system
- **Security**: Built-in encryption with customizable keys
- **ACID Transactions**: Full transaction support with isolation levels
- **Concurrent Operations**: Connection pooling and batch processing
- **Mobile Optimized**: Hybrid storage engine designed for mobile devices
- **Real-time Streams**: Live data change notifications with pattern matching
