# Migrating from ReaxDB 1.x to 2.0

Two things change: the data on disk and the API. Both have a supported upgrade
path.

- **Your data migrates.** 2.0 ships read-only readers for the 1.x SSTable, WAL
  and encryption formats, and a migrator that rewrites their contents into a
  2.0 database. Encrypted 1.x databases are included: pass the original key.
- **Your code keeps compiling.** The 1.x symbols are still there, deprecated,
  and mapped onto the 2.0 API. They are removed in 3.0. Two of them refuse to
  run because they were insecure or meaningless; they throw a message telling
  you what to write instead.

Read [What migration cannot fix](#what-migration-cannot-fix) before you
migrate production data.

## Migrating the data

2.0 never rewrites a 1.x directory on its own. Opening one with the default
settings throws `SchemaVersionException` with instructions:

```dart
await ReaxDB.open(path: 'data/myapp');
// SchemaVersionException: The directory "data/myapp" holds a ReaxDB 1.x
// database (3 SSTable files, 1 WAL files, about 512 entries). ...
```

### Automatic: migrate on first open

```dart
final db = await ReaxDB.open(
  path: 'data/myapp',
  legacyMigration: LegacyMigrationMode.automatic,
);
```

The 1.x files are moved to `data/myapp.1x-backup-<n>` — never deleted — the
data is written into a fresh 2.0 database at `data/myapp`, and the
`MigrationReport` (entries migrated, entries skipped, tombstones applied,
elapsed time, warnings) is logged at info level. Once the migration has run,
the directory is an ordinary 2.0 database and the mode no longer matters; it
is safe to leave the parameter in place.

Since 2.1 the migration is crash-safe. Everything that can fail (detection,
the legacy key) is checked before the 1.x files are touched. The new database
is built in a sibling staging directory and only moved into place once it is
complete. If the app dies midway, the next `ReaxDB.open` of that path
finishes or discards the interrupted migration before opening: you end up
with either the untouched 1.x database or the complete migrated one, never an
empty or partial database.

### Manual: migrate explicitly, then open

Use this when you want the report in hand — to show progress, to check
`warnings`, or to migrate into a different directory.

```dart
final info = await ReaxDB.inspect1x('data/myapp');   // null if not 1.x
if (info != null) {
  final report = await ReaxDB.migrateFrom1x(
    sourcePath: 'data/myapp',
    targetPath: 'data/myapp-v2',
    onProgress: (migrated, total) => print('$migrated / $total'),
  );
  print('${report.entriesMigrated} entries, backup at ${report.backupPath}');
  for (final warning in report.warnings) {
    print('anomaly: $warning');
  }
}
final db = await ReaxDB.open(path: 'data/myapp-v2');
```

`targetPath` defaults to `sourcePath`, which migrates in place.

### Encrypted 1.x databases

An encrypted 1.x database **is** recoverable. Pass the raw key string you gave
to the 1.x `ReaxDB.open(..., encryptionKey: '...')` as `legacyEncryptionKey`,
and the configuration for the new database in `encryption:`:

```dart
final db = await ReaxDB.open(
  path: 'data/myapp',
  legacyMigration: LegacyMigrationMode.automatic,
  legacyEncryptionKey: 'the key 1.x was opened with',
  encryption: EncryptionConfig.aes256FromPassphrase(passphrase: mySecret),
);
```

The legacy key is used for **reading only**. The migrated database is
re-encrypted with the 2.0 scheme: PBKDF2-HMAC-SHA256 with a random
per-database salt stored in the header, AES-256-GCM with a fresh random IV per
value. Nothing in 2.0 can write the 1.x format or the 1.x key derivation
(10,001 rounds of SHA-256 with one salt hardcoded into the package, shared by
every database ever created with it). A wrong `legacyEncryptionKey` fails with
`EncryptionException` rather than producing garbage.

If your 1.x database used `ReaxDB.simple(name, encrypted: true)`, its key was
derived from the database name. Anyone with the package could compute it.
Migrate the data, then treat it as if it had been stored in plaintext: re-key
anything sensitive and rotate credentials.

### Starting over instead

```dart
final db = await ReaxDB.open(
  path: 'data/myapp',
  legacyMigration: LegacyMigrationMode.off,
);
```

The 1.x files stay on disk untouched and the 2.0 engine starts empty at that
path.

## What migration cannot fix

Migration reproduces faithfully what 1.x wrote to disk. It cannot repair data
that 1.x lost or corrupted before you upgraded. The known 1.x defects:

- **Dropped tombstones.** 1.x compaction discarded delete markers, so a key
  you deleted could reappear when an older SSTable level was consulted. If
  that already happened, the resurrected value is what is on disk, and it is
  what migrates.
- **Stale values after compaction.** The 1.x compaction dedup kept, in some
  merges, an older value for a key that had been overwritten. Migration
  applies 1.x's own precedence rules, so you get what 1.x itself would have
  served — not necessarily what you last wrote.
- **Hash collisions for keys longer than 32 bytes.** The 1.x memtable cache
  folded long keys into 256 buckets, so two long keys could shadow each other.
  Values lost that way were never written and cannot be recovered.
- **Lost WAL records.** 1.x acknowledged writes before they were durable, so a
  crash could lose acknowledged writes. Those records are not on disk.

Where the migrator can *see* an anomaly — the same key present in several
levels with different values, an unreadable or truncated record, a record it
had to skip — it records it in `MigrationReport.warnings` and
`MigrationReport.entriesSkipped` instead of dropping it silently. Read the
report. If your data is critical, migrate into a **new** directory
(`targetPath`), verify it against the 1.x original with your own code, and
keep the backup.

## API changes

### Opening

```dart
// 1.x
final db = await ReaxDB.open(
  'myapp',
  config: DatabaseConfig.withAes256Encryption(),
  encryptionKey: 'some string',
  path: 'data/myapp',
);

// 2.0
final db = await ReaxDB.open(
  path: 'data/myapp',
  encryption: EncryptionConfig.aes256(key: keyBytes32),
);
```

The 1.x call took the database name as a **positional** argument. Dart does
not allow one method to declare both an optional positional parameter and
named ones, so that shape could not be folded into `ReaxDB.open`. It lives on
as `ReaxDB.openLegacy`, which is a one-token edit and behaves exactly like the
1.x call (the directory is `path ?? name`):

```dart
// compiles unchanged apart from the method name
final db = await ReaxDB.openLegacy('myapp', config: config, path: 'data/myapp');
```

`DatabaseConfig` still exists, deprecated. `ReaxDB.open` and
`ReaxDB.openLegacy` accept it and map it onto the 2.0 options:

| 1.x `DatabaseConfig` field | 2.0 |
|---|---|
| `memtableSizeMB` | `memtableSizeBytes` (× 1024 × 1024) |
| `syncWrites` | `syncMode: SyncMode.full` / `SyncMode.none` |
| `l1CacheSize` + `l2CacheSize` + `l3CacheSize` | `cacheMaxEntries` (their sum — 2.0 has one cache, not three) |
| `encryptionType` + `encryptionKey` | `encryption: EncryptionConfig...` |
| `pageSize`, `compressionEnabled`, `maxImmutableMemtables`, `cacheSize`, `enableCache` | accepted and **ignored** — they controlled nothing in 1.x either |

When `config` is supplied its values win over the 2.0 defaults for the knobs
above, so do not pass both `config:` and the 2.0 parameters it covers.

`encryptionKey: String` is deprecated but still works, with one deliberate
difference: **the key is derived properly**. 1.x ran 10,001 rounds of SHA-256
with a salt hardcoded in the package; 2.0 runs PBKDF2-HMAC-SHA256 with a
random salt generated per database and stored in the header. A database
created through `encryptionKey:` is therefore NOT byte-compatible with 1.x —
it is a new, correctly protected database. Existing 1.x data must be migrated
(see above), not reopened. Passing both `encryptionKey:` and the 2.0
`encryption:` throws `UnsupportedApiException`.

One more difference: in 1.x, `encryptionKey:` without a config that requested
encryption stored **plaintext**. In 2.0 a key that is handed to us is used:
`encryptionKey:` alone means AES-256. Pass `encryption:
EncryptionConfig.none()` explicitly if you really want plaintext.

### Simple API

```dart
// 1.x
final db = await ReaxDB.simple('myapp', encrypted: true);
final db2 = await ReaxDB.quickStart('myapp');

// 2.0
final db = await ReaxDB.simple(
  'myapp',
  encryption: EncryptionConfig.aes256FromPassphrase(passphrase: secret),
);
```

`encrypted: true` still compiles and **throws `EncryptionException`** naming
the replacement. It derived the key from the database name, so every 1.x
database with the same name shared a guessable key; restoring that would be
restoring a vulnerability. `encrypted: false` is a no-op, so code that passed
the flag as a variable keeps working until the variable is true.

`ReaxDB.quickStart(name)` is deprecated and delegates to `ReaxDB.simple(name)`.

The 1.x simple API maintained a key registry in one value that was rewritten
on every `put`. That is gone: `query`, `count`, `getAll` and `clear` now use
real ordered iteration. Writes are no longer O(total keys), and keys are no
longer lost when the registry write is interrupted.

### Transactions

```dart
// 1.x
await db.transaction((txn) async {
  await txn.put('a', 1);
});
await db.beginEnhancedTransaction(...);   // never committed anything
await db.withTransaction(...);            // same

// 2.0
await db.transaction((tx) async {
  await tx.put('a', 1);
}, isolationLevel: IsolationLevel.serializable, maxAttempts: 3);
```

`EnhancedTransaction`, `EnhancedTransactionManager`,
`beginEnhancedTransaction`, `beginReadOnlyTransaction`, `withTransaction` and
`TransactionType` are **deleted**. The enhanced transaction manager never wrote
to storage: committing one silently discarded the write set. Retry and
optimistic support are now part of the real transaction manager.

### Encryption types

`EncryptionType.xor` is renamed to `EncryptionType.obfuscation`. The old name
survives as a deprecated constant with the same value, so `EncryptionType.xor`
still resolves — but it is a constant, not an enum value, so `case
EncryptionType.xor` in a `switch` no longer compiles; match
`EncryptionType.obfuscation` instead. The XOR
implementation is no longer reachable as an "AES fallback". Use
`EncryptionConfig.obfuscation(key: ...)` only when you genuinely want
non-secret obfuscation.

### Iteration

```dart
// 1.x — both returned an empty map, always
await db.scan(startKey: 'a', endKey: 'z');
await db.scanPrefix('user:');

// 2.0 — real ordered iteration
await for (final e in db.scan<Object?>(startKey: 'a', endKey: 'z')) { }
await for (final e in db.scanPrefix<Object?>('user:')) { }
await for (final k in db.keys(prefix: 'user:')) { }
final page = await db.range<int>('a', 'm', limit: 50);
```

### Indexes

```dart
// 1.x
await db.createIndex('users', 'city');
final names = db.listIndexes();   // List<String>

// 2.0
await db.createIndex('users', ['city']);
await db.createIndex('users', ['city', 'age']);   // compound, new in 2.0
final definitions = db.listIndexes();             // List<IndexDefinition>
await db.dropIndex('users', ['city']);
```

### Queries and collections

`db.collection(name)` used to return a `QueryBuilder`. In 2.0:

- `db.query(name)` returns the `QueryBuilder`.
- `db.collection<T>(name, fromJson: ..., toJson: ...)` returns a typed
  `ReaxCollection<T>`.

### Streams

```dart
// 1.x
db.changeStream;
db.stream('user:*');
db.watch();
db.watchPattern('user:*');

// 2.0
db.watch('*');
db.watch('user:*');
db.watchKey('user:1');
db.watchPrefix('user:');
db.watchCollection('users');
db.changes;                       // ReactiveStream over everything
```

`ChangeType.update` is gone; a write is a `put` and a removal is a `delete`.

### Diagnostics

```dart
// 1.x
await db.getDatabaseInfo();       // fabricated path and timestamps
await db.getStatistics();
db.getPerformanceStats();         // hardcoded "optimization" booleans
db.getEncryptionInfo();

// 2.0
await db.info();                  // real path, real counts
db.statistics();
db.encryptionInfo;
db.cacheStats;
db.storageStats;
db.transactionStats;
```

All four 1.x names still exist, deprecated, and return the real 2.0 data.
`getDatabaseInfo()` returns the 2.0 `DatabaseInfo` — the 1.x `name`,
`createdAt` and `lastAccessed` fields are gone rather than invented, and
`path` is the real path instead of the literal string `'database_path'`.
`getPerformanceStats()` keeps the 1.x shape but only with measured numbers:
the `optimization` block of hardcoded `true` values described nothing and is
gone, and the hit ratios come from the live cache counters instead of dividing
by zero before the first read.

### compareAndSwap

`compareAndSwap` in 1.x did a plain read followed by a plain write and was not
atomic. In 2.0 both run inside one serializable transaction.

### Exceptions

`DatabaseException` is gone, with no shim. It could only have come back as an
alias or a superclass of the new hierarchy, and both would have punched a hole
in it: `ReaxDbException` is `sealed`, which is what lets the analyzer prove a
`switch` over ReaxDB failures is exhaustive. Replace `on DatabaseException`
with `on ReaxDbException`, or match the specific subtype you care about.

New in 2.0: `UnsupportedApiException`, thrown when a 1.x API is called in a way
2.0 cannot honour safely. Its message always names the replacement.

### Barrel exports

`package:reaxdb_dart/reaxdb_dart.dart` no longer exports
`HybridStorageEngine`, `MultiLevelCache`, `EncryptionEngine`, `IndexManager`,
`WriteAheadLog` or the raw transaction manager. Exporting them froze internals
as public API. If you were using one directly, import it from `src/` and be
aware it may change in a minor release.

`MemoryLogOutput` no longer takes a `maxLines` argument — it never had one.

## Behavioural changes that are not signature changes

### Keys

Keys are UTF-8 now. Any non-ASCII key maps to different bytes than in 1.x.
Keys starting with byte `0x00` are rejected with `InvalidKeyException`; that
range is the internal index key space.

### Cache

The cache stores plaintext record envelopes only. In 1.x, `put` cached
ciphertext while `get` cached plaintext, so a read after a write on an
encrypted database double-decrypted and produced garbage. That class of bug is
now impossible: there is only one cached representation.

The cache is also written **after** the durability point, not before. In 1.x a
failed write left a phantom cache entry that read back as if the write had
succeeded.

### Change events

Events fire only after the write is durable. In 1.x they fired before the
storage write in some paths.

### Errors are no longer swallowed

1.x returned `null` or an empty result on decode failures, corrupt data and
unimplemented paths. 2.0 throws. If your code relied on a silent `null`, add
explicit error handling.

### One writer per path

Opening the same directory twice now throws `DatabaseLockedException`
instead of producing two engines that corrupt each other's files. Since 2.1
that covers two isolates of the same process (for example a Flutter
background isolate) and the same directory reached through a symlink or a
differently cased path, not only two processes.

## Renamed, removed and shimmed: the full table

"Shim" means the 1.x symbol still exists and is annotated
`@Deprecated(... Removed in 3.0.)`. Your code compiles today; fix it before
3.0. `dart analyze` lists every use.

| ReaxDB 1.x | 2.0 | Status |
|---|---|---|
| `ReaxDB.open('name', config:, encryptionKey:, path:)` | `ReaxDB.open(path: ...)` | shim: `ReaxDB.openLegacy('name', ...)` (Dart cannot overload the positional shape) |
| `DatabaseConfig` | named parameters of `ReaxDB.open` | shim, mapped; dead fields ignored |
| `DatabaseConfig.withXorEncryption()` | `EncryptionConfig.obfuscation(key:)` | shim |
| `DatabaseConfig.withAes256Encryption()` | `EncryptionConfig.aes256FromPassphrase(...)` | shim |
| `encryptionKey: String` | `encryption: EncryptionConfig` | shim; key derivation is now PBKDF2 + random stored salt |
| `ReaxDB.quickStart(name)` | `ReaxDB.simple(name)` | shim |
| `ReaxDB.simple(name, encrypted: true)` | `ReaxDB.simple(name, encryption: ...)` | shim that **throws** — the key came from the name |
| `EncryptionType.xor` | `EncryptionType.obfuscation` | shim (constant, not an enum value: unusable in `switch` patterns) |
| `getDatabaseInfo()` | `info()` | shim, returns the real 2.0 `DatabaseInfo` |
| `getStatistics()` | `statistics()` | shim |
| `getPerformanceStats()` | `statistics()`, `cacheStats`, `storageStats`, `transactionStats` | shim, measured values only |
| `getEncryptionInfo()` | `encryptionInfo` | shim |
| `changeStream` | `watch('*')` / `changes` | shim |
| `stream(pattern)` | `watch(pattern)` | shim |
| `watchPattern(pattern)` | `watch(pattern)` | shim (returns `ReactiveStream`) |
| `watch()` (no argument) | `changes` | removed — 2.0 `watch` requires a pattern |
| `createIndex(collection, 'field')` | `createIndex(collection, ['field'])` | removed — the signature changed shape |
| `dropIndex(collection, 'field')` | `dropIndex(collection, ['field'])` | removed |
| `listIndexes()` → `List<String>` | `listIndexes()` → `List<IndexDefinition>` | removed |
| `collection(name)` → `QueryBuilder` | `query(name)` | removed — `collection<T>` is now the typed collection |
| `DatabaseException` | `ReaxDbException` and its subtypes | removed — a shim would break the sealed hierarchy |
| `DatabaseEntry`, `StorageConfig`, `CacheStats` | `ReaxEntry<T>`, open parameters, `CacheStatistics` | removed |
| `EnhancedTransaction`, `EnhancedTransactionManager`, `beginEnhancedTransaction`, `beginReadOnlyTransaction`, `withTransaction`, `TransactionType` | `transaction(...)` with `isolationLevel:` and `maxAttempts:` | removed — the enhanced manager never wrote to storage, so no shim can be faithful |
| `ChangeType.update` | `ChangeType.put` | removed |
| `serializeValue`, `deserializeValue`, `encryptData`, `decryptData` | — | removed: engine internals that leaked into the public class |
| barrel exports of `HybridStorageEngine`, `MultiLevelCache`, `EncryptionEngine`, `IndexManager`, transaction manager | — | removed: internals are no longer public API |

New in 2.0 for migration: `LegacyMigrationMode`, `ReaxDB.migrateFrom1x`,
`ReaxDB.inspect1x`, `LegacyMigration`, `LegacyDatabaseInfo`,
`MigrationReport`.

## New in 2.0 you may want to adopt

- Typed collections without code generation.
- TTL (`ttl:` / `expiresAt:`) and `purgeExpired()`.
- Compound indexes.
- `exportTo` / `importFrom` with a checksummed archive.
- `schemaVersion` + `onUpgrade`.
- `SyncMode` per database and per write.
- Real iteration: `scan`, `scanPrefix`, `keys`, `range`.

## Still stuck?

If a 1.x API you depend on is not in the table above, or a migration report
shows warnings you cannot explain, open an issue with the report attached (it
contains no key or value contents). Keep the `.1x-backup-<n>` directory until
you are satisfied: it is the only copy of the original bytes.
