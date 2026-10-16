# ReaxDB — advanced guide

Everything here describes behaviour that exists in the code. Where a guarantee
has limits, the limits are stated.

## Contents

- [Opening a database](#opening-a-database)
- [Isolation levels](#isolation-levels)
- [The write pipeline](#the-write-pipeline)
- [Key space and the reserved 0x00 prefix](#key-space-and-the-reserved-0x00-prefix)
- [Storage engine](#storage-engine)
- [Cache](#cache)
- [Queries, indexes and the total order](#queries-indexes-and-the-total-order)
- [Encryption](#encryption)
- [Logging and redaction](#logging-and-redaction)
- [Concurrency model](#concurrency-model)
- [Operational notes](#operational-notes)

## Opening a database

```dart
final db = await ReaxDB.open(
  path: '/absolute/path/to/app', // on iOS/Android, from path_provider

  // Durability.
  syncMode: SyncMode.full,

  // Encryption.
  encryption: EncryptionConfig.aes256(key: keyBytes32),

  // Storage tuning.
  memtableSizeBytes: 4 * 1024 * 1024,

  // Cache tuning.
  cacheMaxEntries: 10000,
  cacheMaxMemoryBytes: 64 * 1024 * 1024,
  cacheDefaultTtl: null,

  // Change streams: undelivered events one subscription may queue.
  changeStreamBufferSize: 10000,

  // Logging.
  logLevel: LogLevel.warning,
  logOutputs: [ConsoleLogOutput()],
  loggerName: 'app',

  // Transactions.
  defaultIsolationLevel: IsolationLevel.readCommitted,
  lockTimeout: const Duration(seconds: 10),

  // Schema.
  schemaVersion: 1,
  onUpgrade: null,
);
```

`close()` drains work already accepted, then closes the transaction manager,
the index manager, the storage engine (which flushes the memtable and closes
the WAL), the cache, the change-stream hub and the logger, releases the
on-disk lock and unregisters the instance. It is idempotent, a second call
made while closing waits for the first, and a failure in one step does not
skip the others: the lock is always released and the first error is rethrown
at the end.

`ReaxDB.closeAll()` closes every instance open in the current isolate. It
exists for tests and for shutdown hooks.

## Isolation levels

Four levels are implemented. Do not assume more than is listed.

### `IsolationLevel.readCommitted` (default)

Reads take no locks and always observe the latest committed value. Writes take
exclusive key locks held until commit or abort. Dirty reads are impossible
because uncommitted writes never reach storage. Non-repeatable reads are
possible.

**Lost updates are possible too.** Two transactions that each read a counter,
add one and write it back can both commit, and the counter goes up by one:
the read takes no lock, so neither sees the other. Measured: 20 concurrent
increments under `readCommitted` end at 1, under `serializable` at 20. Use
`serializable` (or `compareAndSwap`) for read-modify-write logic.

### `IsolationLevel.repeatableRead`

Reads take shared key locks held until commit or abort and are served from the
transaction's read set on repeat, so a value read twice never changes. Reading
an absent key also locks that key, which blocks a concurrent insert of that
exact key.

### `IsolationLevel.serializable`

`repeatableRead` plus commit-time revalidation of the whole read set, including
keys read as absent. "Read a missing key, someone else inserts it, commit" is
rejected with `TransactionConflictException`.

**Limit:** point reads and writes are serializable. Range and predicate
phantoms are out of scope, because the transaction API has no range read — a
query executed outside the transaction is not part of its read set.

### `IsolationLevel.optimistic`

No locks. Reads record the committed version of each key (taken before the
read, so a commit that lands during it is caught); commit revalidates the
version of every key read or written and fails with
`TransactionConflictException` if any changed. Plain `put`/`delete` calls
bump versions too, so an optimistic commit never overwrites a write made
outside any transaction. First committer wins.

`repeatableRead` and `serializable` currently run the same code: both hold
shared locks on reads and revalidate the read set at commit.

### Retries

`db.transaction(..., maxAttempts: 3)` retries only on
`TransactionConflictException` and `DeadlockException`, with exponential
backoff (`5ms * 2^(attempt-1)`, no jitter). Any other exception aborts and
propagates immediately.

Deadlocks are detected by cycle detection in the lock manager and reported as
`DeadlockException`. `lockTimeout` is a backstop for waits that are not part of
a detected cycle; exceeding it raises `TransactionTimeoutException`.

## The write pipeline

One private method, `ReaxDB._applyMutations`, mutates storage on behalf of a
plain user write; transactions commit through the same lock and the same
ordering (see below). It runs, in order:

1. **Build the batch.** For each mutation, if the key parses as
   `collection:id` and that collection has indexes, the old document is loaded
   (ignoring its expiry, so an expired document's postings are removed too)
   and `IndexManager.buildIndexOps` produces the posting deletes and inserts.
   The value operation is appended to the same list. A TTL reclamation
   re-reads its key here and is skipped if the key was rewritten since it
   was seen expired.
2. **`StorageEngine.writeBatch(ops)` — the durability point.** The WAL append
   inside it returns only when the batch is durable per the sync mode. The
   batch is atomic: after a crash either all of it or none of it is visible,
   so a document is never visible without its index postings.
3. **Optional escalation.** A `sync:` override stronger than the database
   default pushes the WAL to the OS or fsyncs it here.
4. **Cache.** Only now. The cache stores the plaintext record envelope — never
   ciphertext, and never before durability, so a failed write cannot leave a
   phantom entry. A `get` whose storage read overlapped a write of the same
   key does not fill the cache, so it can never put back an older value.
5. **Events.** `ChangeStreamHub.publish` for every mutation.

### Transactions and index maintenance

A commit runs inside the pipeline's lock. Validation, the transaction's
values and their index postings form one atomic `writeBatch`, then the cache
and change events are updated, all before any other write can run. A crash
can therefore never leave a committed document without its postings, and a
plain `put` can never interleave with a commit and leave the cache or an
index disagreeing with storage.

## Key space and the reserved 0x00 prefix

| Prefix | Contents |
| --- | --- |
| `0x00 'i' 'd' 'x' 0x00 ...` | secondary index postings |
| `0x00 'i' 'd' 'x' 'm' 0x00 ...` | index metadata |
| anything else | user keys |

`ReaxDB.encodeKey` rejects an empty key and any key whose first UTF-8 byte is
`0x00` with `InvalidKeyException`. All iteration APIs start at byte `0x01`, so
internal keys are structurally invisible to `scan`, `scanPrefix`, `keys`,
`range`, `exportTo` and `info().entryCount`.

Index postings are keyed by `(index, encoded field values, document id)` with
an empty value, so inserting or removing one posting is a single key write —
never a read-modify-write of a posting list blob.

## Storage engine

- Leveled LSM tree. Writes go to the WAL and then an in-memory memtable; a
  full memtable is flushed inline to an L0 SSTable and the WAL is checkpointed.
- Each SSTable carries a bloom filter and min/max key fences, so a point read
  skips files that cannot contain the key.
- `scan` merges the memtable snapshot with every SSTable level, honouring
  tombstones with newest-wins. It is the single source of truth for iteration.
- `StorageStats.immutableMemtableCount` is always 0: flushes are inline, so
  there is never a sealed-but-unflushed memtable.
- `flush()` persists the memtable and checkpoints the WAL. `compact()` flushes
  and then compacts every level until size targets are met.

Both `flush()` and `compact()` are serialized against the export snapshot and
the write pipeline, so a snapshot scan can never race a compaction that
deletes the files it is reading.

## Cache

One LRU cache with per-entry TTL, bounded by entry count and by memory.

- It stores exactly one representation: the **plaintext record envelope**,
  after decryption and before deserialization. This makes the 1.x
  double-decrypt bug (put cached ciphertext, get cached plaintext)
  structurally impossible.
- A `get` that finds an expired entry removes it and counts one miss and one
  expiration. One logical lookup is one hit or one miss, never three.
- Prefix invalidation is `O(log n + k)` through a key-sorted view.

`db.cacheStats` reports hits, misses, evictions, expirations, entry count,
memory and hit ratio.

## Queries, indexes and the total order

All value comparisons — conditions, `orderBy`, aggregation, `distinct`,
joins — use one total order:

```text
null < false < true < numbers < strings < lists < maps < other
```

`int` and `double` share one numeric space, so `whereEquals('n', 5)` matches
`5.0`, and `-0.0` equals `0`. Strings order by UTF-8 bytes, lists compare
element-wise, maps compare by sorted key/value pairs, and anything else
compares by `toString()`. The comparator is `memcmp` of the index encodings,
refined for large integers (below), so the indexed path and the scan path
agree.

Integers with magnitude above 2^53 share a double encoding with their
neighbours. Comparisons break such ties exactly (2^53 + 1 is greater than
2^53), and the index planner scans an inexact bound inclusively, so the index
returns a superset and the exact re-filter of loaded documents decides.

Comparisons across types follow the total order: `whereGreaterThan('age', 5)`
matches a document whose `age` is the string `'old'` (strings sort after
numbers), and `whereLessThan('age', 50)` matches documents with no `age` (null
sorts first). Add a type condition if your data mixes types.

Without `orderBy`, the order of results, and therefore which documents a
`limit` returns, depends on whether an index served the query. Add `orderBy`
when it matters.

Collection names must not contain `:`, because document keys are
`collection:id` and are split at the first `:`. `query`, `collection` and
`createIndex` reject such names with `QueryException`.

`IndexManager.candidateIds` returns `null` when no index can serve any
condition (the caller scans) and an empty set when an index was consulted and
nothing matched. Those two cases are never conflated. Candidates are
streamed, so a `limit` without `orderBy` (and `findOne`) stops the index scan
as soon as enough documents matched.

`count()` without conditions iterates keys only. With conditions it loads and
checks every candidate document.

Compound indexes are used for an equality prefix plus one trailing range or
`whereIn` component, in field order.

Secondary index changes follow the same rules: `createIndex` registers the
index under the pipeline lock and backfills in chunks, re-reading every
document under the lock so a document changed during the backfill is indexed
by its current version. `dropIndex` removes the index metadata first, so a
crash midway leaves only orphaned postings (cleared by the next
`createIndex` of that index), never an index that reloads incomplete.

## Encryption

See the envelope layout in the README. Additional details:

- The GCM authentication tag is 128 bits and the IV is a fresh 96-bit value
  from `Random.secure()` per value.
- Decryption validates the envelope version and that the stored algorithm id
  matches the configured algorithm before attempting anything.
- Authentication failure (tampering or a wrong key) throws
  `EncryptionException`. There is no path that returns partially decrypted
  bytes.
- `KeyDerivation.defaultIterations` is 210,000 (the 2023+ OWASP figure for
  PBKDF2-HMAC-SHA256) and `minIterations` is 1,000. Lower values are rejected.
  Tests may pass a low count explicitly; production code should not.
- The salt is per database, generated with `Random.secure()` on first open and
  stored in `reaxdb.meta` together with the iteration count.
- Every value is authenticated together with its key (GCM associated data),
  so a ciphertext moved to another key fails to decrypt. Envelopes written
  by 2.0.0 (version 0x01, without associated data) are still read.
- The header stores a key check derived from the key, so a wrong key or
  passphrase is rejected at `open`. A 2.0.0 database gets one on its first
  open by 2.1, after the key has been proven against a stored value.
- PBKDF2 runs on a background isolate (`Isolate.run`); at 210,000 iterations
  it takes about 1.5 s in an AOT build on an Apple M-series machine, more on
  phones, once per open.
- Only values are encrypted. Keys, index postings (which contain the indexed
  field values) and the header are plaintext.

`db.encryptionInfo` reports the **effective** algorithm, never a requested one.

## Logging and redaction

Loggers are per instance:

```dart
final db = await ReaxDB.open(
  path: 'data/app',
  logLevel: LogLevel.debug,
  logOutputs: [ConsoleLogOutput(), MemoryLogOutput()],
);
```

`ReaxLogger.root` remains available for code not tied to a database. The
top-level `logger` alias is deprecated, because it collides with
`package:logger`. `db.close()` closes the database's logger and
the outputs it owns.

File logging lives behind a separate entry point, so the main barrel's
surface stays small:

```dart
import 'package:reaxdb_dart/reaxdb_dart_io.dart';

final db = await ReaxDB.open(
  path: 'data/app',
  logOutputs: [FileLogOutput('logs/reaxdb.log')],
);
```

### No-PII policy

ReaxDB never logs key contents, value contents, encryption keys, passphrases
or derived key material, at any level including debug. When a message must
reference a key or value, use the redaction helpers:

```dart
Redaction.key('user:ada@example.com');  // key#3f2a9c1d(len=21)
Redaction.value({'a': 1});              // <redacted Map, 1 entries>
```

The key fingerprint is an HMAC-SHA256 under a random key chosen per isolate,
so the same key gives the same fingerprint within one run (log lines stay
correlatable), but a fingerprint cannot be reversed by hashing guesses such
as email addresses or phone numbers.

`ReaxEntry.toString()` and `DatabaseChangeEvent.toString()` are redacted the
same way, because those objects routinely end up in log lines and exception
messages.

## Concurrency model

A database is used from one isolate at a time. Within that isolate:

- The storage engine serializes every mutation, flush, compaction and close.
- The transaction manager serializes commits behind one mutex.
- The lock manager provides shared/exclusive key locks with upgrade support
  and cycle-based deadlock detection.
- The database facade serializes pipeline writes against export, flush,
  compaction and close.

Concurrent `await`ed calls from the same isolate are safe. A second open of
an open database fails with `DatabaseLockedException`, whether it comes from
another process, another isolate of this process, or a symlinked or
differently cased path in this isolate. A `flock` on `<path>/LOCK.flock`
(through `dart:ffi`) belongs to one open file, not to the whole process, so
it excludes other isolates and aliases; the `dart:io` advisory lock on
`<path>/LOCK` excludes other processes, including ReaxDB 2.0. They are on
separate files because closing any descriptor of a file drops all of the
process's `fcntl` locks on it. On Windows the `dart:io` lock alone gives the
same guarantee. If
an isolate that holds a database dies without closing it, the lock is
released when that isolate's memory is reclaimed; close databases
explicitly.

## Operational notes

### Choosing a sync mode

- `SyncMode.full` — financial records, anything where losing the last
  acknowledged write is unacceptable. Default.
- `SyncMode.os` — caches and derived data that must survive a crash of your
  process but can be rebuilt after a power cut.
- `SyncMode.none` — throwaway data and benchmarks only.

Prefer batching over lowering the sync mode: `putBatch` costs one durability
barrier for the whole batch.

### Reclaiming space

Deleted and expired entries occupy space until compaction rewrites the level
that holds them. Call `db.compact()` during idle time after a large deletion,
and `db.purgeExpired()` if you rely heavily on TTL.

### Sizing the memtable

A larger `memtableSizeBytes` means fewer flushes and fewer L0 tables, at the
cost of a longer WAL replay on open and more resident memory. The 4 MB default
is a reasonable starting point for mobile.

### Backups

`exportTo` blocks writes for the duration of the scan. On a large database
schedule it when the application is idle. The archive is a single streamed
file with a verified SHA-256 digest, written to a temporary file and renamed
into place. It is plaintext unless you pass `encryption:`; it is created with
the process's default file permissions, so keep it somewhere only your app
can read.

`importFrom` verifies the digest over the whole file first, restores in
batches into a temporary sibling directory, recreates the indexes and only
then moves the result into place. Archives written by 2.0 (format version 1)
are still restored.

### Disk full and other I/O failures

A failed write (for example `ENOSPC`) throws `StorageException` and is not
applied: the write-ahead log is rolled back to the last complete record, so
the database stays readable and later writes succeed once space is free. A
failure in background maintenance after a write is already durable (flushing
the memtable, compaction) does not fail that write; it is logged and retried
on a later write.
