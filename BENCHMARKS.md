# ReaxDB benchmarks

Every number on this page was produced by
[`benchmark/reaxdb_benchmark.dart`](benchmark/reaxdb_benchmark.dart) on the
machine described below, on the date below. Nothing here is estimated,
extrapolated, or copied from anyone else's measurements. If a number is not in
this file, it was not measured.

## What was measured, and how

**Date:** 16 October 2026, ReaxDB 2.1.0. The 2.0.0 comparison column was
measured the same day, on the same machine, with the 2.0.0 harness.

**Machine**

| | |
| --- | --- |
| Model | Apple M5 Pro, 15 logical cores |
| Memory | 24 GiB |
| Storage | Internal NVMe SSD, APFS |
| OS | macOS 27.0 (build 26A428) |
| Dart | 3.13.5 (stable), `macos_arm64` |
| Build | AOT (`dart compile exe`), asserts disabled |

**Command**

```bash
dart compile exe benchmark/reaxdb_benchmark.dart -o reaxdb_benchmark
./reaxdb_benchmark --sizes=1000,10000,100000 --runs=3 --sync=full,none \
  --json=results.json
```

The numbers are from an AOT build because that is how a Flutter app runs in
release mode. Under `dart run` (JIT) most rows are similar, but some are not
representative: in a JIT run that has already exercised the smaller sizes,
`query_indexed` at 100,000 documents measured 517-553 ms, against 205-221 ms
in a fresh JIT process and 218 ms in the AOT build. Benchmark AOT builds, or a
Flutter release build, before drawing conclusions.

Three repetitions of each scenario were run; the reported row is the **median
repetition**, and the `run spread` column is the gap between the slowest and
the fastest repetition, so you can see for yourself how stable each
measurement was. The `query_*` rows are from a second run of the `query`
scenario, made after the last change to the query path.

**Method**

- Every individual operation is timed with a `Stopwatch` at microsecond
  resolution. The first 10% of samples in each repetition are discarded as
  warmup.
- `p50` / `p95` / `p99` / `max` are per-sample latency. Where fewer than 20
  samples exist the tail percentiles are shown as `n/a` rather than invented.
- `ops/sec` counts **items**: a 500-document batch counts as 500 documents, so
  batch and single-write throughput are directly comparable.
- Documents are `{id, name, category, score, active, payload}` with a 200-byte
  payload. Keys are `bench:%09d`.
- Each repetition runs against a freshly created database in a temporary
  directory, which is deleted afterwards.

`benchmark/README.md` documents every flag and scenario if you want to
reproduce or vary this.

## ReaxDB and Hive

Measured on 16 October 2026, on the same machine as everything else on this
page, with [`benchmark/hive_comparison.dart`](benchmark/hive_comparison.dart)
built AOT, against `hive_ce` 2.22.0. Both databases store the same documents
(`{id, name, category, score, active, payload}` with a 200-byte payload).
Every scenario ran three times on a fresh database, and the median
repetition is reported. The ReaxDB numbers are from 2.5.0, which adds
`keepInMemory` and moves LSM maintenance off the write path. The open rows
were measured again after the last change to how memory mode loads.

```bash
dart compile exe benchmark/hive_comparison.dart -o hive_comparison
./hive_comparison --sizes=1000,10000,100000 --runs=3
```

"Durable" means the write is on disk (fsync) before the call returns:
ReaxDB's default, and `put` followed by `flush()` in Hive. Hive's plain `put`
returns before the data is flushed. "ReaxDB memory" is a database opened with
`keepInMemory: true`.

**100,000 documents**

| Scenario | ReaxDB | ReaxDB memory | Hive |
| --- | ---: | ---: | ---: |
| Durable write, one at a time | 19.0k/s | 19.2k/s | 23.5k/s |
| Write that survives an app kill (`SyncMode.os`) | 51.1k/s | | |
| Write with no flush | 144.7k/s (`SyncMode.none`) | | 67.1k/s (`put`) |
| Durable batch of 500 | 199.5k/s | | 309.1k/s |
| Read, warm | 44.3k/s | 0.99M/s (`getSync` 1.16M/s) | 0.78M/s |
| Read, first after reopening | 42.9k/s | 0.61M/s | 0.77M/s |
| Open | 11.0 ms | 177 ms | 261 ms |
| Iterate every document once | 0.18M/s | 0.53M/s | 18.1M/s |
| Query, 20,000 results | 449 ms (indexed) | 24 ms (no index), 74 ms (indexed) | 3.9 ms (`values.where`) |
| Size on disk | 31.5 MB | 31.5 MB | 32.0 MB |

**10,000 documents**

| Scenario | ReaxDB | ReaxDB memory | Hive |
| --- | ---: | ---: | ---: |
| Durable write, one at a time | 21.6k/s | 22.7k/s | 22.7k/s |
| Durable batch of 500 | 336k/s | | 419k/s |
| Read, warm | 1.07M/s | 1.33M/s (`getSync` 2.20M/s) | 2.22M/s |
| Read, first after reopening | 44.2k/s | 0.71M/s | 1.62M/s |
| Open | 1.8 ms | 15 ms | 20 ms |
| Iterate every document once | 0.47M/s | 0.72M/s | 26.5M/s |
| Query, 2,000 results | 4.7 ms (indexed) | 1.1 ms (no index) | 0.23 ms |

What this shows:

- **Durable writes cost about the same in both.** The fsync dominates; at
  100,000 documents ReaxDB is about 20% slower.
- **Writes that skip the flush are faster in ReaxDB**: 144.7k/s with
  `SyncMode.none` against 67.1k/s for Hive's `put` at 100,000 documents.
  `SyncMode.os`, which also survives an app kill, runs at 51.1k/s.
- **Batches are within a factor of 1.5.** Before 2.5.0 a durable
  `putBatch` at 100,000 documents ran at 59.9k/s, a fifth of Hive's; moving
  flushes and compaction off the write path and a constant-time record cache
  brought it to 199.5k/s.
- **With `keepInMemory`, point reads are close to Hive's or faster**: about
  1.3 times faster at 100,000 documents, the same with `getSync` at 10,000.
  The first read of each value after opening decodes it, so it is slower
  than Hive, which decodes everything while opening. Without `keepInMemory`,
  ReaxDB reads from disk.
- **Opening in memory mode is faster than Hive's open**: 177 ms against
  261 ms at 100,000 documents. ReaxDB loads each table with one read and
  decodes values on first use, while Hive decodes everything up front.
  Without `keepInMemory`, ReaxDB opens in about 11 ms.
- **Iteration and queries are still clearly faster in Hive.** Hive hands
  out the objects it stores; ReaxDB returns copies, so changing a result
  never changes the database, and its scans are streams. A query that
  returns 20,000 documents copies 20,000 maps.
- **On disk the two are the same size.**

These are numbers for one machine and one document shape. Benchmark your
own data before deciding.

## 2.1.0 against 2.0.0

At 100,000 documents, both versions built AOT on the same machine:

| Scenario | 2.0.0 | 2.1.0 | Change |
| --- | ---: | ---: | ---: |
| Sequential write, `SyncMode.full`, ops/sec | 16,793 | 19,642 | +17% |
| Random write, `SyncMode.full`, ops/sec | 14,105 | 17,863 | +27% |
| `putBatch` of 500, `SyncMode.full`, docs/sec | 79,505 | 85,430 | +7% |
| Point read, cold, p50 | 25 us | 24 us | same |
| Range scan of 100 entries, p50 | 9.18 ms | 0.62 ms | 15x faster |
| Indexed query, 5,000 matches, p50 | 208 ms | 218 ms | +5% slower |
| Full-scan query, 5,000 matches, p50 | 400 ms | 372 ms | 7% faster |
| Transaction commit, `SyncMode.full`, ops/sec | 18,039 | 18,227 | same |
| Open after a clean close, p50 | 12.50 ms | 11.09 ms | 11% faster |

The range scan gain comes from lazy, chunked SSTable iteration that stops at
the window's end (2.0.0 read every table to its end). The write gains were not
traced to a single change; 2.1 rewrote large parts of the WAL and of table
management. The indexed query is the one row that got slower; at 1,000 and
10,000 documents the difference is 7% and 2%.

## Headline results

### Durability is the biggest lever

`SyncMode.full` fsyncs the write-ahead log before a write is acknowledged;
`SyncMode.none` leaves the write in a buffer. At 100,000 documents:

| Operation | `SyncMode.full` (durable) | `SyncMode.none` (buffered) | Ratio |
| --- | ---: | ---: | ---: |
| Sequential write, p50 | 40 us | 4 us | 10x |
| Sequential write, ops/sec | 19,642 | 76,827 | 3.9x |
| Random write, p50 | 43 us | 6 us | 7.2x |
| Transaction commit, p50 | 48 us | 7 us | 6.9x |

This is the honest trade-off, and it is what the modes mean. The
`reopen_wal_replay` scenario makes the consequence concrete: a child process
writes N documents and is killed without closing the database, then the parent
opens it and counts what survived.

| Documents acknowledged | Recovered with `SyncMode.full` | Recovered with `SyncMode.none` |
| ---: | ---: | ---: |
| 1,000 | 1,000 | 991 |
| 10,000 | 10,000 | 9,856 |
| 100,000 | 100,000 | 99,964 |

`SyncMode.none` lost acknowledged writes in every run. That is the documented
meaning of the mode, not a defect, but it is why `SyncMode.full` is the default
and why the buffered numbers above must never be quoted as "ReaxDB's write
performance" without the caveat.

### Batching amortises the fsync

At 100,000 documents a 500-document `putBatch` costs one WAL sync for the whole
batch:

| Path | `SyncMode.full` | `SyncMode.none` |
| --- | ---: | ---: |
| `put`, one document at a time | 19,642 docs/sec | 76,827 docs/sec |
| `putBatch`, 500 documents | 85,430 docs/sec | 85,956 docs/sec |

Batching is a 4.3x improvement for durable writes, and it makes the sync mode
almost irrelevant: once the fsync is amortised across 500 documents the
bottleneck is the memtable flush and compaction, not the sync. Batch, and keep
the durability.

### Cache hits are not storage reads

These are reported as two separate rows and must not be conflated. The harness
sizes the record cache to hold the whole data set, then measures a first pass
(cache empty, data on disk after a `compact()` and a reopen) and a second pass
over the same keys. The measured cache hit ratio is printed with each row.

| 100,000 documents | p50 | p99 | ops/sec | cache hit ratio |
| --- | ---: | ---: | ---: | ---: |
| `point_read_cold` (SSTable read) | 24 us | 31 us | 41,126 | 0.0% |
| `point_read_warm` (in-process cache) | 1 us | 1 us | 1,278,518 | 100.0% |

A cache hit is roughly 24x faster than a storage read. Publishing only the warm
number would inflate ReaxDB's apparent read throughput by about that factor,
so both are published. An application whose cache is smaller than its working
set lands between the two rows.

"Cold" here means *ReaxDB's* record cache and memtable are empty. The operating
system's page cache is not purged (this harness cannot do that portably), so
`point_read_cold` is an upper bound on read performance, not a cold-boot
number.

### Indexes help, but less as the result set grows

`query_indexed` and `query_full_scan` run the same equality query over two
identical collections, one with a secondary index on the field and one without.

| Documents | Matches returned | Indexed p50 | Full-scan p50 | Speed-up |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 50 | 184 us | 3.67 ms | 20x |
| 10,000 | 500 | 1.80 ms | 37.90 ms | 21x |
| 100,000 | 5,000 | 218 ms | 372 ms | 1.7x |

The index is worth roughly 20x while the result set is small. At 100,000
documents the query matches 5,000 documents, and loading and decoding those
5,000 documents (one storage read each, about 25 us) dominates everything the
index saved. Queries that select a small fraction of a collection benefit from
indexes; queries that select 5% of a large collection barely do. A `limit`
without `orderBy` stops early, so it pays only for the documents it returns.

## Full results

Median of 3 repetitions. `n` is the number of samples in the reported
repetition; tail percentiles are `n/a` where `n < 20`.

| scenario | docs | sync | unit | n | p50 | p95 | p99 | max | ops/sec | run spread | notes |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| sequential_write | 1000 | full | document | 900 | 39.0 us | 51.0 us | 59.0 us | 205.0 us | 24,591 | 13.7% |  |
| sequential_write | 1000 | none | document | 900 | 4.0 us | 5.0 us | 9.0 us | 41.0 us | 235,294 | 17.6% |  |
| sequential_write | 10000 | full | document | 9000 | 43.0 us | 52.0 us | 61.0 us | 496.0 us | 22,888 | 8.1% |  |
| sequential_write | 10000 | none | document | 9000 | 4.0 us | 4.0 us | 6.0 us | 659.0 us | 221,637 | 6.9% |  |
| sequential_write | 100000 | full | document | 90000 | 40.0 us | 53.0 us | 62.0 us | 346 ms | 19,642 | 0.1% |  |
| sequential_write | 100000 | none | document | 90000 | 4.0 us | 5.0 us | 10.0 us | 350 ms | 76,827 | 1.0% |  |
| random_write | 1000 | full | document | 900 | 40.0 us | 51.0 us | 59.0 us | 288.0 us | 24,046 | 9.4% |  |
| random_write | 1000 | none | document | 900 | 5.0 us | 8.0 us | 46.1 us | 148.0 us | 161,406 | 21.9% |  |
| random_write | 10000 | full | document | 9000 | 42.0 us | 53.0 us | 60.0 us | 481.0 us | 22,908 | 4.3% |  |
| random_write | 10000 | none | document | 9000 | 5.0 us | 7.0 us | 9.0 us | 592.0 us | 168,149 | 0.4% |  |
| random_write | 100000 | full | document | 90000 | 43.0 us | 55.0 us | 63.0 us | 604 ms | 17,863 | 0.8% |  |
| random_write | 100000 | none | document | 90000 | 6.0 us | 8.0 us | 10.0 us | 604 ms | 55,656 | 1.3% |  |
| batch_write | 1000 | full | document | 2 | 1.35 ms | n/a | n/a | 1.38 ms | 369,549 | 26.9% | batch=500 |
| batch_write | 1000 | none | document | 2 | 1.29 ms | n/a | n/a | 1.30 ms | 387,147 | 3.4% | batch=500 |
| batch_write | 10000 | full | document | 18 | 1.34 ms | n/a | n/a | 2.20 ms | 345,835 | 7.9% | batch=500 |
| batch_write | 10000 | none | document | 18 | 1.31 ms | n/a | n/a | 2.25 ms | 361,809 | 4.3% | batch=500 |
| batch_write | 100000 | full | document | 180 | 1.39 ms | 3.08 ms | 80.16 ms | 361 ms | 85,430 | 1.4% | batch=500 |
| batch_write | 100000 | none | document | 180 | 1.35 ms | 3.27 ms | 80.64 ms | 357 ms | 85,956 | 0.6% | batch=500 |
| point_read_cold | 1000 | full | read | 900 | 23.0 us | 27.0 us | 29.0 us | 32.0 us | 43,894 | 4.5% | cacheHitRatio=0.0% |
| point_read_warm | 1000 | full | read | 900 | 1.0 us | 1.0 us | 1.0 us | 2.0 us | 1,855,670 | 60.6% | cacheHitRatio=100.0% |
| point_read_cold | 10000 | full | read | 9000 | 23.0 us | 27.0 us | 30.0 us | 1.01 ms | 42,732 | 1.3% | cacheHitRatio=0.0% |
| point_read_warm | 10000 | full | read | 9000 | 1.0 us | 1.0 us | 1.0 us | 701.0 us | 1,473,960 | 5.9% | cacheHitRatio=100.0% |
| point_read_cold | 100000 | full | read | 90000 | 24.0 us | 28.0 us | 31.0 us | 1.25 ms | 41,126 | 1.4% | cacheHitRatio=0.0% |
| point_read_warm | 100000 | full | read | 90000 | 1.0 us | 1.0 us | 1.0 us | 2.15 ms | 1,278,518 | 4.8% | cacheHitRatio=100.0% |
| range_scan | 1000 | full | entry | 18 | 433.0 us | n/a | n/a | 454.0 us | 233,402 | 0.6% | window=100, entriesRead=2000 |
| range_scan | 10000 | full | entry | 90 | 423.0 us | 441.0 us | 664.1 us | 924.0 us | 231,995 | 1.6% | window=100, entriesRead=10000 |
| range_scan | 100000 | full | entry | 450 | 617.0 us | 646.0 us | 713.5 us | 753.0 us | 172,578 | 2.3% | window=100, entriesRead=50000 |
| query_indexed | 1000 | full | query | 36 | 183.5 us | 193.0 us | 521.9 us | 699.0 us | 5,090 | 6.8% | matches=50 |
| query_full_scan | 1000 | full | query | 36 | 3.67 ms | 3.79 ms | 3.80 ms | 3.80 ms | 271.9 | 0.7% | matches=50 |
| query_indexed | 10000 | full | query | 36 | 1.80 ms | 14.60 ms | 15.67 ms | 15.92 ms | 165.4 | 1.1% | matches=500 |
| query_full_scan | 10000 | full | query | 36 | 37.90 ms | 38.54 ms | 38.78 ms | 38.88 ms | 26.4 | 1.7% | matches=500 |
| query_indexed | 100000 | full | query | 9 | 218 ms | n/a | n/a | 263 ms | 4.5 | 1.1% | matches=5000 |
| query_full_scan | 100000 | full | query | 9 | 372 ms | n/a | n/a | 376 ms | 2.7 | 0.6% | matches=5000 |
| transaction_commit | 1000 | full | commit | 900 | 48.0 us | 65.0 us | 78.0 us | 310.0 us | 20,100 | 10.5% | opsPerTx=1 |
| transaction_commit | 1000 | none | commit | 900 | 7.0 us | 8.0 us | 16.0 us | 371.0 us | 130,378 | 1.5% | opsPerTx=1 |
| transaction_commit | 10000 | full | commit | 9000 | 48.0 us | 78.0 us | 200.0 us | 5.00 ms | 18,105 | 10.4% | opsPerTx=1 |
| transaction_commit | 10000 | none | commit | 9000 | 7.0 us | 9.0 us | 33.0 us | 534.0 us | 120,984 | 23.5% | opsPerTx=1 |
| transaction_commit | 100000 | full | commit | 18000 | 48.0 us | 80.0 us | 150.0 us | 21.11 ms | 18,227 | 5.2% | opsPerTx=1 |
| transaction_commit | 100000 | none | commit | 18000 | 7.0 us | 8.0 us | 32.0 us | 21.65 ms | 109,148 | 11.5% | opsPerTx=1 |
| reopen_clean | 1000 | full | open | 4 | 1.02 ms | n/a | n/a | 1.34 ms | 921.2 | 38.3% |  |
| reopen_clean | 10000 | full | open | 4 | 1.74 ms | n/a | n/a | 2.19 ms | 542.2 | 9.2% |  |
| reopen_clean | 100000 | full | open | 4 | 11.09 ms | n/a | n/a | 11.52 ms | 89.6 | 3.4% |  |
| reopen_wal_replay | 1000 | full | open | 1 | 3.63 ms | n/a | n/a | 3.63 ms | 275.3 | 13.1% | acknowledged=1000, recovered=1000 |
| reopen_wal_replay | 1000 | none | open | 1 | 3.64 ms | n/a | n/a | 3.64 ms | 274.4 | 4.6% | acknowledged=1000, recovered=991 |
| reopen_wal_replay | 10000 | full | open | 1 | 26.85 ms | n/a | n/a | 26.85 ms | 37.2 | 6.7% | acknowledged=10000, recovered=10000 |
| reopen_wal_replay | 10000 | none | open | 1 | 27.78 ms | n/a | n/a | 27.78 ms | 36.0 | 0.3% | acknowledged=10000, recovered=9856 |
| reopen_wal_replay | 100000 | full | open | 1 | 460 ms | n/a | n/a | 460 ms | 2.2 | 3.5% | acknowledged=100000, recovered=100000 |
| reopen_wal_replay | 100000 | none | open | 1 | 448 ms | n/a | n/a | 448 ms | 2.2 | 3.8% | acknowledged=100000, recovered=99964 |

## Reading the tails

The `max` column is where maintenance work shows up. In 2.1.0 the slowest
single `put` in the 100,000-document run took 346 ms (`SyncMode.full`) and
350 ms (`SyncMode.none`), and the slowest 500-document batch took 361 ms:
those calls flushed the memtable and ran a multi-level compaction inline.

Since 2.5.0 flushing and compaction run in the background, after the write
that triggered them, and the default level sizes rewrite data about half as
often. Measured the same way with 2.5.0, at 100,000 documents:

| scenario | sync | p99 | max | ops/sec |
| --- | --- | ---: | ---: | ---: |
| sequential_write | full | 99 us | 18.63 ms | 19,580 |
| sequential_write | none | 12 us | 19.17 ms | 146,835 |
| random_write | full | 96 us | 19.52 ms | 19,007 |
| random_write | none | 13 us | 18.19 ms | 128,510 |
| batch_write (500) | full | 19.72 ms | 20.72 ms | 204,562 |
| batch_write (500) | none | 20.54 ms | 23.11 ms | 204,248 |

The worst write is now about 20 ms instead of about 350 ms. The p99 of a
single write rose from about 60 us to about 100 us, because the background
work shares the isolate with the writes; the total is lower. Writes slow
down only if two sealed memtables are already waiting for their flush.

Startup behaves the same way. Opening a cleanly closed 100,000-document
database takes 11.09 ms, because the WAL was checkpointed during `close()` and
there is nothing to replay. Opening the same database after a process was
killed takes 460 ms, because the records written since the last checkpoint
have to be replayed from the log. Frequent checkpoints buy faster recovery.

Range scans now cost about the same per entry at every size: a 100-entry
window costs 4.3 us per entry at 1,000 documents, 4.2 us at 10,000 and 6.2 us
at 100,000 (2.0.0: 7.4 us, 31.1 us and 91.8 us in the same AOT run).

## Limitations of this methodology

Read these before quoting anything above.

1. **One machine, one filesystem, one session.** Everything here is an Apple M5
   Pro with APFS on internal NVMe. A phone, a spinning disk, a network
   filesystem or an emulator will behave differently, sometimes by an order of
   magnitude.
2. **`fsync` is not `F_FULLFSYNC`.** `SyncMode.full` calls
   `RandomAccessFile.flush()`, which is `fsync(2)`. On macOS and APFS that does
   not force the drive's own write cache. The durability being measured is
   "survives a process kill"; durability against sudden power loss on a drive
   with a volatile write cache is a stronger property that these numbers do not
   represent.
3. **The OS page cache is warm.** No scenario purges it, so even the "cold"
   reads benefit from it. Treat `point_read_cold` as an upper bound.
4. **Single isolate, no contention.** Every scenario is strictly sequential.
   Nothing here says how ReaxDB behaves with concurrent writers, readers racing
   a compaction, or transactions contending for the same keys. That is what
   `test/stress/` covers, and it is a correctness suite, not a performance one.
5. **200-byte documents only.** Value size was not varied. Large values (the
   stress suite round-trips 1 MiB values) were not benchmarked.
6. **No encryption.** Every measurement uses `EncryptionConfig.none()`.
   AES-256-GCM adds per-record cost that is not reflected here, and opening a
   passphrase-encrypted database adds the PBKDF2 derivation (about 1.5 s at the
   default 210,000 iterations in an AOT build on this machine, run off the
   calling isolate).
7. **Rows with `n < 20` have no meaningful tail.** `batch_write` at 1,000
   documents is two samples. Treat those rows as an order of magnitude.
8. **A `run spread` above roughly 15% means the machine was noisy.** Several
   small-size rows exceed that, and `point_read_warm` sits at the resolution
   limit of the clock. Prefer the 100,000-document rows, whose spreads are
   mostly under 5%, when you need a number you can rely on.
9. **`--runs=3` is a small sample.** Taking the median of three repetitions
   protects against one unlucky run, not against a systematic bias. Raise
   `--runs` if you need tighter confidence.

## Reproducing this

```bash
git clone https://github.com/dvxlabs520/Reax-BD
cd Reax-BD
dart pub get
dart compile exe benchmark/reaxdb_benchmark.dart -o reaxdb_benchmark
./reaxdb_benchmark --sizes=1000,10000,100000 --runs=3 --sync=full,none \
  --json=results.json
```

`dart run benchmark/reaxdb_benchmark.dart` with the same flags also works (see
the JIT caveat above). The harness prints the machine, Dart version, core
count, assert state and configuration it ran with, so any output you post is
self-describing. See [`benchmark/README.md`](benchmark/README.md) for the
scenario catalogue and the full option list.
