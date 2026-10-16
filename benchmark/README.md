# ReaxDB benchmark harness

A single, self-contained Dart program that measures ReaxDB through its public
API and prints latency percentiles, throughput and the environment the numbers
came from. The results published in [`../BENCHMARKS.md`](../BENCHMARKS.md) are
produced by this harness and nothing else.

## Running it

```bash
# Everything, default sizes (1k and 10k documents), 3 repetitions
dart run benchmark/reaxdb_benchmark.dart

# The exact commands used for BENCHMARKS.md (an AOT build, like a Flutter
# release build)
dart compile exe benchmark/reaxdb_benchmark.dart -o reaxdb_benchmark
./reaxdb_benchmark --sizes=1000,10000,100000 --runs=3 --sync=full,none \
  --json=results.json

# One scenario, one size
dart run benchmark/reaxdb_benchmark.dart --scenarios=point_read --sizes=100000

# What can I run?
dart run benchmark/reaxdb_benchmark.dart --list-scenarios
dart run benchmark/reaxdb_benchmark.dart --help
```

Under `dart run` the VM's JIT compiles the code with type feedback from the
scenarios that ran first, which can skew later rows (see BENCHMARKS.md). Use
an AOT build for numbers you intend to publish.

Every database is created in a fresh temporary directory (under
`--work-dir`, the system temp directory by default) and deleted afterwards, so
runs do not contaminate each other.

### Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--sizes=` | `1000,10000` | Document counts to benchmark, comma-separated |
| `--sync=` | `full,none` | Sync modes for durability-sensitive scenarios |
| `--scenarios=` | all | Subset of scenarios to run |
| `--runs=` | `3` | Repetitions per combination; the median is reported |
| `--warmup=` | `0.1` | Fraction of leading samples discarded |
| `--value-bytes=` | `200` | Payload bytes per document |
| `--batch-size=` | `500` | Documents per `putBatch` call |
| `--seed=` | `20260730` | RNG seed for the shuffled key orders |
| `--work-dir=` | system temp | Where the temporary databases are created |
| `--json=` | none | Also write machine-readable results |

## Scenarios

| Scenario | What it measures |
| --- | --- |
| `sequential_write` | `put()` per document, ascending key order |
| `random_write` | `put()` per document, shuffled key order |
| `batch_write` | `putBatch()` of `--batch-size` documents; one WAL sync per batch |
| `point_read_cold` | `get()` with an empty record cache, after `compact()` and a reopen, so the read is served by the SSTables |
| `point_read_warm` | `get()` of the same keys a second time, served by the in-process record cache |
| `range_scan` | `scan()` of 100 consecutive entries from a random offset |
| `query_indexed` | equality query answered through a secondary index |
| `query_full_scan` | the same query over an identical collection with no index |
| `transaction_commit` | `transaction()` containing one `put`, committed |
| `reopen_clean` | `open()` of a cleanly closed database (WAL already checkpointed) |
| `reopen_wal_replay` | `open()` after a child process died with an uncheckpointed WAL |

`reopen_wal_replay` re-invokes this same script with `--crash-writer=...` in a
child process. The child writes the documents and calls `exit(0)` without
closing the database, so the parent's `open()` has to replay the whole log.
That is also why the scenario doubles as a durability check: under
`SyncMode.full` it fails loudly if a single acknowledged write is missing.

## How to read the output

For every scenario, size and sync mode the harness runs `--runs` repetitions.
Each repetition times **every individual operation** with a `Stopwatch` at
microsecond resolution, discards the first `--warmup` fraction of samples, and
keeps the rest. The repetitions are ranked by throughput and the **median
repetition** is reported, so a single unlucky run cannot set the headline
number.

- `n` — samples in the reported repetition. When `n < 20` the tail
  percentiles are printed as `n/a`, because three data points do not have a
  p99. Use them as an order of magnitude, not a service-level objective.
- `p50` / `p95` / `p99` / `max` — per-sample latency. For `batch_write` and
  `range_scan` a sample is a whole batch or a whole scan, not one document.
- `ops/sec` — **items** per second, derived from the summed sample latencies.
  A batch of 500 documents counts as 500 items, so `batch_write` throughput is
  directly comparable with `sequential_write` throughput.
- `run spread` — the gap between the slowest and the fastest repetition. Above
  roughly 15% the machine was noisy and the numbers should not be quoted.
- `notes` — scenario-specific context, most importantly `cacheHitRatio`
  (0% for the cold reads, 100% for the warm ones) and
  `acknowledged` / `recovered` for the crash-replay scenario.

## What these numbers do and do not mean

- **Cache hits are not storage reads.** `point_read_warm` measures a hash-map
  lookup in this process, `point_read_cold` measures a bloom-filter check plus
  an SSTable read. They are reported as two separate rows on purpose. The
  harness sizes the record cache to hold the whole data set so the warm row is
  genuinely 100% hits; a production configuration with a smaller cache lands
  somewhere between the two rows.
- **Cold does not mean the disk is cold.** After the reopen ReaxDB's own cache
  and memtable are empty, but the operating system's page cache is not, and
  this harness does not (and portably cannot) purge it. `point_read_cold` is
  therefore an upper bound on the achievable read rate, not a first-boot
  number.
- **`SyncMode.full` means `fsync`, not "on the platter".** The WAL calls
  `RandomAccessFile.flush()`, which is `fsync(2)`. On macOS and APFS `fsync`
  does not force the drive's write cache (that needs `F_FULLFSYNC`), so the
  durability being measured is "survives a process kill", which is exactly
  what ReaxDB's contract promises. Durability against sudden power loss on a
  drive with a volatile write cache is a stronger property that these numbers
  do not represent.
- **`SyncMode.none` can lose acknowledged writes.** That is not a defect, it
  is the documented meaning of the mode, and `reopen_wal_replay` prints how
  many records actually survived so the trade-off is visible rather than
  implied.
- **Single isolate, no contention.** Every scenario is sequential. Concurrent
  behaviour is covered by the tests in `test/stress/`, not here.
- **No other database is measured.** The harness benchmarks ReaxDB only.
  Comparing these numbers with figures another project published for another
  machine would be meaningless.
