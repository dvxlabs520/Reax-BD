/// Reproducible ReaxDB benchmark harness.
///
/// Run it with:
///
/// ```
/// dart run benchmark/reaxdb_benchmark.dart
/// dart run benchmark/reaxdb_benchmark.dart --sizes=1000,10000 --sync=full
/// dart run benchmark/reaxdb_benchmark.dart --scenarios=point_read,range_scan
/// ```
///
/// See `benchmark/README.md` for how to read the output.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:reaxdb_dart/reaxdb_dart.dart';

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Runs the harness.
Future<void> main(List<String> arguments) async {
  final BenchOptions options;
  try {
    options = BenchOptions.parse(arguments);
  } on FormatException catch (error) {
    stderr.writeln('reaxdb_benchmark: ${error.message}');
    stderr.writeln(BenchOptions.usage);
    exitCode = 64;
    return;
  }

  if (options.crashWriter != null) {
    // Re-entrant mode: this process is the "crashing writer" used by the
    // reopen_wal_replay scenario. It writes and exits WITHOUT closing the
    // database, leaving an uncheckpointed write-ahead log behind.
    await _runCrashWriter(options.crashWriter!);
    return;
  }

  if (options.showHelp) {
    stdout.writeln(BenchOptions.usage);
    return;
  }
  if (options.listScenarios) {
    stdout.writeln('Available scenarios:');
    for (final Scenario scenario in allScenarios) {
      stdout.writeln('  ${scenario.name.padRight(20)} ${scenario.description}');
    }
    return;
  }

  final Environment environment = Environment.capture();
  stdout.writeln(environment.render());
  stdout.writeln(options.render());
  stdout.writeln('');

  final List<ResultRow> rows = <ResultRow>[];
  final Directory root = Directory(
    options.workDirectory,
  ).createTempSync('reaxdb_bench_');
  try {
    for (final Scenario scenario in options.selectedScenarios) {
      for (final int size in options.sizes) {
        for (final SyncMode sync
            in scenario.durabilitySensitive
                ? options.syncModes
                : options.syncModes.take(1)) {
          final List<ResultRow> produced = await _runScenario(
            scenario: scenario,
            size: size,
            sync: sync,
            options: options,
            root: root,
          );
          rows.addAll(produced);
          for (final ResultRow row in produced) {
            stdout.writeln(row.renderLine());
          }
        }
      }
    }
  } finally {
    if (root.existsSync()) {
      root.deleteSync(recursive: true);
    }
  }

  stdout.writeln('');
  stdout.writeln(ResultRow.renderTable(rows));

  final String? jsonPath = options.jsonOutput;
  if (jsonPath != null) {
    final File file = File(jsonPath);
    await file.parent.create(recursive: true);
    await file.writeAsString(
      const JsonEncoder.withIndent('  ').convert(<String, Object?>{
        'environment': environment.toJson(),
        'options': options.toJson(),
        'results': <Map<String, Object?>>[
          for (final ResultRow row in rows) row.toJson(),
        ],
      }),
    );
    stdout.writeln('');
    stdout.writeln('Wrote JSON results to $jsonPath');
  }
}

/// Runs one scenario at one size and sync mode, over [BenchOptions.runs]
/// repetitions, and returns the rows of the median repetition.
Future<List<ResultRow>> _runScenario({
  required Scenario scenario,
  required int size,
  required SyncMode sync,
  required BenchOptions options,
  required Directory root,
}) async {
  // repetition -> row name -> measurement
  final List<Map<String, Measurement>> repetitions = <Map<String, Measurement>>[
    for (int run = 0; run < options.runs; run++)
      await () async {
        final Directory dir = root.createTempSync(
          '${scenario.name}_${size}_${sync.name}_${run}_',
        );
        try {
          return await scenario.body(
            BenchContext(
              directory: dir.path,
              size: size,
              syncMode: sync,
              options: options,
              random: Random(options.seed + run),
              selfPath: Platform.script.toFilePath(),
            ),
          );
        } finally {
          if (dir.existsSync()) dir.deleteSync(recursive: true);
        }
      }(),
  ];

  final List<String> names = repetitions.first.keys.toList();
  return <ResultRow>[
    for (final String name in names)
      ResultRow.fromRepetitions(
        scenario: name,
        size: size,
        syncMode: sync,
        repetitions: <Measurement>[
          for (final Map<String, Measurement> rep in repetitions) rep[name]!,
        ],
      ),
  ];
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Parsed command-line configuration.
final class BenchOptions {
  /// Creates an option set.
  const BenchOptions({
    required this.sizes,
    required this.syncModes,
    required this.scenarioNames,
    required this.runs,
    required this.warmupFraction,
    required this.valueBytes,
    required this.seed,
    required this.batchSize,
    required this.workDirectory,
    required this.jsonOutput,
    required this.showHelp,
    required this.listScenarios,
    required this.crashWriter,
  });

  /// Help text.
  static const String usage = '''
Usage: dart run benchmark/reaxdb_benchmark.dart [options]

  --sizes=1000,10000,100000   Document counts to benchmark (default 1000,10000)
  --sync=full,none            Sync modes for write scenarios (default full,none)
  --scenarios=a,b             Scenarios to run (default: all)
  --runs=3                    Repetitions per scenario; the median is reported
  --warmup=0.1                Fraction of samples discarded as warmup (0..0.9)
  --value-bytes=200           Payload bytes per document
  --batch-size=500            Documents per batch in batch_write
  --seed=20260730             RNG seed for random key orders
  --work-dir=/tmp             Parent directory for the temporary databases
  --json=results.json         Also write machine-readable results
  --list-scenarios            Print the scenario catalogue and exit
  --help                      Print this message
''';

  /// Document counts to benchmark.
  final List<int> sizes;

  /// Sync modes applied to durability-sensitive scenarios.
  final List<SyncMode> syncModes;

  /// Names of the scenarios to run.
  final List<String> scenarioNames;

  /// Repetitions per scenario/size/sync combination.
  final int runs;

  /// Fraction of the leading samples discarded as warmup.
  final double warmupFraction;

  /// Payload size of a benchmark document, in bytes.
  final int valueBytes;

  /// Seed for the deterministic random key orders.
  final int seed;

  /// Documents per batch in the batch write scenario.
  final int batchSize;

  /// Parent directory holding the temporary databases.
  final String workDirectory;

  /// Optional path for JSON output.
  final String? jsonOutput;

  /// Whether `--help` was passed.
  final bool showHelp;

  /// Whether `--list-scenarios` was passed.
  final bool listScenarios;

  /// Internal re-entrant mode arguments, or null in normal operation.
  final CrashWriterSpec? crashWriter;

  /// The scenarios selected by [scenarioNames].
  List<Scenario> get selectedScenarios {
    if (scenarioNames.isEmpty) return allScenarios;
    return <Scenario>[
      for (final String name in scenarioNames)
        allScenarios.firstWhere(
          (Scenario s) => s.name == name,
          orElse: () => throw FormatException(
            'unknown scenario "$name" (see --list-scenarios)',
          ),
        ),
    ];
  }

  /// Parses [arguments].
  static BenchOptions parse(List<String> arguments) {
    final Map<String, String> flags = <String, String>{};
    final Set<String> switches = <String>{};
    for (final String argument in arguments) {
      if (!argument.startsWith('--')) {
        throw FormatException('unexpected argument "$argument"');
      }
      final int equals = argument.indexOf('=');
      if (equals < 0) {
        switches.add(argument.substring(2));
      } else {
        flags[argument.substring(2, equals)] = argument.substring(equals + 1);
      }
    }

    const Set<String> knownFlags = <String>{
      'sizes',
      'sync',
      'scenarios',
      'runs',
      'warmup',
      'value-bytes',
      'batch-size',
      'seed',
      'work-dir',
      'json',
      'crash-writer',
    };
    const Set<String> knownSwitches = <String>{'help', 'list-scenarios'};
    for (final String key in flags.keys) {
      if (!knownFlags.contains(key)) {
        throw FormatException('unknown option "--$key"');
      }
    }
    for (final String key in switches) {
      if (!knownSwitches.contains(key)) {
        throw FormatException('unknown switch "--$key"');
      }
    }

    return BenchOptions(
      sizes: _intList(flags['sizes'] ?? '1000,10000'),
      syncModes: <SyncMode>[
        for (final String name in (flags['sync'] ?? 'full,none').split(','))
          SyncMode.values.firstWhere(
            (SyncMode m) => m.name == name.trim(),
            orElse: () => throw FormatException('unknown sync mode "$name"'),
          ),
      ],
      scenarioNames: flags['scenarios'] == null
          ? const <String>[]
          : flags['scenarios']!
                .split(',')
                .map((String s) => s.trim())
                .where((String s) => s.isNotEmpty)
                .toList(),
      runs: int.parse(flags['runs'] ?? '3'),
      warmupFraction: double.parse(flags['warmup'] ?? '0.1'),
      valueBytes: int.parse(flags['value-bytes'] ?? '200'),
      batchSize: int.parse(flags['batch-size'] ?? '500'),
      seed: int.parse(flags['seed'] ?? '20260730'),
      workDirectory: flags['work-dir'] ?? Directory.systemTemp.path,
      jsonOutput: flags['json'],
      showHelp: switches.contains('help'),
      listScenarios: switches.contains('list-scenarios'),
      crashWriter: flags['crash-writer'] == null
          ? null
          : CrashWriterSpec.parse(flags['crash-writer']!),
    );
  }

  static List<int> _intList(String raw) => <int>[
    for (final String part in raw.split(','))
      if (part.trim().isNotEmpty) int.parse(part.trim()),
  ];

  /// A human-readable description of this configuration.
  String render() => <String>[
    'Configuration',
    '  sizes            : ${sizes.join(', ')}',
    '  sync modes       : ${syncModes.map((SyncMode m) => m.name).join(', ')}',
    '  runs per scenario: $runs (median reported)',
    '  warmup discarded : ${(warmupFraction * 100).toStringAsFixed(0)}% of '
        'leading samples',
    '  document payload : $valueBytes bytes',
    '  batch size       : $batchSize documents',
    '  RNG seed         : $seed',
    '  scenarios        : '
        '${selectedScenarios.map((Scenario s) => s.name).join(', ')}',
  ].join('\n');

  /// JSON form of this configuration.
  Map<String, Object?> toJson() => <String, Object?>{
    'sizes': sizes,
    'syncModes': <String>[for (final SyncMode m in syncModes) m.name],
    'runs': runs,
    'warmupFraction': warmupFraction,
    'valueBytes': valueBytes,
    'batchSize': batchSize,
    'seed': seed,
    'scenarios': <String>[for (final Scenario s in selectedScenarios) s.name],
  };
}

/// Arguments of the re-entrant crash-writer mode.
final class CrashWriterSpec {
  /// Creates a spec.
  const CrashWriterSpec({
    required this.path,
    required this.count,
    required this.syncMode,
    required this.valueBytes,
  });

  /// Parses the `path:count:sync:valueBytes` encoding.
  factory CrashWriterSpec.parse(String raw) {
    final int last = raw.lastIndexOf(':');
    final int third = raw.lastIndexOf(':', last - 1);
    final int second = raw.lastIndexOf(':', third - 1);
    return CrashWriterSpec(
      path: raw.substring(0, second),
      count: int.parse(raw.substring(second + 1, third)),
      syncMode: SyncMode.values.byName(raw.substring(third + 1, last)),
      valueBytes: int.parse(raw.substring(last + 1)),
    );
  }

  /// Database directory to populate.
  final String path;

  /// Number of documents to write.
  final int count;

  /// Sync mode of the writer.
  final SyncMode syncMode;

  /// Payload size of each document.
  final int valueBytes;

  /// The `--crash-writer=` encoding of this spec.
  String encode() => '$path:$count:${syncMode.name}:$valueBytes';
}

Future<void> _runCrashWriter(CrashWriterSpec spec) async {
  final ReaxDB db = await ReaxDB.open(
    path: spec.path,
    syncMode: spec.syncMode,
    // A large memtable keeps everything in the WAL: nothing is flushed to an
    // SSTable, so the next open must replay the whole log.
    memtableSizeBytes: 512 * 1024 * 1024,
  );
  for (int i = 0; i < spec.count; i++) {
    await db.put(documentKey(i), benchDocument(i, spec.valueBytes));
  }
  // Deliberately NOT closing: exit while the WAL is uncheckpointed.
  exit(0);
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

/// A snapshot of the machine and runtime the numbers were produced on.
final class Environment {
  /// Creates an environment snapshot.
  const Environment({
    required this.dartVersion,
    required this.operatingSystem,
    required this.operatingSystemVersion,
    required this.processors,
    required this.assertsEnabled,
    required this.executable,
    required this.timestamp,
  });

  /// Captures the current environment.
  factory Environment.capture() {
    bool asserts = false;
    assert(() {
      asserts = true;
      return true;
    }());
    return Environment(
      dartVersion: Platform.version,
      operatingSystem: Platform.operatingSystem,
      operatingSystemVersion: Platform.operatingSystemVersion,
      processors: Platform.numberOfProcessors,
      assertsEnabled: asserts,
      executable: Platform.resolvedExecutable,
      timestamp: DateTime.now(),
    );
  }

  /// The full `Platform.version` string.
  final String dartVersion;

  /// Operating system identifier.
  final String operatingSystem;

  /// Operating system version string.
  final String operatingSystemVersion;

  /// Logical processor count.
  final int processors;

  /// Whether Dart asserts are active (true under `dart run`, false in AOT).
  final bool assertsEnabled;

  /// Path of the running executable.
  final String executable;

  /// When the run started.
  final DateTime timestamp;

  /// A human-readable rendering.
  String render() => <String>[
    'Environment',
    '  dart             : $dartVersion',
    '  os               : $operatingSystem $operatingSystemVersion',
    '  logical cpus     : $processors',
    '  asserts enabled  : $assertsEnabled'
        '${assertsEnabled ? "  (slower; pass no --enable-asserts for "
                  "production-like numbers)" : ""}',
    '  executable       : $executable',
    '  started          : ${timestamp.toIso8601String()}',
  ].join('\n');

  /// JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'dartVersion': dartVersion,
    'operatingSystem': operatingSystem,
    'operatingSystemVersion': operatingSystemVersion,
    'processors': processors,
    'assertsEnabled': assertsEnabled,
    'executable': executable,
    'timestamp': timestamp.toIso8601String(),
  };
}

// ---------------------------------------------------------------------------
// Measurement primitives
// ---------------------------------------------------------------------------

/// The measured latencies of one scenario repetition.
final class Measurement {
  /// Creates a measurement.
  Measurement({
    required this.latenciesMicros,
    required this.wallMicros,
    required this.itemsPerSample,
    required this.unit,
    this.notes = const <String, Object?>{},
  });

  /// Per-operation latencies, in microseconds, warmup already discarded.
  final List<int> latenciesMicros;

  /// Wall-clock duration of the whole measured region, in microseconds.
  final int wallMicros;

  /// How many logical items (documents, entries) one sample covers.
  final int itemsPerSample;

  /// The name of one sample, for example `write` or `scan of 100 entries`.
  final String unit;

  /// Scenario-specific annotations printed next to the result.
  final Map<String, Object?> notes;

  /// Samples in this measurement.
  int get sampleCount => latenciesMicros.length;

  /// Items covered by this measurement.
  int get itemCount => sampleCount * itemsPerSample;

  /// Items per second computed from the summed sample latencies.
  double get itemsPerSecond {
    final int total = latenciesMicros.fold<int>(0, (int a, int b) => a + b);
    if (total == 0) return double.infinity;
    return itemCount * 1000000 / total;
  }
}

/// Collects latency samples for a scenario.
final class SampleRecorder {
  /// Creates a recorder that discards the first [warmup] samples.
  SampleRecorder({required this.warmup});

  /// How many leading samples to discard.
  final int warmup;

  final List<int> _samples = <int>[];
  int _seen = 0;
  final Stopwatch _wall = Stopwatch();

  /// Times [action] and records its latency.
  Future<T> measure<T>(Future<T> Function() action) async {
    if (!_wall.isRunning && _seen >= warmup) _wall.start();
    final Stopwatch sw = Stopwatch()..start();
    final T result = await action();
    sw.stop();
    if (_seen >= warmup) _samples.add(sw.elapsedMicroseconds);
    _seen++;
    return result;
  }

  /// Builds the measurement.
  Measurement finish({
    required String unit,
    int itemsPerSample = 1,
    Map<String, Object?> notes = const <String, Object?>{},
  }) {
    _wall.stop();
    return Measurement(
      latenciesMicros: _samples,
      wallMicros: _wall.elapsedMicroseconds,
      itemsPerSample: itemsPerSample,
      unit: unit,
      notes: notes,
    );
  }
}

/// One reported line of results: the median repetition of a scenario.
final class ResultRow {
  /// Creates a result row.
  const ResultRow({
    required this.scenario,
    required this.size,
    required this.syncMode,
    required this.unit,
    required this.sampleCount,
    required this.repetitionCount,
    required this.p50,
    required this.p95,
    required this.p99,
    required this.minMicros,
    required this.maxMicros,
    required this.itemsPerSecond,
    required this.spreadPercent,
    required this.notes,
  });

  /// Reduces [repetitions] to the median repetition by throughput.
  factory ResultRow.fromRepetitions({
    required String scenario,
    required int size,
    required SyncMode syncMode,
    required List<Measurement> repetitions,
  }) {
    final List<Measurement> ordered = repetitions.toList()
      ..sort(
        (Measurement a, Measurement b) =>
            a.itemsPerSecond.compareTo(b.itemsPerSecond),
      );
    final Measurement median = ordered[ordered.length ~/ 2];
    final double slowest = ordered.first.itemsPerSecond;
    final double fastest = ordered.last.itemsPerSecond;
    final List<int> sorted = median.latenciesMicros.toList()..sort();
    return ResultRow(
      scenario: scenario,
      size: size,
      syncMode: syncMode,
      unit: median.unit,
      sampleCount: sorted.length,
      repetitionCount: repetitions.length,
      p50: percentile(sorted, 0.50),
      p95: percentile(sorted, 0.95),
      p99: percentile(sorted, 0.99),
      minMicros: sorted.isEmpty ? 0 : sorted.first,
      maxMicros: sorted.isEmpty ? 0 : sorted.last,
      itemsPerSecond: median.itemsPerSecond,
      spreadPercent: fastest <= 0 ? 0 : (fastest - slowest) / fastest * 100,
      notes: median.notes,
    );
  }

  /// Scenario (row) name.
  final String scenario;

  /// Document count the scenario ran at.
  final String unit;

  /// Document count the scenario ran at.
  final int size;

  /// Sync mode in effect.
  final SyncMode syncMode;

  /// Samples in the median repetition.
  final int sampleCount;

  /// How many repetitions were run.
  final int repetitionCount;

  /// 50th percentile sample latency, microseconds.
  final double p50;

  /// 95th percentile sample latency, microseconds.
  final double p95;

  /// 99th percentile sample latency, microseconds.
  final double p99;

  /// Fastest sample, microseconds.
  final int minMicros;

  /// Slowest sample, microseconds.
  final int maxMicros;

  /// Items per second in the median repetition.
  final double itemsPerSecond;

  /// Spread between the slowest and fastest repetition, in percent.
  final double spreadPercent;

  /// Scenario-specific annotations.
  final Map<String, Object?> notes;

  /// Whether there are enough samples for tail percentiles to mean anything.
  bool get hasTail => sampleCount >= 20;

  /// A progress line printed while the harness runs.
  String renderLine() =>
      '  ${scenario.padRight(20)} size=${size.toString().padLeft(7)} '
      'sync=${syncMode.name.padRight(4)} '
      'p50=${_us(p50).padLeft(10)} '
      'ops/s=${_num(itemsPerSecond).padLeft(11)}'
      '${notes.isEmpty ? '' : '  ${_renderNotes(notes)}'}';

  /// Renders [rows] as a Markdown table.
  static String renderTable(List<ResultRow> rows) {
    final StringBuffer out = StringBuffer();
    out.writeln(
      '| scenario | docs | sync | unit | n | p50 | p95 | p99 | max | '
      'ops/sec | run spread | notes |',
    );
    out.writeln(
      '| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | '
      '---: | --- |',
    );
    for (final ResultRow row in rows) {
      out.writeln(
        '| ${row.scenario} | ${row.size} | ${row.syncMode.name} | ${row.unit} '
        '| ${row.sampleCount} | ${_us(row.p50)} '
        '| ${row.hasTail ? _us(row.p95) : "n/a"} '
        '| ${row.hasTail ? _us(row.p99) : "n/a"} '
        '| ${_us(row.maxMicros.toDouble())} '
        '| ${_num(row.itemsPerSecond)} '
        '| ${row.spreadPercent.toStringAsFixed(1)}% '
        '| ${_renderNotes(row.notes)} |',
      );
    }
    return out.toString();
  }

  /// JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'scenario': scenario,
    'size': size,
    'syncMode': syncMode.name,
    'unit': unit,
    'samples': sampleCount,
    'repetitions': repetitionCount,
    'p50Micros': p50,
    'p95Micros': hasTail ? p95 : null,
    'p99Micros': hasTail ? p99 : null,
    'minMicros': minMicros,
    'maxMicros': maxMicros,
    'itemsPerSecond': itemsPerSecond,
    'runSpreadPercent': spreadPercent,
    'notes': notes,
  };

  static String _renderNotes(Map<String, Object?> notes) => notes.entries
      .map((MapEntry<String, Object?> e) => '${e.key}=${e.value}')
      .join(', ');

  static String _us(double micros) {
    if (micros >= 100000) return '${(micros / 1000).toStringAsFixed(0)} ms';
    if (micros >= 1000) return '${(micros / 1000).toStringAsFixed(2)} ms';
    return '${micros.toStringAsFixed(1)} us';
  }

  static String _num(double value) {
    if (value.isInfinite) return 'inf';
    if (value >= 1000) {
      final String digits = value.round().toString();
      final StringBuffer out = StringBuffer();
      for (int i = 0; i < digits.length; i++) {
        if (i > 0 && (digits.length - i) % 3 == 0) out.write(',');
        out.write(digits[i]);
      }
      return out.toString();
    }
    return value.toStringAsFixed(1);
  }
}

/// Linear-interpolated [fraction] percentile of the ascending [sorted].
double percentile(List<int> sorted, double fraction) {
  if (sorted.isEmpty) return 0;
  if (sorted.length == 1) return sorted.first.toDouble();
  final double position = fraction * (sorted.length - 1);
  final int low = position.floor();
  final int high = position.ceil();
  if (low == high) return sorted[low].toDouble();
  return sorted[low] + (sorted[high] - sorted[low]) * (position - low);
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// Everything a scenario needs to run one repetition.
final class BenchContext {
  /// Creates a context.
  const BenchContext({
    required this.directory,
    required this.size,
    required this.syncMode,
    required this.options,
    required this.random,
    required this.selfPath,
  });

  /// A private, empty directory for this repetition.
  final String directory;

  /// Document count.
  final int size;

  /// Sync mode to open the database with.
  final SyncMode syncMode;

  /// The whole option set.
  final BenchOptions options;

  /// Deterministic RNG seeded per repetition.
  final Random random;

  /// Path to this script, for the re-entrant crash writer.
  final String selfPath;

  /// Number of leading samples discarded for a loop of [total] samples.
  int warmupFor(int total) =>
      min(total ~/ 2, (total * options.warmupFraction).round());

  /// Opens a database in [directory] with this context's settings.
  ///
  /// The record cache is sized to hold every document so the warm-cache
  /// scenario really is warm; the cold-cache scenario reports its measured
  /// hit ratio so the distinction stays visible in the output.
  Future<ReaxDB> open({int? memtableSizeBytes, String? subdirectory}) =>
      ReaxDB.open(
        path: subdirectory == null
            ? directory
            : '$directory${Platform.pathSeparator}$subdirectory',
        syncMode: syncMode,
        memtableSizeBytes: memtableSizeBytes ?? 4 * 1024 * 1024,
        cacheMaxEntries: size + 1024,
        cacheMaxMemoryBytes: 512 * 1024 * 1024,
      );

  /// A shuffled `0..size-1` permutation.
  List<int> shuffledIndices() =>
      List<int>.generate(size, (int i) => i)..shuffle(random);
}

/// One benchmark scenario.
final class Scenario {
  /// Creates a scenario.
  const Scenario({
    required this.name,
    required this.description,
    required this.durabilitySensitive,
    required this.body,
  });

  /// Scenario name, used by `--scenarios`.
  final String name;

  /// One-line description printed by `--list-scenarios`.
  final String description;

  /// Whether running it under several sync modes is meaningful.
  final bool durabilitySensitive;

  /// Runs one repetition, returning one or more named measurements.
  final Future<Map<String, Measurement>> Function(BenchContext ctx) body;
}

/// Key of benchmark document [index].
String documentKey(int index) => 'bench:${index.toString().padLeft(9, '0')}';

/// The document written by every scenario, with a [payloadBytes] payload.
Map<String, dynamic> benchDocument(int index, int payloadBytes) =>
    <String, dynamic>{
      'id': index,
      'name': 'user-$index',
      'category': 'cat-${index % 20}',
      'score': index % 1000,
      'active': index.isEven,
      'payload': 'x' * payloadBytes,
    };

/// The catalogue of scenarios.
final List<Scenario> allScenarios = <Scenario>[
  Scenario(
    name: 'sequential_write',
    description: 'put() of documents in ascending key order',
    durabilitySensitive: true,
    body: _sequentialWrite,
  ),
  Scenario(
    name: 'random_write',
    description: 'put() of documents in a shuffled key order',
    durabilitySensitive: true,
    body: _randomWrite,
  ),
  Scenario(
    name: 'batch_write',
    description: 'putBatch() of --batch-size documents per call',
    durabilitySensitive: true,
    body: _batchWrite,
  ),
  Scenario(
    name: 'point_read',
    description: 'get() cold cache (from SSTables) and warm cache, separately',
    durabilitySensitive: false,
    body: _pointRead,
  ),
  Scenario(
    name: 'range_scan',
    description: 'scan() of 100 consecutive entries from a random offset',
    durabilitySensitive: false,
    body: _rangeScan,
  ),
  Scenario(
    name: 'query',
    description: 'equality query served by a secondary index vs a full scan',
    durabilitySensitive: false,
    body: _query,
  ),
  Scenario(
    name: 'transaction_commit',
    description: 'transaction() with a single put, committed',
    durabilitySensitive: true,
    body: _transactionCommit,
  ),
  Scenario(
    name: 'reopen_clean',
    description: 'open() of a cleanly closed database (no WAL replay)',
    durabilitySensitive: false,
    body: _reopenClean,
  ),
  Scenario(
    name: 'reopen_wal_replay',
    description: 'open() after a process died with an uncheckpointed WAL',
    durabilitySensitive: true,
    body: _reopenWalReplay,
  ),
];

Future<Map<String, Measurement>> _sequentialWrite(BenchContext ctx) async {
  final ReaxDB db = await ctx.open();
  try {
    final SampleRecorder recorder = SampleRecorder(
      warmup: ctx.warmupFor(ctx.size),
    );
    for (int i = 0; i < ctx.size; i++) {
      await recorder.measure(
        () => db.put(documentKey(i), benchDocument(i, ctx.options.valueBytes)),
      );
    }
    return <String, Measurement>{
      'sequential_write': recorder.finish(unit: 'document'),
    };
  } finally {
    await db.close();
  }
}

Future<Map<String, Measurement>> _randomWrite(BenchContext ctx) async {
  final ReaxDB db = await ctx.open();
  try {
    final List<int> order = ctx.shuffledIndices();
    final SampleRecorder recorder = SampleRecorder(
      warmup: ctx.warmupFor(ctx.size),
    );
    for (final int i in order) {
      await recorder.measure(
        () => db.put(documentKey(i), benchDocument(i, ctx.options.valueBytes)),
      );
    }
    return <String, Measurement>{
      'random_write': recorder.finish(unit: 'document'),
    };
  } finally {
    await db.close();
  }
}

Future<Map<String, Measurement>> _batchWrite(BenchContext ctx) async {
  final ReaxDB db = await ctx.open();
  try {
    final int batch = min(ctx.options.batchSize, ctx.size);
    final int batches = max(1, ctx.size ~/ batch);
    final SampleRecorder recorder = SampleRecorder(
      warmup: ctx.warmupFor(batches),
    );
    for (int b = 0; b < batches; b++) {
      final Map<String, Object?> entries = <String, Object?>{
        for (int i = b * batch; i < (b + 1) * batch; i++)
          documentKey(i): benchDocument(i, ctx.options.valueBytes),
      };
      await recorder.measure(() => db.putBatch(entries));
    }
    return <String, Measurement>{
      'batch_write': recorder.finish(
        unit: 'document',
        itemsPerSample: batch,
        notes: <String, Object?>{'batch': batch},
      ),
    };
  } finally {
    await db.close();
  }
}

Future<Map<String, Measurement>> _pointRead(BenchContext ctx) async {
  // Setup (not measured): write everything, force it all onto disk, then
  // reopen so both the record cache and the memtable start empty. A "cold"
  // read therefore goes to the SSTables; only the OS page cache may still be
  // warm, which the README calls out.
  final ReaxDB writer = await ctx.open();
  for (int i = 0; i < ctx.size; i++) {
    await writer.put(documentKey(i), benchDocument(i, ctx.options.valueBytes));
  }
  await writer.compact();
  await writer.close();

  final ReaxDB db = await ctx.open();
  try {
    final List<int> order = ctx.shuffledIndices();
    final int warmup = ctx.warmupFor(ctx.size);

    final CacheStatistics before = db.cacheStats;
    final SampleRecorder cold = SampleRecorder(warmup: warmup);
    for (final int i in order) {
      final Object? value = await cold.measure(
        () => db.get<Map<String, dynamic>>(documentKey(i)),
      );
      if (value == null) throw StateError('missing key ${documentKey(i)}');
    }
    final CacheStatistics afterCold = db.cacheStats;

    final SampleRecorder warm = SampleRecorder(warmup: warmup);
    for (final int i in order) {
      await warm.measure(() => db.get<Map<String, dynamic>>(documentKey(i)));
    }
    final CacheStatistics afterWarm = db.cacheStats;

    return <String, Measurement>{
      'point_read_cold': cold.finish(
        unit: 'read',
        notes: <String, Object?>{'cacheHitRatio': _ratio(before, afterCold)},
      ),
      'point_read_warm': warm.finish(
        unit: 'read',
        notes: <String, Object?>{'cacheHitRatio': _ratio(afterCold, afterWarm)},
      ),
    };
  } finally {
    await db.close();
  }
}

String _ratio(CacheStatistics before, CacheStatistics after) {
  final int hits = after.hits - before.hits;
  final int misses = after.misses - before.misses;
  final int total = hits + misses;
  if (total == 0) return 'n/a';
  return '${(hits * 100 / total).toStringAsFixed(1)}%';
}

Future<Map<String, Measurement>> _rangeScan(BenchContext ctx) async {
  const int window = 100;
  final ReaxDB writer = await ctx.open();
  for (int i = 0; i < ctx.size; i++) {
    await writer.put(documentKey(i), benchDocument(i, ctx.options.valueBytes));
  }
  await writer.compact();
  await writer.close();

  final ReaxDB db = await ctx.open();
  try {
    final int scans = min(500, max(20, ctx.size ~/ window));
    final SampleRecorder recorder = SampleRecorder(
      warmup: ctx.warmupFor(scans),
    );
    int total = 0;
    for (int s = 0; s < scans; s++) {
      final int start = ctx.random.nextInt(max(1, ctx.size - window));
      final List<ReaxEntry<Map<String, dynamic>>> entries = await recorder
          .measure(
            () => db
                .scan<Map<String, dynamic>>(
                  startKey: documentKey(start),
                  limit: window,
                )
                .toList(),
          );
      total += entries.length;
    }
    return <String, Measurement>{
      'range_scan': recorder.finish(
        unit: 'entry',
        itemsPerSample: window,
        notes: <String, Object?>{'window': window, 'entriesRead': total},
      ),
    };
  } finally {
    await db.close();
  }
}

Future<Map<String, Measurement>> _query(BenchContext ctx) async {
  final ReaxDB db = await ctx.open();
  try {
    for (int i = 0; i < ctx.size; i++) {
      final Map<String, dynamic> doc = benchDocument(i, ctx.options.valueBytes);
      await db.put('indexed:${i.toString().padLeft(9, '0')}', doc);
      await db.put('plain:${i.toString().padLeft(9, '0')}', doc);
    }
    await db.createIndex('indexed', <String>['category']);
    await db.compact();

    final int iterations = ctx.size <= 10000 ? 40 : 10;
    final int warmup = ctx.warmupFor(iterations);

    final SampleRecorder indexed = SampleRecorder(warmup: warmup);
    int indexedMatches = 0;
    for (int i = 0; i < iterations; i++) {
      final List<Map<String, dynamic>> found = await indexed.measure(
        () => db.where('indexed', 'category', 'cat-${i % 20}'),
      );
      indexedMatches = found.length;
    }

    final SampleRecorder scanned = SampleRecorder(warmup: warmup);
    int scannedMatches = 0;
    for (int i = 0; i < iterations; i++) {
      final List<Map<String, dynamic>> found = await scanned.measure(
        () => db.where('plain', 'category', 'cat-${i % 20}'),
      );
      scannedMatches = found.length;
    }

    return <String, Measurement>{
      'query_indexed': indexed.finish(
        unit: 'query',
        notes: <String, Object?>{'matches': indexedMatches},
      ),
      'query_full_scan': scanned.finish(
        unit: 'query',
        notes: <String, Object?>{'matches': scannedMatches},
      ),
    };
  } finally {
    await db.close();
  }
}

Future<Map<String, Measurement>> _transactionCommit(BenchContext ctx) async {
  final ReaxDB db = await ctx.open();
  try {
    final int count = min(ctx.size, 20000);
    final SampleRecorder recorder = SampleRecorder(
      warmup: ctx.warmupFor(count),
    );
    for (int i = 0; i < count; i++) {
      await recorder.measure(
        () => db.transaction<void>((ReaxTransaction tx) async {
          await tx.put(
            documentKey(i),
            benchDocument(i, ctx.options.valueBytes),
          );
        }),
      );
    }
    return <String, Measurement>{
      'transaction_commit': recorder.finish(
        unit: 'commit',
        notes: <String, Object?>{'opsPerTx': 1},
      ),
    };
  } finally {
    await db.close();
  }
}

Future<Map<String, Measurement>> _reopenClean(BenchContext ctx) async {
  final ReaxDB writer = await ctx.open();
  for (int i = 0; i < ctx.size; i++) {
    await writer.put(documentKey(i), benchDocument(i, ctx.options.valueBytes));
  }
  await writer.close();

  const int opens = 5;
  final SampleRecorder recorder = SampleRecorder(warmup: 1);
  for (int i = 0; i < opens; i++) {
    final ReaxDB db = await recorder.measure(() => ctx.open());
    await db.close();
  }
  return <String, Measurement>{'reopen_clean': recorder.finish(unit: 'open')};
}

Future<Map<String, Measurement>> _reopenWalReplay(BenchContext ctx) async {
  // A child process writes and dies without closing, so the WAL holds every
  // record and the measured open() must replay all of them.
  final String dbPath = '${ctx.directory}${Platform.pathSeparator}replay';
  final String crashWriter =
      '--crash-writer='
      '${CrashWriterSpec(path: dbPath, count: ctx.size, syncMode: ctx.syncMode, valueBytes: ctx.options.valueBytes).encode()}';
  // Under `dart run` the executable is the Dart VM; compiled with
  // `dart compile exe` it is the harness itself.
  final bool compiled = !Platform.script.path.endsWith('.dart');
  final ProcessResult result = await Process.run(
    Platform.resolvedExecutable,
    compiled
        ? <String>[crashWriter]
        : <String>['run', ctx.selfPath, crashWriter],
  );
  if (result.exitCode != 0) {
    throw StateError(
      'crash writer failed (${result.exitCode}): ${result.stderr}',
    );
  }

  final SampleRecorder recorder = SampleRecorder(warmup: 0);
  final ReaxDB db = await recorder.measure(
    () => ReaxDB.open(path: dbPath, syncMode: ctx.syncMode),
  );

  // How much of the acknowledged data actually came back. With SyncMode.full
  // this must be all of it; with SyncMode.none the writes were never forced
  // to disk, and whatever is missing is the honest cost of that choice.
  int recovered = 0;
  await for (final String _ in db.keys()) {
    recovered++;
  }
  await db.close();
  if (ctx.syncMode == SyncMode.full && recovered != ctx.size) {
    throw StateError(
      'SyncMode.full lost acknowledged writes: recovered $recovered of '
      '${ctx.size}',
    );
  }

  return <String, Measurement>{
    'reopen_wal_replay': recorder.finish(
      unit: 'open',
      notes: <String, Object?>{
        'acknowledged': ctx.size,
        'recovered': recovered,
      },
    ),
  };
}
