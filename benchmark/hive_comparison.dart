// Side-by-side benchmark of ReaxDB and Hive (hive_ce) on the same machine,
// with the same documents and the same methodology.
//
// Build it AOT, like a Flutter release build, before reading the numbers:
//
//   dart compile exe benchmark/hive_comparison.dart -o hive_comparison
//   ./hive_comparison --sizes=1000,10000,100000 --runs=3
//
// --only=<text> runs just the scenarios whose name contains <text>.
//
// Every operation is timed on its own. The first 10% of samples of each
// repetition are discarded as warmup, each scenario runs --runs times on a
// fresh database, and the median repetition (by throughput) is reported.
import 'dart:io';
import 'dart:math';

import 'package:hive_ce/hive_ce.dart';
import 'package:reaxdb_dart/reaxdb_dart.dart';

const int _batchSize = 500;
const int _payloadBytes = 200;
const List<String> _categories = <String>['a', 'b', 'c', 'd', 'e'];

Future<void> main(List<String> args) async {
  final Map<String, String> options = <String, String>{
    for (final String arg in args)
      if (arg.startsWith('--') && arg.contains('='))
        arg.substring(2, arg.indexOf('=')): arg.substring(arg.indexOf('=') + 1),
  };
  final List<int> sizes = (options['sizes'] ?? '1000,10000')
      .split(',')
      .map(int.parse)
      .toList();
  final int runs = int.parse(options['runs'] ?? '3');
  // Runs only the scenarios whose name contains this text.
  final String? only = options['only'];
  final Directory work = await Directory(
    options['work-dir'] ?? Directory.systemTemp.path,
  ).createTemp('reax_vs_hive_');

  bool asserts = false;
  assert(() {
    asserts = true;
    return true;
  }());
  stdout
    ..writeln('ReaxDB vs Hive (hive_ce)')
    ..writeln('  dart     : ${Platform.version}')
    ..writeln(
      '  os       : ${Platform.operatingSystem} '
      '${Platform.operatingSystemVersion}',
    )
    ..writeln('  cpus     : ${Platform.numberOfProcessors}')
    ..writeln('  asserts  : $asserts${asserts ? '  (use an AOT build)' : ''}')
    ..writeln('  runs     : $runs (median repetition reported)')
    ..writeln(
      '  document : {id, name, category, score, active, payload} '
      'with a $_payloadBytes-byte payload',
    )
    ..writeln();

  try {
    for (final int n in sizes) {
      stdout.writeln('== $n documents');
      stdout.writeln(
        '${'scenario'.padRight(34)}${'ops/s'.padLeft(12)}'
        '${'p50 us'.padLeft(10)}${'p99 us'.padLeft(10)}  note',
      );
      for (final _Scenario scenario in _scenarios) {
        if (only != null && !scenario.name.contains(only)) continue;
        final List<_Result> reps = <_Result>[];
        for (int r = 0; r < runs; r++) {
          final Directory dir = await work.createTemp('run_');
          try {
            reps.add(await scenario.run(dir.path, n, r));
          } finally {
            await ReaxDB.closeAll();
            await Hive.close();
            await dir.delete(recursive: true);
          }
        }
        reps.sort((_Result a, _Result b) => a.opsPerSec.compareTo(b.opsPerSec));
        stdout.writeln(reps[reps.length ~/ 2].render(scenario.name));
      }
      stdout.writeln();
    }
  } finally {
    await work.delete(recursive: true);
  }
}

// -- documents ----------------------------------------------------------------

String _key(int i) => 'doc:${i.toString().padLeft(9, '0')}';

Map<String, dynamic> _doc(int i) => <String, dynamic>{
  'id': i,
  'name': 'name $i',
  'category': _categories[i % _categories.length],
  'score': i % 1000,
  'active': i.isEven,
  'payload': 'x' * _payloadBytes,
};

List<int> _shuffled(int n, int seed) =>
    List<int>.generate(n, (int i) => i)..shuffle(Random(seed));

// -- measurement ----------------------------------------------------------------

final class _Recorder {
  _Recorder(int expected) : _warmup = expected ~/ 10;

  final int _warmup;
  final List<int> _samples = <int>[];
  int _seen = 0;

  Future<T> time<T>(Future<T> Function() action) async {
    final Stopwatch sw = Stopwatch()..start();
    final T result = await action();
    sw.stop();
    if (_seen++ >= _warmup) _samples.add(sw.elapsedMicroseconds);
    return result;
  }

  T timeSync<T>(T Function() action) {
    final Stopwatch sw = Stopwatch()..start();
    final T result = action();
    sw.stop();
    if (_seen++ >= _warmup) _samples.add(sw.elapsedMicroseconds);
    return result;
  }

  /// Times a whole loop as one sample, for operations too fast to time one
  /// by one.
  Future<void> timeLoop(Future<void> Function() loop) async {
    final Stopwatch sw = Stopwatch()..start();
    await loop();
    sw.stop();
    _samples.add(sw.elapsedMicroseconds);
  }

  _Result finish({int itemsPerSample = 1, String note = ''}) =>
      _Result(_samples, itemsPerSample, note);
}

final class _Result {
  _Result(List<int> samples, this.itemsPerSample, this.note)
    : samples = List<int>.of(samples)..sort();

  final List<int> samples;
  final int itemsPerSample;
  final String note;

  double get opsPerSec {
    final int total = samples.fold<int>(0, (int a, int b) => a + b);
    if (total == 0) return double.infinity;
    return samples.length * itemsPerSample * 1000000 / total;
  }

  int _pct(double f) => samples.isEmpty
      ? 0
      : samples[min(samples.length - 1, (samples.length * f).floor())];

  String render(String name) {
    final String ops = opsPerSec.isInfinite
        ? 'n/a'
        : opsPerSec >= 1000
        ? '${(opsPerSec / 1000).toStringAsFixed(1)}k'
        : opsPerSec.toStringAsFixed(1);
    final bool perOp = itemsPerSample == 1 && samples.isNotEmpty;
    return '${name.padRight(34)}${ops.padLeft(12)}'
        '${(perOp ? '${_pct(0.5)}' : 'n/a').padLeft(10)}'
        '${(perOp && samples.length >= 20 ? '${_pct(0.99)}' : 'n/a').padLeft(10)}'
        '  $note';
  }
}

// -- helpers to fill and reopen -------------------------------------------------

Future<ReaxDB> _reax(
  String path, {
  SyncMode sync = SyncMode.full,
  bool memory = false,
}) => ReaxDB.open(path: '$path/reax', syncMode: sync, keepInMemory: memory);

Future<Box<Map<dynamic, dynamic>>> _hive(String path) async {
  Hive.init('$path/hive');
  return Hive.openBox<Map<dynamic, dynamic>>('bench');
}

Future<void> _fillReax(ReaxDB db, int n) async {
  for (int start = 0; start < n; start += _batchSize) {
    await db.putBatch(<String, Object?>{
      for (int i = start; i < min(n, start + _batchSize); i++) _key(i): _doc(i),
    });
  }
}

Future<void> _fillHive(Box<Map<dynamic, dynamic>> box, int n) async {
  for (int start = 0; start < n; start += _batchSize) {
    await box.putAll(<String, Map<dynamic, dynamic>>{
      for (int i = start; i < min(n, start + _batchSize); i++) _key(i): _doc(i),
    });
  }
  await box.flush();
}

int _dirBytes(String path) {
  final Directory dir = Directory(path);
  if (!dir.existsSync()) return 0;
  int total = 0;
  for (final FileSystemEntity e in dir.listSync(recursive: true)) {
    if (e is File) total += e.lengthSync();
  }
  return total;
}

String _mb(int bytes) => '${(bytes / (1024 * 1024)).toStringAsFixed(1)} MB';

// -- scenarios --------------------------------------------------------------------

final class _Scenario {
  const _Scenario(this.name, this.run);

  final String name;
  final Future<_Result> Function(String path, int n, int rep) run;
}

final List<_Scenario> _scenarios = <_Scenario>[
  // Single writes. "Durable" means flushed to disk before the call returns:
  // ReaxDB's default, and put + flush() in Hive.
  _Scenario('write, durable   ReaxDB full', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    final _Recorder r = _Recorder(n);
    for (int i = 0; i < n; i++) {
      await r.time(() => db.put(_key(i), _doc(i)));
    }
    return r.finish();
  }),
  _Scenario('write, durable   ReaxDB memory', (p, n, _) async {
    final ReaxDB db = await _reax(p, memory: true);
    final _Recorder r = _Recorder(n);
    for (int i = 0; i < n; i++) {
      await r.time(() => db.put(_key(i), _doc(i)));
    }
    return r.finish(note: 'keepInMemory: true');
  }),
  _Scenario('write, durable   Hive put+flush', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    final _Recorder r = _Recorder(n);
    for (int i = 0; i < n; i++) {
      await r.time(() async {
        await box.put(_key(i), _doc(i));
        await box.flush();
      });
    }
    return r.finish();
  }),
  _Scenario('write            ReaxDB os', (p, n, _) async {
    final ReaxDB db = await _reax(p, sync: SyncMode.os);
    final _Recorder r = _Recorder(n);
    for (int i = 0; i < n; i++) {
      await r.time(() => db.put(_key(i), _doc(i)));
    }
    return r.finish(note: 'survives an app kill');
  }),
  _Scenario('write            ReaxDB none', (p, n, _) async {
    final ReaxDB db = await _reax(p, sync: SyncMode.none);
    final _Recorder r = _Recorder(n);
    for (int i = 0; i < n; i++) {
      await r.time(() => db.put(_key(i), _doc(i)));
    }
    return r.finish(note: 'no guarantee');
  }),
  _Scenario('write            Hive put', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    final _Recorder r = _Recorder(n);
    for (int i = 0; i < n; i++) {
      await r.time(() => box.put(_key(i), _doc(i)));
    }
    return r.finish(note: 'no flush');
  }),
  // Batches of 500.
  _Scenario('batch, durable   ReaxDB putBatch', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    final _Recorder r = _Recorder(n ~/ _batchSize);
    for (int start = 0; start < n; start += _batchSize) {
      final Map<String, Object?> batch = <String, Object?>{
        for (int i = start; i < min(n, start + _batchSize); i++)
          _key(i): _doc(i),
      };
      await r.time(() => db.putBatch(batch));
    }
    return r.finish(itemsPerSample: _batchSize);
  }),
  _Scenario('batch, durable   Hive putAll+flush', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    final _Recorder r = _Recorder(n ~/ _batchSize);
    for (int start = 0; start < n; start += _batchSize) {
      final Map<String, Map<dynamic, dynamic>> batch =
          <String, Map<dynamic, dynamic>>{
            for (int i = start; i < min(n, start + _batchSize); i++)
              _key(i): _doc(i),
          };
      await r.time(() async {
        await box.putAll(batch);
        await box.flush();
      });
    }
    return r.finish(itemsPerSample: _batchSize);
  }),
  // Reads of every key in random order, in the same process that wrote them.
  // A read can take well under a microsecond, so the whole loop is timed.
  _Scenario('read, warm       ReaxDB get', (p, n, rep) async {
    final ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    for (final int i in _shuffled(n, rep)) {
      await db.get<Map<String, dynamic>>(_key(i));
    }
    final List<int> order = _shuffled(n, rep + 100);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        await db.get<Map<String, dynamic>>(_key(i));
      }
    });
    final CacheStatistics stats = db.cacheStats;
    return r.finish(
      itemsPerSample: n,
      note: 'cache hit ratio ${stats.hitRatio.toStringAsFixed(2)}',
    );
  }),
  _Scenario('read, warm       ReaxDB memory get', (p, n, rep) async {
    final ReaxDB db = await _reax(p, memory: true);
    await _fillReax(db, n);
    for (final int i in _shuffled(n, rep)) {
      await db.get<Map<String, dynamic>>(_key(i));
    }
    final List<int> order = _shuffled(n, rep + 100);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        await db.get<Map<String, dynamic>>(_key(i));
      }
    });
    return r.finish(itemsPerSample: n, note: 'keepInMemory: true');
  }),
  _Scenario('read, warm       ReaxDB memory getSync', (p, n, rep) async {
    final ReaxDB db = await _reax(p, memory: true);
    await _fillReax(db, n);
    for (final int i in _shuffled(n, rep)) {
      await db.get<Map<String, dynamic>>(_key(i));
    }
    final List<int> order = _shuffled(n, rep + 100);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        db.getSync<Map<String, dynamic>>(_key(i));
      }
    });
    return r.finish(itemsPerSample: n, note: 'synchronous, from memory');
  }),
  _Scenario('read, warm       Hive get', (p, n, rep) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    await _fillHive(box, n);
    for (final int i in _shuffled(n, rep)) {
      box.get(_key(i));
    }
    final List<int> order = _shuffled(n, rep + 100);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        box.get(_key(i));
      }
    });
    return r.finish(itemsPerSample: n, note: 'synchronous, from memory');
  }),
  // Reads after a clean close and reopen: the first read of each key.
  _Scenario('read, reopened   ReaxDB get', (p, n, rep) async {
    ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    await db.close();
    db = await _reax(p);
    final List<int> order = _shuffled(n, rep);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        await db.get<Map<String, dynamic>>(_key(i));
      }
    });
    return r.finish(itemsPerSample: n, note: 'served from disk');
  }),
  _Scenario('read, reopened   ReaxDB memory get', (p, n, rep) async {
    ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    await db.close();
    db = await _reax(p, memory: true);
    final List<int> order = _shuffled(n, rep);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        await db.get<Map<String, dynamic>>(_key(i));
      }
    });
    return r.finish(itemsPerSample: n, note: 'open loaded everything');
  }),
  _Scenario('read, reopened   Hive get', (p, n, rep) async {
    Box<Map<dynamic, dynamic>> box = await _hive(p);
    await _fillHive(box, n);
    await box.close();
    box = await Hive.openBox<Map<dynamic, dynamic>>('bench');
    final List<int> order = _shuffled(n, rep);
    final _Recorder r = _Recorder(0);
    await r.timeLoop(() async {
      for (final int i in order) {
        box.get(_key(i));
      }
    });
    return r.finish(itemsPerSample: n, note: 'open loaded everything');
  }),
  // Opening a database that holds n documents.
  _Scenario('open             ReaxDB', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    await db.close();
    final _Recorder r = _Recorder(1);
    final ReaxDB reopened = await r.time(() => _reax(p));
    await reopened.close();
    return r.finish(note: 'one open; ops/s = opens per second');
  }),
  _Scenario('open             ReaxDB memory', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    await db.close();
    final _Recorder r = _Recorder(1);
    final ReaxDB reopened = await r.time(() => _reax(p, memory: true));
    await reopened.close();
    return r.finish(note: 'loads every document');
  }),
  _Scenario('open             Hive', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    await _fillHive(box, n);
    await box.close();
    final _Recorder r = _Recorder(1);
    final Box<Map<dynamic, dynamic>> reopened = await r.time(
      () => Hive.openBox<Map<dynamic, dynamic>>('bench'),
    );
    await reopened.close();
    return r.finish(note: 'one open; ops/s = opens per second');
  }),
  // Reading every document once.
  _Scenario('iterate all      ReaxDB scanPrefix', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    final _Recorder r = _Recorder(1);
    final int count = await r.time(() async {
      int c = 0;
      await for (final _ in db.scanPrefix<Map<String, dynamic>>('doc:')) {
        c++;
      }
      return c;
    });
    if (count != n) throw StateError('scan saw $count of $n');
    return r.finish(itemsPerSample: n, note: 'ops/s = documents per second');
  }),
  _Scenario('iterate all      ReaxDB memory', (p, n, _) async {
    final ReaxDB db = await _reax(p, memory: true);
    await _fillReax(db, n);
    final _Recorder r = _Recorder(1);
    final int count = await r.time(() async {
      int c = 0;
      await for (final _ in db.scanPrefix<Map<String, dynamic>>('doc:')) {
        c++;
      }
      return c;
    });
    if (count != n) throw StateError('scan saw $count of $n');
    return r.finish(itemsPerSample: n, note: 'ops/s = documents per second');
  }),
  _Scenario('iterate all      Hive values', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    await _fillHive(box, n);
    final _Recorder r = _Recorder(1);
    final int count = r.timeSync(() {
      int c = 0;
      for (final Map<dynamic, dynamic> _ in box.values) {
        c++;
      }
      return c;
    });
    if (count != n) throw StateError('values saw $count of $n');
    return r.finish(itemsPerSample: n, note: 'ops/s = documents per second');
  }),
  // Finding the documents of one category (a fifth of the data).
  _Scenario('query            ReaxDB indexed', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    await db.createIndex('doc', <String>['category']);
    await _fillReax(db, n);
    final _Recorder r = _Recorder(20);
    for (int q = 0; q < 20; q++) {
      final String category = _categories[q % _categories.length];
      await r.time(
        () => db.query('doc').whereEquals('category', category).find(),
      );
    }
    return r.finish(note: 'ops/s = queries per second');
  }),
  _Scenario('query            ReaxDB memory indexed', (p, n, _) async {
    final ReaxDB db = await _reax(p, memory: true);
    await db.createIndex('doc', <String>['category']);
    await _fillReax(db, n);
    final _Recorder r = _Recorder(20);
    for (int q = 0; q < 20; q++) {
      final String category = _categories[q % _categories.length];
      await r.time(
        () => db.query('doc').whereEquals('category', category).find(),
      );
    }
    return r.finish(note: 'ops/s = queries per second');
  }),
  _Scenario('query            ReaxDB memory no index', (p, n, _) async {
    final ReaxDB db = await _reax(p, memory: true);
    await _fillReax(db, n);
    final _Recorder r = _Recorder(20);
    for (int q = 0; q < 20; q++) {
      final String category = _categories[q % _categories.length];
      await r.time(
        () => db.query('doc').whereEquals('category', category).find(),
      );
    }
    return r.finish(note: 'ops/s = queries per second');
  }),
  _Scenario('query            Hive values.where', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    await _fillHive(box, n);
    final _Recorder r = _Recorder(20);
    for (int q = 0; q < 20; q++) {
      final String category = _categories[q % _categories.length];
      r.timeSync(
        () => box.values.where((Map<dynamic, dynamic> d) {
          return d['category'] == category;
        }).toList(),
      );
    }
    return r.finish(note: 'ops/s = queries per second');
  }),
  // Size on disk after writing n documents and closing.
  _Scenario('disk size        ReaxDB', (p, n, _) async {
    final ReaxDB db = await _reax(p);
    await _fillReax(db, n);
    await db.compact();
    await db.close();
    return _Result(const <int>[], 1, _mb(_dirBytes('$p/reax')));
  }),
  _Scenario('disk size        Hive', (p, n, _) async {
    final Box<Map<dynamic, dynamic>> box = await _hive(p);
    await _fillHive(box, n);
    await box.compact();
    await box.close();
    return _Result(const <int>[], 1, _mb(_dirBytes('$p/hive')));
  }),
];
