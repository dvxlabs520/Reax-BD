/// Builds REAL ReaxDB 1.4.1 databases to test the 2.x legacy readers against.
///
/// Nothing here hand-writes 1.x bytes. The 1.4.1 sources are extracted from
/// git (they are the tree at the commit that this 2.0 work started from) into
/// a throwaway package under the system temp directory, `dart pub get` is run
/// once, and a generator script written by [_generatorSource] is executed with
/// the real 1.x engine to create the fixture databases and to record what 1.x
/// itself reads back from them.
///
/// The generated material never enters the package directory and is removed in
/// [LegacyFixtures.dispose].
library;

import 'dart:convert';
import 'dart:io';

import 'package:path/path.dart' as p;

/// A set of ReaxDB 1.4.1 databases plus the values 1.x reads back from them.
final class LegacyFixtures {
  LegacyFixtures._(this._root, this._scenarios);

  final Directory _root;
  final Map<String, Map<String, Object?>> _scenarios;

  /// Scenario names that were generated.
  Iterable<String> get scenarios => _scenarios.keys;

  /// The pristine 1.x database directory for [scenario].
  ///
  /// Do not mutate it; use [copyOf] for anything that writes.
  String pathOf(String scenario) {
    _require(scenario);
    return p.join(_root.path, 'db', scenario);
  }

  /// What the 1.x engine itself reads back from [scenario] after reopening
  /// the database, keyed by key and encoded by [encodeValue].
  Map<String, Object?> expected(String scenario) {
    _require(scenario);
    return _scenarios[scenario]!;
  }

  void _require(String scenario) {
    if (!_scenarios.containsKey(scenario)) {
      throw StateError(
        'Unknown legacy fixture scenario "$scenario"; generated: '
        '${_scenarios.keys.join(', ')}',
      );
    }
  }

  /// Copies the [scenario] database into a fresh directory under [into] and
  /// returns its path, so a test can migrate it without touching the fixture.
  Future<String> copyOf(String scenario, Directory into) async {
    final String destination = p.join(into.path, scenario);
    await _copyDirectory(Directory(pathOf(scenario)), Directory(destination));
    return destination;
  }

  /// Encodes a value so a 1.x readback and a 2.x readback can be compared
  /// without depending on JSON's inability to express bytes.
  static Object? encodeValue(Object? value) {
    if (value is List<int>) {
      return <String, Object?>{r'$bytes': base64Encode(value)};
    }
    return value;
  }

  /// Renders [value] as a stable string for equality assertions.
  static String canonical(Object? value) => jsonEncode(encodeValue(value));

  /// Removes every generated database and temporary fixture material.
  Future<void> dispose() async {
    if (await _root.exists()) {
      await _root.delete(recursive: true);
    }
  }

  /// Git revision of the last 1.4.1 release sources.
  ///
  /// Fixtures must never be generated from HEAD once 2.x lands: the generator
  /// runs the real 1.x engine, so the extracted tree has to stay frozen at the
  /// pre-2.0 tip (the commit this rewrite started from).
  static const String legacySourcesRevision =
      '5a2fcc454828c0f3dcc6f5770f32846c49af0b99';

  /// How many times [build] runs the generator before giving up.
  static const int generationAttempts = 3;

  /// Extracts, builds and runs ReaxDB 1.4.1 to produce every fixture.
  ///
  /// Throws [StateError] with the underlying output when any step fails, so a
  /// broken fixture pipeline can never be mistaken for a passing test.
  ///
  /// Each call uses its own temp package directory so parallel test isolates
  /// cannot race on a shared extract/pub-get cache.
  static Future<LegacyFixtures> build() async {
    final String packageRoot = _packageRoot();
    final Directory legacyPackage = await Directory.systemTemp.createTemp(
      'reaxdb_legacy_1x_${legacySourcesRevision.substring(0, 12)}_',
    );
    try {
      await _prepareLegacyPackage(packageRoot, legacyPackage);
    } catch (_) {
      if (await legacyPackage.exists()) {
        await legacyPackage.delete(recursive: true);
      }
      rethrow;
    }

    // The generator already regenerates a scenario that hit the 1.x WAL
    // flush race (see [_generatorSource]); retrying the whole process is a
    // second line of defence for a race it could not attribute to a
    // scenario, for example one that fires while the process exits.
    Directory? root;
    String? outputPath;
    final List<String> failures = <String>[];
    try {
      for (int attempt = 1; attempt <= generationAttempts; attempt++) {
        final Directory candidate = await Directory.systemTemp.createTemp(
          'reaxdb_legacy_fixtures_',
        );
        final String output = p.join(candidate.path, 'expected.json');
        final ProcessResult result = await Process.run('dart', <String>[
          'run',
          p.join(legacyPackage.path, 'bin', 'generate_fixtures.dart'),
          p.join(candidate.path, 'db'),
          output,
        ], workingDirectory: legacyPackage.path);
        if (result.exitCode == 0) {
          root = candidate;
          outputPath = output;
          break;
        }
        await candidate.delete(recursive: true);
        failures.add(
          'attempt $attempt (exit ${result.exitCode}):\n'
          'stdout:\n${result.stdout}\nstderr:\n${result.stderr}',
        );
      }
    } finally {
      // The extracted 1.x package is only needed for generation.
      if (await legacyPackage.exists()) {
        await legacyPackage.delete(recursive: true);
      }
    }
    if (root == null || outputPath == null) {
      throw StateError(
        'Generating ReaxDB 1.x fixtures failed $generationAttempts times.\n'
        '${failures.join('\n')}',
      );
    }

    final Object? decoded = jsonDecode(await File(outputPath).readAsString());
    if (decoded is! Map<String, dynamic>) {
      throw StateError('The 1.x fixture generator produced malformed output');
    }
    final Map<String, Map<String, Object?>> scenarios =
        <String, Map<String, Object?>>{
          for (final MapEntry<String, dynamic> entry in decoded.entries)
            entry.key: Map<String, Object?>.from(entry.value as Map),
        };
    return LegacyFixtures._(root, scenarios);
  }

  static Future<void> _prepareLegacyPackage(
    String packageRoot,
    Directory target,
  ) async {
    await target.create(recursive: true);

    // Write the archive outside [target] so extract cannot race with the file.
    final String archive = p.join(
      Directory.systemTemp.path,
      'reaxdb_legacy_${identityHashCode(target)}_${DateTime.now().microsecondsSinceEpoch}.tar',
    );
    try {
      _run('git', <String>[
        '-C',
        packageRoot,
        'archive',
        '--format=tar',
        '-o',
        archive,
        legacySourcesRevision,
        'lib',
      ]);
      _run('tar', <String>['-xf', archive, '-C', target.path]);
    } finally {
      final File archiveFile = File(archive);
      if (await archiveFile.exists()) await archiveFile.delete();
    }

    await File(p.join(target.path, 'pubspec.yaml')).writeAsString(_pubspec);
    final Directory bin = Directory(p.join(target.path, 'bin'));
    await bin.create();
    await File(
      p.join(bin.path, 'generate_fixtures.dart'),
    ).writeAsString(_generatorSource);

    final ProcessResult pubGet = await Process.run('dart', <String>[
      'pub',
      'get',
    ], workingDirectory: target.path);
    if (pubGet.exitCode != 0) {
      throw StateError(
        'dart pub get failed for the extracted ReaxDB 1.x package.\n'
        'stdout:\n${pubGet.stdout}\nstderr:\n${pubGet.stderr}',
      );
    }
  }

  static String _packageRoot() {
    Directory directory = Directory.current;
    while (true) {
      // `.git` is a directory in a normal checkout and a file in a worktree.
      if (File(p.join(directory.path, 'pubspec.yaml')).existsSync() &&
          FileSystemEntity.typeSync(p.join(directory.path, '.git')) !=
              FileSystemEntityType.notFound) {
        return directory.path;
      }
      final Directory parent = directory.parent;
      if (parent.path == directory.path) {
        throw StateError(
          'Could not locate the package git checkout from '
          '${Directory.current.path}; the 1.x fixtures are extracted from it',
        );
      }
      directory = parent;
    }
  }

  static String _run(String executable, List<String> arguments) {
    final ProcessResult result = Process.runSync(executable, arguments);
    if (result.exitCode != 0) {
      throw StateError(
        '$executable ${arguments.join(' ')} failed (exit ${result.exitCode}): '
        '${result.stderr}',
      );
    }
    return result.stdout as String;
  }

  static const String _pubspec = '''
name: reaxdb_dart
description: ReaxDB 1.4.1 sources, extracted from git to generate test fixtures.
version: 1.4.1
publish_to: none
environment:
  sdk: ^3.7.2
dependencies:
  path: ^1.8.3
  crypto: ^3.0.3
  pointycastle: ^4.0.0
''';

  /// The generator, run by the real 1.4.1 engine.
  ///
  /// It writes one database per scenario, closes it, reopens it (so the
  /// fixture exercises 1.x WAL recovery exactly as a user's app would) and
  /// records what 1.x reads back. That readback is the ground truth the 2.x
  /// migration is compared against.
  ///
  /// ## The 1.x WAL flush race
  ///
  /// 1.4.1's `WriteAheadLog` flushes from a 1 ms `Timer` that nothing awaits
  /// and that `_rotateLogFile`/`close` do not wait for, so now and then the
  /// timer writes to an `IOSink` that is already closed and the process dies
  /// with an unhandled `Bad state: StreamSink is closed`. The entries in that
  /// flush are lost, so a scenario that hit the race does not reflect what
  /// its code wrote. The generator does not touch the 1.x sources; instead
  /// each scenario runs in its own error zone, waits for stray timers after
  /// its last close, and is regenerated from an empty directory when exactly
  /// that error was observed. Any other error still fails generation.
  static const String _generatorSource = r'''
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/reaxdb_dart.dart';

Object? encodeValue(Object? value) {
  if (value is List<int>) {
    return <String, Object?>{r'$bytes': base64Encode(value)};
  }
  return value;
}

Future<Map<String, Object?>> readBack(
  String path,
  List<String> keys, {
  DatabaseConfig? config,
  String? encryptionKey,
}) async {
  final db = await ReaxDB.open(
    'fixture',
    path: path,
    config: config,
    encryptionKey: encryptionKey,
  );
  final out = <String, Object?>{};
  for (final key in keys) {
    final value = await db.get(key);
    if (value != null) out[key] = encodeValue(value);
  }
  await db.close();
  return out;
}

int fold(String key) {
  var f = 0;
  for (final unit in key.codeUnits) {
    f ^= unit;
  }
  return f;
}

Future<Map<String, Object?>> plain(String path) async {
  final db = await ReaxDB.open('fixture', path: path);
  await db.put('user:1', {'name': 'Alice', 'age': 30, 'tags': ['a', 'b']});
  await db.put('user:2', {'name': 'Bob', 'age': null});
  await db.put('greeting', 'hello world');
  await db.put('unicode', 'café über naïve');
  await db.put('café:1', {'accented': 'key'});
  await db.put('count', 42);
  await db.put('negative', -7);
  await db.put('ratio', 3.5);
  await db.put('flag', true);
  await db.put('off', false);
  await db.put('blob', <int>[0, 1, 2, 250, 255]);
  await db.put('list', <String>['x', 'y', 'z']);
  await db.put('empty:string', '');
  await db.close();
  return readBack(path, [
    'user:1', 'user:2', 'greeting', 'unicode', 'café:1', 'count', 'negative',
    'ratio', 'flag', 'off', 'blob', 'list', 'empty:string',
  ]);
}

Future<Map<String, Object?>> encryptedAes(String path) async {
  final db = await ReaxDB.open(
    'fixture',
    path: path,
    config: DatabaseConfig.withAes256Encryption(),
    encryptionKey: 'super-secret-legacy-key-2025',
  );
  await db.put('secret:1', {'card': '4111111111111111', 'cvv': 123});
  await db.put('secret:2', 'top secret string');
  await db.put('secret:3', 987654321);
  await db.close();
  return readBack(
    path,
    ['secret:1', 'secret:2', 'secret:3'],
    config: DatabaseConfig.withAes256Encryption(),
    encryptionKey: 'super-secret-legacy-key-2025',
  );
}

Future<Map<String, Object?>> encryptedXor(String path) async {
  final db = await ReaxDB.open(
    'fixture',
    path: path,
    config: DatabaseConfig.withXorEncryption(),
    encryptionKey: 'legacy-xor-key-42',
  );
  await db.put('obf:1', {'value': 'hidden'});
  await db.put('obf:2', 'plain-ish');
  await db.close();
  return readBack(
    path,
    ['obf:1', 'obf:2'],
    config: DatabaseConfig.withXorEncryption(),
    encryptionKey: 'legacy-xor-key-42',
  );
}

Future<Map<String, Object?>> deletes(String path) async {
  final db = await ReaxDB.open('fixture', path: path);
  final keys = <String>[];
  for (var i = 0; i < 10; i++) {
    keys.add('item:$i');
    await db.put('item:$i', {'index': i});
  }
  await db.put('item:3', {'index': 3, 'updated': true});
  for (final i in <int>[0, 2, 5, 9]) {
    await db.delete('item:$i');
  }
  await db.put('item:5', {'index': 5, 'resurrected': true});
  await db.close();
  return readBack(path, keys);
}

Future<Map<String, Object?>> longKeys(String path) async {
  final db = await ReaxDB.open('fixture', path: path);
  final keys = <String>[];
  final folds = <int>{};
  var i = 0;
  while (keys.length < 20) {
    final key = 'long-key-with-a-lot-of-padding-bytes-$i';
    i++;
    if (key.length <= 32) continue;
    if (!folds.add(fold(key))) continue;
    keys.add(key);
    await db.put(key, {'key': key, 'length': key.length});
  }
  await db.close();
  return readBack(path, keys);
}

Future<Map<String, Object?>> collidingLongKeys(String path) async {
  final db = await ReaxDB.open('fixture', path: path);
  const base = 'collision-demo-key-padding-padding-';
  final a = base + 'ab';
  final b = base + 'ba';
  if (a.length <= 32 || fold(a) != fold(b)) {
    throw StateError('fixture precondition failed: keys must collide');
  }
  await db.put(a, {'which': 'a'});
  await db.put(b, {'which': 'b'});
  await db.close();
  return readBack(path, [a, b]);
}

Future<Map<String, Object?>> bulk(String path) async {
  final config = DatabaseConfig(
    memtableSizeMB: 1,
    pageSize: 4096,
    l1CacheSize: 100,
    l2CacheSize: 100,
    l3CacheSize: 100,
    compressionEnabled: false,
    syncWrites: false,
    maxImmutableMemtables: 1,
    cacheSize: 10,
    enableCache: true,
  );
  final db = await ReaxDB.open('fixture', path: path, config: config);
  final keys = <String>[];
  final filler = 'x' * 1000;
  for (var i = 0; i < 6000; i++) {
    final key = 'bulk:${i.toString().padLeft(5, '0')}';
    keys.add(key);
    await db.put(key, '$filler-$i');
  }
  // Overwrite a slice so a stale copy exists in an older level.
  for (var i = 0; i < 200; i++) {
    await db.put('bulk:${i.toString().padLeft(5, '0')}', 'overwritten-$i');
  }
  await db.compact();
  await db.close();
  return readBack(path, keys, config: config);
}

Future<Map<String, Object?>> simpleApi(String path) async {
  final db = await SimpleReaxDB.open('fixture', path: path);
  await db.put('note:1', {'text': 'remember the milk'});
  await db.put('note:2', {'text': 'ship reaxdb 2.0'});
  await db.put('config', {'theme': 'dark'});
  await db.putAll({'bulk:a': 1, 'bulk:b': 2});
  await db.delete('note:2');
  await db.close();
  return readBack(path, [
    'note:1', 'note:2', 'config', 'bulk:a', 'bulk:b',
    '__reaxdb_simple_keys__',
  ]);
}

Future<Map<String, Object?>> indexed(String path) async {
  final db = await ReaxDB.open('fixture', path: path);
  await db.createIndex('users', 'age');
  final keys = <String>[];
  for (var i = 0; i < 5; i++) {
    final key = 'users:$i';
    keys.add(key);
    await db.put(key, {'name': 'user$i', 'age': 20 + i});
  }
  await db.close();
  return readBack(path, keys);
}

Future<Map<String, Object?>> empty(String path) async {
  final db = await ReaxDB.open('fixture', path: path);
  await db.close();
  return <String, Object?>{};
}

Future<void> main(List<String> args) async {
  final root = Directory(args[0]);
  await root.create(recursive: true);

  final builders = <String, Future<Map<String, Object?>> Function(String)>{
    'plain': plain,
    'encrypted_aes': encryptedAes,
    'encrypted_xor': encryptedXor,
    'deletes': deletes,
    'long_keys': longKeys,
    'colliding_long_keys': collidingLongKeys,
    'bulk': bulk,
    'simple_api': simpleApi,
    'indexed': indexed,
    'empty': empty,
  };

  final result = <String, Map<String, Object?>>{};
  for (final entry in builders.entries) {
    final path = '${root.path}/${entry.key}';
    result[entry.key] = await generate(entry.key, path, entry.value);
    stderr.writeln('generated ${entry.key}');
  }

  await File(args[1]).writeAsString(jsonEncode(result));
}

const int maxScenarioAttempts = 8;

/// The 1.4.1 bug described in LegacyFixtures._generatorSource.
bool isWalFlushRace(Object error) =>
    error is StateError && error.message.contains('StreamSink is closed');

/// Runs [build] in its own error zone until one attempt completes without
/// hitting the 1.x WAL flush race.
Future<Map<String, Object?>> generate(
  String name,
  String path,
  Future<Map<String, Object?>> Function(String) build,
) async {
  for (var attempt = 1; ; attempt++) {
    final directory = Directory(path);
    if (await directory.exists()) await directory.delete(recursive: true);
    await directory.create(recursive: true);

    final stray = <Object>[];
    final done = Completer<Map<String, Object?>>();
    runZonedGuarded(() async {
      try {
        final readBack = await build(path);
        // 1.x flush timers are 1 ms (WAL) and 100 us (write buffer); give
        // any that are still pending time to fire inside this zone before
        // the attempt is judged.
        await Future<void>.delayed(const Duration(milliseconds: 100));
        done.complete(readBack);
      } catch (error, stack) {
        done.completeError(error, stack);
      }
    }, (Object error, StackTrace stack) => stray.add(error));

    Object? failure;
    Map<String, Object?>? readBack;
    try {
      readBack = await done.future;
    } catch (error) {
      failure = error;
    }
    final errors = <Object>[if (failure != null) failure, ...stray];
    if (errors.isEmpty) return readBack!;
    final unexpected = errors.where((Object e) => !isWalFlushRace(e));
    if (unexpected.isNotEmpty) {
      throw StateError('scenario $name failed: ${unexpected.first}');
    }
    if (attempt >= maxScenarioAttempts) {
      throw StateError(
        'scenario $name hit the 1.x WAL flush race $attempt times in a row',
      );
    }
    stderr.writeln(
      'regenerating $name: the 1.x WAL flush race fired (attempt $attempt)',
    );
  }
}
''';
}

Future<void> _copyDirectory(Directory source, Directory destination) async {
  await destination.create(recursive: true);
  for (final FileSystemEntity entity in await source.list().toList()) {
    final String name = p.basename(entity.path);
    if (entity is Directory) {
      await _copyDirectory(entity, Directory(p.join(destination.path, name)));
    } else if (entity is File) {
      await entity.copy(p.join(destination.path, name));
    }
  }
}
