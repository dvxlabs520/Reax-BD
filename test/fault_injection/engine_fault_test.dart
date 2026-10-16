import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/lsm_storage_engine.dart';
import 'package:reaxdb_dart/src/core/util/byte_key.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('engine_fault_test_');
  });

  tearDown(() async {
    await deleteTempDirectory(dir);
  });

  Directory tablesDir() => Directory(p.join(dir.path, 'tables'));

  Future<LsmStorageEngine> openEngine({
    SyncMode syncMode = SyncMode.full,
    int memtableSizeBytes = 4 * 1024 * 1024,
  }) => LsmStorageEngine.open(
    directory: dir.path,
    syncMode: syncMode,
    memtableSizeBytes: memtableSizeBytes,
  );

  group('Engine fault injection', () {
    test(
      'SyncMode.full: no acknowledged write is lost after abandonment',
      () async {
        final engine = await openEngine();
        final acked = <String>[];
        for (var i = 0; i < 50; i++) {
          await engine.put(_b('acked-$i'), _b('value-$i'));
          acked.add('acked-$i');
        }
        await engine.delete(_b('acked-13'));
        // Process "killed": no close, no flush.

        final recovered = await openEngine();
        for (final key in acked) {
          if (key == 'acked-13') {
            expect(await recovered.get(_b(key)), isNull);
          } else {
            expect(
              await recovered.get(_b(key)),
              isNotNull,
              reason: '$key was acknowledged and must survive',
            );
          }
        }
        await recovered.close();
      },
    );

    test(
      'partial SSTable (truncated) is rejected with CorruptionException',
      () async {
        final engine = await openEngine();
        for (var i = 0; i < 50; i++) {
          await engine.put(_b('key-$i'), Uint8List(200));
        }
        await engine.flush();
        await engine.close();

        // Truncate the manifest-referenced table to simulate a torn write
        // that somehow reached its final name.
        final sst = await tablesDir()
            .list()
            .where((e) => e.path.endsWith('.sst'))
            .cast<File>()
            .first;
        final bytes = await sst.readAsBytes();
        await sst.writeAsBytes(bytes.sublist(0, bytes.length ~/ 3));

        await expectLater(openEngine(), throwsA(isA<CorruptionException>()));
      },
    );

    test('leftover .sst.tmp from a crashed table write is removed', () async {
      final engine = await openEngine();
      await engine.put(_b('k'), _b('v'));
      await engine.close();

      final tmp = File(p.join(tablesDir().path, 'sst-000000000099.sst.tmp'));
      await tmp.writeAsBytes([1, 2, 3]);

      final reopened = await openEngine();
      expect(await tmp.exists(), isFalse);
      expect(await reopened.get(_b('k')), _b('v'));
      await reopened.close();
    });

    test('orphan tables from a compaction killed before the manifest update '
        'are ignored and cleaned', () async {
      final engine = await openEngine();
      for (var i = 0; i < 20; i++) {
        await engine.put(_b('key-$i'), _b('good-$i'));
      }
      await engine.flush();
      await engine.close();

      // Simulate a compaction that wrote its output table but died before
      // publishing the manifest: a valid-looking orphan .sst appears.
      final sst = await tablesDir()
          .list()
          .where((e) => e.path.endsWith('.sst'))
          .cast<File>()
          .first;
      final orphanPath = p.join(tablesDir().path, 'sst-000000000777.sst');
      await sst.copy(orphanPath);

      final recovered = await openEngine();
      expect(await File(orphanPath).exists(), isFalse);
      for (var i = 0; i < 20; i++) {
        expect(await recovered.get(_b('key-$i')), _b('good-$i'));
      }
      await recovered.close();
    });

    test('abandoning the engine right after compaction leaves a consistent '
        'database (superseded files cleaned on reopen)', () async {
      var engine = await openEngine(memtableSizeBytes: 2 * 1024);
      for (var i = 0; i < 300; i++) {
        await engine.put(
          _b('key-${i.toString().padLeft(4, '0')}'),
          Uint8List(64),
        );
      }
      for (var i = 0; i < 300; i += 3) {
        await engine.delete(_b('key-${i.toString().padLeft(4, '0')}'));
      }
      await engine.compact();
      // Abandon without close: superseded compaction inputs still exist.

      engine = await openEngine();
      for (var i = 0; i < 300; i++) {
        final key = _b('key-${i.toString().padLeft(4, '0')}');
        if (i % 3 == 0) {
          expect(await engine.get(key), isNull);
        } else {
          expect(await engine.get(key), isNotNull);
        }
      }
      final scanned = await engine.scan().toList();
      expect(scanned.length, 200);
      await engine.close();
    });

    test('opening twice in a row replays idempotently', () async {
      final engine = await openEngine();
      await engine.put(_b('once'), _b('1'));
      await engine.delete(_b('never'));
      // Abandon without close.

      final first = await openEngine();
      expect(await first.get(_b('once')), _b('1'));
      await first.close();
      final second = await openEngine();
      expect(await second.get(_b('once')), _b('1'));
      expect((await second.scan().toList()).length, 1);
      await second.close();
    });

    test(
      '10k random keys round-trip through write, flush, compact, reopen',
      () async {
        final rng = Random(42);
        final expected = <ByteKey, Uint8List>{};
        Uint8List randomKey() {
          final kind = rng.nextInt(3);
          if (kind == 0) {
            // Long ASCII keys (> 32 bytes).
            return _b('long-key-${rng.nextInt(1 << 30)}'.padRight(40, 'x'));
          }
          if (kind == 1) {
            // Non-ASCII UTF-8 keys.
            return _b('usuario-ñ-🦤-${rng.nextInt(1 << 30)}');
          }
          // Raw binary keys, any byte values, 1-64 bytes.
          return Uint8List.fromList(
            List.generate(1 + rng.nextInt(64), (_) => rng.nextInt(256)),
          );
        }

        var engine = await openEngine(
          syncMode: SyncMode.os,
          memtableSizeBytes: 256 * 1024,
        );
        for (var i = 0; i < 10000; i++) {
          final key = randomKey();
          final value = Uint8List.fromList(
            List.generate(rng.nextInt(128), (_) => rng.nextInt(256)),
          );
          expected[ByteKey(key)] = value;
          await engine.put(key, value);
        }
        await engine.flush();
        await engine.compact();
        await engine.close();

        engine = await openEngine(syncMode: SyncMode.os);
        for (final entry in expected.entries) {
          expect(
            await engine.get(entry.key.bytes),
            entry.value,
            reason: 'key ${entry.key} must survive the full lifecycle',
          );
        }
        // Scan must return exactly the live key set, ordered.
        final scanned = await engine.scan().toList();
        expect(scanned.length, expected.length);
        for (var i = 1; i < scanned.length; i++) {
          expect(
            ByteKey.compareBytes(scanned[i - 1].key, scanned[i].key),
            lessThan(0),
            reason: 'scan output must be strictly ordered',
          );
        }
        await engine.close();
      },
      timeout: const Timeout(Duration(minutes: 3)),
    );
  });
}
