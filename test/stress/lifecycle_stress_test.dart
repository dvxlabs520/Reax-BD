/// Stress tests for expiry and for the open/write/close lifecycle.
///
/// The TTL cases avoid sleeping wherever they can: an entry written with an
/// `expiresAt` already in the past is expired deterministically, which makes
/// the assertions exact instead of timing-dependent.
@Timeout(Duration(minutes: 3))
library;

import 'dart:io';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

/// Total bytes of the write-ahead log segments under [databasePath].
int walBytes(String databasePath) {
  final Directory directory = Directory('$databasePath/wal');
  if (!directory.existsSync()) return 0;
  return directory.listSync().whereType<File>().fold<int>(
    0,
    (int sum, File file) => sum + file.lengthSync(),
  );
}

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_stress_life_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  group('TTL under load', () {
    test('expired entries are never readable, by any read path', () async {
      final ReaxDB db = await ReaxDB.open(
        path: '${root.path}/ttl',
        syncMode: SyncMode.none,
      );
      const int each = 400;
      final DateTime past = DateTime.now().subtract(const Duration(hours: 1));
      final DateTime future = DateTime.now().add(const Duration(hours: 1));

      // Interleave expired and live entries so they share memtables, tables
      // and scan ranges.
      for (int i = 0; i < each; i++) {
        await db.put(
          'ttl:${(i * 2).toString().padLeft(4, '0')}',
          <String, dynamic>{'v': i},
          expiresAt: past,
        );
        await db.put(
          'ttl:${(i * 2 + 1).toString().padLeft(4, '0')}',
          <String, dynamic>{'v': i},
          expiresAt: future,
        );
      }
      await db.flush();

      // Concurrent readers hammering both sets at once. Reading an expired
      // key also reclaims it through the write pipeline, so this exercises
      // reads and writes racing each other.
      await Future.wait<void>(<Future<void>>[
        for (int i = 0; i < each; i++)
          () async {
            final String expired = 'ttl:${(i * 2).toString().padLeft(4, '0')}';
            final String live = 'ttl:${(i * 2 + 1).toString().padLeft(4, '0')}';
            expect(await db.get<Map<String, dynamic>>(expired), isNull);
            expect(await db.exists(expired), isFalse);
            expect((await db.get<Map<String, dynamic>>(live))!['v'], i);
            expect(await db.exists(live), isTrue);
          }(),
      ]);

      final List<String> keys = await db.keys(prefix: 'ttl:').toList();
      expect(keys.length, each);
      for (final String key in keys) {
        final int index = int.parse(key.substring(4));
        expect(index.isOdd, isTrue, reason: '$key is expired but was listed');
      }

      final List<ReaxEntry<Map<String, dynamic>>> scanned = await db
          .scan<Map<String, dynamic>>(startKey: 'ttl:')
          .toList();
      expect(scanned.length, each);

      await db.close();
    });

    test('purgeExpired reclaims the entries and is idempotent', () async {
      final ReaxDB db = await ReaxDB.open(
        path: '${root.path}/purge',
        syncMode: SyncMode.none,
        memtableSizeBytes: 64 * 1024,
      );
      const int expired = 500;
      const int live = 250;
      final DateTime past = DateTime.now().subtract(const Duration(hours: 1));

      for (int i = 0; i < expired; i++) {
        await db.put('gone:$i', <String, dynamic>{'v': i}, expiresAt: past);
      }
      for (int i = 0; i < live; i++) {
        await db.put('stay:$i', <String, dynamic>{'v': i});
      }
      await db.flush();

      expect(
        await db.purgeExpired(),
        expired,
        reason: 'purgeExpired must find every expired entry',
      );
      expect(
        await db.purgeExpired(),
        0,
        reason:
            'a second purge must find nothing: the first one reclaimed them '
            'rather than merely hiding them',
      );

      await db.compact();
      await db.close();

      final ReaxDB reopened = await ReaxDB.open(
        path: '${root.path}/purge',
        syncMode: SyncMode.none,
      );
      expect(await reopened.purgeExpired(), 0);
      expect(await reopened.keys().toList(), hasLength(live));
      for (int i = 0; i < live; i++) {
        expect((await reopened.get<Map<String, dynamic>>('stay:$i'))!['v'], i);
      }
      await reopened.close();
    });

    test('entries expiring during a run stop being readable', () async {
      final ReaxDB db = await ReaxDB.open(
        path: '${root.path}/ttl_clock',
        syncMode: SyncMode.none,
      );
      const int count = 200;
      for (int i = 0; i < count; i++) {
        await db.put('soon:$i', <String, dynamic>{
          'v': i,
        }, ttl: const Duration(milliseconds: 250));
      }
      expect((await db.get<Map<String, dynamic>>('soon:0'))!['v'], 0);

      await Future<void>.delayed(const Duration(milliseconds: 600));

      for (int i = 0; i < count; i++) {
        expect(
          await db.get<Map<String, dynamic>>('soon:$i'),
          isNull,
          reason: 'soon:$i outlived its TTL',
        );
      }
      expect(await db.keys(prefix: 'soon:').toList(), isEmpty);
      await db.close();
    });
  });

  group('open/close lifecycle', () {
    test(
      '25 open/write/close cycles keep every key and bound the WAL',
      () async {
        final String path = '${root.path}/cycle';
        const int cycles = 25;
        const int perCycle = 50;
        final List<int> walSizes = <int>[];

        for (int cycle = 0; cycle < cycles; cycle++) {
          final ReaxDB db = await ReaxDB.open(
            path: path,
            syncMode: SyncMode.full,
          );
          for (int i = 0; i < perCycle; i++) {
            await db.put('c$cycle:k$i', <String, dynamic>{
              'cycle': cycle,
              'i': i,
              'pad': 'p' * 200,
            });
          }

          // Everything written by every earlier cycle is still here.
          for (int earlier = 0; earlier <= cycle; earlier++) {
            final Map<String, dynamic>? value = await db
                .get<Map<String, dynamic>>('c$earlier:k0');
            expect(
              value,
              isNotNull,
              reason: 'cycle $earlier data lost by cycle $cycle',
            );
            expect(value!['cycle'], earlier);
          }

          await db.close();
          walSizes.add(walBytes(path));
        }

        // A clean close flushes the memtable and checkpoints the log, so the
        // WAL must not accumulate across cycles.
        expect(
          walSizes.last,
          lessThanOrEqualTo(64 * 1024),
          reason: 'the WAL grew without bound: $walSizes',
        );
        expect(
          walSizes.last,
          lessThanOrEqualTo(walSizes[4] + 64 * 1024),
          reason: 'the WAL grows with the cycle count: $walSizes',
        );

        final ReaxDB db = await ReaxDB.open(path: path);
        for (int cycle = 0; cycle < cycles; cycle++) {
          for (int i = 0; i < perCycle; i++) {
            final Map<String, dynamic>? value = await db
                .get<Map<String, dynamic>>('c$cycle:k$i');
            expect(value, isNotNull, reason: 'c$cycle:k$i lost');
            expect(value!['cycle'], cycle);
            expect(value['i'], i);
          }
        }
        expect(await db.keys().toList(), hasLength(cycles * perCycle));
        await db.close();
      },
    );

    test(
      'reopening while already open throws DatabaseLockedException',
      () async {
        final String path = '${root.path}/locked';
        final ReaxDB db = await ReaxDB.open(path: path);
        await expectLater(
          ReaxDB.open(path: path),
          throwsA(isA<DatabaseLockedException>()),
        );
        await db.close();
        final ReaxDB again = await ReaxDB.open(path: path);
        await again.close();
      },
    );

    test(
      'operations racing close either finish or throw a typed error',
      () async {
        final ReaxDB db = await ReaxDB.open(
          path: '${root.path}/racing_close',
          syncMode: SyncMode.none,
        );
        final List<Future<Object?>> pending = <Future<Object?>>[
          for (int i = 0; i < 100; i++)
            db
                .put('race:$i', <String, dynamic>{'v': i})
                .then<Object?>((_) => null, onError: (Object e) => e),
        ];
        await db.close();
        final List<Object?> outcomes = await Future.wait<Object?>(pending);
        for (final Object failure in outcomes.whereType<Object>()) {
          expect(failure, isA<DatabaseClosedException>());
        }

        // Whatever was accepted before close() must have survived it: close
        // drains the work it already took on.
        final ReaxDB reopened = await ReaxDB.open(
          path: '${root.path}/racing_close',
          syncMode: SyncMode.none,
        );
        for (int i = 0; i < 100; i++) {
          if (outcomes[i] == null) {
            expect(
              (await reopened.get<Map<String, dynamic>>('race:$i'))?['v'],
              i,
              reason: 'race:$i was acknowledged but lost by close()',
            );
          }
        }
        await reopened.close();
      },
    );
  });
}
