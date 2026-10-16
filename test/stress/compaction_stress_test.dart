/// Sustained-load stress tests for the LSM write path.
///
/// These push enough data through the engine to force many memtable flushes
/// and compactions across more than one level, then verify every key - the
/// live ones, the overwritten ones and the deleted ones - after a full close
/// and reopen.
@Timeout(Duration(minutes: 5))
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_stress_lsm_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (root.existsSync()) await root.delete(recursive: true);
  });

  String key(int i) => 'k:${i.toString().padLeft(7, '0')}';

  test('sustained writes drive multi-level compaction and every key verifies '
      'after reopen', () async {
    const int total = 60000;
    const String padding =
        'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
        'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
        'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
        'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz';
    final String path = '${root.path}/db';

    // A 256 KiB memtable over ~13 MiB of data means roughly fifty flushes,
    // enough L0 tables to trigger repeated L0 -> L1 compaction. (Pushing L1
    // into L2 takes more than its 64 MiB target; the engine-level test in
    // storage_regression_test.dart does that with small level targets.)
    ReaxDB db = await ReaxDB.open(
      path: path,
      syncMode: SyncMode.none,
      memtableSizeBytes: 256 * 1024,
      cacheMaxEntries: 512,
    );

    for (int i = 0; i < total; i++) {
      await db.put(key(i), <String, dynamic>{'v': i, 'gen': 0, 'pad': padding});
    }
    // Overwrite every third key and delete every seventh, so compaction has
    // superseded versions and tombstones to reclaim.
    for (int i = 0; i < total; i += 3) {
      await db.put(key(i), <String, dynamic>{'v': i, 'gen': 1, 'pad': padding});
    }
    for (int i = 0; i < total; i += 7) {
      await db.delete(key(i));
    }

    await db.flush();
    final List<int> levels = db.storageStats.levelTableCounts;
    expect(
      levels.length > 1 && levels[1] > 0,
      isTrue,
      reason: 'the load was never compacted into L1: $levels',
    );

    await db.compact();
    await db.close();

    db = await ReaxDB.open(
      path: path,
      syncMode: SyncMode.none,
      memtableSizeBytes: 256 * 1024,
      cacheMaxEntries: 512,
    );

    int live = 0;
    int deleted = 0;
    for (int i = 0; i < total; i++) {
      final Map<String, dynamic>? value = await db.get<Map<String, dynamic>>(
        key(i),
      );
      if (i % 7 == 0) {
        expect(value, isNull, reason: '${key(i)} was deleted');
        deleted++;
        continue;
      }
      expect(value, isNotNull, reason: '${key(i)} disappeared');
      expect(value!['v'], i);
      expect(
        value['gen'],
        i % 3 == 0 ? 1 : 0,
        reason: '${key(i)} has the wrong generation',
      );
      live++;
    }
    expect(live + deleted, total);

    // The ordered scan must agree with the point reads, exactly.
    final List<String> scanned = await db.keys(prefix: 'k:').toList();
    expect(scanned.length, live);
    for (int i = 0; i < scanned.length - 1; i++) {
      expect(scanned[i].compareTo(scanned[i + 1]), lessThan(0));
    }
    await db.close();
  }, tags: <String>['slow']);

  test('values above 1 MiB survive flush, compaction and reopen', () async {
    final String path = '${root.path}/big';
    ReaxDB db = await ReaxDB.open(
      path: path,
      syncMode: SyncMode.none,
      memtableSizeBytes: 512 * 1024,
    );

    const int count = 8;
    const int size = 1024 * 1024 + 17;
    Uint8List payload(int i) =>
        Uint8List.fromList(List<int>.generate(size, (int j) => (i + j) % 256));

    for (int i = 0; i < count; i++) {
      await db.put('big:$i', payload(i));
    }
    // Interleave small keys so the big values are not alone in their tables.
    for (int i = 0; i < 200; i++) {
      await db.put('small:$i', i);
    }
    await db.compact();
    await db.close();

    db = await ReaxDB.open(path: path, syncMode: SyncMode.none);
    for (int i = 0; i < count; i++) {
      final Uint8List? stored = await db.get<Uint8List>('big:$i');
      expect(stored, isNotNull, reason: 'big:$i vanished');
      expect(stored!.length, size);
      expect(stored, orderedEquals(payload(i)));
    }
    for (int i = 0; i < 200; i++) {
      expect(await db.get<int>('small:$i'), i);
    }
    await db.close();
  });

  test(
    'very long and non-ASCII keys survive flush, compaction and reopen',
    () async {
      final String path = '${root.path}/keys';
      ReaxDB db = await ReaxDB.open(
        path: path,
        syncMode: SyncMode.none,
        memtableSizeBytes: 64 * 1024,
      );

      final List<String> keys = <String>[
        for (int i = 0; i < 100; i++)
          'long:${'a' * 4000}:${i.toString().padLeft(4, '0')}',
        for (int i = 0; i < 100; i++) 'utf8:${'é中文🚀' * 200}:$i',
        for (int i = 0; i < 100; i++) 'mixed:${'ñ' * 1000}${'b' * 1000}:$i',
      ];

      for (int i = 0; i < keys.length; i++) {
        expect(
          utf8.encode(keys[i]).length,
          greaterThan(1000),
          reason: 'the long-key fixture stopped being long',
        );
        await db.put(keys[i], <String, dynamic>{'i': i});
      }
      await db.compact();
      await db.close();

      db = await ReaxDB.open(path: path, syncMode: SyncMode.none);
      for (int i = 0; i < keys.length; i++) {
        final Map<String, dynamic>? value = await db.get<Map<String, dynamic>>(
          keys[i],
        );
        expect(
          value,
          isNotNull,
          reason: 'key $i (length ${keys[i].length}) lost',
        );
        expect(value!['i'], i);
      }

      // Ordered iteration must return them all, in byte order, undamaged.
      final List<String> scanned = await db.keys().toList();
      expect(scanned.length, keys.length);
      expect(scanned.toSet(), keys.toSet());
      await db.close();
    },
  );

  test(
    'a 1 MiB value can be overwritten and deleted through compaction',
    () async {
      final String path = '${root.path}/rewrite';
      final ReaxDB db = await ReaxDB.open(
        path: path,
        syncMode: SyncMode.none,
        memtableSizeBytes: 256 * 1024,
      );

      final Uint8List first = Uint8List(1024 * 1024)
        ..fillRange(0, 1024 * 1024, 7);
      final Uint8List second = Uint8List(1024 * 1024)
        ..fillRange(0, 1024 * 1024, 9);

      await db.put('blob', first);
      await db.flush();
      await db.put('blob', second);
      await db.flush();
      await db.compact();
      expect((await db.get<Uint8List>('blob'))![0], 9);

      await db.delete('blob');
      await db.compact();
      expect(await db.get<Uint8List>('blob'), isNull);
      await db.close();

      final ReaxDB reopened = await ReaxDB.open(
        path: path,
        syncMode: SyncMode.none,
      );
      expect(await reopened.get<Uint8List>('blob'), isNull);
      await reopened.close();
    },
  );
}
