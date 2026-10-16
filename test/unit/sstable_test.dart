import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/storage/sstable.dart';
import 'package:reaxdb_dart/src/core/util/byte_key.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('sstable_test_');
  });

  tearDown(() async {
    if (await dir.exists()) await dir.delete(recursive: true);
  });

  String path(String name) => p.join(dir.path, name);

  group('SSTable', () {
    test('round-trips entries, tombstones, and empty values', () async {
      final entries = [
        SSTableEntry(_b('alpha'), _b('1')),
        SSTableEntry.tombstone(_b('beta')),
        SSTableEntry(_b('empty'), Uint8List(0)),
        SSTableEntry(_b('gamma'), _b('3')),
      ];
      final table = await SSTable.create(
        filePath: path('a.sst'),
        entries: entries,
      );
      expect((await table.get(_b('alpha')))!.value, _b('1'));
      final tomb = await table.get(_b('beta'));
      expect(tomb!.isTombstone, isTrue);
      final empty = await table.get(_b('empty'));
      expect(empty!.isTombstone, isFalse);
      expect(empty.value, isEmpty);
      expect(await table.get(_b('missing')), isNull);
      await table.close();
    });

    test('reopen after close reads the same data', () async {
      final table = await SSTable.create(
        filePath: path('b.sst'),
        entries: [SSTableEntry(_b('k'), _b('v'))],
      );
      await table.close();
      final reopened = await SSTable.open(path('b.sst'));
      expect((await reopened.get(_b('k')))!.value, _b('v'));
      expect(reopened.minKey, _b('k'));
      expect(reopened.maxKey, _b('k'));
      await reopened.close();
    });

    test('creation is atomic: no .tmp file remains', () async {
      final table = await SSTable.create(
        filePath: path('c.sst'),
        entries: [SSTableEntry(_b('k'), _b('v'))],
      );
      expect(await File(path('c.sst.tmp')).exists(), isFalse);
      expect(await File(path('c.sst')).exists(), isTrue);
      await table.close();
    });

    test('range scan honors bounds and reverse order', () async {
      final table = await SSTable.create(
        filePath: path('d.sst'),
        entries: [
          for (final k in ['a', 'b', 'c', 'd', 'e'])
            SSTableEntry(_b(k), _b('v$k')),
        ],
      );
      final forward = await table
          .range(startKey: _b('b'), endKey: _b('e'))
          .map((e) => utf8.decode(e.key))
          .toList();
      expect(forward, ['b', 'c', 'd']);
      final reversed = await table
          .range(startKey: _b('b'), endKey: _b('e'), reverse: true)
          .map((e) => utf8.decode(e.key))
          .toList();
      expect(reversed, ['d', 'c', 'b']);
      await table.close();
    });

    test('concurrent reads return correct values (no shared cursor)', () async {
      final rng = Random(11);
      final entries = <SSTableEntry>[];
      for (var i = 0; i < 500; i++) {
        entries.add(
          SSTableEntry(
            _b('key-${i.toString().padLeft(4, '0')}'),
            Uint8List.fromList(
              List.generate(50 + rng.nextInt(200), (_) => rng.nextInt(256)),
            ),
          ),
        );
      }
      final table = await SSTable.create(
        filePath: path('e.sst'),
        entries: entries,
      );
      final futures = <Future<void>>[];
      for (var round = 0; round < 4; round++) {
        for (final e in entries) {
          futures.add(
            table.get(e.key).then((got) {
              expect(got, isNotNull);
              expect(got!.value, e.value);
            }),
          );
        }
      }
      await Future.wait(futures);
      await table.close();
    });

    test('binary and non-ASCII keys round-trip in order', () async {
      final keys = <Uint8List>[
        Uint8List.fromList([0]),
        Uint8List.fromList([0, 0, 1]),
        _b('ascii'),
        _b('ñandú'),
        Uint8List.fromList(List.generate(64, (i) => 255 - i)),
      ]..sort(ByteKey.compareBytes);
      final table = await SSTable.create(
        filePath: path('f.sst'),
        entries: [for (final k in keys) SSTableEntry(k, k)],
      );
      for (final k in keys) {
        expect((await table.get(k))!.value, k);
      }
      final scanned = await table.scanAll().map((e) => e.key).toList();
      expect(scanned, keys);
      await table.close();
    });

    test('rejects a truncated file', () async {
      final table = await SSTable.create(
        filePath: path('g.sst'),
        entries: [
          for (var i = 0; i < 100; i++) SSTableEntry(_b('k$i'), _b('v$i')),
        ],
      );
      await table.close();
      final file = File(path('g.sst'));
      final bytes = await file.readAsBytes();
      await file.writeAsBytes(bytes.sublist(0, bytes.length ~/ 2));
      expect(
        () => SSTable.open(path('g.sst')),
        throwsA(isA<CorruptionException>()),
      );
    });

    test('rejects a corrupted meta block', () async {
      final table = await SSTable.create(
        filePath: path('h.sst'),
        entries: [SSTableEntry(_b('k'), _b('v'))],
      );
      await table.close();
      final file = File(path('h.sst'));
      final bytes = await file.readAsBytes();
      // Flip a byte in the meta region (just before the 20-byte footer).
      bytes[bytes.length - 25] ^= 0xff;
      await file.writeAsBytes(bytes);
      expect(
        () => SSTable.open(path('h.sst')),
        throwsA(isA<CorruptionException>()),
      );
    });

    test('detects corruption in a data record on read', () async {
      final table = await SSTable.create(
        filePath: path('i.sst'),
        entries: [SSTableEntry(_b('key'), Uint8List(100))],
      );
      await table.close();
      final file = File(path('i.sst'));
      final bytes = await file.readAsBytes();
      // Corrupt the value bytes inside the first data record.
      bytes[40] ^= 0xff;
      await file.writeAsBytes(bytes);
      final reopened = await SSTable.open(path('i.sst'));
      await expectLater(
        reopened.get(_b('key')),
        throwsA(isA<CorruptionException>()),
      );
      await reopened.close();
    });
  });
}
