import 'dart:convert';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/storage/memtable.dart';
import 'package:reaxdb_dart/src/core/util/byte_key.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  group('MemTable', () {
    test('put/lookup round-trip, including empty values', () {
      final table = MemTable(maxSizeBytes: 1 << 20);
      table.put(_b('a'), _b('value-a'));
      table.put(_b('empty'), Uint8List(0));
      expect(table.lookup(_b('a'))!.value, _b('value-a'));
      final empty = table.lookup(_b('empty'))!;
      expect(empty.isTombstone, isFalse);
      expect(empty.value, isEmpty);
      expect(table.lookup(_b('missing')), isNull);
    });

    test('delete stores a tombstone distinguishable from absence', () {
      final table = MemTable(maxSizeBytes: 1 << 20);
      table.put(_b('k'), _b('v'));
      table.delete(_b('k'));
      final slot = table.lookup(_b('k'));
      expect(slot, isNotNull);
      expect(slot!.isTombstone, isTrue);
      expect(table.lookup(_b('never-existed')), isNull);
    });

    test('entries iteration includes tombstones in sorted order', () {
      final table = MemTable(maxSizeBytes: 1 << 20);
      table.put(_b('c'), _b('3'));
      table.put(_b('a'), _b('1'));
      table.delete(_b('b'));
      final entries = table.entries.toList();
      expect(entries.map((e) => e.key.asString), ['a', 'b', 'c']);
      expect(entries[1].value.isTombstone, isTrue);
    });

    test('long binary keys never collide', () {
      final table = MemTable(maxSizeBytes: 1 << 20);
      // Same XOR fold, different keys (the old cache collapsed these).
      final k1 = Uint8List.fromList(List.generate(40, (i) => i % 7));
      final k2 = Uint8List.fromList(List.generate(40, (i) => (39 - i) % 7));
      table.put(k1, _b('one'));
      table.put(k2, _b('two'));
      expect(table.lookup(k1)!.value, _b('one'));
      expect(table.lookup(k2)!.value, _b('two'));
      expect(table.entryCount, 2);
    });

    test('size accounting shrinks on overwrite and counts tombstones', () {
      final table = MemTable(maxSizeBytes: 1 << 20);
      table.put(_b('k'), Uint8List(1000));
      final large = table.sizeBytes;
      table.put(_b('k'), Uint8List(10));
      expect(table.sizeBytes, lessThan(large));
      table.delete(_b('k'));
      expect(table.sizeBytes, greaterThan(0));
      expect(table.entryCount, 1);
    });

    test('isFull triggers at maxSizeBytes', () {
      final table = MemTable(maxSizeBytes: 500);
      expect(table.isFull, isFalse);
      table.put(_b('k'), Uint8List(600));
      expect(table.isFull, isTrue);
    });

    test('range respects inclusive start and exclusive end', () {
      final table = MemTable(maxSizeBytes: 1 << 20);
      for (final k in ['a', 'b', 'c', 'd']) {
        table.put(_b(k), _b('v$k'));
      }
      final slice = table
          .range(ByteKey.fromString('b'), ByteKey.fromString('d'))
          .map((e) => e.key.asString)
          .toList();
      expect(slice, ['b', 'c']);
    });
  });
}
