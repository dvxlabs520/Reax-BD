import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/core/wal/write_ahead_log.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  late Directory dir;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('wal_test_');
  });

  tearDown(() async {
    if (await dir.exists()) await dir.delete(recursive: true);
  });

  group('WriteAheadLog', () {
    test('append assigns increasing sequence numbers', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      final s1 = await wal.append(WalEntry.put(_b('a'), _b('1')));
      final s2 = await wal.append(WalEntry.put(_b('b'), _b('2')));
      expect(s2, s1 + 1);
      expect(wal.lastSequenceNumber, s2);
      await wal.close();
    });

    test('replay returns appended entries with types intact', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.append(WalEntry.put(_b('k1'), _b('v1')));
      await wal.append(WalEntry.delete(_b('k1')));
      await wal.append(WalEntry.put(_b('k2'), Uint8List(0)));
      await wal.close();

      final reopened = await WriteAheadLog.open(directory: dir.path);
      final entries = await reopened.replay().toList();
      expect(entries.length, 3);
      expect(entries[0].type, WalEntryType.put);
      expect(entries[0].key, _b('k1'));
      expect(entries[0].value, _b('v1'));
      expect(entries[1].type, WalEntryType.delete);
      expect(entries[1].key, _b('k1'));
      expect(entries[2].type, WalEntryType.put);
      expect(entries[2].value, isEmpty);
      await reopened.close();
    });

    test('should handle mixed operations deterministically', () async {
      // Regression for the historically flaky test: puts, deletes, and a
      // checkpoint must replay with exact types and content every time.
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.append(WalEntry.put(_b('a'), _b('1')));
      await wal.append(WalEntry.delete(_b('a')));
      final coveredUpTo = await wal.append(WalEntry.put(_b('b'), _b('2')));
      await wal.checkpoint(coveredUpTo);
      await wal.append(WalEntry.put(_b('c'), _b('3')));
      await wal.append(WalEntry.delete(_b('b')));
      await wal.close();

      for (var round = 0; round < 3; round++) {
        final reopened = await WriteAheadLog.open(directory: dir.path);
        final entries = await reopened.replay().toList();
        expect(entries.map((e) => e.type).toList(), [
          WalEntryType.put,
          WalEntryType.delete,
        ], reason: 'round $round');
        expect(entries[0].key, _b('c'));
        expect(entries[0].value, _b('3'));
        expect(entries[1].key, _b('b'));
        await reopened.close();
      }
    });

    test('appendAll persists a batch with ordered sequences', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.appendAll([
        WalEntry.put(_b('x'), _b('1')),
        WalEntry.put(_b('y'), _b('2')),
        WalEntry.put(_b('z'), _b('3')),
      ]);
      expect(wal.lastSequenceNumber, 3);
      await wal.close();

      final reopened = await WriteAheadLog.open(directory: dir.path);
      final entries = await reopened.replay().toList();
      expect(entries.map((e) => e.sequenceNumber), [1, 2, 3]);
      await reopened.close();
    });

    test('replay discards transactions without a commit', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.append(WalEntry.txBegin(1));
      await wal.append(
        WalEntry.put(_b('committed'), _b('yes'), transactionId: 1),
      );
      await wal.append(WalEntry.txCommit(1));
      await wal.append(WalEntry.txBegin(2));
      await wal.append(
        WalEntry.put(_b('uncommitted'), _b('no'), transactionId: 2),
      );
      // No commit for tx 2 (simulated crash mid-transaction).
      await wal.append(WalEntry.txBegin(3));
      await wal.append(WalEntry.put(_b('aborted'), _b('no'), transactionId: 3));
      await wal.append(WalEntry.txAbort(3));
      await wal.append(WalEntry.put(_b('plain'), _b('yes')));
      await wal.close();

      final reopened = await WriteAheadLog.open(directory: dir.path);
      final keys = await reopened
          .replay()
          .map((e) => utf8.decode(e.key!))
          .toList();
      expect(keys, ['committed', 'plain']);
      await reopened.close();
    });

    test('checkpoint deletes fully obsolete files', () async {
      final wal = await WriteAheadLog.open(
        directory: dir.path,
        maxFileBytes: 256,
      );
      for (var i = 0; i < 50; i++) {
        await wal.append(WalEntry.put(_b('key-$i'), Uint8List(64)));
      }
      final before = await _walFileCount(dir);
      expect(before, greaterThan(2));
      await wal.checkpoint(wal.lastSequenceNumber);
      final after = await _walFileCount(dir);
      expect(after, lessThan(before));
      final entries = await wal.replay().toList();
      expect(entries, isEmpty);
      await wal.close();
    });

    test('replay is idempotent', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.append(WalEntry.put(_b('k'), _b('v')));
      await wal.append(WalEntry.delete(_b('gone')));
      final first = await wal.replay().toList();
      final second = await wal.replay().toList();
      expect(second.length, first.length);
      for (var i = 0; i < first.length; i++) {
        expect(second[i].sequenceNumber, first[i].sequenceNumber);
        expect(second[i].type, first[i].type);
        expect(second[i].key, first[i].key);
        expect(second[i].value, first[i].value);
      }
      await wal.close();
    });

    test('SyncMode.none still recovers after clean close', () async {
      final wal = await WriteAheadLog.open(
        directory: dir.path,
        syncMode: SyncMode.none,
      );
      await wal.append(WalEntry.put(_b('buffered'), _b('v')));
      await wal.close();
      final reopened = await WriteAheadLog.open(directory: dir.path);
      final entries = await reopened.replay().toList();
      expect(entries.single.key, _b('buffered'));
      await reopened.close();
    });

    test('append after close throws DatabaseClosedException', () async {
      final wal = await WriteAheadLog.open(directory: dir.path);
      await wal.close();
      expect(
        () => wal.append(WalEntry.put(_b('k'), _b('v'))),
        throwsA(isA<DatabaseClosedException>()),
      );
    });
  });
}

Future<int> _walFileCount(Directory dir) async {
  var count = 0;
  await for (final e in dir.list()) {
    if (e is File && e.path.endsWith('.wal')) count++;
  }
  return count;
}
