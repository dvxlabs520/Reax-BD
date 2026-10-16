/// Unit tests for the ReaxDB 1.x readers, run against files written by the
/// real 1.4.1 engine (see [LegacyFixtures]).
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/src/core/errors/exceptions.dart';
import 'package:reaxdb_dart/src/legacy/legacy.dart';
import 'package:test/test.dart';

import 'legacy_fixtures.dart';

void main() {
  late LegacyFixtures fixtures;

  setUpAll(() async {
    fixtures = await LegacyFixtures.build();
  });

  tearDownAll(() async {
    await fixtures.dispose();
  });

  Future<List<String>> sstablesOf(String scenario) async {
    final Directory directory = Directory(
      p.join(fixtures.pathOf(scenario), 'lsm'),
    );
    return (await directory.list().toList())
        .whereType<File>()
        .map((File f) => f.path)
        .where((String f) => f.endsWith('.sst'))
        .toList()
      ..sort();
  }

  group('LegacySSTableReader', () {
    test('parses every SSTable a 1.x database wrote', () async {
      final List<String> files = await sstablesOf('plain');
      expect(files, isNotEmpty);
      int records = 0;
      for (final String file in files) {
        final LegacySSTable table = await LegacySSTableReader.read(file);
        expect(table.truncated, isFalse, reason: file);
        expect(table.skippedRecords, 0, reason: file);
        expect(table.level, greaterThanOrEqualTo(0));
        records += table.records.length;
      }
      expect(records, greaterThan(0));
    });

    test('finds the 1.x index trailer at the front of the trailer', () async {
      // ReaxDB 1.4.1 wrote [index length][index json] at the END of the file
      // but its own reader took the length from the LAST four bytes, so
      // `SSTable._loadIndex` never loaded anything and a reopened 1.x
      // database could not serve a read from any SSTable. This reader looks
      // in the place the WRITER used, so the index is available and is used
      // to cross-check the sequential scan.
      for (final String scenario in <String>['plain', 'deletes', 'bulk']) {
        for (final String file in await sstablesOf(scenario)) {
          final LegacySSTable table = await LegacySSTableReader.read(file);
          expect(table.indexTrailerReadable, isTrue, reason: file);
          expect(table.skippedRecords, 0, reason: file);
          expect(table.warnings, isEmpty, reason: file);
          expect(table.records, isNotEmpty, reason: file);
        }
      }
    });

    test('reads keys and values that decode as 1.x values', () async {
      final List<String> files = await sstablesOf('plain');
      final Map<String, Object?> found = <String, Object?>{};
      for (final String file in files) {
        final LegacySSTable table = await LegacySSTableReader.read(file);
        for (final LegacySSTableRecord record in table.records) {
          if (record.isTombstone) continue;
          found[String.fromCharCodes(record.key)] = LegacyValueCodec.decode(
            record.value!,
          );
        }
      }
      expect(found['greeting'], 'hello world');
      expect(found['count'], 42);
      expect(found['ratio'], 3.5);
      expect(found['flag'], true);
      expect(found['user:1'], <String, Object?>{
        'name': 'Alice',
        'age': 30,
        'tags': <String>['a', 'b'],
      });
    });

    test(
      'records the LSM level and creation time from the file name',
      () async {
        for (final String file in await sstablesOf('bulk')) {
          final LegacySSTable table = await LegacySSTableReader.read(file);
          final String name = p.basename(file);
          expect(name, startsWith('level_${table.level}_'));
          expect(
            table.createdAt.millisecondsSinceEpoch,
            int.parse(name.split('_')[2].replaceAll('.sst', '')),
          );
        }
      },
    );

    test('the bulk fixture really exercised flush and compaction', () async {
      final List<String> files = await sstablesOf('bulk');
      expect(files.length, greaterThan(1));
      final List<int> levels = <int>[
        for (final String f in files) (await LegacySSTableReader.read(f)).level,
      ];
      expect(
        levels.any((int level) => level > 0),
        isTrue,
        reason: 'the fixture must contain a compacted level',
      );
    });

    test('an index pointing at the wrong offset is reported', () async {
      final Directory work = await Directory.systemTemp.createTemp('sst_index');
      addTearDown(() => work.delete(recursive: true));
      final String source = (await sstablesOf('plain')).first;
      final LegacySSTable original = await LegacySSTableReader.read(source);
      expect(original.indexTrailerReadable, isTrue);

      final Uint8List bytes = await File(source).readAsBytes();
      final int dataEnd =
          original.records.last.offset + _recordLength(original.records.last);
      final Map<String, int> index = <String, int>{
        for (final LegacySSTableRecord r in original.records)
          String.fromCharCodes(r.key): r.offset,
      };
      index[index.keys.first] = dataEnd - 1; // no record starts here
      final List<int> json = utf8.encode(jsonEncode(index));
      final Uint8List rebuilt = Uint8List(dataEnd + 4 + json.length);
      rebuilt.setRange(0, dataEnd, bytes);
      ByteData.sublistView(
        rebuilt,
        dataEnd,
        dataEnd + 4,
      ).setUint32(0, json.length, Endian.little);
      rebuilt.setRange(dataEnd + 4, rebuilt.length, json);
      final String copy = p.join(work.path, p.basename(source));
      await File(copy).writeAsBytes(rebuilt);

      final LegacySSTable table = await LegacySSTableReader.read(copy);
      expect(table.indexTrailerReadable, isTrue);
      expect(table.skippedRecords, greaterThan(0));
      expect(table.warnings.join('\n'), contains('index'));
      // The scan still recovered every record; only the index disagreed.
      expect(table.records.length, original.records.length);
    });

    test('a truncated file yields warnings, not an exception', () async {
      final Directory work = await Directory.systemTemp.createTemp('sst_trunc');
      addTearDown(() => work.delete(recursive: true));
      final String source = (await sstablesOf('plain')).first;
      final Uint8List bytes = await File(source).readAsBytes();
      final String copy = p.join(work.path, p.basename(source));
      await File(
        copy,
      ).writeAsBytes(Uint8List.sublistView(bytes, 0, bytes.length ~/ 2));

      final LegacySSTable table = await LegacySSTableReader.read(copy);
      expect(table.truncated, isTrue);
      expect(table.warnings, isNotEmpty);
      expect(table.skippedRecords, greaterThan(0));
      expect(table.records, isNotEmpty);
      final LegacySSTable full = await LegacySSTableReader.read(source);
      expect(table.records.length, lessThan(full.records.length));
    });

    test('a file shorter than a record is reported, not thrown', () async {
      final Directory work = await Directory.systemTemp.createTemp('sst_tiny');
      addTearDown(() => work.delete(recursive: true));
      final String copy = p.join(work.path, 'level_0_1.sst');
      await File(copy).writeAsBytes(<int>[1, 2]);
      final LegacySSTable table = await LegacySSTableReader.read(copy);
      expect(table.records, isEmpty);
      expect(table.warnings, isNotEmpty);
      expect(table.truncated, isTrue);
    });
  });

  group('LegacyWalReader', () {
    test('replays a 1.x WAL in sequence order', () async {
      final LegacyWalScan scan = await LegacyWalReader.read(
        p.join(fixtures.pathOf('deletes'), 'wal'),
      );
      expect(scan.fileCount, greaterThan(0));
      expect(scan.entries, isNotEmpty);
      expect(scan.skippedRecords, 0);
      expect(scan.truncatedFiles, isEmpty);
      for (int i = 1; i < scan.entries.length; i++) {
        expect(
          scan.entries[i].sequenceNumber,
          greaterThanOrEqualTo(scan.entries[i - 1].sequenceNumber),
        );
      }
      expect(
        scan.entries.any(
          (LegacyWalEntry e) => e.type == LegacyWalEntryType.delete,
        ),
        isTrue,
      );
      expect(
        scan.entries.any(
          (LegacyWalEntry e) => e.type == LegacyWalEntryType.put,
        ),
        isTrue,
      );
    });

    test('delete records carry a key and no value', () async {
      final LegacyWalScan scan = await LegacyWalReader.read(
        p.join(fixtures.pathOf('deletes'), 'wal'),
      );
      final Iterable<LegacyWalEntry> deletes = scan.entries.where(
        (LegacyWalEntry e) => e.type == LegacyWalEntryType.delete,
      );
      expect(deletes, isNotEmpty);
      for (final LegacyWalEntry entry in deletes) {
        expect(entry.value, isNull);
        expect(String.fromCharCodes(entry.key), startsWith('item:'));
      }
    });

    test('a torn tail stops the scan and is reported', () async {
      final Directory work = await Directory.systemTemp.createTemp('wal_trunc');
      addTearDown(() => work.delete(recursive: true));
      final Directory walDirectory = Directory(
        p.join(fixtures.pathOf('deletes'), 'wal'),
      );
      int copied = 0;
      for (final FileSystemEntity entity
          in await walDirectory.list().toList()) {
        if (entity is! File) continue;
        final File target = await entity.copy(
          p.join(work.path, p.basename(entity.path)),
        );
        final Uint8List bytes = await target.readAsBytes();
        if (bytes.length > 16) {
          await target.writeAsBytes(
            Uint8List.sublistView(bytes, 0, bytes.length - 7),
          );
          copied++;
        }
      }
      expect(copied, greaterThan(0));

      final LegacyWalScan scan = await LegacyWalReader.read(work.path);
      expect(scan.truncatedFiles, isNotEmpty);
      expect(scan.warnings, isNotEmpty);
      expect(scan.skippedRecords, greaterThan(0));
      expect(scan.entries, isNotEmpty);
    });

    test('a missing WAL directory reads as empty', () async {
      final LegacyWalScan scan = await LegacyWalReader.read(
        p.join(Directory.systemTemp.path, 'reaxdb-no-such-wal-directory'),
      );
      expect(scan.entries, isEmpty);
      expect(scan.fileCount, 0);
    });
  });

  group('LegacyValueCodec', () {
    test('decodes every 1.x type marker', () {
      expect(LegacyValueCodec.decode(_string('hi')), 'hi');
      expect(LegacyValueCodec.decode(_int(-9)), -9);
      expect(LegacyValueCodec.decode(_double(1.5)), 1.5);
      expect(LegacyValueCodec.decode(Uint8List.fromList(<int>[3, 1])), true);
      expect(LegacyValueCodec.decode(Uint8List.fromList(<int>[3, 0])), false);
      expect(
        LegacyValueCodec.decode(_json(<String, Object?>{'a': 1})),
        <String, Object?>{'a': 1},
      );
    });

    test('rejects a declared length that does not match the record', () {
      final Uint8List bytes = _string('hello');
      bytes[1] = 99;
      expect(
        () => LegacyValueCodec.decode(bytes),
        throwsA(isA<SerializationException>()),
      );
      expect(LegacyValueCodec.isValid(bytes), isFalse);
    });

    test('rejects an unknown type marker and an empty record', () {
      expect(
        () => LegacyValueCodec.decode(Uint8List.fromList(<int>[42, 0])),
        throwsA(isA<SerializationException>()),
      );
      expect(
        () => LegacyValueCodec.decode(Uint8List(0)),
        throwsA(isA<SerializationException>()),
      );
    });

    test('rejects random bytes, which is what makes key probing work', () {
      int accepted = 0;
      for (int seed = 0; seed < 500; seed++) {
        final Uint8List noise = Uint8List.fromList(<int>[
          for (int i = 0; i < 24; i++) (seed * 31 + i * 17) & 0xFF,
        ]);
        if (LegacyValueCodec.isValid(noise)) accepted++;
      }
      expect(accepted, 0);
    });
  });

  group('LegacyDecryptor', () {
    test('reproduces the 1.x AES key derivation', () {
      const String password = 'super-secret-legacy-key-2025';
      List<int> digest = sha256.convert(<int>[
        ...utf8.encode(password),
        ...utf8.encode('ReaxDB_Salt_v1_2025'),
      ]).bytes;
      for (int i = 0; i < 10000; i++) {
        digest = sha256.convert(digest).bytes;
      }
      expect(LegacyDecryptor.deriveAes256Key(password), digest);
    });

    test('decrypts real 1.x AES-256-GCM records', () async {
      final LegacyDecryptor decryptor = LegacyDecryptor.aes256(
        'super-secret-legacy-key-2025',
      );
      final Map<String, Object?> found = <String, Object?>{};
      for (final String file in await sstablesOf('encrypted_aes')) {
        final LegacySSTable table = await LegacySSTableReader.read(file);
        for (final LegacySSTableRecord record in table.records) {
          if (record.isTombstone) continue;
          found[String.fromCharCodes(record.key)] = LegacyValueCodec.decode(
            decryptor.decrypt(record.value!),
          );
        }
      }
      expect(found['secret:2'], 'top secret string');
      expect(found['secret:3'], 987654321);
    });

    test('a wrong AES key fails the authentication tag', () async {
      final LegacyDecryptor decryptor = LegacyDecryptor.aes256('not-the-key');
      final LegacySSTable table = await LegacySSTableReader.read(
        (await sstablesOf('encrypted_aes')).first,
      );
      final LegacySSTableRecord record = table.records.firstWhere(
        (LegacySSTableRecord r) => !r.isTombstone,
      );
      expect(
        () => decryptor.decrypt(record.value!),
        throwsA(isA<EncryptionException>()),
      );
    });

    test('decrypts real 1.x xor records', () async {
      final LegacyDecryptor decryptor = LegacyDecryptor.xor(
        'legacy-xor-key-42',
      );
      final Map<String, Object?> found = <String, Object?>{};
      for (final String file in await sstablesOf('encrypted_xor')) {
        final LegacySSTable table = await LegacySSTableReader.read(file);
        for (final LegacySSTableRecord record in table.records) {
          if (record.isTombstone) continue;
          found[String.fromCharCodes(record.key)] = LegacyValueCodec.decode(
            decryptor.decrypt(record.value!),
          );
        }
      }
      expect(found['obf:2'], 'plain-ish');
    });

    test('a wrong xor key produces bytes that are not a 1.x value', () async {
      final LegacyDecryptor decryptor = LegacyDecryptor.xor('wrong-xor-key');
      final LegacySSTable table = await LegacySSTableReader.read(
        (await sstablesOf('encrypted_xor')).first,
      );
      final LegacySSTableRecord record = table.records.firstWhere(
        (LegacySSTableRecord r) => !r.isTombstone,
      );
      expect(
        LegacyValueCodec.isValid(decryptor.decrypt(record.value!)),
        isFalse,
      );
    });

    test('a plaintext decryptor returns its input unchanged', () {
      final Uint8List data = Uint8List.fromList(<int>[1, 2, 3]);
      expect(LegacyDecryptor.none().decrypt(data), same(data));
      expect(LegacyDecryptor.none().mode, LegacyEncryptionMode.none);
    });

    test('an empty key is rejected', () {
      expect(
        () => LegacyDecryptor.aes256(''),
        throwsA(isA<EncryptionException>()),
      );
      expect(
        () => LegacyDecryptor.xor(''),
        throwsA(isA<EncryptionException>()),
      );
      expect(
        () => LegacyDecryptor.forMode(LegacyEncryptionMode.aes256, null),
        throwsA(isA<EncryptionException>()),
      );
    });
  });
}

Uint8List _string(String value) {
  final List<int> bytes = utf8.encode(value);
  final Uint8List out = Uint8List(5 + bytes.length);
  out[0] = 0x00;
  ByteData.sublistView(out, 1, 5).setUint32(0, bytes.length, Endian.little);
  out.setRange(5, out.length, bytes);
  return out;
}

Uint8List _int(int value) {
  final Uint8List out = Uint8List(9);
  out[0] = 0x01;
  ByteData.sublistView(out, 1, 9).setInt64(0, value, Endian.little);
  return out;
}

Uint8List _double(double value) {
  final Uint8List out = Uint8List(9);
  out[0] = 0x02;
  ByteData.sublistView(out, 1, 9).setFloat64(0, value, Endian.little);
  return out;
}

Uint8List _json(Object? value) {
  final List<int> bytes = utf8.encode(jsonEncode(value));
  final Uint8List out = Uint8List(5 + bytes.length);
  out[0] = 0xFF;
  ByteData.sublistView(out, 1, 5).setUint32(0, bytes.length, Endian.little);
  out.setRange(5, out.length, bytes);
  return out;
}

int _recordLength(LegacySSTableRecord record) =>
    4 + record.key.length + 4 + (record.value?.length ?? 0);
