/// Read-only reader for ReaxDB 1.x `.sst` files.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

/// One record recovered from a 1.x SSTable.
final class LegacySSTableRecord {
  /// Creates a record with the raw key and value bytes found on disk.
  const LegacySSTableRecord({
    required this.key,
    required this.value,
    required this.offset,
  });

  /// Raw key bytes exactly as 1.x wrote them.
  final Uint8List key;

  /// Raw (still encrypted, if the database was encrypted) value bytes, or
  /// null when the record is a 1.x tombstone.
  ///
  /// ReaxDB 1.x represented a tombstone inside an SSTable as a zero-length
  /// value: `LsmTree.get` returned null as soon as it found an empty value.
  final Uint8List? value;

  /// Byte offset of the record inside the SSTable file.
  final int offset;

  /// Whether this record marks the key as deleted.
  bool get isTombstone => value == null;
}

/// The contents of one ReaxDB 1.x SSTable file.
final class LegacySSTable {
  /// Creates a description of a parsed 1.x SSTable.
  const LegacySSTable({
    required this.path,
    required this.level,
    required this.createdAt,
    required this.records,
    required this.warnings,
    required this.skippedRecords,
    required this.indexTrailerReadable,
    required this.truncated,
  });

  /// Absolute path of the `.sst` file.
  final String path;

  /// LSM level taken from the `level_<n>_<timestamp>.sst` file name.
  final int level;

  /// Creation time taken from the file name, used to order tables inside a
  /// level exactly the way 1.x did.
  final DateTime createdAt;

  /// Records recovered from the file, in the order 1.x stored them
  /// (ascending by key).
  final List<LegacySSTableRecord> records;

  /// Human readable problems found while parsing.
  final List<String> warnings;

  /// Number of records or index entries that could not be recovered.
  final int skippedRecords;

  /// Whether the trailing key index was present and parseable.
  ///
  /// Read from the position the 1.x WRITER used, which is not the position
  /// the 1.x reader looked at; see [LegacySSTableReader]. When true, the
  /// index has been cross-checked against the sequential scan.
  final bool indexTrailerReadable;

  /// Whether the file ends in the middle of a record.
  final bool truncated;
}

/// Parses the ReaxDB 1.x SSTable format.
///
/// ## Format (from `SSTable._writeEntries` and `_writeIndex` in 1.4.1)
///
/// ```text
/// records, ascending by key:
///   [key length u32 LE][key bytes][value length u32 LE][value bytes]
/// trailer:
///   [index length u32 LE][utf8 of jsonEncode({keyString: offset, ...})]
/// ```
///
/// There is no magic number, no format version and no checksum anywhere in
/// the file, so damage can only be detected through structural invariants.
///
/// ## The 1.x index is at the wrong end of the file
///
/// `SSTable._writeIndex` appended the 4-byte index length and THEN the index
/// JSON, but `SSTable._loadIndex` read the length from the LAST four bytes of
/// the file and expected the JSON to sit in front of it. The two never agree,
/// so `_loadIndex` always failed its own validation and returned silently
/// with an empty index. The consequences in 1.4.1 were:
///
/// * `SSTable.get` always returned null for a reopened database, so an LSM
///   level could never serve a read. Everything 1.x returned after a restart
///   came from replaying the write-ahead log into the memtable.
/// * `SSTable.getAllEntries` iterated that empty index, so a compaction that
///   ran after a restart merged nothing and then deleted the level it had
///   just "merged". SSTables created earlier in the same process still had
///   their in-memory index, which is why compaction appeared to work.
///
/// This reader looks for the trailer where the WRITER put it, so the index is
/// available. It does not depend on it either: it walks the records
/// sequentially from offset 0, recognises the trailer when it reaches it, and
/// then uses the index only to cross-check the scan. Damage therefore costs
/// the records after the break instead of the whole file, and data that 1.x
/// itself could no longer read is recovered — which the migrator reports
/// rather than presenting as if 1.x had served it.
abstract final class LegacySSTableReader {
  /// Suffix of a 1.x SSTable file.
  static const String extension = '.sst';

  /// Reads and parses the 1.x SSTable at [filePath].
  ///
  /// Never throws for damaged content: unreadable records are omitted from
  /// [LegacySSTable.records], counted in [LegacySSTable.skippedRecords] and
  /// described in [LegacySSTable.warnings], so the caller can report partial
  /// recovery instead of losing the rest of the file.
  static Future<LegacySSTable> read(String filePath) async {
    final File file = File(filePath);
    final String name = p.basename(filePath);
    final List<String> warnings = <String>[];
    final _FileIdentity identity = _parseFileName(name, warnings);

    if (!await file.exists()) {
      warnings.add('$name: file disappeared before it could be read');
      return _empty(filePath, identity, warnings);
    }

    final Uint8List bytes = await file.readAsBytes();
    if (bytes.isEmpty) return _empty(filePath, identity, warnings);

    final List<LegacySSTableRecord> records = <LegacySSTableRecord>[];
    Map<String, int>? index;
    int skipped = 0;
    bool truncated = false;
    int offset = 0;

    while (offset < bytes.length) {
      final Map<String, int>? trailer = _tryReadTrailer(bytes, offset);
      if (trailer != null) {
        index = trailer;
        offset = bytes.length;
        break;
      }
      final _RawRecord? record = _parseRecord(bytes, offset, bytes.length);
      if (record == null) {
        truncated = true;
        skipped++;
        _addWarning(
          warnings,
          '$name: the file ends in the middle of a record at offset $offset '
          '(${bytes.length - offset} byte(s) left); everything before it was '
          'recovered',
        );
        break;
      }
      records.add(
        LegacySSTableRecord(
          key: Uint8List.fromList(record.key),
          value: record.value.isEmpty ? null : Uint8List.fromList(record.value),
          offset: offset,
        ),
      );
      offset = record.end;
    }

    if (index == null && !truncated) {
      _addWarning(
        warnings,
        '$name: no readable key index trailer. ReaxDB 1.4.1 wrote the index '
        'length before the index instead of after it, so 1.x could not read '
        'this file either; ${records.length} record(s) were recovered by '
        'scanning it.',
      );
    } else if (index != null) {
      skipped += _crossCheckIndex(index, records, name, warnings);
    }

    return LegacySSTable(
      path: filePath,
      level: identity.level,
      createdAt: identity.createdAt,
      records: records,
      warnings: warnings,
      skippedRecords: skipped,
      indexTrailerReadable: index != null,
      truncated: truncated,
    );
  }

  static LegacySSTable _empty(
    String filePath,
    _FileIdentity identity,
    List<String> warnings,
  ) => LegacySSTable(
    path: filePath,
    level: identity.level,
    createdAt: identity.createdAt,
    records: const <LegacySSTableRecord>[],
    warnings: warnings,
    skippedRecords: 0,
    indexTrailerReadable: false,
    truncated: false,
  );

  /// The LSM level and creation time encoded in the name of the 1.x SSTable
  /// at [filePath], exactly as [read] reports them in [LegacySSTable.level]
  /// and [LegacySSTable.createdAt], without reading the file.
  ///
  /// A name that does not match `level_<n>_<timestamp>.sst` is level 0,
  /// created at the epoch. Lets a caller order tables by 1.x read precedence
  /// before reading them, so they can be processed one at a time.
  static ({int level, DateTime createdAt}) identify(String filePath) {
    final _FileIdentity identity =
        _matchFileName(p.basename(filePath)) ?? _FileIdentity.unknown;
    return (level: identity.level, createdAt: identity.createdAt);
  }

  static _FileIdentity _parseFileName(String name, List<String> warnings) {
    final _FileIdentity? identity = _matchFileName(name);
    if (identity == null) {
      warnings.add(
        '$name: file name does not match the 1.x pattern '
        'level_<level>_<timestamp>.sst; assuming level 0',
      );
      return _FileIdentity.unknown;
    }
    return identity;
  }

  static _FileIdentity? _matchFileName(String name) {
    final RegExpMatch? match = RegExp(
      r'^level_(\d+)_(\d+)\.sst$',
    ).firstMatch(name);
    if (match == null) return null;
    return _FileIdentity(
      int.parse(match.group(1)!),
      DateTime.fromMillisecondsSinceEpoch(int.parse(match.group(2)!)),
    );
  }

  /// Recognises the index trailer that starts at [offset].
  ///
  /// The trailer is `[length u32 LE][json]` and always runs to the end of the
  /// file, so a candidate is only accepted when its declared length covers
  /// exactly the remaining bytes and those bytes are a JSON object of
  /// `key -> offset` pairs.
  static Map<String, int>? _tryReadTrailer(Uint8List bytes, int offset) {
    if (offset + 4 > bytes.length) return null;
    final int declared = ByteData.sublistView(
      bytes,
      offset,
      offset + 4,
    ).getUint32(0, Endian.little);
    if (declared != bytes.length - offset - 4) return null;
    if (declared == 0) return null;
    if (bytes[offset + 4] != 0x7B) return null; // '{'
    try {
      final Object? decoded = jsonDecode(
        utf8.decode(Uint8List.sublistView(bytes, offset + 4)),
      );
      if (decoded is! Map<String, dynamic>) return null;
      final Map<String, int> out = <String, int>{};
      for (final MapEntry<String, dynamic> entry in decoded.entries) {
        final Object? value = entry.value;
        if (value is! int) return null;
        out[entry.key] = value;
      }
      return out;
    } on FormatException {
      return null;
    }
  }

  static int _crossCheckIndex(
    Map<String, int> index,
    List<LegacySSTableRecord> records,
    String name,
    List<String> warnings,
  ) {
    final Map<int, LegacySSTableRecord> byOffset = <int, LegacySSTableRecord>{
      for (final LegacySSTableRecord record in records) record.offset: record,
    };
    int missing = 0;
    for (final MapEntry<String, int> entry in index.entries) {
      final LegacySSTableRecord? record = byOffset[entry.value];
      if (record == null) {
        missing++;
        _addWarning(
          warnings,
          '$name: the index points at offset ${entry.value}, where no record '
          'could be read',
        );
        continue;
      }
      if (String.fromCharCodes(record.key) != entry.key) {
        missing++;
        _addWarning(
          warnings,
          '$name: the record at offset ${entry.value} holds a different key '
          'than the index claims',
        );
      }
    }
    return missing;
  }

  static _RawRecord? _parseRecord(Uint8List bytes, int offset, int limit) {
    if (offset < 0 || offset + 4 > limit) return null;
    final int keyLength = ByteData.sublistView(
      bytes,
      offset,
      offset + 4,
    ).getUint32(0, Endian.little);
    final int keyStart = offset + 4;
    if (keyLength == 0 || keyStart + keyLength + 4 > limit) return null;
    final int valueLengthStart = keyStart + keyLength;
    final int valueLength = ByteData.sublistView(
      bytes,
      valueLengthStart,
      valueLengthStart + 4,
    ).getUint32(0, Endian.little);
    final int valueStart = valueLengthStart + 4;
    if (valueStart + valueLength > limit) return null;
    return _RawRecord(
      Uint8List.sublistView(bytes, keyStart, keyStart + keyLength),
      Uint8List.sublistView(bytes, valueStart, valueStart + valueLength),
      valueStart + valueLength,
    );
  }

  static void _addWarning(List<String> warnings, String warning) {
    const int maxWarnings = 32;
    if (warnings.length < maxWarnings) {
      warnings.add(warning);
    } else if (warnings.length == maxWarnings) {
      warnings.add('further warnings for this file were suppressed');
    }
  }
}

final class _FileIdentity {
  const _FileIdentity(this.level, this.createdAt);

  static final _FileIdentity unknown = _FileIdentity(
    0,
    DateTime.fromMillisecondsSinceEpoch(0),
  );

  final int level;
  final DateTime createdAt;
}

final class _RawRecord {
  const _RawRecord(this.key, this.value, this.end);

  final Uint8List key;
  final Uint8List value;
  final int end;
}
