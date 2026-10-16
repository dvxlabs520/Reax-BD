/// Read-only reader for the ReaxDB 1.x write-ahead log.
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

/// The operation a 1.x WAL record describes.
enum LegacyWalEntryType {
  /// A key was written. Index 0 in the 1.x `WALEntryType` enum.
  put,

  /// A key was deleted. Index 1 in the 1.x `WALEntryType` enum.
  delete,

  /// A checkpoint marker. Index 2 in the 1.x `WALEntryType` enum.
  ///
  /// ReaxDB 1.x recovery ignored these completely: `WriteAheadLog.recover`
  /// returned every record from every log file, checkpoints included, and the
  /// storage engine only acted on `put` and `delete`. Replaying from the last
  /// checkpoint would therefore lose data relative to what 1.x served, so the
  /// migrator ignores them too.
  checkpoint,
}

/// One record recovered from a 1.x WAL file.
final class LegacyWalEntry {
  /// Creates a WAL record.
  const LegacyWalEntry({
    required this.type,
    required this.key,
    required this.value,
    required this.sequenceNumber,
    required this.timestampMs,
    required this.sourceFile,
  });

  /// The operation.
  final LegacyWalEntryType type;

  /// Raw key bytes exactly as 1.x wrote them.
  final Uint8List key;

  /// Raw (still encrypted, if the database was encrypted) value bytes, or
  /// null for a delete, a checkpoint, or a zero-length payload.
  final Uint8List? value;

  /// The 1.x global sequence number; replay order is ascending by this value.
  final int sequenceNumber;

  /// Wall clock time 1.x recorded for the record.
  final int timestampMs;

  /// Name of the `.wal` file the record came from.
  final String sourceFile;
}

/// The result of replaying a 1.x WAL directory.
final class LegacyWalScan {
  /// Creates a WAL scan result.
  const LegacyWalScan({
    required this.entries,
    required this.warnings,
    required this.fileCount,
    required this.skippedRecords,
    required this.truncatedFiles,
  });

  /// Every recovered record, sorted the way 1.x sorted them: ascending by
  /// [LegacyWalEntry.sequenceNumber], ties broken by file order and then by
  /// position inside the file.
  final List<LegacyWalEntry> entries;

  /// Human readable problems found while parsing.
  final List<String> warnings;

  /// Number of `.wal` files found.
  final int fileCount;

  /// Number of records that could not be parsed.
  final int skippedRecords;

  /// Names of files whose tail was torn or malformed.
  final List<String> truncatedFiles;
}

/// Parses the ReaxDB 1.x write-ahead log format.
///
/// ## Format (from `WriteAheadLog._flushPendingWrites` in 1.4.1)
///
/// ```text
/// per record:
///   [payload length u32 LE][payload]
/// payload:
///   [type u8][sequence u64 LE][timestamp ms u64 LE]
///   [key length u32 LE][key bytes][value length u32 LE][value bytes]
/// ```
///
/// There is no file header and no checksum, and the writer only called
/// `IOSink.flush()` (not `fsync`), so a crash can leave a torn tail. 1.x
/// recovery stopped at the first record it could not read; this reader does
/// the same, but reports the stop instead of hiding it.
///
/// Files are visited in ascending file-name order
/// (`wal_<zero padded ms>.wal`), matching `WriteAheadLog.recover`.
abstract final class LegacyWalReader {
  /// Suffix of a 1.x WAL file.
  static const String extension = '.wal';

  /// Name of the WAL subdirectory inside a 1.x database directory.
  static const String directoryName = 'wal';

  /// Reads every `.wal` file in [walDirectory].
  ///
  /// Returns an empty scan when the directory does not exist. Never throws
  /// for damaged content.
  static Future<LegacyWalScan> read(String walDirectory) async {
    final Directory directory = Directory(walDirectory);
    if (!await directory.exists()) {
      return const LegacyWalScan(
        entries: <LegacyWalEntry>[],
        warnings: <String>[],
        fileCount: 0,
        skippedRecords: 0,
        truncatedFiles: <String>[],
      );
    }

    final List<File> files =
        (await directory.list().toList())
            .whereType<File>()
            .where((File f) => f.path.endsWith(extension))
            .toList()
          ..sort(
            (File a, File b) =>
                p.basename(a.path).compareTo(p.basename(b.path)),
          );

    final List<_OrderedEntry> ordered = <_OrderedEntry>[];
    final List<String> warnings = <String>[];
    final List<String> truncated = <String>[];
    int skipped = 0;

    for (int fileIndex = 0; fileIndex < files.length; fileIndex++) {
      final File file = files[fileIndex];
      final String name = p.basename(file.path);
      final Uint8List bytes = await file.readAsBytes();
      int offset = 0;
      int recordIndex = 0;

      while (offset < bytes.length) {
        if (offset + 4 > bytes.length) {
          truncated.add(name);
          warnings.add(
            '$name: ${bytes.length - offset} trailing byte(s) are shorter '
            'than a record length prefix; the tail was torn by a crash',
          );
          skipped++;
          break;
        }
        final int payloadLength = ByteData.sublistView(
          bytes,
          offset,
          offset + 4,
        ).getUint32(0, Endian.little);
        final int payloadStart = offset + 4;
        if (payloadLength == 0 || payloadStart + payloadLength > bytes.length) {
          truncated.add(name);
          warnings.add(
            '$name: record at offset $offset declares $payloadLength bytes '
            'but only ${bytes.length - payloadStart} remain; stopped reading '
            'this file here',
          );
          skipped++;
          break;
        }
        final LegacyWalEntry? entry = _parsePayload(
          Uint8List.sublistView(
            bytes,
            payloadStart,
            payloadStart + payloadLength,
          ),
          name,
        );
        if (entry == null) {
          warnings.add(
            '$name: record at offset $offset is malformed; stopped reading '
            'this file here',
          );
          truncated.add(name);
          skipped++;
          break;
        }
        ordered.add(_OrderedEntry(entry, fileIndex, recordIndex));
        recordIndex++;
        offset = payloadStart + payloadLength;
      }
    }

    ordered.sort((_OrderedEntry a, _OrderedEntry b) {
      final int bySequence = a.entry.sequenceNumber.compareTo(
        b.entry.sequenceNumber,
      );
      if (bySequence != 0) return bySequence;
      final int byFile = a.fileIndex.compareTo(b.fileIndex);
      if (byFile != 0) return byFile;
      return a.recordIndex.compareTo(b.recordIndex);
    });

    for (int i = 1; i < ordered.length; i++) {
      if (ordered[i].entry.sequenceNumber ==
          ordered[i - 1].entry.sequenceNumber) {
        warnings.add(
          'Duplicate WAL sequence number ${ordered[i].entry.sequenceNumber} '
          'in ${ordered[i - 1].entry.sourceFile} and '
          '${ordered[i].entry.sourceFile}; ReaxDB 1.x ordered these records '
          'arbitrarily, so the winning value may differ from what 1.x served',
        );
        break;
      }
    }

    return LegacyWalScan(
      entries: <LegacyWalEntry>[for (final _OrderedEntry e in ordered) e.entry],
      warnings: warnings,
      fileCount: files.length,
      skippedRecords: skipped,
      truncatedFiles: truncated,
    );
  }

  static LegacyWalEntry? _parsePayload(Uint8List payload, String sourceFile) {
    if (payload.length < 25) return null;
    final int typeIndex = payload[0];
    if (typeIndex >= LegacyWalEntryType.values.length) return null;
    final ByteData view = ByteData.sublistView(payload);
    final int sequenceNumber = view.getUint64(1, Endian.little);
    final int timestampMs = view.getUint64(9, Endian.little);
    final int keyLength = view.getUint32(17, Endian.little);
    final int keyStart = 21;
    if (keyStart + keyLength + 4 > payload.length) return null;
    final int valueLengthStart = keyStart + keyLength;
    final int valueLength = view.getUint32(valueLengthStart, Endian.little);
    final int valueStart = valueLengthStart + 4;
    if (valueStart + valueLength > payload.length) return null;
    return LegacyWalEntry(
      type: LegacyWalEntryType.values[typeIndex],
      key: Uint8List.fromList(
        Uint8List.sublistView(payload, keyStart, keyStart + keyLength),
      ),
      value: valueLength > 0
          ? Uint8List.fromList(
              Uint8List.sublistView(
                payload,
                valueStart,
                valueStart + valueLength,
              ),
            )
          : null,
      sequenceNumber: sequenceNumber,
      timestampMs: timestampMs,
      sourceFile: sourceFile,
    );
  }
}

final class _OrderedEntry {
  const _OrderedEntry(this.entry, this.fileIndex, this.recordIndex);

  final LegacyWalEntry entry;
  final int fileIndex;
  final int recordIndex;
}
