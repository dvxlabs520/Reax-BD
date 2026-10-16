/// Durable write-ahead log.
///
/// Every mutation is recorded here before it is applied to the storage
/// engine. [WriteAheadLog.append] returns only after the record is durable
/// according to the configured [SyncMode].
///
/// On-disk format: one or more `wal-<firstSequence>.wal` files. Each file
/// starts with the shared header (`magic u32`, `version u16`) followed by
/// framed records (`crc32 u32`, `length varint`, payload) produced by
/// `RecordCodec`. A `<segment>.corrupt-tail` file next to a segment holds
/// bytes cut off as a torn tail during recovery (kept for diagnosis, removed
/// with the segment).
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../errors/exceptions.dart';
import '../platform/native_io.dart';
import '../storage/io_faults.dart';
import '../util/record_codec.dart';
import '../util/varint.dart';

/// Kind of a [WalEntry].
enum WalEntryType {
  /// A key/value insertion or update.
  put,

  /// A key deletion (tombstone).
  delete,

  /// Marks the start of a transaction.
  txBegin,

  /// Marks a transaction as committed; its writes become visible on replay.
  txCommit,

  /// Marks a transaction as aborted; its writes are discarded on replay.
  txAbort,

  /// Records that everything up to a sequence number is durable in storage.
  checkpoint,
}

/// A single write-ahead log record.
final class WalEntry {
  /// Creates a WAL entry.
  ///
  /// [sequenceNumber] is assigned by the log on append; the value passed here
  /// is ignored by [WriteAheadLog.append] and only meaningful on entries
  /// produced by [WriteAheadLog.replay].
  const WalEntry({
    required this.type,
    this.sequenceNumber = 0,
    this.key,
    this.value,
    this.transactionId,
    this.timestampMs = 0,
  });

  /// Convenience constructor for a put record.
  WalEntry.put(Uint8List key, Uint8List value, {int? transactionId})
    : this(
        type: WalEntryType.put,
        key: key,
        value: value,
        transactionId: transactionId,
        timestampMs: DateTime.now().millisecondsSinceEpoch,
      );

  /// Convenience constructor for a delete (tombstone) record.
  WalEntry.delete(Uint8List key, {int? transactionId})
    : this(
        type: WalEntryType.delete,
        key: key,
        transactionId: transactionId,
        timestampMs: DateTime.now().millisecondsSinceEpoch,
      );

  /// Convenience constructor for a transaction-begin record.
  WalEntry.txBegin(int transactionId)
    : this(
        type: WalEntryType.txBegin,
        transactionId: transactionId,
        timestampMs: DateTime.now().millisecondsSinceEpoch,
      );

  /// Convenience constructor for a transaction-commit record.
  WalEntry.txCommit(int transactionId)
    : this(
        type: WalEntryType.txCommit,
        transactionId: transactionId,
        timestampMs: DateTime.now().millisecondsSinceEpoch,
      );

  /// Convenience constructor for a transaction-abort record.
  WalEntry.txAbort(int transactionId)
    : this(
        type: WalEntryType.txAbort,
        transactionId: transactionId,
        timestampMs: DateTime.now().millisecondsSinceEpoch,
      );

  /// Monotonic sequence number assigned by the log.
  final int sequenceNumber;

  /// Record kind.
  final WalEntryType type;

  /// Key bytes for [WalEntryType.put] and [WalEntryType.delete] records.
  final Uint8List? key;

  /// Value bytes for [WalEntryType.put] records. May be empty (zero-length
  /// values are legitimate data, not tombstones).
  final Uint8List? value;

  /// Owning transaction, or null for autonomous writes.
  final int? transactionId;

  /// Wall-clock creation time in milliseconds since epoch.
  final int timestampMs;

  /// Returns a copy with [sequenceNumber] replaced.
  WalEntry _withSequence(int seq) => WalEntry(
    type: type,
    sequenceNumber: seq,
    key: key,
    value: value,
    transactionId: transactionId,
    timestampMs: timestampMs,
  );

  static const int _flagHasKey = 1;
  static const int _flagHasValue = 2;
  static const int _flagHasTx = 4;

  /// Encodes this entry as a record payload (framing is added by the log).
  Uint8List encode() {
    final out = BytesBuilder(copy: false);
    out.addByte(type.index);
    final fixed = ByteData(16)
      ..setUint64(0, sequenceNumber, Endian.little)
      ..setUint64(8, timestampMs, Endian.little);
    out.add(fixed.buffer.asUint8List());
    var flags = 0;
    if (key != null) flags |= _flagHasKey;
    if (value != null) flags |= _flagHasValue;
    if (transactionId != null) flags |= _flagHasTx;
    out.addByte(flags);
    if (transactionId != null) {
      final tx = ByteData(8)..setUint64(0, transactionId!, Endian.little);
      out.add(tx.buffer.asUint8List());
    }
    if (key != null) {
      Varint.write(out, key!.length);
      out.add(key!);
    }
    if (value != null) {
      Varint.write(out, value!.length);
      out.add(value!);
    }
    return out.takeBytes();
  }

  /// Decodes an entry from a record [payload].
  ///
  /// Throws [CorruptionException] if the payload is malformed.
  static WalEntry decode(Uint8List payload, {String? path, int? offset}) {
    try {
      var pos = 0;
      final typeIndex = payload[pos++];
      if (typeIndex >= WalEntryType.values.length) {
        throw CorruptionException(
          'Unknown WAL entry type $typeIndex',
          path: path,
          offset: offset,
        );
      }
      final type = WalEntryType.values[typeIndex];
      final fixed = ByteData.sublistView(payload, pos, pos + 16);
      final seq = fixed.getUint64(0, Endian.little);
      final ts = fixed.getUint64(8, Endian.little);
      pos += 16;
      final flags = payload[pos++];
      int? txId;
      if (flags & _flagHasTx != 0) {
        txId = ByteData.sublistView(
          payload,
          pos,
          pos + 8,
        ).getUint64(0, Endian.little);
        pos += 8;
      }
      Uint8List? key;
      if (flags & _flagHasKey != 0) {
        final (len, n) = Varint.read(payload, pos);
        pos += n;
        key = Uint8List.sublistView(payload, pos, pos + len);
        pos += len;
      }
      Uint8List? value;
      if (flags & _flagHasValue != 0) {
        final (len, n) = Varint.read(payload, pos);
        pos += n;
        value = Uint8List.sublistView(payload, pos, pos + len);
        pos += len;
      }
      if (pos != payload.length) {
        throw CorruptionException(
          'WAL entry payload has ${payload.length - pos} trailing bytes',
          path: path,
          offset: offset,
        );
      }
      return WalEntry(
        type: type,
        sequenceNumber: seq,
        key: key,
        value: value,
        transactionId: txId,
        timestampMs: ts,
      );
    } on CorruptionException {
      rethrow;
    } catch (e) {
      throw CorruptionException(
        'Malformed WAL entry payload',
        path: path,
        offset: offset,
        cause: e,
      );
    }
  }
}

/// How durable an acknowledged append is.
enum SyncMode {
  /// Records are buffered in memory and written opportunistically. Fastest;
  /// a crash may lose recently acknowledged writes.
  none,

  /// Records are handed to the operating system (`write`) before the append
  /// returns. Survives process death, not power loss.
  os,

  /// Records are fsynced (`RandomAccessFile.flush`) before the append
  /// returns. Survives power loss.
  full,
}

/// Outcome of scanning one WAL segment.
final class _SegmentScan {
  _SegmentScan({
    required this.validEnd,
    required this.lastSequence,
    this.badOffset,
    this.shortHeader = false,
  });

  /// Offset just past the last valid frame.
  final int validEnd;

  /// Highest sequence number among the valid frames, or null if none.
  final int? lastSequence;

  /// Where the first damaged or incomplete frame starts, if any.
  final int? badOffset;

  /// Whether the file is shorter than the segment header.
  final bool shortHeader;

  bool get isClean => badOffset == null && !shortHeader;
}

/// Transaction outcomes and the checkpoint floor gathered by a replay scan.
final class _Outcomes {
  int checkpointFloor = 0;
  final Set<int> committed = {};
  final Set<int> aborted = {};

  void observe(WalEntry e) {
    switch (e.type) {
      case WalEntryType.checkpoint:
        final upTo = WriteAheadLog._checkpointTarget(e);
        if (upTo > checkpointFloor) checkpointFloor = upTo;
      case WalEntryType.txCommit:
        committed.add(e.transactionId!);
      case WalEntryType.txAbort:
        aborted.add(e.transactionId!);
      case WalEntryType.put:
      case WalEntryType.delete:
      case WalEntryType.txBegin:
        break;
    }
  }

  void addAll(_Outcomes other) {
    if (other.checkpointFloor > checkpointFloor) {
      checkpointFloor = other.checkpointFloor;
    }
    committed.addAll(other.committed);
    aborted.addAll(other.aborted);
  }
}

/// Streaming reader over the frames of one WAL segment.
///
/// Reads in bounded chunks (growing only to fit a single oversized frame),
/// so memory use does not depend on the segment size.
final class _SegmentReader {
  _SegmentReader._(this.path, this._raf, this.length) : _end = length;

  static const int _chunkBytes = 256 * 1024;

  static Future<_SegmentReader> open(String path, {int? limit}) async {
    final raf = await File(path).open();
    try {
      final reader = _SegmentReader._(path, raf, await raf.length());
      if (limit != null && limit < reader._end) reader._end = limit;
      return reader;
    } catch (_) {
      await raf.close();
      rethrow;
    }
  }

  final String path;
  final RandomAccessFile _raf;

  /// File length when the reader was opened.
  final int length;

  /// Reading never goes past this offset.
  int _end;

  Uint8List _buf = Uint8List(0);

  /// File offset of `_buf[0]`.
  int _bufStart = 0;

  /// Read position within [_buf].
  int _pos = 0;

  /// Offset of the first damaged or incomplete frame, once found.
  int? badOffset;

  /// Offset just past the last valid frame returned so far.
  int get validEnd => _bufStart + _pos;

  /// Validates the segment header. Returns false when the file is shorter
  /// than a header; throws [CorruptionException] on a wrong magic/version.
  Future<bool> readHeader() async {
    const size = RecordCodec.fileHeaderSize;
    if (length < size) return false;
    final header = Uint8List(size);
    await _raf.setPosition(0);
    await _raf.readInto(header);
    RecordCodec.readFileHeader(
      header,
      WriteAheadLog.magic,
      WriteAheadLog.formatVersion,
      path: path,
    );
    _bufStart = size;
    _buf = Uint8List(0);
    _pos = 0;
    return true;
  }

  /// Returns up to [max] further valid entries with their frame sizes.
  ///
  /// An empty result means the scan is over: either a clean end of the
  /// segment or a damaged/incomplete frame at [badOffset].
  Future<List<(WalEntry, int)>> next({int max = 1024}) async {
    final out = <(WalEntry, int)>[];
    while (out.length < max && badOffset == null) {
      final reader = RecordReader(_buf, _pos);
      final r = reader.next();
      if (r.status == RecordReadStatus.ok) {
        final WalEntry e;
        try {
          e = WalEntry.decode(
            r.payload!,
            path: path,
            offset: _bufStart + r.recordOffset,
          );
        } on CorruptionException {
          badOffset = _bufStart + r.recordOffset;
          break;
        }
        out.add((e, reader.offset - _pos));
        _pos = reader.offset;
        continue;
      }
      if (r.status == RecordReadStatus.crcMismatch) {
        badOffset = _bufStart + r.recordOffset;
        break;
      }
      // endOfBuffer or tornTail: more bytes are needed to decide.
      if (_bufStart + _buf.length >= _end) {
        if (r.status == RecordReadStatus.tornTail) {
          badOffset = _bufStart + r.recordOffset;
        }
        break;
      }
      await _refill();
    }
    return out;
  }

  Future<void> _refill() async {
    final remaining = _buf.length - _pos;
    final fileOffset = _bufStart + _buf.length;
    var want = (remaining * 2 > _chunkBytes ? remaining * 2 : _chunkBytes);
    want -= remaining;
    final toRead = want < _end - fileOffset ? want : _end - fileOffset;
    final next = Uint8List(remaining + toRead);
    next.setRange(0, remaining, _buf, _pos);
    await _raf.setPosition(fileOffset);
    final got = await _raf.readInto(next, remaining);
    _bufStart += _pos;
    _pos = 0;
    if (got < toRead) {
      // The file shrank underneath us; treat what we have as the end.
      _end = fileOffset + got;
      _buf = Uint8List.sublistView(next, 0, remaining + got);
    } else {
      _buf = next;
    }
  }

  /// Reads `[from, _end)` in one piece (used only on the error path).
  Future<Uint8List> readRest(int from) async {
    final n = length - from;
    if (n <= 0) return Uint8List(0);
    final bytes = Uint8List(n);
    await _raf.setPosition(from);
    final got = await _raf.readInto(bytes);
    return got == n ? bytes : Uint8List.sublistView(bytes, 0, got);
  }

  Future<void> close() => _raf.close();
}

/// Durable, checksummed write-ahead log.
///
/// Write failures never leave garbage in front of later records: the
/// segment is truncated back to the end of the last complete frame (or, if
/// even that fails, the log switches to a fresh segment), the failed batch
/// throws, and its sequence numbers are reused by the next batch.
final class WriteAheadLog {
  WriteAheadLog._(this._directory, this.syncMode, this._maxFileBytes);

  /// File-format magic: "RWL1" little-endian.
  static const int magic = 0x314C5752;

  /// File-format version.
  static const int formatVersion = 1;

  static const String _extension = '.wal';
  static const String _corruptTailSuffix = '.corrupt-tail';
  static const int _bufferFlushBytes = 64 * 1024;

  /// Smallest possible encoded [WalEntry] payload: type, sequence,
  /// timestamp and flags.
  static const int _minPayloadBytes = 18;

  final String _directory;

  /// Durability level applied to every append.
  final SyncMode syncMode;

  final int _maxFileBytes;

  RandomAccessFile? _file;
  String _filePath = '';

  /// End of the last frame completely handed to the OS in the active
  /// segment. Everything past it is garbage from a failed write.
  int _goodOffset = 0;

  /// Highest sequence number acknowledged to a caller.
  int _lastSequence = 0;

  /// Highest sequence number handed to the OS (excludes [_pending]).
  int _writtenSequence = 0;

  int _bytesSinceCheckpoint = 0;

  /// Set when a failed write could not be rolled back yet.
  bool _poisoned = false;

  bool _closed = false;

  /// What open() learned about the newest segment, so the first replay
  /// does not scan it again while it is unchanged.
  ({String path, int validEnd, _Outcomes outcomes})? _openScan;

  /// Bytes accepted under [SyncMode.none] not yet handed to the OS.
  final BytesBuilder _pending = BytesBuilder(copy: false);

  /// Serializes appends, checkpoints, and close.
  Future<void> _lock = Future<void>.value();

  /// Opens (or creates) the log stored in [directory].
  ///
  /// Repairs the most recent file after a crash: a file shorter than its
  /// header is treated as empty, and a torn tail (an incomplete or damaged
  /// final region with no valid record after it) is copied to
  /// `<segment>.corrupt-tail` and truncated. Throws [CorruptionException]
  /// if valid records follow a damaged one, because that is corruption of
  /// acknowledged data rather than a torn write.
  static Future<WriteAheadLog> open({
    required String directory,
    SyncMode syncMode = SyncMode.full,
    int maxFileBytes = 64 * 1024 * 1024,
  }) async {
    await Directory(directory).create(recursive: true);
    final wal = WriteAheadLog._(directory, syncMode, maxFileBytes);
    await wal._recoverTailAndOpen();
    return wal;
  }

  /// Highest sequence number assigned so far (0 if none).
  int get lastSequenceNumber => _lastSequence;

  /// Approximate log bytes written since the last [checkpoint] (at open:
  /// the size of all retained segments). Replay cost grows with it.
  int get bytesSinceCheckpoint => _bytesSinceCheckpoint;

  /// Sorted list of log files, oldest first.
  Future<List<File>> _listFiles() async {
    final files = <File>[];
    await for (final e in Directory(_directory).list()) {
      if (e is File && e.path.endsWith(_extension)) files.add(e);
    }
    files.sort((a, b) => p.basename(a.path).compareTo(p.basename(b.path)));
    return files;
  }

  static String _fileName(int firstSequence) =>
      'wal-${firstSequence.toString().padLeft(20, '0')}$_extension';

  Future<void> _recoverTailAndOpen() async {
    final files = await _listFiles();
    if (files.isEmpty) {
      await _rotate();
      return;
    }
    for (final f in files) {
      final n = await f.length() - RecordCodec.fileHeaderSize;
      if (n > 0) _bytesSinceCheckpoint += n;
    }
    // Only the last file needs repair before appending: it holds the
    // highest sequence numbers and may carry a torn tail from a crash.
    // Earlier files are validated by replay().
    final last = files.last;
    final outcomes = _Outcomes();
    final scan = await _checkedScan(
      last,
      isLast: true,
      repair: true,
      onEntry: (e, _) => outcomes.observe(e),
    );
    _openScan = (
      path: last.path,
      validEnd: scan.shortHeader ? RecordCodec.fileHeaderSize : scan.validEnd,
      outcomes: outcomes,
    );
    _lastSequence = scan.lastSequence ?? 0;
    if (scan.lastSequence == null) {
      // An empty newest segment is named after the next sequence to write.
      final first = _firstSequenceOf(last.path);
      if (first != null) {
        if (first - 1 > _lastSequence) _lastSequence = first - 1;
      } else {
        // Foreign name: never reuse sequences already present earlier.
        for (var i = files.length - 2; i >= 0; i--) {
          final s = await _lastSequenceIn(files[i]);
          if (s == null) continue;
          if (s > _lastSequence) _lastSequence = s;
          break;
        }
      }
    }
    _writtenSequence = _lastSequence;
    _filePath = last.path;
    final raf = await File(_filePath).open(mode: FileMode.append);
    _goodOffset = await raf.length();
    await raf.setPosition(_goodOffset);
    _file = raf;
  }

  /// Highest sequence number among the valid leading frames of [file].
  static Future<int?> _lastSequenceIn(File file) async {
    final reader = await _SegmentReader.open(file.path);
    int? last;
    try {
      if (!await reader.readHeader()) return null;
      while (true) {
        final batch = await reader.next();
        if (batch.isEmpty) return last;
        for (final (e, _) in batch) {
          if (last == null || e.sequenceNumber > last) last = e.sequenceNumber;
        }
      }
    } on CorruptionException {
      return last;
    } finally {
      await reader.close();
    }
  }

  /// Scans [file] and applies the recovery policy to a damaged region.
  ///
  /// [onEntry] sees every valid entry in order. A damaged region is a torn
  /// write (repaired when [repair] is true, ignored otherwise) only if no
  /// valid record from a later write follows it and, for a non-final
  /// segment, the next segment continues the sequence without a gap
  /// ([nextFirst] is its first sequence number). Anything else throws
  /// [CorruptionException] and leaves the file untouched.
  Future<_SegmentScan> _checkedScan(
    File file, {
    required bool isLast,
    int? nextFirst,
    bool repair = false,
    void Function(WalEntry entry, int frameBytes)? onEntry,
  }) async {
    final reader = await _SegmentReader.open(file.path);
    final _SegmentScan scan;
    Uint8List tail = Uint8List(0);
    try {
      if (!await reader.readHeader()) {
        scan = _SegmentScan(validEnd: 0, lastSequence: null, shortHeader: true);
      } else {
        int? lastSeq;
        while (true) {
          final batch = await reader.next();
          if (batch.isEmpty) break;
          for (final (e, n) in batch) {
            if (lastSeq == null || e.sequenceNumber > lastSeq) {
              lastSeq = e.sequenceNumber;
            }
            onEntry?.call(e, n);
          }
        }
        scan = _SegmentScan(
          validEnd: reader.validEnd,
          lastSequence: lastSeq,
          badOffset: reader.badOffset,
        );
        final bad = scan.badOffset;
        if (bad != null) tail = await reader.readRest(bad);
      }
    } finally {
      await reader.close();
    }
    if (scan.isClean) return scan;

    final first = _firstSequenceOf(file.path);
    final base = scan.lastSequence ?? (first == null ? 0 : first - 1);
    if (!isLast && nextFirst != base + 1) {
      throw CorruptionException(
        scan.shortHeader
            ? 'Non-final WAL segment is shorter than its header'
            : 'Damaged record in non-final WAL file',
        path: file.path,
        offset: scan.badOffset ?? 0,
      );
    }
    if (scan.shortHeader) {
      // Crash between creating the segment and its header reaching disk.
      if (repair) await _rewriteHeader(file);
      return scan;
    }
    if (_laterWriteSurvives(tail, base)) {
      throw CorruptionException(
        'WAL record checksum mismatch: valid records follow the damaged '
        'one, so this is corruption, not a torn write',
        path: file.path,
        offset: scan.badOffset,
      );
    }
    if (repair) await _preserveAndTruncate(file, scan.badOffset!, tail);
    return scan;
  }

  /// Whether [tail] (bytes from a damaged frame to end of file) contains a
  /// valid record that belongs to a write *after* the damaged one.
  ///
  /// Valid frames that all belong to one transaction are the remains of a
  /// single torn batch (its pages may reach disk out of order) and do not
  /// count: the batch was never acknowledged and is dropped atomically.
  static bool _laterWriteSurvives(Uint8List tail, int lastGoodSequence) {
    int? txOfAll;
    var found = 0;
    var i = 1;
    while (i + 5 + _minPayloadBytes <= tail.length) {
      final frameEnd = _validFrameEndAt(tail, i, lastGoodSequence);
      if (frameEnd == null) {
        i++;
        continue;
      }
      final e = _decodeAt(tail, i);
      found++;
      final tx = e.transactionId;
      if (tx == null || (found > 1 && tx != txOfAll)) return true;
      txOfAll = tx;
      i = frameEnd;
    }
    return false;
  }

  /// End offset of a valid WAL frame starting at [i] in [bytes] whose
  /// sequence number is plausibly newer than [lastGoodSequence], or null.
  static int? _validFrameEndAt(Uint8List bytes, int i, int lastGoodSequence) {
    var length = 0;
    var shift = 0;
    var j = i + 4;
    while (true) {
      if (j >= bytes.length || j - i - 4 >= Varint.maxBytes) return null;
      final b = bytes[j++];
      length |= (b & 0x7f) << shift;
      if (b < 0x80) break;
      shift += 7;
    }
    if (length < _minPayloadBytes || j + length > bytes.length) return null;
    // Cheap plausibility checks before paying for a CRC.
    if (bytes[j] >= WalEntryType.values.length) return null;
    if (bytes[j + 17] & ~7 != 0) return null;
    final seq = ByteData.sublistView(
      bytes,
      j + 1,
      j + 9,
    ).getUint64(0, Endian.little);
    if (seq <= lastGoodSequence || seq > lastGoodSequence + (1 << 32)) {
      return null;
    }
    final payload = Uint8List.sublistView(bytes, j, j + length);
    final crc = ByteData.sublistView(
      bytes,
      i,
      i + 4,
    ).getUint32(0, Endian.little);
    if (Crc32.compute(payload) != crc) return null;
    try {
      WalEntry.decode(payload);
    } on CorruptionException {
      return null;
    }
    return j + length;
  }

  static WalEntry _decodeAt(Uint8List bytes, int i) {
    final (length, n) = Varint.read(bytes, i + 4);
    final start = i + 4 + n;
    return WalEntry.decode(Uint8List.sublistView(bytes, start, start + length));
  }

  /// Saves the torn [tail] next to the segment, then cuts it off.
  static Future<void> _preserveAndTruncate(
    File file,
    int offset,
    Uint8List tail,
  ) async {
    if (tail.isNotEmpty) {
      try {
        await File(
          '${file.path}$_corruptTailSuffix',
        ).writeAsBytes(tail, flush: true);
      } catch (_) {
        // The copy is a diagnostic aid; a torn tail is safe to drop.
      }
    }
    await _truncateFile(file, offset);
  }

  static Future<void> _rewriteHeader(File file) async {
    final raf = await file.open(mode: FileMode.write);
    try {
      final header = BytesBuilder(copy: false);
      RecordCodec.writeFileHeader(header, magic, formatVersion);
      await raf.writeFrom(header.takeBytes());
      await raf.flush();
    } finally {
      await raf.close();
    }
  }

  static Future<void> _truncateFile(File file, int length) async {
    final raf = await file.open(mode: FileMode.append);
    try {
      await raf.truncate(length);
      await raf.flush();
    } finally {
      await raf.close();
    }
  }

  /// Runs [action] holding the append lock.
  Future<T> _synchronized<T>(Future<T> Function() action) {
    final result = _lock.then((_) => action());
    _lock = result.then((_) {}, onError: (_) {});
    return result;
  }

  void _ensureOpen() {
    if (_closed) {
      throw const DatabaseClosedException('Write-ahead log is closed');
    }
  }

  // -- writing ------------------------------------------------------------

  /// Appends [entry] and returns its assigned sequence number.
  ///
  /// Returns only after the entry is durable per [syncMode].
  Future<int> append(WalEntry entry) async {
    await appendAll([entry]);
    return _lastSequence;
  }

  /// Appends [entries] with a single durability barrier for the whole batch.
  ///
  /// Sequence numbers are assigned in list order. If the write fails the
  /// batch throws, none of it is (or later becomes) part of the log, and
  /// the sequence numbers are not consumed.
  Future<void> appendAll(List<WalEntry> entries) {
    if (entries.isEmpty) return Future<void>.value();
    return _synchronized(() async {
      _ensureOpen();
      final buf = BytesBuilder(copy: false);
      var seq = _lastSequence;
      for (final e in entries) {
        RecordCodec.writeRecord(buf, e._withSequence(++seq).encode());
      }
      final bytes = buf.takeBytes();
      if (syncMode == SyncMode.none &&
          _pending.length + bytes.length < _bufferFlushBytes) {
        _pending.add(bytes);
      } else {
        await _writeOut(bytes, seq, fsync: syncMode == SyncMode.full);
      }
      _lastSequence = seq;
      _bytesSinceCheckpoint += bytes.length;
      await _maybeRotate();
    });
  }

  /// Escalates the durability of everything appended so far to [mode].
  ///
  /// Used by the database facade to honor a per-write sync override that is
  /// stronger than the log's configured [syncMode]: the batch is appended
  /// normally and then this method pushes it to the OS ([SyncMode.os]) or
  /// fsyncs it ([SyncMode.full]) before the write is acknowledged. Passing a
  /// mode weaker than or equal to what already happened is a no-op.
  Future<void> syncTo(SyncMode mode) {
    if (mode == SyncMode.none) return Future<void>.value();
    return _synchronized(() async {
      _ensureOpen();
      await _writeOut(
        Uint8List(0),
        _lastSequence,
        fsync: mode == SyncMode.full,
      );
    });
  }

  /// Hands [_pending] plus [frames] to the OS (and fsyncs when [fsync]).
  ///
  /// [lastSequence] is the highest sequence number contained in the bytes
  /// written. On failure the segment is cut back to the last good offset,
  /// previously pending bytes are kept for a retry, and the error is
  /// rethrown.
  Future<void> _writeOut(
    Uint8List frames,
    int lastSequence, {
    required bool fsync,
  }) async {
    await _ensureWritable();
    final prior = _pending.takeBytes();
    final Uint8List data;
    if (prior.isEmpty) {
      data = frames;
    } else if (frames.isEmpty) {
      data = prior;
    } else {
      data =
          (BytesBuilder(copy: false)
                ..add(prior)
                ..add(frames))
              .takeBytes();
    }
    final raf = _file!;
    try {
      if (data.isNotEmpty) {
        await IoFaults.write(raf, data, IoOp.walWrite, _filePath);
      }
      if (fsync) {
        IoFaults.check(IoOp.walSync, _filePath);
        await raf.flush();
      }
    } catch (_) {
      if (prior.isNotEmpty) _pending.add(prior);
      if (data.isNotEmpty) await _rollBack();
      rethrow;
    }
    _goodOffset += data.length;
    if (data.isNotEmpty) _writtenSequence = lastSequence;
  }

  /// Cuts the active segment back to [_goodOffset] after a failed write.
  Future<void> _rollBack() async {
    _poisoned = true;
    _poisoned = !await _truncateToGood();
  }

  Future<bool> _truncateToGood() async {
    final raf = _file;
    if (raf == null) return false;
    try {
      IoFaults.check(IoOp.walTruncate, _filePath);
      await raf.truncate(_goodOffset);
      await raf.setPosition(_goodOffset);
      await raf.flush();
      return true;
    } catch (_) {
      return false;
    }
  }

  /// Makes sure the next write lands right after the last good frame:
  /// retries the rollback of a failed write, or moves to a new segment.
  Future<void> _ensureWritable() async {
    if (!_poisoned && _file != null) return;
    if (await _truncateToGood()) {
      _poisoned = false;
      return;
    }
    // The garbage cannot be removed in place. A fresh segment that starts
    // right after the last written sequence lets recovery prove the damaged
    // tail held nothing that was acknowledged.
    await _rotate();
  }

  /// Rotates to a new segment once the active one is large enough.
  /// Failures are deferred: the batch that got here is already durable.
  Future<void> _maybeRotate() async {
    if (_goodOffset + _pending.length < _maxFileBytes) return;
    try {
      await _writeOut(Uint8List(0), _lastSequence, fsync: false);
      await _rotate();
    } catch (_) {
      // Retried after the next append.
    }
  }

  /// Starts a new segment named after the next sequence to be written.
  ///
  /// The header is fsynced regardless of [syncMode] so a crash can never
  /// leave a header-less newest segment. On failure the previous segment
  /// stays active (unless it had to be replaced in place).
  Future<void> _rotate() async {
    final path = p.join(_directory, _fileName(_writtenSequence + 1));
    final old = _file;
    if (old != null && path == _filePath) {
      // The active segment holds no frame (only a header and possibly
      // garbage), so it is simply recreated.
      _file = null;
      try {
        await old.close();
      } catch (_) {
        // Replaced below either way.
      }
    }
    final RandomAccessFile raf;
    try {
      IoFaults.check(IoOp.walCreate, path);
      raf = await File(path).open(mode: FileMode.write);
    } catch (_) {
      if (_file == null) _poisoned = true;
      rethrow;
    }
    try {
      final header = BytesBuilder(copy: false);
      RecordCodec.writeFileHeader(header, magic, formatVersion);
      await raf.writeFrom(header.takeBytes());
      await raf.flush();
      // Make the new segment's directory entry durable too.
      syncDirectory(_directory);
    } catch (_) {
      try {
        await raf.close();
      } catch (_) {
        // Already failing.
      }
      if (_file == null) {
        _poisoned = true;
      } else {
        try {
          await File(path).delete();
        } catch (_) {
          // A header-less newest segment is repaired on open.
        }
      }
      rethrow;
    }
    final previous = _file;
    _file = raf;
    _filePath = path;
    _goodOffset = RecordCodec.fileHeaderSize;
    _poisoned = false;
    if (previous != null) {
      try {
        await previous.flush();
        await previous.close();
      } catch (_) {
        // Its records were already handed to the OS (and fsynced under
        // SyncMode.full); nothing more can be done for it.
      }
    }
  }

  // -- reading ------------------------------------------------------------

  /// Replays committed entries recorded after the last checkpoint.
  ///
  /// Yields only [WalEntryType.put] and [WalEntryType.delete] entries, in
  /// sequence order. Writes belonging to a transaction without a matching
  /// [WalEntryType.txCommit] are discarded, as are writes covered by the
  /// newest [WalEntryType.checkpoint]. Segments are streamed in bounded
  /// chunks (two passes: outcomes first, then entries), never loaded whole.
  /// A torn tail left by a failed write in a non-final segment is accepted
  /// (and truncated) only when the next segment continues the sequence
  /// without a gap; any other damage throws [CorruptionException].
  /// Idempotent: replaying twice yields the same entries.
  Stream<WalEntry> replay() async* {
    _ensureOpen();
    final files = await _listFiles();
    final outcomes = _Outcomes();
    final ends = <int>[];
    for (var i = 0; i < files.length; i++) {
      final cached = _openScan;
      if (cached != null &&
          cached.path == files[i].path &&
          await files[i].length() == cached.validEnd) {
        // Scanned (and repaired) by open() and not appended to since.
        outcomes.addAll(cached.outcomes);
        ends.add(cached.validEnd);
        continue;
      }
      final isLast = i == files.length - 1;
      final scan = await _checkedScan(
        files[i],
        isLast: isLast,
        nextFirst: isLast ? null : _firstSequenceOf(files[i + 1].path),
        // The active segment was repaired at open and may be mid-append.
        repair: files[i].path != _filePath,
        onEntry: (e, _) => outcomes.observe(e),
      );
      ends.add(scan.validEnd);
    }
    final checkpointFloor = outcomes.checkpointFloor;
    final committed = outcomes.committed;
    final aborted = outcomes.aborted;
    for (var i = 0; i < files.length; i++) {
      if (ends[i] == 0) continue;
      final reader = await _SegmentReader.open(files[i].path, limit: ends[i]);
      try {
        if (!await reader.readHeader()) continue;
        while (true) {
          final batch = await reader.next();
          if (batch.isEmpty) break;
          for (final (e, _) in batch) {
            if (e.sequenceNumber <= checkpointFloor) continue;
            if (e.type != WalEntryType.put && e.type != WalEntryType.delete) {
              continue;
            }
            final tx = e.transactionId;
            if (tx != null &&
                (!committed.contains(tx) || aborted.contains(tx))) {
              continue;
            }
            yield e;
          }
        }
      } finally {
        await reader.close();
      }
    }
  }

  static int _checkpointTarget(WalEntry e) {
    final v = e.value;
    if (v == null || v.length != 8) {
      throw CorruptionException('Malformed checkpoint record');
    }
    return ByteData.sublistView(v).getUint64(0, Endian.little);
  }

  /// Records that all entries with sequence numbers up to and including
  /// [upToSequence] are durably persisted by the storage layer, then deletes
  /// log files made fully obsolete by that fact.
  Future<void> checkpoint(int upToSequence) {
    return _synchronized(() async {
      _ensureOpen();
      // Everything pending must be on disk before the files that hold it
      // can be retired.
      await _writeOut(Uint8List(0), _lastSequence, fsync: true);
      // Rotate first so the checkpoint record lands in a fresh file and the
      // previous file becomes eligible for deletion.
      await _rotate();
      final target = ByteData(8)..setUint64(0, upToSequence, Endian.little);
      final seq = _lastSequence + 1;
      final entry = WalEntry(
        type: WalEntryType.checkpoint,
        value: target.buffer.asUint8List(),
        timestampMs: DateTime.now().millisecondsSinceEpoch,
      )._withSequence(seq);
      final buf = BytesBuilder(copy: false);
      RecordCodec.writeRecord(buf, entry.encode());
      final frame = buf.takeBytes();
      // A checkpoint authorizes file deletion; it must itself be durable.
      await _writeOut(frame, seq, fsync: true);
      _lastSequence = seq;
      _bytesSinceCheckpoint = 0;

      final files = await _listFiles();
      for (var i = 0; i + 1 < files.length; i++) {
        final nextFirst = _firstSequenceOf(files[i + 1].path);
        // File i only contains sequences < nextFirst.
        if (nextFirst != null && nextFirst - 1 <= upToSequence) {
          try {
            await files[i].delete();
            final tail = File('${files[i].path}$_corruptTailSuffix');
            if (await tail.exists()) await tail.delete();
          } catch (_) {
            // Retried by the next checkpoint.
          }
        }
      }
    });
  }

  static int? _firstSequenceOf(String filePath) {
    final name = p.basename(filePath);
    if (!name.startsWith('wal-') || !name.endsWith(_extension)) return null;
    return int.tryParse(name.substring(4, name.length - _extension.length));
  }

  /// Flushes and closes the log. Safe to call more than once.
  ///
  /// Throws if bytes buffered under [SyncMode.none] could not be written;
  /// the file handle is released either way.
  Future<void> close() {
    return _synchronized(() async {
      if (_closed) return;
      _closed = true;
      try {
        await _writeOut(Uint8List(0), _lastSequence, fsync: true);
      } finally {
        try {
          await _file?.close();
        } catch (_) {
          // Nothing left to release.
        }
        _file = null;
      }
    });
  }
}
