/// Immutable sorted string table (SSTable).
///
/// On-disk layout:
/// ```text
/// [magic u32][version u16]                      file header
/// framed data records                           crc32 + varint len + payload
/// framed meta record                            index, bloom, fences
/// [metaOffset u64][metaLength u64][magic u32]   fixed 20-byte footer
/// ```
/// A data-record payload is `[flag u8][keyLen varint][key][value...]` where
/// flag bit 0 marks a tombstone (tombstones carry no value bytes; an empty
/// value is legitimate data). Files are created under a temporary name and
/// atomically renamed into place, so a partially written table is never
/// visible under its final name.
///
/// The meta record stores a full key index, but only every
/// [SSTable.indexInterval]-th key is kept in memory. For the other entries
/// the table keeps just a 32-bit key fingerprint and the record offset
/// (8 bytes per entry instead of a full key copy): a lookup binary-searches
/// the sparse keys to find its block, picks the record whose fingerprint
/// matches, and reads only that record, verifying its checksum and full
/// key. The bloom filter keeps most lookups for absent keys from touching
/// the file at all. Range scans read whole blocks in bounded chunks.
library;

import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../errors/exceptions.dart';
import '../platform/native_io.dart';
import '../util/byte_key.dart';
import '../util/record_codec.dart';
import '../util/varint.dart';
import 'bloom_filter.dart';
import 'io_faults.dart';

/// One entry stored in (or destined for) an SSTable.
final class SSTableEntry {
  /// Creates a live entry.
  const SSTableEntry(this.key, Uint8List this.value);

  /// Creates a tombstone entry for [key].
  const SSTableEntry.tombstone(this.key) : value = null;

  /// The key bytes.
  final Uint8List key;

  /// The value bytes, or null when this entry is a tombstone.
  final Uint8List? value;

  /// Whether this entry marks a deletion.
  bool get isTombstone => value == null;
}

/// An idle read handle parked in the process-wide LRU.
final class _IdleHandle {
  _IdleHandle(this.pool, this.raf);

  final _HandlePool pool;
  final RandomAccessFile raf;
}

/// Process-wide bound on idle SSTable read handles.
///
/// Every table keeps its own small pool so concurrent reads never share a
/// cursor, but idle handles across all tables are capped here: the oldest
/// idle handle is closed when the cap is exceeded, keeping the number of
/// open file descriptors bounded no matter how many tables exist.
final class _IdleHandleLru {
  _IdleHandleLru(this.maxIdle);

  static final _IdleHandleLru instance = _IdleHandleLru(64);

  final int maxIdle;
  final LinkedHashSet<_IdleHandle> _idle = LinkedHashSet.identity();

  void add(_IdleHandle h) {
    _idle.add(h);
    while (_idle.length > maxIdle) {
      final oldest = _idle.first;
      _idle.remove(oldest);
      oldest.pool._evict(oldest);
    }
  }

  void remove(_IdleHandle h) => _idle.remove(h);
}

/// Bounded pool of read handles so concurrent reads never share a cursor.
final class _HandlePool {
  _HandlePool(this.path, this.capacity);

  final String path;
  final int capacity;
  final List<_IdleHandle> _free = [];
  final Queue<Completer<RandomAccessFile>> _waiters = Queue();
  int _opened = 0;
  bool _closed = false;

  /// Handles currently open (idle or in use).
  int get openHandles => _opened;

  Future<RandomAccessFile> acquire() async {
    if (_closed) {
      throw const DatabaseClosedException('SSTable is closed');
    }
    if (_free.isNotEmpty) {
      final h = _free.removeLast();
      _IdleHandleLru.instance.remove(h);
      return h.raf;
    }
    if (_opened < capacity) {
      _opened++;
      try {
        return await File(path).open();
      } catch (_) {
        _opened--;
        rethrow;
      }
    }
    final c = Completer<RandomAccessFile>();
    _waiters.add(c);
    return c.future;
  }

  void release(RandomAccessFile raf) {
    if (_waiters.isNotEmpty) {
      _waiters.removeFirst().complete(raf);
    } else if (_closed) {
      _opened--;
      unawaited(raf.close().catchError((Object _) => raf));
    } else {
      final h = _IdleHandle(this, raf);
      _free.add(h);
      _IdleHandleLru.instance.add(h);
    }
  }

  /// Closes an idle handle chosen for eviction by the LRU.
  void _evict(_IdleHandle h) {
    if (!_free.remove(h)) return;
    _opened--;
    unawaited(h.raf.close().catchError((Object _) => h.raf));
  }

  Future<void> close() async {
    _closed = true;
    while (_waiters.isNotEmpty) {
      _waiters.removeFirst().completeError(
        const DatabaseClosedException('SSTable is closed'),
      );
    }
    final idle = List.of(_free);
    _free.clear();
    for (final h in idle) {
      _IdleHandleLru.instance.remove(h);
      _opened--;
      try {
        await h.raf.close();
      } catch (_) {
        // The descriptor is gone either way.
      }
    }
  }
}

/// A decoded data record that still references its read buffer.
final class _RawRecord {
  _RawRecord(this.payload, this.keyStart, this.keyEnd);

  final Uint8List payload;
  final int keyStart;
  final int keyEnd;

  bool get isTombstone => payload[0] & SSTable._flagTombstone != 0;

  Uint8List get keyView => Uint8List.sublistView(payload, keyStart, keyEnd);

  /// Detached copy, so the read buffer can be collected.
  SSTableEntry toEntry() {
    final key = Uint8List.fromList(keyView);
    if (isTombstone) return SSTableEntry.tombstone(key);
    return SSTableEntry(
      key,
      Uint8List.fromList(Uint8List.sublistView(payload, keyEnd)),
    );
  }
}

/// An immutable, checksummed, sorted table of key/value entries.
final class SSTable {
  SSTable._(
    this.path,
    this._blockKeys,
    this._offsets,
    this._fingerprints,
    this._entryCount,
    this._bloom,
    this.minKey,
    this.maxKey,
    this.fileSizeBytes,
    this._pool,
  );

  /// File-format magic: "RXS1" little-endian.
  static const int magic = 0x31535852;

  /// File-format version.
  static const int formatVersion = 1;

  /// Every this-many-th key of the on-disk index is kept in memory.
  static const int indexInterval = 16;

  /// Upper bound on bytes read per I/O by [range].
  static const int scanChunkBytes = 64 * 1024;

  static const int _footerSize = 20;
  static const int _flagTombstone = 1;
  static const int _readHandles = 4;

  /// Absolute path of the `.sst` file.
  final String path;

  /// Smallest key in the table.
  final Uint8List minKey;

  /// Largest key in the table.
  final Uint8List maxKey;

  /// Total file size in bytes.
  final int fileSizeBytes;

  /// First key of each block of [indexInterval] consecutive records.
  final List<Uint8List> _blockKeys;

  /// File offset of every record, plus one trailing element marking the
  /// end of the data region: record `i` spans `[_offsets[i], _offsets[i+1])`.
  final List<int> _offsets;

  /// 32-bit fingerprint of every record's key (see [_fingerprint]).
  final Uint32List _fingerprints;

  final int _entryCount;
  final BloomFilter _bloom;
  final _HandlePool _pool;

  /// Number of entries, tombstones included.
  int get entryCount => _entryCount;

  /// Read handles currently open on this table (idle or in use).
  int get openHandleCount => _pool.openHandles;

  /// Writes [entries] (sorted ascending by key, unique) to [filePath],
  /// creating the file atomically via a temporary name plus rename, then
  /// opens it.
  ///
  /// On failure the temporary file is removed and nothing exists under
  /// [filePath] unless the rename itself already happened (the caller owns
  /// cleanup of the final name in that case).
  static Future<SSTable> create({
    required String filePath,
    required List<SSTableEntry> entries,
    double bloomFalsePositiveRate = 0.01,
  }) async {
    if (entries.isEmpty) {
      throw ArgumentError('Cannot create an empty SSTable');
    }
    final tmpPath = '$filePath.tmp';
    final buf = BytesBuilder(copy: false);
    RecordCodec.writeFileHeader(buf, magic, formatVersion);

    final bloom = BloomFilter(
      expectedEntries: entries.length,
      falsePositiveRate: bloomFalsePositiveRate,
    );
    final index = BytesBuilder(copy: false);
    for (final e in entries) {
      final payload = BytesBuilder(copy: false);
      payload.addByte(e.isTombstone ? _flagTombstone : 0);
      Varint.write(payload, e.key.length);
      payload.add(e.key);
      if (!e.isTombstone) payload.add(e.value!);
      final offset = buf.length;
      RecordCodec.writeRecord(buf, payload.takeBytes());
      Varint.write(index, e.key.length);
      index.add(e.key);
      Varint.write(index, offset);
      Varint.write(index, buf.length - offset);
      bloom.add(e.key);
    }

    final metaOffset = buf.length;
    final meta = BytesBuilder(copy: false);
    Varint.write(meta, entries.length);
    final minKey = entries.first.key;
    final maxKey = entries.last.key;
    Varint.write(meta, minKey.length);
    meta.add(minKey);
    Varint.write(meta, maxKey.length);
    meta.add(maxKey);
    final bloomBytes = bloom.toBytes();
    Varint.write(meta, bloomBytes.length);
    meta.add(bloomBytes);
    meta.add(index.takeBytes());
    RecordCodec.writeRecord(buf, meta.takeBytes());
    final metaLength = buf.length - metaOffset;

    final footer = ByteData(_footerSize)
      ..setUint64(0, metaOffset, Endian.little)
      ..setUint64(8, metaLength, Endian.little)
      ..setUint32(16, magic, Endian.little);
    buf.add(footer.buffer.asUint8List());

    try {
      final raf = await File(tmpPath).open(mode: FileMode.write);
      try {
        await IoFaults.write(raf, buf.takeBytes(), IoOp.sstableWrite, tmpPath);
        await raf.flush();
      } finally {
        await raf.close();
      }
      IoFaults.check(IoOp.sstableRename, tmpPath);
      // Atomic rename, made durable by fsyncing the directory. The manifest
      // (also renamed) decides membership, and unreferenced tables are
      // deleted on open.
      await File(tmpPath).rename(filePath);
      syncDirectory(p.dirname(filePath));
    } catch (_) {
      try {
        final tmp = File(tmpPath);
        if (await tmp.exists()) await tmp.delete();
      } catch (_) {
        // Best effort: `.sst.tmp` leftovers are removed on the next open.
      }
      rethrow;
    }
    return open(filePath);
  }

  /// Opens and validates an existing table.
  ///
  /// Throws [CorruptionException] when the file is truncated, has a bad
  /// magic, the meta block fails its checksum, or the index does not
  /// describe a contiguous run of data records.
  static Future<SSTable> open(String filePath) async {
    final file = File(filePath);
    final size = await file.length();
    if (size < RecordCodec.fileHeaderSize + _footerSize) {
      throw CorruptionException(
        'SSTable too small ($size bytes)',
        path: filePath,
      );
    }
    final raf = await file.open();
    try {
      final header = Uint8List(RecordCodec.fileHeaderSize);
      await raf.readInto(header);
      RecordCodec.readFileHeader(header, magic, formatVersion, path: filePath);

      await raf.setPosition(size - _footerSize);
      final footerBytes = Uint8List(_footerSize);
      await raf.readInto(footerBytes);
      final footer = ByteData.sublistView(footerBytes);
      if (footer.getUint32(16, Endian.little) != magic) {
        throw CorruptionException(
          'SSTable footer magic mismatch',
          path: filePath,
          offset: size - 4,
        );
      }
      final metaOffset = footer.getUint64(0, Endian.little);
      final metaLength = footer.getUint64(8, Endian.little);
      if (metaOffset < RecordCodec.fileHeaderSize ||
          metaOffset + metaLength != size - _footerSize) {
        throw CorruptionException(
          'SSTable footer describes an invalid meta block',
          path: filePath,
          offset: size - _footerSize,
        );
      }
      await raf.setPosition(metaOffset);
      final metaFrame = Uint8List(metaLength);
      await raf.readInto(metaFrame);
      final reader = RecordReader(metaFrame, 0);
      final r = reader.next();
      if (r.status != RecordReadStatus.ok || reader.offset != metaLength) {
        throw CorruptionException(
          'SSTable meta block failed validation',
          path: filePath,
          offset: metaOffset,
        );
      }
      final meta = r.payload!;
      var pos = 0;
      final (count, n0) = Varint.read(meta, pos);
      pos += n0;
      final (minLen, n1) = Varint.read(meta, pos);
      pos += n1;
      final minKey = Uint8List.fromList(
        Uint8List.sublistView(meta, pos, pos + minLen),
      );
      pos += minLen;
      final (maxLen, n2) = Varint.read(meta, pos);
      pos += n2;
      final maxKey = Uint8List.fromList(
        Uint8List.sublistView(meta, pos, pos + maxLen),
      );
      pos += maxLen;
      final (bloomLen, n3) = Varint.read(meta, pos);
      pos += n3;
      final bloom = BloomFilter.fromBytes(
        Uint8List.sublistView(meta, pos, pos + bloomLen),
      );
      pos += bloomLen;
      final blockKeys = <Uint8List>[];
      final List<int> offsets = size < 0xFFFFFFFF
          ? Uint32List(count + 1)
          : Int64List(count + 1);
      final fingerprints = Uint32List(count);
      var expectedOffset = RecordCodec.fileHeaderSize;
      for (var i = 0; i < count; i++) {
        final (keyLen, a) = Varint.read(meta, pos);
        pos += a;
        final keyStart = pos;
        pos += keyLen;
        final (offset, b) = Varint.read(meta, pos);
        pos += b;
        final (length, c) = Varint.read(meta, pos);
        pos += c;
        if (offset != expectedOffset || length <= 0) {
          throw CorruptionException(
            'SSTable index is not contiguous at entry $i',
            path: filePath,
            offset: metaOffset,
          );
        }
        expectedOffset += length;
        offsets[i] = offset;
        fingerprints[i] = _fingerprint(meta, keyStart, keyStart + keyLen);
        if (i % indexInterval == 0) {
          blockKeys.add(
            Uint8List.fromList(
              Uint8List.sublistView(meta, keyStart, keyStart + keyLen),
            ),
          );
        }
      }
      if (pos != meta.length) {
        throw CorruptionException(
          'SSTable meta block has trailing bytes',
          path: filePath,
          offset: metaOffset,
        );
      }
      if (count > 0 && expectedOffset != metaOffset) {
        throw CorruptionException(
          'SSTable index does not cover the data region',
          path: filePath,
          offset: metaOffset,
        );
      }
      offsets[count] = expectedOffset;
      return SSTable._(
        filePath,
        blockKeys,
        offsets,
        fingerprints,
        count,
        bloom,
        minKey,
        maxKey,
        size,
        _HandlePool(filePath, _readHandles),
      );
    } on CorruptionException {
      rethrow;
    } on RangeError catch (e) {
      throw CorruptionException(
        'SSTable meta block is malformed',
        path: filePath,
        cause: e,
      );
    } finally {
      await raf.close();
    }
  }

  int get _blockCount => _blockKeys.length;

  /// File offset where block [b] starts (`b == _blockCount` gives the end
  /// of the data region).
  int _blockStart(int b) {
    final i = b * indexInterval;
    return _offsets[i < _entryCount ? i : _entryCount];
  }

  /// FNV-1a over `bytes[start, end)`, 32 bits.
  static int _fingerprint(Uint8List bytes, int start, int end) {
    var h = 0x811c9dc5;
    for (var i = start; i < end; i++) {
      h = ((h ^ bytes[i]) * 0x01000193) & 0xFFFFFFFF;
    }
    return h;
  }

  /// Index of the first block whose first key is `>= key` (or `> key` when
  /// [inclusive] is false).
  int _blockLowerBound(Uint8List key, {bool inclusive = true}) {
    var lo = 0;
    var hi = _blockKeys.length;
    while (lo < hi) {
      final mid = (lo + hi) >> 1;
      final cmp = ByteKey.compareBytes(_blockKeys[mid], key);
      if (inclusive ? cmp < 0 : cmp <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  /// Block that would contain [key]: the last block whose first key is
  /// `<= key`, or 0 when [key] sorts before every block.
  int _blockContaining(Uint8List key) {
    final b = _blockLowerBound(key, inclusive: false) - 1;
    return b < 0 ? 0 : b;
  }

  /// Looks up [key].
  ///
  /// Returns null when the key is not in this table; otherwise the entry
  /// (which may be a tombstone).
  Future<SSTableEntry?> get(Uint8List key) async {
    if (ByteKey.compareBytes(key, minKey) < 0 ||
        ByteKey.compareBytes(key, maxKey) > 0) {
      return null;
    }
    if (!_bloom.mightContain(key)) return null;
    final from = _blockContaining(key) * indexInterval;
    final to = from + indexInterval < _entryCount
        ? from + indexInterval
        : _entryCount;
    final fp = _fingerprint(key, 0, key.length);
    for (var i = from; i < to; i++) {
      if (_fingerprints[i] != fp) continue;
      final begin = _offsets[i];
      final bytes = await _readSpan(begin, _offsets[i + 1] - begin);
      final rec = _nextRecord(RecordReader(bytes, 0), begin);
      if (rec == null) {
        throw CorruptionException(
          'SSTable index points past its data',
          path: path,
          offset: begin,
        );
      }
      // Fingerprints can collide; the checksummed key is authoritative.
      if (ByteKey.compareBytes(rec.keyView, key) == 0) return rec.toEntry();
    }
    return null;
  }

  /// Reads the bytes in `[offset, offset + length)` using a pooled handle so
  /// concurrent reads never interleave a shared cursor.
  Future<Uint8List> _readSpan(int offset, int length) async {
    final raf = await _pool.acquire();
    final bytes = Uint8List(length);
    try {
      await raf.setPosition(offset);
      final got = await raf.readInto(bytes);
      if (got != length) {
        throw CorruptionException(
          'Short read from SSTable',
          path: path,
          offset: offset,
        );
      }
    } finally {
      _pool.release(raf);
    }
    return bytes;
  }

  /// Next validated data record from [reader], or null at end of buffer.
  _RawRecord? _nextRecord(RecordReader reader, int baseOffset) {
    final r = reader.next();
    if (r.status == RecordReadStatus.endOfBuffer) return null;
    if (r.status != RecordReadStatus.ok) {
      throw CorruptionException(
        'SSTable data record failed validation',
        path: path,
        offset: baseOffset + r.recordOffset,
      );
    }
    final payload = r.payload!;
    try {
      final (keyLen, n) = Varint.read(payload, 1);
      final keyStart = 1 + n;
      final keyEnd = keyStart + keyLen;
      if (keyEnd > payload.length) throw RangeError('key overruns record');
      return _RawRecord(payload, keyStart, keyEnd);
    } catch (e) {
      throw CorruptionException(
        'SSTable data record is malformed',
        path: path,
        offset: baseOffset + r.recordOffset,
        cause: e,
      );
    }
  }

  /// Decodes every record of the block range `[fromBlock, toBlock)`.
  Future<List<SSTableEntry>> _readBlocks(int fromBlock, int toBlock) async {
    final begin = _blockStart(fromBlock);
    final bytes = await _readSpan(begin, _blockStart(toBlock) - begin);
    final out = <SSTableEntry>[];
    final reader = RecordReader(bytes, 0);
    while (true) {
      final rec = _nextRecord(reader, begin);
      if (rec == null) return out;
      out.add(rec.toEntry());
    }
  }

  /// Initial read size for a scan expected to consume about [limitHint]
  /// entries: small for short scans, growing to [scanChunkBytes].
  int _initialChunkBytes(int? limitHint) {
    if (limitHint == null) return scanChunkBytes;
    final dataBytes = _offsets[_entryCount] - _offsets[0];
    final perEntry = _entryCount == 0 ? 1 : dataBytes ~/ _entryCount + 1;
    final want = (limitHint + 1) * perEntry;
    return want < scanChunkBytes ? want : scanChunkBytes;
  }

  /// Streams entries with `startKey <= key < endKey` in key order
  /// ([reverse] flips direction). Null bounds are unbounded.
  ///
  /// Reads are lazy and chunked (whole index blocks, at most about
  /// [scanChunkBytes] per read), so cancelling the subscription stops all
  /// further I/O. [limitHint], the number of entries the caller expects to
  /// consume, only sizes the first read.
  Stream<SSTableEntry> range({
    Uint8List? startKey,
    Uint8List? endKey,
    bool reverse = false,
    int? limitHint,
  }) async* {
    if (_entryCount == 0) return;
    // Blocks [lo, hi) may hold keys in the range.
    final lo = startKey == null ? 0 : _blockContaining(startKey);
    final hi = endKey == null ? _blockCount : _blockLowerBound(endKey);
    if (lo >= hi) return;
    var chunk = _initialChunkBytes(limitHint);
    if (!reverse) {
      var b = lo;
      while (b < hi) {
        var e = b + 1;
        while (e < hi && _blockStart(e + 1) - _blockStart(b) <= chunk) {
          e++;
        }
        final entries = await _readBlocks(b, e);
        for (final entry in entries) {
          if (startKey != null &&
              ByteKey.compareBytes(entry.key, startKey) < 0) {
            continue;
          }
          if (endKey != null && ByteKey.compareBytes(entry.key, endKey) >= 0) {
            return;
          }
          yield entry;
        }
        b = e;
        chunk = scanChunkBytes;
      }
    } else {
      var e = hi;
      while (e > lo) {
        var b = e - 1;
        while (b > lo && _blockStart(e) - _blockStart(b - 1) <= chunk) {
          b--;
        }
        final entries = await _readBlocks(b, e);
        for (var i = entries.length - 1; i >= 0; i--) {
          final entry = entries[i];
          if (endKey != null && ByteKey.compareBytes(entry.key, endKey) >= 0) {
            continue;
          }
          if (startKey != null &&
              ByteKey.compareBytes(entry.key, startKey) < 0) {
            return;
          }
          yield entry;
        }
        e = b;
        chunk = scanChunkBytes;
      }
    }
  }

  /// Streams every entry in key order (used by compaction).
  Stream<SSTableEntry> scanAll() => range();

  /// Releases read handles. The file itself is immutable and remains valid.
  Future<void> close() => _pool.close();
}
