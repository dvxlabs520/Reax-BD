/// The decoded copy of every user entry, for `ReaxDB.open(keepInMemory:)`.
library;

import 'dart:collection';
import 'dart:math' show min;
import 'dart:typed_data';

import '../util/code_point_order.dart';

/// One decoded entry: the user value and its optional expiry.
final class MemoryEntry {
  /// Creates an entry.
  const MemoryEntry(this.value, this.expiresAt);

  /// The decoded value, exactly as a read from disk would return it.
  final Object? value;

  /// Absolute expiry instant, or null when the entry never expires.
  final DateTime? expiresAt;

  /// Whether this entry is expired at [now].
  bool isExpiredAt(DateTime now) {
    final DateTime? at = expiresAt;
    return at != null && !now.isBefore(at);
  }
}

/// Every user entry of a database, decoded and kept in memory.
///
/// It mirrors the user key space of the storage engine exactly, expired
/// entries included until they are reclaimed, because the write pipeline
/// relies on it for the previous value of a key (index maintenance). Readers
/// check expiry themselves.
///
/// Keys are ordered like the storage engine orders their UTF-8 bytes (code
/// point order), so scans return the same order in both modes.
final class MemoryStore {
  final HashMap<String, MemoryEntry> _entries = HashMap<String, MemoryEntry>();
  // The keys in order, for scans. Values are looked up in [_entries].
  final _SortedKeys _ordered = _SortedKeys();

  /// Number of entries, expired ones included.
  int get length => _entries.length;

  /// The entry for [key], or null.
  MemoryEntry? operator [](String key) => _entries[key];

  /// Stores [value] under [key].
  void put(String key, Object? value, DateTime? expiresAt) {
    if (_entries[key] == null) _ordered.add(key);
    _entries[key] = MemoryEntry(value, expiresAt);
  }

  /// Removes [key].
  void remove(String key) {
    if (_entries.remove(key) != null) _ordered.remove(key);
  }

  /// Removes every entry.
  void clear() {
    _entries.clear();
    _ordered.clear();
  }

  /// The entries with keys in `[start, end)` that are live at [now], in key
  /// order or reverse key order, at most [limit] of them.
  ///
  /// A null bound is unbounded. The result is a snapshot: writes made after
  /// this returns do not change it, the same as a scan of the storage engine.
  /// Entries are immutable, so the snapshot holds the values as they were.
  List<MapEntry<String, MemoryEntry>> snapshot(
    String? start,
    String? end, {
    required DateTime now,
    bool reverse = false,
    int? limit,
  }) {
    final List<MapEntry<String, MemoryEntry>> out =
        <MapEntry<String, MemoryEntry>>[];
    if (limit != null && limit <= 0) return out;
    bool add(String key) {
      final MemoryEntry entry = _entries[key]!;
      if (!entry.isExpiredAt(now)) {
        out.add(MapEntry<String, MemoryEntry>(key, entry));
      }
      return limit == null || out.length < limit;
    }

    if (!reverse) {
      _ordered.walkForward(start, end, add);
    } else {
      _ordered.walkBackward(start, end, add);
    }
    return out;
  }

  /// Calls [visit] with the live entries in `[start, end)`, in key order,
  /// until it returns false. Nothing may change the store meanwhile, so
  /// [visit] must not await.
  void walk(
    String? start,
    String? end,
    DateTime now,
    bool Function(String key, MemoryEntry entry) visit,
  ) {
    _ordered.walkForward(start, end, (String key) {
      final MemoryEntry entry = _entries[key]!;
      return entry.isExpiredAt(now) || visit(key, entry);
    });
  }

  /// Orders keys by code point, which is the order of their UTF-8 bytes.
  static int compareKeys(String a, String b) => compareCodePoints(a, b);

  /// The smallest string greater than every string that starts with
  /// [prefix], or null when there is none (the prefix is all U+10FFFF).
  static String? prefixUpperBound(String prefix) {
    final List<int> runes = prefix.runes.toList();
    while (runes.isNotEmpty) {
      final int last = runes.removeLast();
      if (last < 0x10FFFF) {
        // Skip the surrogate range, which holds no code points.
        runes.add(last + 1 == 0xD800 ? 0xE000 : last + 1);
        return String.fromCharCodes(runes);
      }
    }
    return null;
  }
}

/// A deep copy of a decoded value, so callers can change what they read
/// without changing what is stored.
///
/// Keeps the types a decode produces: `Map<String, dynamic>`,
/// `List<dynamic>` and `Uint8List`. Everything else is immutable.
Object? copyValue(Object? value) {
  if (value is Map) {
    return <String, dynamic>{
      for (final MapEntry<dynamic, dynamic> entry in value.entries)
        entry.key as String: copyValue(entry.value),
    };
  }
  if (value is Uint8List) return Uint8List.fromList(value);
  if (value is List) {
    return <dynamic>[for (final Object? element in value) copyValue(element)];
  }
  return value;
}

/// Keys kept sorted with [MemoryStore.compareKeys], in chunks.
///
/// A chunk holds up to [_maxChunk] keys, so an insert or a removal shifts at
/// most that many elements, finding a position is two binary searches, and a
/// walk is a plain loop over arrays.
final class _SortedKeys {
  static const int _maxChunk = 1024;

  final List<List<String>> _chunks = <List<String>>[];

  /// Inserts [key], which must not be present.
  void add(String key) {
    if (_chunks.isEmpty) {
      _chunks.add(<String>[key]);
      return;
    }
    final int c = _chunkFor(key);
    final List<String> chunk = _chunks[c];
    chunk.insert(_lowerBound(chunk, key), key);
    if (chunk.length > _maxChunk) {
      final int half = chunk.length ~/ 2;
      _chunks.insert(c + 1, chunk.sublist(half));
      chunk.removeRange(half, chunk.length);
    }
  }

  /// Removes [key], which must be present.
  void remove(String key) {
    final int c = _chunkFor(key);
    final List<String> chunk = _chunks[c];
    final int at = _lowerBound(chunk, key);
    if (at < chunk.length && chunk[at] == key) chunk.removeAt(at);
    if (chunk.isEmpty) _chunks.removeAt(c);
  }

  void clear() => _chunks.clear();

  /// Calls [visit] with the keys in `[start, end)` in order, until it
  /// returns false. Null bounds are unbounded.
  void walkForward(String? start, String? end, bool Function(String) visit) {
    if (_chunks.isEmpty) return;
    int c = start == null ? 0 : _chunkFor(start);
    int i = start == null ? 0 : _lowerBound(_chunks[c], start);
    for (; c < _chunks.length; c++, i = 0) {
      final List<String> chunk = _chunks[c];
      for (; i < chunk.length; i++) {
        final String key = chunk[i];
        if (end != null && MemoryStore.compareKeys(key, end) >= 0) return;
        if (!visit(key)) return;
      }
    }
  }

  /// Calls [visit] with the keys in `[start, end)` in reverse order, until it
  /// returns false. Null bounds are unbounded.
  void walkBackward(String? start, String? end, bool Function(String) visit) {
    if (_chunks.isEmpty) return;
    int c = end == null ? _chunks.length - 1 : _chunkFor(end);
    int i = end == null
        ? _chunks[c].length - 1
        : _lowerBound(_chunks[c], end) - 1;
    for (; c >= 0; c--, i = c >= 0 ? _chunks[c].length - 1 : 0) {
      final List<String> chunk = _chunks[c];
      for (i = min(i, chunk.length - 1); i >= 0; i--) {
        final String key = chunk[i];
        if (start != null && MemoryStore.compareKeys(key, start) < 0) return;
        if (!visit(key)) return;
      }
    }
  }

  /// The chunk [key] belongs in: the last one whose first key is not
  /// greater than [key], or the first chunk.
  int _chunkFor(String key) {
    int low = 0;
    int high = _chunks.length - 1;
    while (low < high) {
      final int mid = (low + high + 1) >> 1;
      if (MemoryStore.compareKeys(_chunks[mid].first, key) <= 0) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  /// The first index in [chunk] whose key is not less than [key].
  static int _lowerBound(List<String> chunk, String key) {
    int low = 0;
    int high = chunk.length;
    while (low < high) {
      final int mid = (low + high) >> 1;
      if (MemoryStore.compareKeys(chunk[mid], key) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }
}
