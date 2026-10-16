/// Sorted in-memory write buffer.
library;

import 'dart:collection';
import 'dart:typed_data';

import '../util/byte_key.dart';

/// A value slot in the memtable.
///
/// Distinguishes "key present with a value" from "key present as a
/// tombstone"; absence is represented by [MemTable.lookup] returning null.
final class MemTableValue {
  /// Creates a live value slot.
  const MemTableValue(this.value);

  /// Creates a tombstone slot.
  const MemTableValue.tombstone() : value = null;

  /// The stored bytes, or null when this slot is a tombstone. A zero-length
  /// value is legitimate data.
  final Uint8List? value;

  /// Whether this slot marks a deletion.
  bool get isTombstone => value == null;
}

/// Sorted in-memory table buffering writes before they are flushed to an
/// SSTable. Tombstone-aware: deletions are stored explicitly so they can
/// shadow older on-disk versions.
final class MemTable {
  /// Creates a memtable that reports [isFull] once its content reaches
  /// [maxSizeBytes].
  MemTable({required this.maxSizeBytes});

  /// Approximate per-entry bookkeeping overhead in bytes.
  static const int entryOverheadBytes = 40;

  /// Size threshold at which the table should be flushed.
  final int maxSizeBytes;

  final SplayTreeMap<ByteKey, MemTableValue> _entries = SplayTreeMap();
  final Set<MemTableCursor> _cursors = Set.identity();
  int _sizeBytes = 0;

  /// Stores [key] = [value], replacing any previous slot.
  void put(Uint8List key, Uint8List value) {
    _set(ByteKey(key), MemTableValue(value));
  }

  /// Records a tombstone for [key].
  void delete(Uint8List key) {
    _set(ByteKey(key), const MemTableValue.tombstone());
  }

  void _set(ByteKey key, MemTableValue slot) {
    if (_cursors.isNotEmpty) _detachCursors();
    final old = _entries[key];
    if (old != null) {
      _sizeBytes -= _slotSize(key, old);
    }
    _entries[key] = slot;
    _sizeBytes += _slotSize(key, slot);
  }

  static int _slotSize(ByteKey key, MemTableValue slot) =>
      key.bytes.length + (slot.value?.length ?? 0) + entryOverheadBytes;

  /// Looks up [key].
  ///
  /// Returns null when the key is absent from this table, a tombstone slot
  /// when it was deleted here, or a live slot with its value.
  MemTableValue? lookup(Uint8List key) => _entries[ByteKey(key)];

  /// All slots (including tombstones) in ascending key order.
  Iterable<MapEntry<ByteKey, MemTableValue>> get entries => _entries.entries;

  /// Slots within [start] (inclusive) and [end] (exclusive), ascending.
  ///
  /// Lazy and positioned by tree search, so a narrow range costs
  /// `O(k log n)` rather than a walk from the first entry. Not a snapshot:
  /// use [openCursor] when writes may happen during iteration.
  Iterable<MapEntry<ByteKey, MemTableValue>> range(
    ByteKey? start,
    ByteKey? end,
  ) sync* {
    ByteKey? last;
    while (true) {
      final k = _nextKey(last, start, end, false);
      if (k == null) return;
      last = k;
      yield MapEntry(k, _entries[k]!);
    }
  }

  /// Opens a snapshot cursor over `[start, end)`, descending when
  /// [reverse] is true.
  ///
  /// The cursor reads the live tree lazily; just before the first mutation
  /// after it was opened, the cursor copies the part of its range it has not
  /// consumed yet, so it always observes the table as of [openCursor]. Close
  /// it when done to skip that copy.
  MemTableCursor openCursor(
    ByteKey? start,
    ByteKey? end, {
    bool reverse = false,
  }) {
    final c = MemTableCursor._(this, start, end, reverse);
    _cursors.add(c);
    return c;
  }

  void _detachCursors() {
    final open = List.of(_cursors);
    _cursors.clear();
    for (final c in open) {
      c._detach();
    }
  }

  /// The key after [last] within `[start, end)` in iteration order, or null.
  ByteKey? _nextKey(ByteKey? last, ByteKey? start, ByteKey? end, bool reverse) {
    if (!reverse) {
      final ByteKey? k;
      if (last != null) {
        k = _entries.firstKeyAfter(last);
      } else if (start == null) {
        k = _entries.isEmpty ? null : _entries.firstKey();
      } else {
        k = _entries.containsKey(start) ? start : _entries.firstKeyAfter(start);
      }
      if (k == null || (end != null && k.compareTo(end) >= 0)) return null;
      return k;
    }
    final ByteKey? k;
    if (last != null) {
      k = _entries.lastKeyBefore(last);
    } else if (end == null) {
      k = _entries.isEmpty ? null : _entries.lastKey();
    } else {
      k = _entries.lastKeyBefore(end);
    }
    if (k == null || (start != null && k.compareTo(start) < 0)) return null;
    return k;
  }

  /// Number of slots, tombstones included.
  int get entryCount => _entries.length;

  /// Approximate bytes held, including tombstone bookkeeping.
  int get sizeBytes => _sizeBytes;

  /// Whether the table has reached [maxSizeBytes].
  bool get isFull => _sizeBytes >= maxSizeBytes;

  /// Whether the table holds no slots at all.
  bool get isEmpty => _entries.isEmpty;
}

/// Snapshot iterator returned by [MemTable.openCursor].
final class MemTableCursor {
  MemTableCursor._(this._table, this._start, this._end, this._reverse);

  MemTable? _table;
  final ByteKey? _start;
  final ByteKey? _end;
  final bool _reverse;
  ByteKey? _last;
  List<MapEntry<ByteKey, MemTableValue>>? _copy;
  int _copyPos = 0;

  /// The next slot in iteration order, or null when the range is exhausted.
  MapEntry<ByteKey, MemTableValue>? next() {
    final copy = _copy;
    if (copy != null) {
      if (_copyPos < copy.length) return copy[_copyPos++];
      close();
      return null;
    }
    final t = _table;
    if (t == null) return null;
    final k = t._nextKey(_last, _start, _end, _reverse);
    if (k == null) {
      close();
      return null;
    }
    _last = k;
    return MapEntry(k, t._entries[k]!);
  }

  /// Copies the unconsumed remainder so later table writes cannot leak in.
  void _detach() {
    final t = _table;
    if (t == null) return;
    final rest = <MapEntry<ByteKey, MemTableValue>>[];
    var last = _last;
    while (true) {
      final k = t._nextKey(last, _start, _end, _reverse);
      if (k == null) break;
      last = k;
      rest.add(MapEntry(k, t._entries[k]!));
    }
    _copy = rest;
    _table = null;
  }

  /// Releases the cursor. Further [next] calls return null.
  void close() {
    _table?._cursors.remove(this);
    _table = null;
    _copy = const [];
    _copyPos = 0;
  }
}
