/// In-memory cache for decoded database values.
///
/// The former "multi-level" cache was a single LRU in practice: only L1 was
/// ever written, evicted entries were discarded instead of demoted, and one
/// logical miss was counted three times. It has been replaced by [ReaxCache],
/// one well-implemented LRU cache with TTL support, byte-accurate memory
/// accounting and honest statistics.
///
/// ## Cached representation (IMPORTANT for callers)
///
/// A [ReaxCache] stores exactly ONE representation of a value: the PLAINTEXT
/// serialized bytes, i.e. after decryption and before deserialization. Never
/// insert ciphertext. Storing a single representation makes the historical
/// double-decrypt bug (put caching ciphertext while get cached plaintext)
/// structurally impossible: whatever `get` returns is directly decodable.
library;

import 'dart:collection';
import 'dart:typed_data';

/// Point-in-time counters for a [ReaxCache].
final class CacheStatistics {
  /// Creates a statistics snapshot.
  const CacheStatistics({
    required this.hits,
    required this.misses,
    required this.evictions,
    required this.expirations,
    required this.entryCount,
    required this.memoryBytes,
  });

  /// Lookups that returned a live value.
  final int hits;

  /// Lookups that found nothing (or only an expired entry). One logical
  /// lookup counts as exactly one hit or one miss.
  final int misses;

  /// Entries removed to respect capacity limits.
  final int evictions;

  /// Entries removed because their TTL elapsed.
  final int expirations;

  /// Live entries currently cached.
  final int entryCount;

  /// Approximate memory held by cached entries, in bytes.
  final int memoryBytes;

  /// Fraction of lookups served from cache, in `[0, 1]`.
  double get hitRatio {
    final int total = hits + misses;
    return total > 0 ? hits / total : 0.0;
  }

  @override
  String toString() =>
      'CacheStatistics(entries: $entryCount, hits: $hits, misses: $misses, '
      'hitRatio: ${(hitRatio * 100).toStringAsFixed(1)}%)';
}

/// Mutable cache entry; mutated in place on hits so a lookup allocates
/// nothing.
final class _CacheEntry {
  _CacheEntry(this.key, this.value, this.size, this.expiresAt);

  final String key;
  Uint8List value;
  int size;
  DateTime? expiresAt;
  int accessCount = 1;

  /// Neighbours in recency order ([_newer] is more recently used).
  _CacheEntry? _older;
  _CacheEntry? _newer;

  bool isExpired(DateTime now) {
    final DateTime? at = expiresAt;
    return at != null && !now.isBefore(at);
  }
}

/// LRU cache with per-entry TTL, bounded by entry count and memory.
///
/// - `get`/`put`/`remove` are O(1): a hash map finds the entry and an
///   intrusive linked list keeps the recency order. No allocation happens on
///   a cache hit.
/// - Expired entries behave as absent: a `get` removes them and counts one
///   miss and one expiration. [removeExpired] reclaims them eagerly.
/// - [removePrefix] and [invalidatePattern] sweep every entry, O(n). The
///   database never calls them on its write path.
final class ReaxCache {
  /// Creates a cache bounded by [maxEntries] and [maxMemoryBytes].
  ///
  /// [defaultTtl] applies to entries stored without an explicit TTL; null
  /// means entries do not expire by default.
  ReaxCache({
    int maxEntries = 10000,
    int maxMemoryBytes = 64 * 1024 * 1024,
    this._defaultTtl,
  }) : _maxEntries = maxEntries,
       _maxMemoryBytes = maxMemoryBytes {
    if (maxEntries <= 0) {
      throw ArgumentError.value(maxEntries, 'maxEntries', 'must be positive');
    }
    if (maxMemoryBytes <= 0) {
      throw ArgumentError.value(
        maxMemoryBytes,
        'maxMemoryBytes',
        'must be positive',
      );
    }
  }

  /// Fixed per-entry overhead added to the accounted size.
  static const int _entryOverheadBytes = 64;

  final int _maxEntries;
  final int _maxMemoryBytes;
  final Duration? _defaultTtl;

  final HashMap<String, _CacheEntry> _entries = HashMap<String, _CacheEntry>();

  /// Least and most recently used ends of the recency list.
  _CacheEntry? _oldest;
  _CacheEntry? _newest;

  int _memoryBytes = 0;
  int _hits = 0;
  int _misses = 0;
  int _evictions = 0;
  int _expirations = 0;

  /// Number of live entries.
  int get length => _entries.length;

  /// Approximate memory held by live entries.
  int get memoryBytes => _memoryBytes;

  /// Current statistics.
  CacheStatistics get stats => CacheStatistics(
    hits: _hits,
    misses: _misses,
    evictions: _evictions,
    expirations: _expirations,
    entryCount: _entries.length,
    memoryBytes: _memoryBytes,
  );

  /// Returns the cached plaintext bytes for [key], or null when absent or
  /// expired. A hit refreshes the entry's recency without allocating.
  Uint8List? get(String key) {
    final _CacheEntry? entry = _entries[key];
    if (entry == null) {
      _misses++;
      return null;
    }
    if (entry.isExpired(DateTime.now())) {
      _removeEntry(entry);
      _expirations++;
      _misses++;
      return null;
    }
    _touch(entry);
    entry.accessCount++;
    _hits++;
    return entry.value;
  }

  /// Whether a live (non-expired) entry exists for [key]. Does not affect
  /// recency or statistics.
  bool containsKey(String key) {
    final _CacheEntry? entry = _entries[key];
    return entry != null && !entry.isExpired(DateTime.now());
  }

  /// Stores plaintext [value] under [key].
  ///
  /// [ttl] overrides the cache's default TTL; [expiresAt] pins an absolute
  /// expiry instant and wins over [ttl]. Storing may evict least recently
  /// used entries to respect the capacity bounds.
  void put(String key, Uint8List value, {Duration? ttl, DateTime? expiresAt}) {
    final int size = key.length + value.length + _entryOverheadBytes;
    final DateTime? expiry =
        expiresAt ??
        (ttl != null
            ? DateTime.now().add(ttl)
            : (_defaultTtl != null ? DateTime.now().add(_defaultTtl) : null));

    final _CacheEntry? existing = _entries[key];
    if (existing != null) {
      // Out of the list while the others are evicted, so it is never the
      // entry evicted to make room for itself.
      _unlink(existing);
      _entries.remove(key);
      _memoryBytes -= existing.size;
      existing.value = value;
      existing.size = size;
      existing.expiresAt = expiry;
      _evictWhile(size);
      _entries[key] = existing;
      _append(existing);
      _memoryBytes += size;
      return;
    }

    _evictWhile(size);
    final _CacheEntry entry = _CacheEntry(key, value, size, expiry);
    _entries[key] = entry;
    _append(entry);
    _memoryBytes += size;
  }

  /// Removes [key] if present.
  void remove(String key) {
    final _CacheEntry? entry = _entries[key];
    if (entry != null) {
      _removeEntry(entry);
    }
  }

  /// Removes every entry whose key starts with [prefix]. O(n).
  void removePrefix(String prefix) {
    if (prefix.isEmpty) {
      clear();
      return;
    }
    final List<_CacheEntry> matching = <_CacheEntry>[
      for (final _CacheEntry entry in _entries.values)
        if (entry.key.startsWith(prefix)) entry,
    ];
    for (final _CacheEntry entry in matching) {
      _removeEntry(entry);
    }
  }

  /// Invalidates keys matching [pattern].
  ///
  /// `*` clears the cache; `prefix*` (no other special characters) removes
  /// by prefix; any other pattern is treated as a regular expression. Both
  /// sweep every key, O(n).
  void invalidatePattern(String pattern) {
    if (pattern == '*') {
      clear();
      return;
    }
    if (pattern.endsWith('*')) {
      final String prefix = pattern.substring(0, pattern.length - 1);
      if (!_hasRegexSpecials(prefix)) {
        removePrefix(prefix);
        return;
      }
    }
    if (!_hasRegexSpecials(pattern)) {
      remove(pattern);
      return;
    }
    final RegExp regex = RegExp(pattern);
    final List<String> toRemove = [
      for (final String key in _entries.keys)
        if (regex.hasMatch(key)) key,
    ];
    for (final String key in toRemove) {
      remove(key);
    }
  }

  /// Removes every expired entry now and returns how many were removed.
  int removeExpired() {
    final DateTime now = DateTime.now();
    final List<_CacheEntry> expired = <_CacheEntry>[
      for (final _CacheEntry entry in _entries.values)
        if (entry.isExpired(now)) entry,
    ];
    for (final _CacheEntry entry in expired) {
      _removeEntry(entry);
      _expirations++;
    }
    return expired.length;
  }

  /// Removes every entry. Statistics counters are preserved.
  void clear() {
    _entries.clear();
    _oldest = null;
    _newest = null;
    _memoryBytes = 0;
  }

  void _removeEntry(_CacheEntry entry) {
    _entries.remove(entry.key);
    _unlink(entry);
    _memoryBytes -= entry.size;
  }

  /// Marks [entry] as the most recently used.
  void _touch(_CacheEntry entry) {
    if (identical(entry, _newest)) return;
    _unlink(entry);
    _append(entry);
  }

  void _append(_CacheEntry entry) {
    entry._older = _newest;
    entry._newer = null;
    final _CacheEntry? newest = _newest;
    if (newest != null) {
      newest._newer = entry;
    } else {
      _oldest = entry;
    }
    _newest = entry;
  }

  void _unlink(_CacheEntry entry) {
    final _CacheEntry? older = entry._older;
    final _CacheEntry? newer = entry._newer;
    if (older != null) {
      older._newer = newer;
    } else if (identical(_oldest, entry)) {
      _oldest = newer;
    }
    if (newer != null) {
      newer._older = older;
    } else if (identical(_newest, entry)) {
      _newest = older;
    }
    entry._older = null;
    entry._newer = null;
  }

  void _evictWhile(int incomingSize) {
    while (_entries.isNotEmpty &&
        (_entries.length >= _maxEntries ||
            _memoryBytes + incomingSize > _maxMemoryBytes)) {
      final _CacheEntry entry = _oldest!;
      _removeEntry(entry);
      if (entry.isExpired(DateTime.now())) {
        _expirations++;
      } else {
        _evictions++;
      }
    }
  }

  static final RegExp _regexSpecials = RegExp(r'[.\\+?\[\]()^${}|*]');

  static bool _hasRegexSpecials(String s) => _regexSpecials.hasMatch(s);
}
