/// In-memory cache for decoded database values.
///
/// The former "multi-level" cache was a single LRU in practice: only L1 was
/// ever written, evicted entries were discarded instead of demoted, and one
/// logical miss was counted three times. It has been replaced by [ReaxCache],
/// one well-implemented LRU cache with TTL support, byte-accurate memory
/// accounting, honest statistics, and O(log n + k) prefix invalidation.
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
  _CacheEntry(this.value, this.size, this.expiresAt);

  Uint8List value;
  int size;
  DateTime? expiresAt;
  int accessCount = 1;

  bool isExpired(DateTime now) {
    final DateTime? at = expiresAt;
    return at != null && !now.isBefore(at);
  }
}

/// LRU cache with per-entry TTL, bounded by entry count and memory.
///
/// - `get`/`put`/`remove` are O(log n) (a sorted view is maintained for
///   prefix invalidation); no allocation happens on a cache hit.
/// - Expired entries behave as absent: a `get` removes them and counts one
///   miss and one expiration. [removeExpired] reclaims them eagerly.
/// - [invalidatePattern] handles `*`, `prefix*` and exact keys in O(log n +
///   matches) via the sorted view; only patterns with other wildcard shapes
///   fall back to a full O(n) sweep.
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

  /// Recency order: first entry is least recently used.
  final LinkedHashMap<String, _CacheEntry> _lru = LinkedHashMap();

  /// Key-sorted view of the same entries, for prefix operations.
  final SplayTreeMap<String, _CacheEntry> _byKey = SplayTreeMap();

  int _memoryBytes = 0;
  int _hits = 0;
  int _misses = 0;
  int _evictions = 0;
  int _expirations = 0;

  /// Number of live entries.
  int get length => _lru.length;

  /// Approximate memory held by live entries.
  int get memoryBytes => _memoryBytes;

  /// Current statistics.
  CacheStatistics get stats => CacheStatistics(
    hits: _hits,
    misses: _misses,
    evictions: _evictions,
    expirations: _expirations,
    entryCount: _lru.length,
    memoryBytes: _memoryBytes,
  );

  /// Returns the cached plaintext bytes for [key], or null when absent or
  /// expired. A hit refreshes the entry's recency without allocating.
  Uint8List? get(String key) {
    final _CacheEntry? entry = _lru[key];
    if (entry == null) {
      _misses++;
      return null;
    }
    if (entry.isExpired(DateTime.now())) {
      _removeEntry(key, entry);
      _expirations++;
      _misses++;
      return null;
    }
    // Refresh recency: reinsert the SAME entry object (no allocation).
    _lru.remove(key);
    _lru[key] = entry;
    entry.accessCount++;
    _hits++;
    return entry.value;
  }

  /// Whether a live (non-expired) entry exists for [key]. Does not affect
  /// recency or statistics.
  bool containsKey(String key) {
    final _CacheEntry? entry = _lru[key];
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

    final _CacheEntry? existing = _lru.remove(key);
    if (existing != null) {
      _memoryBytes -= existing.size;
      existing.value = value;
      existing.size = size;
      existing.expiresAt = expiry;
      _evictWhile(size);
      _lru[key] = existing;
      _memoryBytes += size;
      return;
    }

    _evictWhile(size);
    final _CacheEntry entry = _CacheEntry(value, size, expiry);
    _lru[key] = entry;
    _byKey[key] = entry;
    _memoryBytes += size;
  }

  /// Removes [key] if present.
  void remove(String key) {
    final _CacheEntry? entry = _lru[key];
    if (entry != null) {
      _removeEntry(key, entry);
    }
  }

  /// Removes every entry whose key starts with [prefix].
  ///
  /// O(log n + k) where k is the number of removed entries.
  void removePrefix(String prefix) {
    if (prefix.isEmpty) {
      clear();
      return;
    }
    String? key = _byKey.containsKey(prefix)
        ? prefix
        : _byKey.firstKeyAfter(prefix);
    while (key != null && key.startsWith(prefix)) {
      final String next = key;
      final String? following = _byKey.firstKeyAfter(next);
      _removeEntry(next, _byKey[next]!);
      key = following;
    }
  }

  /// Invalidates keys matching [pattern].
  ///
  /// `*` clears the cache; `prefix*` (no other special characters) uses the
  /// O(log n + k) prefix path; any other pattern is treated as a regular
  /// expression and requires a full O(n) key sweep.
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
      for (final String key in _lru.keys)
        if (regex.hasMatch(key)) key,
    ];
    for (final String key in toRemove) {
      remove(key);
    }
  }

  /// Removes every expired entry now and returns how many were removed.
  int removeExpired() {
    final DateTime now = DateTime.now();
    final List<String> expired = [
      for (final MapEntry<String, _CacheEntry> e in _lru.entries)
        if (e.value.isExpired(now)) e.key,
    ];
    for (final String key in expired) {
      _removeEntry(key, _lru[key]!);
      _expirations++;
    }
    return expired.length;
  }

  /// Removes every entry. Statistics counters are preserved.
  void clear() {
    _lru.clear();
    _byKey.clear();
    _memoryBytes = 0;
  }

  void _removeEntry(String key, _CacheEntry entry) {
    _lru.remove(key);
    _byKey.remove(key);
    _memoryBytes -= entry.size;
  }

  void _evictWhile(int incomingSize) {
    while (_lru.isNotEmpty &&
        (_lru.length >= _maxEntries ||
            _memoryBytes + incomingSize > _maxMemoryBytes)) {
      final String lruKey = _lru.keys.first;
      final _CacheEntry entry = _lru[lruKey]!;
      _removeEntry(lruKey, entry);
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
