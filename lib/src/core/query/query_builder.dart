/// Fluent query building and execution.
library;

import '../cache/memory_store.dart' show copyValue;
import '../errors/exceptions.dart';
import '../indexing/index_manager.dart';
import '../indexing/secondary_index.dart';
import 'aggregation.dart';
import 'document_store.dart';

/// Operators available for database queries.
enum QueryOperator {
  /// Field equals the value (index total order: `5` matches `5.0`,
  /// lists and maps compare structurally, and `null` is queryable).
  equals,

  /// Field differs from the value.
  notEquals,

  /// Field is strictly greater than the value.
  greaterThan,

  /// Field is greater than or equal to the value.
  greaterThanOrEqual,

  /// Field is strictly less than the value.
  lessThan,

  /// Field is less than or equal to the value.
  lessThanOrEqual,

  /// Field is within `[start, end]`, both inclusive.
  between,

  /// Field equals one of the listed values.
  inList,

  /// String field contains the substring, or list field contains the value.
  contains,
}

/// A single condition of a query.
final class QueryCondition {
  /// Creates a condition comparing [field] against [value] using [operator].
  ///
  /// [field] may be a dotted path addressing nested maps (`address.city`).
  const QueryCondition({
    required this.field,
    required this.operator,
    required this.value,
  });

  /// Dotted path of the document field.
  final String field;

  /// Comparison operator.
  final QueryOperator operator;

  /// Value to compare against.
  final dynamic value;
}

/// Fluent interface for building and executing collection queries.
///
/// Non-indexed queries scan the entire collection via the [DocumentStore]
/// (which is backed by `StorageEngine.scan`), so every document is visible no
/// matter what its id looks like. When an [IndexManager] is provided and an
/// index can serve part of the query, candidate ids come from the index and
/// every condition is still re-checked against the loaded documents.
///
/// All value comparisons - conditions, `orderBy`, aggregation - use the
/// single total order defined in `secondary_index.dart`, so indexed and
/// scanned execution can never disagree.
class QueryBuilder {
  /// Creates a query over [collection].
  QueryBuilder({
    required this.collection,
    required this._store,
    this._indexManager,
  });

  /// The collection this query targets.
  final String collection;

  final DocumentStore _store;
  final IndexManager? _indexManager;
  final List<QueryCondition> _conditions = [];
  final List<_JoinSpec> _joins = [];
  int? _limitValue;
  int? _offsetValue;
  String? _orderByField;
  bool _orderDescending = false;
  bool _orderNullsLast = false;
  AggregationBuilder? _aggregationBuilder;
  String? _textSearchQuery;
  String? _textSearchField;

  /// Adds a condition comparing [field] to [value] with [operator].
  QueryBuilder where(String field, QueryOperator operator, dynamic value) {
    _conditions.add(
      QueryCondition(field: field, operator: operator, value: value),
    );
    return this;
  }

  /// Adds an equality condition. `value` may be null to match documents
  /// whose field is null or missing.
  QueryBuilder whereEquals(String field, dynamic value) =>
      where(field, QueryOperator.equals, value);

  /// Adds an inequality condition.
  QueryBuilder whereNotEquals(String field, dynamic value) =>
      where(field, QueryOperator.notEquals, value);

  /// Adds a strictly-greater-than condition.
  QueryBuilder whereGreaterThan(String field, dynamic value) =>
      where(field, QueryOperator.greaterThan, value);

  /// Adds a greater-than-or-equal condition.
  QueryBuilder whereGreaterThanOrEqual(String field, dynamic value) =>
      where(field, QueryOperator.greaterThanOrEqual, value);

  /// Adds a strictly-less-than condition.
  QueryBuilder whereLessThan(String field, dynamic value) =>
      where(field, QueryOperator.lessThan, value);

  /// Adds a less-than-or-equal condition.
  QueryBuilder whereLessThanOrEqual(String field, dynamic value) =>
      where(field, QueryOperator.lessThanOrEqual, value);

  /// Adds an inclusive range condition.
  QueryBuilder whereBetween(String field, dynamic start, dynamic end) =>
      where(field, QueryOperator.between, [start, end]);

  /// Adds a membership condition.
  QueryBuilder whereIn(String field, List<dynamic> values) =>
      where(field, QueryOperator.inList, values);

  /// Adds a containment condition (substring for strings, element for lists).
  QueryBuilder whereContains(String field, dynamic value) =>
      where(field, QueryOperator.contains, value);

  /// Sorts results by [field].
  ///
  /// Nulls sort first by default (they are the smallest values in the total
  /// order); pass [nullsLast] to move them to the end.
  QueryBuilder orderBy(
    String field, {
    bool descending = false,
    bool nullsLast = false,
  }) {
    _orderByField = field;
    _orderDescending = descending;
    _orderNullsLast = nullsLast;
    return this;
  }

  /// Limits the number of results. Must not be negative.
  QueryBuilder limit(int count) {
    if (count < 0) {
      throw const QueryException('limit must not be negative');
    }
    _limitValue = count;
    return this;
  }

  /// Skips the first [count] results. Must not be negative.
  QueryBuilder offset(int count) {
    if (count < 0) {
      throw const QueryException('offset must not be negative');
    }
    _offsetValue = count;
    return this;
  }

  /// Adds a case-insensitive text search over all string fields, or over
  /// [field] only when given.
  QueryBuilder search(String query, {String? field}) {
    _textSearchQuery = query;
    _textSearchField = field;
    return this;
  }

  /// Joins matching documents of [collection] where its [foreignField]
  /// equals this query's [localField]. Matches are attached to each result
  /// under `_joined_<collection>`.
  QueryBuilder join(String collection, String localField, String foreignField) {
    _joins.add(_JoinSpec(collection, localField, foreignField));
    return this;
  }

  /// Configures an aggregation to run with [executeAggregation].
  QueryBuilder aggregate(void Function(AggregationBuilder) builder) {
    _aggregationBuilder = AggregationBuilder();
    builder(_aggregationBuilder!);
    return this;
  }

  /// Executes the query and returns all matching documents.
  Future<List<Map<String, dynamic>>> find() async {
    final List<DocumentRecord> records = await _findRecords();
    final List<Map<String, dynamic>> results = [
      for (final DocumentRecord record in records) _output(record.document),
    ];
    if (_joins.isNotEmpty) {
      await _applyJoins(results);
    }
    return results;
  }

  /// Executes the query and returns the first matching document, or null.
  ///
  /// Does not mutate this builder: calling [find] afterwards still returns
  /// every match.
  Future<Map<String, dynamic>?> findOne() async {
    final List<DocumentRecord> records = await _findRecords(limitOverride: 1);
    if (records.isEmpty) return null;
    final List<Map<String, dynamic>> results = [
      _output(records.first.document),
    ];
    if (_joins.isNotEmpty) {
      await _applyJoins(results);
    }
    return results.first;
  }

  /// Counts matching documents without materializing the result list.
  ///
  /// With no conditions this only iterates keys and never decodes documents.
  /// With conditions every candidate document is loaded and checked, so the
  /// cost is that of the equivalent [find] minus the result list.
  Future<int> count() async {
    if (_conditions.isEmpty && _textSearchQuery == null) {
      int total = 0;
      await for (final String _ in _store.scanDocumentIds(collection)) {
        total++;
      }
      return _applyWindowToCount(total);
    }
    int matched = 0;
    await _forEachMatching((DocumentRecord _) {
      matched++;
      return true;
    });
    return _applyWindowToCount(matched);
  }

  /// Executes the configured aggregation over the matching documents.
  Future<dynamic> executeAggregation() async {
    final AggregationBuilder? builder = _aggregationBuilder;
    if (builder == null) {
      throw const QueryException(
        'No aggregation defined. Use aggregate() first.',
      );
    }
    return builder.execute(await find());
  }

  /// Returns the distinct values of [field] among matching documents.
  ///
  /// Distinctness follows the index total order, so `1` and `1.0` are one
  /// value and equal lists/maps deduplicate structurally.
  Future<List<dynamic>> distinct(String field) async {
    final List<dynamic> values = [];
    final Map<String, List<dynamic>> buckets = <String, List<dynamic>>{};
    for (final Map<String, dynamic> doc in await find()) {
      final dynamic value = extractFieldValue(doc, field);
      if (value == null) continue;
      final List<dynamic> seen = buckets.putIfAbsent(
        totalOrderBucket(value),
        () => <dynamic>[],
      );
      if (!seen.any((dynamic v) => valuesEqual(v, value))) {
        seen.add(value);
        values.add(value);
      }
    }
    return values;
  }

  /// Applies [updates] to every matching document and returns how many were
  /// written. Documents are addressed by the storage key they were loaded
  /// from, never by an id reconstructed from their fields.
  ///
  /// All the writes are one atomic batch: after a crash either every
  /// matching document has the update or none has. Documents are matched
  /// before the batch is written, so a write made in between can be
  /// overwritten; use a transaction for read-modify-write logic.
  Future<int> update(Map<String, dynamic> updates) async {
    final List<DocumentRecord> records = await _findRecords();
    await _store.putDocuments(collection, <String, Map<String, dynamic>>{
      for (final DocumentRecord record in records)
        record.id: <String, dynamic>{..._output(record.document), ...updates},
    });
    return records.length;
  }

  /// Deletes every matching document, in one atomic batch, and returns how
  /// many were removed.
  Future<int> delete() async {
    final List<DocumentRecord> records = await _findRecords();
    await _store.deleteDocuments(collection, <String>[
      for (final DocumentRecord record in records) record.id,
    ]);
    return records.length;
  }

  // --- execution ---------------------------------------------------------

  /// [document] as it may be handed to a caller: a copy when the store
  /// shares its stored objects.
  Map<String, dynamic> _output(Map<String, dynamic> document) =>
      _store.sharesDocuments
      ? copyValue(document) as Map<String, dynamic>
      : document;

  int _applyWindowToCount(int matched) {
    final int afterOffset = (matched - (_offsetValue ?? 0)).clamp(0, matched);
    final int? limitValue = _limitValue;
    return limitValue == null
        ? afterOffset
        : (afterOffset > limitValue ? limitValue : afterOffset);
  }

  /// Calls [visit] with every matching record (unordered, no offset/limit
  /// applied) until it returns false.
  ///
  /// On the index path candidates are consumed lazily and loaded in chunks,
  /// so a caller that stops early (a `limit` without `orderBy`, `findOne`)
  /// also stops the index scan. A store that can hand its documents over
  /// synchronously (a database that keeps every value in memory) is read
  /// without an await per document.
  Future<void> _forEachMatching(
    bool Function(DocumentRecord record) visit,
  ) async {
    final IndexManager? indexManager = _indexManager;
    final Stream<List<String>>? candidates =
        indexManager != null && _conditions.isNotEmpty
        ? indexManager.candidateIdChunks(collection, _conditions)
        : null;
    if (candidates != null) {
      await for (final List<String> chunk in candidates) {
        final List<DocumentRecord> records =
            _store.getDocumentsNow(collection, chunk) ??
            await _store.getDocuments(collection, chunk);
        for (final DocumentRecord record in records) {
          if (_matches(record.document) && !visit(record)) return;
        }
      }
      return;
    }
    if (_store.visitDocumentsNow(
      collection,
      (DocumentRecord record) => !_matches(record.document) || visit(record),
    )) {
      return;
    }
    await for (final DocumentRecord record in _store.scanDocuments(
      collection,
    )) {
      if (_matches(record.document) && !visit(record)) return;
    }
  }

  Future<List<DocumentRecord>> _findRecords({int? limitOverride}) async {
    final int? effectiveLimit = limitOverride ?? _limitValue;
    final int effectiveOffset = _offsetValue ?? 0;

    final List<DocumentRecord> matched = [];
    final bool canTerminateEarly =
        _orderByField == null && effectiveLimit != null;
    final int? stopAt = canTerminateEarly
        ? effectiveOffset + effectiveLimit
        : null;

    await _forEachMatching((DocumentRecord record) {
      matched.add(record);
      return stopAt == null || matched.length < stopAt;
    });

    if (_orderByField != null) {
      _sortRecords(matched);
    }

    final int start = effectiveOffset.clamp(0, matched.length);
    int end = matched.length;
    if (effectiveLimit != null && start + effectiveLimit < end) {
      end = start + effectiveLimit;
    }
    return matched.sublist(start, end);
  }

  bool _matches(Map<String, dynamic> doc) {
    for (final QueryCondition condition in _conditions) {
      if (!_matchesCondition(doc, condition)) return false;
    }
    final String? query = _textSearchQuery;
    if (query != null && !_matchesSearch(doc, query.toLowerCase())) {
      return false;
    }
    return true;
  }

  bool _matchesCondition(Map<String, dynamic> doc, QueryCondition condition) {
    final dynamic fieldValue = extractFieldValue(doc, condition.field);
    switch (condition.operator) {
      case QueryOperator.equals:
        return valuesEqual(fieldValue, condition.value);
      case QueryOperator.notEquals:
        return !valuesEqual(fieldValue, condition.value);
      case QueryOperator.greaterThan:
        return compareValues(fieldValue, condition.value) > 0;
      case QueryOperator.greaterThanOrEqual:
        return compareValues(fieldValue, condition.value) >= 0;
      case QueryOperator.lessThan:
        return compareValues(fieldValue, condition.value) < 0;
      case QueryOperator.lessThanOrEqual:
        return compareValues(fieldValue, condition.value) <= 0;
      case QueryOperator.between:
        final dynamic value = condition.value;
        if (value is! List || value.length != 2) {
          throw const QueryException(
            'whereBetween expects a [start, end] pair',
          );
        }
        return compareValues(fieldValue, value[0]) >= 0 &&
            compareValues(fieldValue, value[1]) <= 0;
      case QueryOperator.inList:
        final dynamic values = condition.value;
        if (values is! List) {
          throw const QueryException('whereIn expects a List value');
        }
        return values.any((dynamic v) => valuesEqual(fieldValue, v));
      case QueryOperator.contains:
        if (fieldValue is String && condition.value is String) {
          return fieldValue.contains(condition.value as String);
        }
        if (fieldValue is List) {
          return fieldValue.any((dynamic v) => valuesEqual(v, condition.value));
        }
        return false;
    }
  }

  bool _matchesSearch(Map<String, dynamic> doc, String searchLower) {
    final String? field = _textSearchField;
    if (field != null) {
      final dynamic value = extractFieldValue(doc, field);
      return value != null &&
          value.toString().toLowerCase().contains(searchLower);
    }
    return _searchInMap(doc, searchLower);
  }

  bool _searchInMap(Map<String, dynamic> doc, String searchLower) {
    for (final dynamic value in doc.values) {
      if (value is String && value.toLowerCase().contains(searchLower)) {
        return true;
      }
      if (value is Map<String, dynamic> && _searchInMap(value, searchLower)) {
        return true;
      }
    }
    return false;
  }

  void _sortRecords(List<DocumentRecord> records) {
    final String field = _orderByField!;
    records.sort((DocumentRecord a, DocumentRecord b) {
      final dynamic av = extractFieldValue(a.document, field);
      final dynamic bv = extractFieldValue(b.document, field);
      if (av == null || bv == null) {
        if (av == null && bv == null) return 0;
        // Null placement is absolute, unaffected by descending order.
        final int nullResult = _orderNullsLast ? 1 : -1;
        return av == null ? nullResult : -nullResult;
      }
      final int comparison = compareValues(av, bv);
      return _orderDescending ? -comparison : comparison;
    });
  }

  /// Hash join: each joined collection is scanned once and bucketed by its
  /// foreign field value, instead of one sub-query per outer row.
  Future<void> _applyJoins(List<Map<String, dynamic>> results) async {
    for (final _JoinSpec spec in _joins) {
      final Map<String, List<Map<String, dynamic>>> buckets = {};
      await for (final DocumentRecord record in _store.scanDocuments(
        spec.collection,
      )) {
        final dynamic foreignValue = extractFieldValue(
          record.document,
          spec.foreignField,
        );
        if (foreignValue == null) continue;
        final String bucketKey = _joinKey(foreignValue);
        buckets.putIfAbsent(bucketKey, () => []).add(_output(record.document));
      }
      for (int i = 0; i < results.length; i++) {
        final dynamic localValue = extractFieldValue(
          results[i],
          spec.localField,
        );
        if (localValue == null) continue;
        final List<Map<String, dynamic>>? matches =
            buckets[_joinKey(localValue)];
        if (matches != null && matches.isNotEmpty) {
          results[i] = {...results[i], '_joined_${spec.collection}': matches};
        }
      }
    }
  }

  /// Value-equality join key derived from the index encoding, so `5` joins
  /// with `5.0` and structural values join correctly.
  static String _joinKey(dynamic value) {
    final StringBuffer buffer = StringBuffer();
    for (final int byte in IndexValueCodec.encodeValue(value)) {
      buffer.write(byte.toRadixString(16).padLeft(2, '0'));
    }
    return buffer.toString();
  }
}

final class _JoinSpec {
  const _JoinSpec(this.collection, this.localField, this.foreignField);

  final String collection;
  final String localField;
  final String foreignField;
}
