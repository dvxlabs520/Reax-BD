/// Aggregation support for queries.
///
/// `min` and `max` use the database-wide total order from
/// `secondary_index.dart`, so mixed-type fields (for example an `int` in one
/// document and a `double` or `String` in another) aggregate without
/// throwing and agree with query sorting.
library;

import '../indexing/secondary_index.dart';

/// Aggregation functions available to [AggregationBuilder].
enum AggregationFunction {
  /// Number of documents (or of documents with a non-null field).
  count,

  /// Sum of numeric field values.
  sum,

  /// Average of numeric field values.
  avg,

  /// Smallest field value under the total order.
  min,

  /// Largest field value under the total order.
  max,

  /// Number of distinct field values under the total order.
  distinct,
}

/// Result of a single aggregation.
final class AggregationResult {
  /// Creates an aggregation result.
  const AggregationResult({
    required this.function,
    this.field,
    required this.value,
    this.metadata = const {},
  });

  /// The aggregation that produced this result.
  final AggregationFunction function;

  /// The aggregated field, when the function targets one.
  final String? field;

  /// The aggregated value.
  final dynamic value;

  /// Extra data, for example the list of distinct values.
  final Map<String, dynamic> metadata;

  @override
  String toString() =>
      'AggregationResult(${function.name}'
      '${field != null ? '($field)' : ''}: $value)';
}

/// One group produced by a grouped aggregation.
final class GroupByResult {
  /// Creates a group result.
  const GroupByResult({
    required this.groupKey,
    required this.documents,
    this.aggregations = const {},
  });

  /// The shared value of the group-by field.
  final dynamic groupKey;

  /// The documents in this group.
  final List<Map<String, dynamic>> documents;

  /// Aggregations computed over [documents].
  final Map<String, AggregationResult> aggregations;
}

/// Collects aggregation specifications and executes them over documents.
final class AggregationBuilder {
  final List<_AggregationSpec> _aggregations = [];
  String? _groupByField;

  /// Adds a count aggregation; with [field], counts non-null values only.
  AggregationBuilder count([String? field]) {
    _aggregations.add(
      _AggregationSpec(function: AggregationFunction.count, field: field),
    );
    return this;
  }

  /// Adds a sum over the numeric values of [field].
  AggregationBuilder sum(String field) {
    _aggregations.add(
      _AggregationSpec(function: AggregationFunction.sum, field: field),
    );
    return this;
  }

  /// Adds an average over the numeric values of [field].
  AggregationBuilder avg(String field) {
    _aggregations.add(
      _AggregationSpec(function: AggregationFunction.avg, field: field),
    );
    return this;
  }

  /// Adds a minimum over the values of [field].
  AggregationBuilder min(String field) {
    _aggregations.add(
      _AggregationSpec(function: AggregationFunction.min, field: field),
    );
    return this;
  }

  /// Adds a maximum over the values of [field].
  AggregationBuilder max(String field) {
    _aggregations.add(
      _AggregationSpec(function: AggregationFunction.max, field: field),
    );
    return this;
  }

  /// Adds a distinct-value count over [field].
  AggregationBuilder distinct(String field) {
    _aggregations.add(
      _AggregationSpec(function: AggregationFunction.distinct, field: field),
    );
    return this;
  }

  /// Groups documents by [field] before aggregating.
  AggregationBuilder groupBy(String field) {
    _groupByField = field;
    return this;
  }

  /// Runs the configured aggregations over [documents].
  ///
  /// Returns a `Map<String, AggregationResult>` for plain aggregations or a
  /// `List<GroupByResult>` when [groupBy] was used.
  dynamic execute(List<Map<String, dynamic>> documents) {
    if (_groupByField != null) {
      return documents.isEmpty ? <GroupByResult>[] : _executeGrouped(documents);
    }
    return _executeSimple(documents);
  }

  Map<String, AggregationResult> _executeSimple(
    List<Map<String, dynamic>> documents,
  ) {
    final Map<String, AggregationResult> results = {};
    for (final _AggregationSpec spec in _aggregations) {
      results[spec.resultKey] = _executeAggregation(spec, documents);
    }
    return results;
  }

  List<GroupByResult> _executeGrouped(List<Map<String, dynamic>> documents) {
    final List<dynamic> groupKeys = [];
    final List<List<Map<String, dynamic>>> groupDocs = [];
    // Groups by bucket, so finding a document's group is O(1) instead of a
    // pass over every group.
    final Map<String, List<int>> buckets = <String, List<int>>{};

    for (final Map<String, dynamic> doc in documents) {
      final dynamic groupKey = extractFieldValue(doc, _groupByField!);
      final List<int> candidates = buckets.putIfAbsent(
        totalOrderBucket(groupKey),
        () => <int>[],
      );
      int index = -1;
      for (final int i in candidates) {
        if (valuesEqual(groupKeys[i], groupKey)) {
          index = i;
          break;
        }
      }
      if (index < 0) {
        groupKeys.add(groupKey);
        groupDocs.add([]);
        index = groupKeys.length - 1;
        candidates.add(index);
      }
      groupDocs[index].add(doc);
    }

    return [
      for (int i = 0; i < groupKeys.length; i++)
        GroupByResult(
          groupKey: groupKeys[i],
          documents: groupDocs[i],
          aggregations: {
            for (final _AggregationSpec spec in _aggregations)
              spec.resultKey: _executeAggregation(spec, groupDocs[i]),
          },
        ),
    ];
  }

  AggregationResult _executeAggregation(
    _AggregationSpec spec,
    List<Map<String, dynamic>> documents,
  ) {
    switch (spec.function) {
      case AggregationFunction.count:
        final String? field = spec.field;
        final int count = field == null
            ? documents.length
            : documents
                  .where(
                    (Map<String, dynamic> doc) =>
                        extractFieldValue(doc, field) != null,
                  )
                  .length;
        return AggregationResult(
          function: spec.function,
          field: spec.field,
          value: count,
        );

      case AggregationFunction.sum:
        num sum = 0;
        for (final Map<String, dynamic> doc in documents) {
          final dynamic value = extractFieldValue(doc, spec.field!);
          if (value is num) sum += value;
        }
        return AggregationResult(
          function: spec.function,
          field: spec.field,
          value: sum,
        );

      case AggregationFunction.avg:
        num sum = 0;
        int count = 0;
        for (final Map<String, dynamic> doc in documents) {
          final dynamic value = extractFieldValue(doc, spec.field!);
          if (value is num) {
            sum += value;
            count++;
          }
        }
        return AggregationResult(
          function: spec.function,
          field: spec.field,
          value: count > 0 ? sum / count : 0,
        );

      case AggregationFunction.min:
        dynamic minValue;
        bool found = false;
        for (final Map<String, dynamic> doc in documents) {
          final dynamic value = extractFieldValue(doc, spec.field!);
          if (value == null) continue;
          if (!found || compareValues(value, minValue) < 0) {
            minValue = value;
            found = true;
          }
        }
        return AggregationResult(
          function: spec.function,
          field: spec.field,
          value: minValue,
        );

      case AggregationFunction.max:
        dynamic maxValue;
        bool found = false;
        for (final Map<String, dynamic> doc in documents) {
          final dynamic value = extractFieldValue(doc, spec.field!);
          if (value == null) continue;
          if (!found || compareValues(value, maxValue) > 0) {
            maxValue = value;
            found = true;
          }
        }
        return AggregationResult(
          function: spec.function,
          field: spec.field,
          value: maxValue,
        );

      case AggregationFunction.distinct:
        final List<dynamic> distinctValues = [];
        for (final Map<String, dynamic> doc in documents) {
          final dynamic value = extractFieldValue(doc, spec.field!);
          if (value == null) continue;
          if (!distinctValues.any((dynamic v) => valuesEqual(v, value))) {
            distinctValues.add(value);
          }
        }
        return AggregationResult(
          function: spec.function,
          field: spec.field,
          value: distinctValues.length,
          metadata: {'values': distinctValues},
        );
    }
  }
}

final class _AggregationSpec {
  const _AggregationSpec({required this.function, this.field});

  final AggregationFunction function;
  final String? field;

  String get resultKey =>
      field != null ? '${function.name}_$field' : function.name;
}
