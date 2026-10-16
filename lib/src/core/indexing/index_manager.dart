/// Secondary index management.
///
/// Index entries live in the SAME [StorageEngine] as documents, under the
/// reserved prefixes defined in `secondary_index.dart`, so a document write
/// and its index maintenance commit atomically through one `writeBatch`.
/// The write pipeline calls [IndexManager.buildIndexOps] with the old and new
/// versions of a document and appends the returned operations to the batch
/// that writes the document itself; no index write happens outside that
/// batch.
library;

import 'dart:convert';
import 'dart:typed_data';

import '../errors/exceptions.dart';
import '../query/query_builder.dart';
import '../storage/storage_engine.dart';
import 'secondary_index.dart';

/// Decodes the stored bytes of the document under [key] into a map, or
/// returns null when the bytes are not a document (for example a raw binary
/// value).
typedef DocumentDecoder =
    Map<String, dynamic>? Function(Uint8List key, Uint8List bytes);

/// Runs [action] while no other write of the database can run.
typedef ExclusiveRunner = Future<T> Function<T>(Future<T> Function() action);

/// Manages secondary indexes stored inside the primary [StorageEngine].
///
/// Contract highlights:
/// - [buildIndexOps] is called inside the write pipeline with the old
///   document (null if new) and returns the index mutations that must be
///   committed atomically with the document write.
/// - [createIndex] backfills from existing data via [StorageEngine.scan].
/// - [candidateIds] returns null when no index can serve the query; an empty
///   set always means "an index was consulted and no document matches".
final class IndexManager {
  /// Creates an index manager over [storage].
  ///
  /// [decodeDocument] converts stored document bytes (as produced by the
  /// facade's serialization, after any decryption) back into a map; it is
  /// used for backfill during [createIndex].
  ///
  /// [exclusive] serializes index definition changes and backfill chunks
  /// with the database's write pipeline; without it they run unguarded,
  /// which is only safe when nothing else writes concurrently.
  IndexManager({
    required this._storage,
    required this._decodeDocument,
    ExclusiveRunner? exclusive,
  }) : _exclusive = exclusive ?? _unguarded;

  static const int _backfillBatchSize = 500;

  static Future<T> _unguarded<T>(Future<T> Function() action) => action();

  final StorageEngine _storage;
  final DocumentDecoder _decodeDocument;
  final ExclusiveRunner _exclusive;

  /// Loaded definitions keyed by `collection NUL fieldSpec`.
  final Map<String, IndexDefinition> _indexes = {};

  static String _key(String collection, String fieldSpec) =>
      '$collection\u0000$fieldSpec';

  /// Loads index definitions persisted in storage metadata keys.
  Future<void> loadIndexes() async {
    final Uint8List? end = IndexKeyCodec.prefixUpperBound(indexMetaPrefix);
    await for (final KeyValue pair in _storage.scan(
      startKey: indexMetaPrefix,
      endKey: end,
    )) {
      final Uint8List tail = Uint8List.sublistView(
        pair.key,
        indexMetaPrefix.length,
      );
      final int split = tail.indexOf(0x00);
      if (split < 0) {
        throw CorruptionException('Malformed index metadata key', path: null);
      }
      final String collection = utf8.decode(
        Uint8List.sublistView(tail, 0, split),
      );
      final String fieldSpec = utf8.decode(
        Uint8List.sublistView(tail, split + 1),
      );
      final IndexDefinition definition = IndexDefinition(
        collection: collection,
        fields: fieldSpec.split(fieldSpecSeparator),
      );
      _indexes[_key(collection, fieldSpec)] = definition;
    }
  }

  /// Creates a single-field index on [field] of [collection] and backfills
  /// it from every existing document.
  Future<void> createIndex(String collection, String field) =>
      createCompoundIndex(collection, [field]);

  /// Creates a compound index over [fields] of [collection], in significance
  /// order, and backfills it from every existing document.
  ///
  /// Throws [QueryException] when the index already exists or a name is
  /// invalid. Dotted paths address nested fields.
  Future<void> createCompoundIndex(
    String collection,
    List<String> fields,
  ) async {
    validateCollectionName(collection);
    final IndexDefinition definition = IndexDefinition(
      collection: collection,
      fields: fields,
    );
    final String mapKey = _key(collection, definition.fieldSpec);

    // Register under the pipeline lock so every write from now on maintains
    // the index, and clear postings a crashed dropIndex may have left.
    await _exclusive(() async {
      if (_indexes.containsKey(mapKey)) {
        throw QueryException('Index ${definition.name} already exists');
      }
      _indexes[mapKey] = definition;
    });
    try {
      await _deletePostings(definition);
      await _backfill(definition);
      await _exclusive(
        () => _storage.put(
          IndexKeyCodec.metaKey(definition),
          utf8.encode(jsonEncode({'fields': fields})),
        ),
      );
    } catch (_) {
      await _exclusive(() async => _indexes.remove(mapKey));
      rethrow;
    }
  }

  /// Drops the index over [fields] of [collection] and deletes all of its
  /// postings and metadata.
  ///
  /// The metadata key goes first, in the same step that stops maintenance:
  /// a crash midway can leave orphaned postings (ignored, and cleared by a
  /// later [createCompoundIndex]) but never an index that reloads with
  /// postings missing.
  Future<void> dropIndex(String collection, List<String> fields) async {
    final IndexDefinition definition = IndexDefinition(
      collection: collection,
      fields: fields,
    );
    final String mapKey = _key(collection, definition.fieldSpec);
    await _exclusive(() async {
      if (!_indexes.containsKey(mapKey)) {
        throw QueryException('Index ${definition.name} does not exist');
      }
      await _storage.delete(IndexKeyCodec.metaKey(definition));
      _indexes.remove(mapKey);
    });
    await _deletePostings(definition);
  }

  /// Deletes every posting of [definition], in chunks.
  Future<void> _deletePostings(IndexDefinition definition) async {
    final Uint8List head = IndexKeyCodec.entryHead(definition);
    final Uint8List? end = IndexKeyCodec.prefixUpperBound(head);
    List<WriteOp> batch = [];
    await for (final KeyValue pair in _storage.scan(
      startKey: head,
      endKey: end,
    )) {
      batch.add(WriteOp.delete(pair.key));
      if (batch.length >= _backfillBatchSize) {
        final List<WriteOp> chunk = batch;
        await _exclusive(() => _storage.writeBatch(chunk));
        batch = [];
      }
    }
    if (batch.isNotEmpty) {
      await _exclusive(() => _storage.writeBatch(batch));
    }
  }

  /// Whether an index exists whose first field is [field].
  bool hasIndex(String collection, String field) => _indexes.values.any(
    (IndexDefinition d) =>
        d.collection == collection && d.fields.first == field,
  );

  /// All loaded index definitions, optionally restricted to [collection].
  List<IndexDefinition> listIndexes([String? collection]) => _indexes.values
      .where(
        (IndexDefinition d) => collection == null || d.collection == collection,
      )
      .toList();

  /// Builds the index mutations for one document write.
  ///
  /// Called INSIDE the write pipeline with the previous version of the
  /// document ([oldDoc], null when the document is new) and the new version
  /// ([newDoc], null when the document is being deleted). The returned
  /// operations MUST be committed in the same [StorageEngine.writeBatch] as
  /// the document itself so a crash can never leave a document invisible to
  /// indexed queries.
  ///
  /// Null or missing indexed fields are indexed under a null marker, so
  /// `whereEquals(field, null)` is servable. Old postings are always removed
  /// when the encoded values change, including in-place mutations of lists
  /// and maps (change detection compares encodings, not object identity).
  Future<List<WriteOp>> buildIndexOps({
    required String collection,
    required String docId,
    Map<String, dynamic>? oldDoc,
    Map<String, dynamic>? newDoc,
  }) async {
    final List<WriteOp> ops = [];
    for (final IndexDefinition definition in _indexes.values) {
      if (definition.collection != collection) continue;

      Uint8List? oldKey;
      if (oldDoc != null) {
        oldKey = IndexKeyCodec.entryKey(definition, [
          for (final String f in definition.fields)
            extractFieldValue(oldDoc, f),
        ], docId);
      }
      Uint8List? newKey;
      if (newDoc != null) {
        newKey = IndexKeyCodec.entryKey(definition, [
          for (final String f in definition.fields)
            extractFieldValue(newDoc, f),
        ], docId);
      }

      if (oldKey != null &&
          newKey != null &&
          compareBytes(oldKey, newKey) == 0) {
        continue; // Indexed values unchanged.
      }
      if (oldKey != null) ops.add(WriteOp.delete(oldKey));
      if (newKey != null) ops.add(WriteOp.put(newKey, Uint8List(0)));
    }
    return ops;
  }

  /// Returns the ids of documents that satisfy the index-servable subset of
  /// [conditions], or null when NO index can serve any of them (the caller
  /// must fall back to a collection scan).
  ///
  /// An empty set is a definitive answer: an index was consulted and no
  /// document matches. The caller must still re-check every condition on the
  /// loaded documents, because an index may serve only a subset of them.
  Future<Set<String>?> candidateIds(
    String collection,
    List<QueryCondition> conditions,
  ) async {
    final Stream<List<String>>? chunks = candidateIdChunks(
      collection,
      conditions,
    );
    if (chunks == null) return null;
    final Set<String> ids = <String>{};
    await for (final List<String> chunk in chunks) {
      ids.addAll(chunk);
    }
    return ids;
  }

  /// [candidateIds] as a lazy stream of chunks of at most [chunkSize] ids,
  /// in index order and without duplicates, so a caller that needs only the
  /// first few matches stops the index scan early by cancelling. Chunked
  /// because a stream event per id costs more than the lookup it feeds.
  /// Null when no index can serve any of [conditions].
  Stream<List<String>>? candidateIdChunks(
    String collection,
    List<QueryCondition> conditions, {
    int chunkSize = 256,
  }) {
    if (conditions.isEmpty) return null;

    _IndexPlan? best;
    for (final IndexDefinition definition in _indexes.values) {
      if (definition.collection != collection) continue;
      final _IndexPlan? plan = _plan(definition, conditions);
      if (plan != null && (best == null || plan.consumed > best.consumed)) {
        best = plan;
      }
    }
    if (best == null) return null;
    return _execute(best, chunkSize);
  }

  /// Releases in-memory index state.
  Future<void> close() async {
    _indexes.clear();
  }

  // --- internals ---------------------------------------------------------

  /// Builds postings for every existing document of the collection.
  ///
  /// The scan only discovers keys. Each chunk is then re-read and written
  /// under the pipeline lock, so a document changed or deleted while the
  /// backfill runs is indexed by its CURRENT version (or not at all), never
  /// by the stale one the scan saw.
  Future<void> _backfill(IndexDefinition definition) async {
    final Uint8List start = utf8.encode('${definition.collection}:');
    final Uint8List? end = IndexKeyCodec.prefixUpperBound(start);
    List<Uint8List> keys = [];

    Future<void> indexChunk(List<Uint8List> chunk) => _exclusive(() async {
      final List<WriteOp> batch = [];
      for (final Uint8List key in chunk) {
        final Uint8List? stored = await _storage.get(key);
        if (stored == null) continue;
        final Map<String, dynamic>? document = _decodeDocument(key, stored);
        if (document == null) continue;
        final String docId = utf8.decode(
          Uint8List.sublistView(key, start.length),
        );
        batch.add(
          WriteOp.put(
            IndexKeyCodec.entryKey(definition, [
              for (final String f in definition.fields)
                extractFieldValue(document, f),
            ], docId),
            Uint8List(0),
          ),
        );
      }
      if (batch.isNotEmpty) await _storage.writeBatch(batch);
    });

    await for (final KeyValue pair in _storage.scan(
      startKey: start,
      endKey: end,
    )) {
      keys.add(pair.key);
      if (keys.length >= _backfillBatchSize) {
        await indexChunk(keys);
        keys = [];
      }
    }
    if (keys.isNotEmpty) await indexChunk(keys);
  }

  _IndexPlan? _plan(
    IndexDefinition definition,
    List<QueryCondition> conditions,
  ) {
    final List<dynamic> eqValues = [];
    int consumed = 0;
    List<dynamic>? inValues;
    _RangeBounds? range;

    for (final String field in definition.fields) {
      final List<QueryCondition> onField = conditions
          .where((QueryCondition c) => c.field == field)
          .toList();
      if (onField.isEmpty) break;

      QueryCondition? eq;
      for (final QueryCondition c in onField) {
        if (c.operator == QueryOperator.equals) {
          eq = c;
          break;
        }
      }
      if (eq != null) {
        eqValues.add(eq.value);
        consumed++;
        continue;
      }

      // Try a single inList condition.
      QueryCondition? inCond;
      for (final QueryCondition c in onField) {
        if (c.operator == QueryOperator.inList) {
          inCond = c;
          break;
        }
      }
      if (inCond != null) {
        final dynamic values = inCond.value;
        if (values is! List) {
          throw const QueryException('whereIn expects a List value');
        }
        inValues = values;
        consumed++;
      } else {
        // Merge every range-style condition on this field.
        final _RangeBounds bounds = _RangeBounds();
        bool any = false;
        for (final QueryCondition c in onField) {
          bool used = true;
          switch (c.operator) {
            case QueryOperator.greaterThan:
              bounds.tightenLower(c.value, inclusive: false);
            case QueryOperator.greaterThanOrEqual:
              bounds.tightenLower(c.value, inclusive: true);
            case QueryOperator.lessThan:
              bounds.tightenUpper(c.value, inclusive: false);
            case QueryOperator.lessThanOrEqual:
              bounds.tightenUpper(c.value, inclusive: true);
            case QueryOperator.between:
              final dynamic v = c.value;
              if (v is! List || v.length != 2) {
                throw const QueryException(
                  'whereBetween expects a [start, end] pair',
                );
              }
              bounds.tightenLower(v[0], inclusive: true);
              bounds.tightenUpper(v[1], inclusive: true);
            case QueryOperator.equals:
            case QueryOperator.notEquals:
            case QueryOperator.inList:
            case QueryOperator.contains:
              used = false;
          }
          if (used) {
            any = true;
            consumed++;
          }
        }
        if (!any) break;
        range = bounds;
      }
      break; // Only the component after the equality prefix can be non-equal.
    }

    if (consumed == 0) return null;
    return _IndexPlan(
      definition: definition,
      eqValues: eqValues,
      inValues: inValues,
      range: range,
      consumed: consumed,
    );
  }

  Stream<List<String>> _execute(_IndexPlan plan, int chunkSize) async* {
    final IndexDefinition definition = plan.definition;
    final Uint8List head = IndexKeyCodec.entryHead(definition);

    Uint8List base(List<dynamic> values) {
      final BytesBuilder out = BytesBuilder(copy: false);
      out.add(head);
      for (final dynamic value in values) {
        IndexValueCodec.writeValue(out, value);
      }
      return out.toBytes();
    }

    final Set<String> seen = <String>{};

    Stream<List<String>> scanRange(
      Uint8List startKey,
      Uint8List? endKey,
    ) async* {
      List<String> chunk = <String>[];
      await for (final KeyValue pair in _storage.scan(
        startKey: startKey,
        endKey: endKey,
      )) {
        final String id = IndexKeyCodec.docIdFromEntryKey(definition, pair.key);
        if (!seen.add(id)) continue;
        chunk.add(id);
        if (chunk.length >= chunkSize) {
          yield chunk;
          chunk = <String>[];
        }
      }
      if (chunk.isNotEmpty) yield chunk;
    }

    // All postings whose leading components equal [values] exactly.
    Stream<List<String>> scanEquality(List<dynamic> values) {
      final Uint8List b = base(values);
      if (values.length == definition.fields.length) {
        // Next byte is the doc id tag 0x01.
        return scanRange(
          IndexKeyCodec.withSuffix(b, IndexValueCodec.docIdTag),
          IndexKeyCodec.withSuffix(b, IndexValueCodec.docIdTag + 1),
        );
      }
      // Next byte is a value tag; excludes 0xFF string continuations.
      return scanRange(
        IndexKeyCodec.withSuffix(b, IndexValueCodec.tagNull),
        IndexKeyCodec.withSuffix(b, IndexValueCodec.valueTagUpperBound),
      );
    }

    if (plan.inValues != null) {
      for (final dynamic value in plan.inValues!) {
        yield* scanEquality([...plan.eqValues, value]);
      }
      return;
    }

    final _RangeBounds? range = plan.range;
    if (range == null) {
      yield* scanEquality(plan.eqValues);
      return;
    }

    final Uint8List b = base(plan.eqValues);
    Uint8List startKey;
    if (range.lower == _RangeBounds.unset) {
      startKey = b;
    } else {
      final Uint8List withValue = base([...plan.eqValues, range.lower]);
      // An inexact bound (an integer beyond 2^53) shares its encoding with
      // neighbours, so it is always scanned inclusively; the exact re-filter
      // of loaded documents applies the real bound.
      startKey = range.lowerInclusive || !isExactlyEncodable(range.lower)
          ? withValue
          : IndexKeyCodec.withSuffix(
              withValue,
              IndexValueCodec.boundarySentinel,
            );
    }
    Uint8List? endKey;
    if (range.upper == _RangeBounds.unset) {
      endKey = IndexKeyCodec.withSuffix(b, IndexValueCodec.boundarySentinel);
    } else {
      final Uint8List withValue = base([...plan.eqValues, range.upper]);
      endKey = range.upperInclusive || !isExactlyEncodable(range.upper)
          ? IndexKeyCodec.withSuffix(
              withValue,
              IndexValueCodec.boundarySentinel,
            )
          : withValue;
    }
    yield* scanRange(startKey, endKey);
  }
}

final class _IndexPlan {
  _IndexPlan({
    required this.definition,
    required this.eqValues,
    required this.inValues,
    required this.range,
    required this.consumed,
  });

  final IndexDefinition definition;
  final List<dynamic> eqValues;
  final List<dynamic>? inValues;
  final _RangeBounds? range;
  final int consumed;
}

final class _RangeBounds {
  /// Sentinel meaning "no bound" (null is a legal bound value).
  static const Object unset = _Unset();

  dynamic lower = unset;
  bool lowerInclusive = true;
  dynamic upper = unset;
  bool upperInclusive = true;

  void tightenLower(dynamic value, {required bool inclusive}) {
    if (lower == unset || compareValues(value, lower) > 0) {
      lower = value;
      lowerInclusive = inclusive;
    } else if (compareValues(value, lower) == 0 && !inclusive) {
      lowerInclusive = false;
    }
  }

  void tightenUpper(dynamic value, {required bool inclusive}) {
    if (upper == unset || compareValues(value, upper) < 0) {
      upper = value;
      upperInclusive = inclusive;
    } else if (compareValues(value, upper) == 0 && !inclusive) {
      upperInclusive = false;
    }
  }
}

final class _Unset {
  const _Unset();
}
