/// Typed collections without code generation.
library;

import 'dart:async';

import 'core/query/query_builder.dart';
import 'domain/entities/database_entity.dart';
import 'reaxdb.dart';

/// A change to a document of a typed collection.
final class CollectionChange<T> {
  /// Creates a collection change.
  const CollectionChange({
    required this.type,
    required this.id,
    required this.value,
    required this.timestamp,
  });

  /// Whether the document was written or removed.
  final ChangeType type;

  /// Document id within the collection.
  final String id;

  /// The new document for a put, null for a delete.
  final T? value;

  /// When the change was published.
  final DateTime timestamp;

  @override
  String toString() =>
      'CollectionChange(${type.name}, id: $id, at: '
      '${timestamp.toIso8601String()})';
}

/// A typed view over one collection of documents.
///
/// Obtain instances from [ReaxDB.collection]. There is no code generation:
/// the collection converts with the [fromJson] and [toJson] functions you
/// pass in. Documents are stored under the ordinary `collection:id` key
/// shape, so a typed collection and a raw `db.put('users:1', {...})` address
/// the same data and share secondary indexes and change streams.
///
/// Every write goes through the database's single write pipeline.
final class ReaxCollection<T> {
  /// Creates a typed view of [name] backed by [db].
  const ReaxCollection({
    required this.name,
    required this._db,
    required this.fromJson,
    required this.toJson,
  });

  /// The collection name, used as the key prefix.
  final String name;

  /// Converts a stored document into a [T].
  final T Function(Map<String, dynamic> json) fromJson;

  /// Converts a [T] into a storable document.
  final Map<String, dynamic> Function(T value) toJson;

  final ReaxDB _db;

  String _key(String id) => '$name:$id';

  /// Stores [value] under [id].
  Future<void> put(String id, T value, {Duration? ttl, DateTime? expiresAt}) =>
      _db.put(_key(id), toJson(value), ttl: ttl, expiresAt: expiresAt);

  /// Stores every entry of [values] in one atomic batch.
  Future<void> putAll(
    Map<String, T> values, {
    Duration? ttl,
    DateTime? expiresAt,
  }) => _db.putBatch(
    <String, Object?>{
      for (final MapEntry<String, T> e in values.entries)
        _key(e.key): toJson(e.value),
    },
    ttl: ttl,
    expiresAt: expiresAt,
  );

  /// Reads the document [id], or null when absent or expired.
  Future<T?> get(String id) async {
    final Map<String, dynamic>? json = await _db.get<Map<String, dynamic>>(
      _key(id),
    );
    return json == null ? null : fromJson(json);
  }

  /// Removes the document [id].
  Future<void> delete(String id) => _db.delete(_key(id));

  /// Whether a live document exists under [id].
  Future<bool> exists(String id) => _db.exists(_key(id));

  /// Streams every document of this collection in key order.
  Stream<T> all() async* {
    await for (final ReaxEntry<Map<String, dynamic>> entry
        in _db.scanPrefix<Map<String, dynamic>>('$name:')) {
      yield fromJson(entry.value);
    }
  }

  /// Returns the documents whose [field] satisfies [operator] against
  /// [value], using a secondary index when one can serve the condition.
  Future<List<T>> where(
    String field,
    QueryOperator operator,
    Object? value,
  ) async => <T>[
    for (final Map<String, dynamic> json
        in await _db.query(name).where(field, operator, value).find())
      fromJson(json),
  ];

  /// Returns the documents matching a query built by [build].
  ///
  /// ```dart
  /// final adults = await users.find((q) => q
  ///     .whereGreaterThanOrEqual('age', 18)
  ///     .orderBy('name')
  ///     .limit(20));
  /// ```
  Future<List<T>> find([void Function(QueryBuilder query)? build]) async {
    final QueryBuilder query = _db.query(name);
    build?.call(query);
    return <T>[
      for (final Map<String, dynamic> json in await query.find())
        fromJson(json),
    ];
  }

  /// Returns a raw query builder over this collection.
  QueryBuilder query() => _db.query(name);

  /// Creates a secondary index over [fields] of this collection.
  Future<void> createIndex(List<String> fields) =>
      _db.createIndex(name, fields);

  /// Counts the documents of this collection.
  Future<int> count() => _db.query(name).count();

  /// Streams changes to this collection, decoded to [T].
  ///
  /// Pass [id] to watch a single document.
  Stream<CollectionChange<T>> watch({String? id}) {
    final Stream<DatabaseChangeEvent> source = id == null
        ? _db.watchCollection(name)
        : _db.watchKey(_key(id));
    return source.map((DatabaseChangeEvent event) {
      final Object? value = event.value;
      return CollectionChange<T>(
        type: event.type,
        id: event.key.substring(name.length + 1),
        value: value is Map<String, dynamic> ? fromJson(value) : null,
        timestamp: event.timestamp,
      );
    });
  }
}
