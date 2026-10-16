/// Shared in-memory test doubles for the query/index/cache/stream suites.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:reaxdb_dart/src/core/indexing/index_manager.dart';
import 'package:reaxdb_dart/src/core/indexing/secondary_index.dart';
import 'package:reaxdb_dart/src/core/query/document_store.dart';
import 'package:reaxdb_dart/src/core/storage/storage_engine.dart';

/// Byte-ordered in-memory [StorageEngine] used to test the layers above
/// storage against the real scan contract (ordered, bounded, atomic batch).
final class InMemoryStorageEngine implements StorageEngine {
  final List<Uint8List> _keys = [];
  final Map<String, Uint8List> _values = {};

  static String _hex(Uint8List bytes) =>
      bytes.map((int b) => b.toRadixString(16).padLeft(2, '0')).join();

  int _lowerBound(Uint8List key) {
    int low = 0;
    int high = _keys.length;
    while (low < high) {
      final int mid = (low + high) >> 1;
      if (compareBytes(_keys[mid], key) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  void _apply(WriteOp op) {
    final String hexKey = _hex(op.key);
    final int at = _lowerBound(op.key);
    final bool exists =
        at < _keys.length && compareBytes(_keys[at], op.key) == 0;
    if (op.isDelete) {
      if (exists) {
        _keys.removeAt(at);
        _values.remove(hexKey);
      }
    } else {
      if (!exists) {
        _keys.insert(at, Uint8List.fromList(op.key));
      }
      _values[hexKey] = Uint8List.fromList(op.value!);
    }
  }

  @override
  Future<void> put(Uint8List key, Uint8List value) async {
    _apply(WriteOp.put(key, value));
  }

  @override
  Future<Uint8List?> get(Uint8List key) async => _values[_hex(key)];

  @override
  Future<void> delete(Uint8List key) async {
    _apply(WriteOp.delete(key));
  }

  @override
  Future<void> writeBatch(List<WriteOp> ops, {int? transactionId}) async {
    for (final WriteOp op in ops) {
      _apply(op);
    }
  }

  @override
  Stream<KeyValue> scan({
    Uint8List? startKey,
    Uint8List? endKey,
    int? limit,
    bool reverse = false,
  }) async* {
    // Snapshot so concurrent writes during iteration cannot corrupt it.
    final List<Uint8List> snapshot = [
      for (final Uint8List key in _keys)
        if ((startKey == null || compareBytes(key, startKey) >= 0) &&
            (endKey == null || compareBytes(key, endKey) < 0))
          key,
    ];
    final Iterable<Uint8List> ordered = reverse ? snapshot.reversed : snapshot;
    int emitted = 0;
    for (final Uint8List key in ordered) {
      if (limit != null && emitted >= limit) break;
      final Uint8List? value = _values[_hex(key)];
      if (value == null) continue;
      emitted++;
      yield KeyValue(key, value);
    }
  }

  @override
  Future<void> flush() async {}

  @override
  Future<void> compact() async {}

  @override
  Future<void> close() async {}

  @override
  StorageStats get stats => StorageStats(
    memtableEntries: _keys.length,
    memtableSizeBytes: 0,
    immutableMemtableCount: 0,
    sstableCount: 0,
    sstableSizeBytes: 0,
    levelTableCounts: const [],
    lastSequenceNumber: 0,
  );
}

/// Decodes stored JSON document bytes, returning null for non-documents.
Map<String, dynamic>? decodeJsonDocument(Uint8List bytes) {
  try {
    final dynamic decoded = jsonDecode(utf8.decode(bytes));
    return decoded is Map<String, dynamic> ? decoded : null;
  } on FormatException {
    return null;
  }
}

/// [DocumentStore] that mimics the facade's single write pipeline: document
/// write and index maintenance commit in ONE [StorageEngine.writeBatch].
final class PipelineDocumentStore implements DocumentStore {
  /// Creates a store over [engine] maintaining [indexManager]'s indexes.
  PipelineDocumentStore(this.engine, this.indexManager);

  /// The backing engine.
  final InMemoryStorageEngine engine;

  @override
  bool get sharesDocuments => false;

  @override
  bool visitDocumentsNow(
    String collection,
    bool Function(DocumentRecord record) visit,
  ) => false;

  @override
  List<DocumentRecord>? getDocumentsNow(String collection, List<String> ids) =>
      null;

  /// The index manager whose ops join every document batch.
  final IndexManager indexManager;

  static Uint8List _docKey(String collection, String id) =>
      utf8.encode('$collection:$id');

  @override
  Future<Map<String, dynamic>?> getDocument(
    String collection,
    String id,
  ) async {
    final Uint8List? bytes = await engine.get(_docKey(collection, id));
    return bytes == null ? null : decodeJsonDocument(bytes);
  }

  @override
  Future<List<DocumentRecord>> getDocuments(
    String collection,
    List<String> ids,
  ) async {
    final List<DocumentRecord> found = <DocumentRecord>[];
    for (final String id in ids) {
      final Map<String, dynamic>? document = await getDocument(collection, id);
      if (document != null) found.add(DocumentRecord(id, document));
    }
    return found;
  }

  @override
  Future<void> putDocument(
    String collection,
    String id,
    Map<String, dynamic> document,
  ) async {
    final Map<String, dynamic>? oldDoc = await getDocument(collection, id);
    final List<WriteOp> ops = [
      WriteOp.put(_docKey(collection, id), utf8.encode(jsonEncode(document))),
      ...await indexManager.buildIndexOps(
        collection: collection,
        docId: id,
        oldDoc: oldDoc,
        newDoc: document,
      ),
    ];
    await engine.writeBatch(ops);
  }

  @override
  Future<void> deleteDocument(String collection, String id) async {
    final Map<String, dynamic>? oldDoc = await getDocument(collection, id);
    final List<WriteOp> ops = [
      WriteOp.delete(_docKey(collection, id)),
      ...await indexManager.buildIndexOps(
        collection: collection,
        docId: id,
        oldDoc: oldDoc,
        newDoc: null,
      ),
    ];
    await engine.writeBatch(ops);
  }

  @override
  Future<void> putDocuments(
    String collection,
    Map<String, Map<String, dynamic>> documents,
  ) async {
    for (final MapEntry<String, Map<String, dynamic>> e in documents.entries) {
      await putDocument(collection, e.key, e.value);
    }
  }

  @override
  Future<void> deleteDocuments(String collection, List<String> ids) async {
    for (final String id in ids) {
      await deleteDocument(collection, id);
    }
  }

  @override
  Stream<DocumentRecord> scanDocuments(String collection) async* {
    final Uint8List start = utf8.encode('$collection:');
    final Uint8List? end = IndexKeyCodec.prefixUpperBound(start);
    await for (final KeyValue pair in engine.scan(
      startKey: start,
      endKey: end,
    )) {
      final Map<String, dynamic>? document = decodeJsonDocument(pair.value);
      if (document == null) continue;
      yield DocumentRecord(
        utf8.decode(Uint8List.sublistView(pair.key, start.length)),
        document,
      );
    }
  }

  @override
  Stream<String> scanDocumentIds(String collection) =>
      scanDocuments(collection).map((DocumentRecord r) => r.id);
}

/// Builds a connected engine + index manager + pipeline store trio.
({
  InMemoryStorageEngine engine,
  IndexManager indexManager,
  PipelineDocumentStore store,
})
buildHarness() {
  final InMemoryStorageEngine engine = InMemoryStorageEngine();
  final IndexManager indexManager = IndexManager(
    storage: engine,
    decodeDocument: (Uint8List _, Uint8List bytes) => decodeJsonDocument(bytes),
  );
  final PipelineDocumentStore store = PipelineDocumentStore(
    engine,
    indexManager,
  );
  return (engine: engine, indexManager: indexManager, store: store);
}
