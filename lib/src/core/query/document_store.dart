/// Document access abstraction used by the query layer.
///
/// [QueryBuilder] never talks to the storage engine or the database facade
/// directly; it depends on this narrow interface. The facade implements it on
/// top of the single write pipeline, so query-driven updates and deletes flow
/// through WAL, storage, secondary indexes, cache invalidation and change
/// events exactly like any other mutation.
library;

/// A document together with the id it was loaded under.
///
/// Query mutations (`update`, `delete`) always address documents by this id,
/// never by an id reconstructed from document fields, so documents without an
/// `id` field are updated in place instead of being duplicated.
final class DocumentRecord {
  /// Creates a record for [document] stored under [id].
  DocumentRecord(String id, this.document) : _id = id, _key = id, _offset = 0;

  /// Creates a record for [document] stored under [key], whose id starts at
  /// [offset]. The id is cut out only if someone asks for it.
  DocumentRecord.fromKey(String key, int offset, this.document)
    : _key = key,
      _offset = offset;

  final String _key;
  final int _offset;
  String? _id;

  /// The id component of the storage key (`collection:id`).
  String get id => _id ??= _key.substring(_offset);

  /// The decoded document.
  final Map<String, dynamic> document;
}

/// Read/write access to the documents of named collections.
///
/// Implementations must back [scanDocuments] and [scanDocumentIds] with
/// `StorageEngine.scan` over the collection key prefix, so every document is
/// visible regardless of its id shape (string ids, large numeric ids, UUIDs).
abstract interface class DocumentStore {
  /// Whether the documents this store yields are the stored objects
  /// themselves rather than fresh copies. The query layer then copies
  /// whatever it hands to a caller, and never changes what it reads.
  bool get sharesDocuments;

  /// Calls [visit] with every live document of [collection] in key order,
  /// synchronously, until it returns false. Returns false, without calling
  /// [visit], when this store can only stream them ([scanDocuments]).
  ///
  /// [visit] must not await: the walk sees the store as it is right now.
  bool visitDocumentsNow(
    String collection,
    bool Function(DocumentRecord record) visit,
  );

  /// The live documents of [ids], synchronously, or null when this store
  /// can only load them asynchronously ([getDocuments]).
  List<DocumentRecord>? getDocumentsNow(String collection, List<String> ids);

  /// Streams every document of [collection] in storage key order.
  Stream<DocumentRecord> scanDocuments(String collection);

  /// Streams every document id of [collection] without decoding documents.
  Stream<String> scanDocumentIds(String collection);

  /// Loads one document, or null when absent.
  Future<Map<String, dynamic>?> getDocument(String collection, String id);

  /// Loads the documents of [ids] that exist, in the order given.
  ///
  /// Batched so an implementation can amortise per-lookup work.
  Future<List<DocumentRecord>> getDocuments(
    String collection,
    List<String> ids,
  );

  /// Writes [document] under [id] through the write pipeline.
  Future<void> putDocument(
    String collection,
    String id,
    Map<String, dynamic> document,
  );

  /// Deletes the document under [id] through the write pipeline.
  Future<void> deleteDocument(String collection, String id);

  /// Writes every document of [documents], keyed by id, in one atomic
  /// batch through the write pipeline.
  Future<void> putDocuments(
    String collection,
    Map<String, Map<String, dynamic>> documents,
  );

  /// Deletes the documents of [ids] in one atomic batch through the write
  /// pipeline.
  Future<void> deleteDocuments(String collection, List<String> ids);
}
