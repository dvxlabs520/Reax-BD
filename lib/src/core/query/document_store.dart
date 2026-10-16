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
  const DocumentRecord(this.id, this.document);

  /// The id component of the storage key (`collection:id`).
  final String id;

  /// The decoded document.
  final Map<String, dynamic> document;
}

/// Read/write access to the documents of named collections.
///
/// Implementations must back [scanDocuments] and [scanDocumentIds] with
/// `StorageEngine.scan` over the collection key prefix, so every document is
/// visible regardless of its id shape (string ids, large numeric ids, UUIDs).
abstract interface class DocumentStore {
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
}
