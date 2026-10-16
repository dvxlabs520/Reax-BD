/// Index definitions and the order-preserving value codec.
///
/// Secondary index entries live in the same [StorageEngine] key space as
/// documents, under a reserved prefix that begins with a 0x00 byte so it can
/// never collide with document keys (which are UTF-8 strings of the form
/// `collection:id` and therefore never start with 0x00).
///
/// One storage key exists per `(index, field values, document id)` posting, so
/// inserting or removing a posting is O(1) and lookups are range scans. The
/// stored value of a posting is empty; all information lives in the key.
///
/// ## Entry key layout
///
/// ```text
/// 0x00 'i' 'd' 'x' 0x00        reserved entry prefix
/// utf8(collection) 0x00        collection name (0x00 forbidden in names)
/// utf8(fieldSpec)  0x00        fields joined by 0x1F (unit separator)
/// enc(v1) .. enc(vN)           one self-delimiting encoded value per field
/// 0x01 escaped(docId) 0x00     document id component (tag 0x01)
/// ```
///
/// ## Total value ordering
///
/// Values of any type compare under one documented total order:
/// `null < false < true < numbers < strings < lists < maps < other`.
/// Numbers form a single space: `int` and `double` with the same numeric
/// value encode identically, so `whereEquals(field, 5)` matches `5.0`, and
/// `-0.0` is the same value as `0`.
///
/// The byte encoding stores numbers as doubles, so integers with magnitude
/// above 2^53 can share an encoding with their neighbours. [compareValues]
/// therefore compares numbers EXACTLY (2^53 + 1 is greater than 2^53), and
/// the index planner widens any bound that is not exactly representable, so
/// the index only ever returns a superset of the matches and the exact
/// re-filter of loaded documents decides. Strings order by UTF-8 bytes;
/// lists compare element-wise; maps compare by sorted key/value pairs; any
/// other type compares by its `toString()`. Apart from that refinement of
/// large integers, [compareValues] is the memcmp of the encodings, so the
/// index path and the scan path agree.
library;

import 'dart:convert';
import 'dart:typed_data';

import '../errors/exceptions.dart';
import '../util/code_point_order.dart';

/// Reserved key prefix for index entry keys: `0x00 i d x 0x00`.
final Uint8List indexEntryPrefix = Uint8List.fromList(const [
  0x00,
  0x69,
  0x64,
  0x78,
  0x00,
]);

/// Reserved key prefix for index metadata keys: `0x00 i d x m 0x00`.
///
/// Differs from [indexEntryPrefix] at byte 4 (0x6D vs 0x00), so the two key
/// families can never overlap.
final Uint8List indexMetaPrefix = Uint8List.fromList(const [
  0x00,
  0x69,
  0x64,
  0x78,
  0x6D,
  0x00,
]);

/// Separator used between field names inside an index field spec.
const String fieldSpecSeparator = '\u001F';

/// Immutable description of a (possibly compound) secondary index.
final class IndexDefinition {
  /// Creates an index definition over [fields] of [collection].
  ///
  /// Throws [QueryException] when names contain reserved characters or the
  /// field list is empty.
  IndexDefinition({required this.collection, required List<String> fields})
    : fields = List.unmodifiable(fields) {
    if (fields.isEmpty) {
      throw const QueryException('An index needs at least one field');
    }
    _validateName(collection, 'collection');
    for (final String field in fields) {
      _validateName(field, 'field');
    }
  }

  /// Collection the index belongs to.
  final String collection;

  /// Indexed fields, in significance order. Dotted paths address nested
  /// fields (for example `address.city`).
  final List<String> fields;

  /// Canonical single-string form of [fields].
  String get fieldSpec => fields.join(fieldSpecSeparator);

  /// Human readable name, for example `users.age` or `users.city+age`.
  String get name => '$collection.${fields.join('+')}';

  static void _validateName(String value, String what) {
    if (value.isEmpty) {
      throw QueryException('Index $what name must not be empty');
    }
    if (value.contains('\u0000') || value.contains(fieldSpecSeparator)) {
      throw QueryException(
        'Index $what name "$value" contains a reserved character',
      );
    }
  }

  @override
  bool operator ==(Object other) =>
      other is IndexDefinition &&
      other.collection == collection &&
      other.fieldSpec == fieldSpec;

  @override
  int get hashCode => Object.hash(collection, fieldSpec);

  @override
  String toString() => 'IndexDefinition($name)';
}

/// Throws [QueryException] unless [collection] can name a collection.
///
/// Document keys have the shape `collection:id` and are split at the FIRST
/// `:`, so a collection name containing `:` would be read back as a
/// different, shorter collection: its documents would silently miss index
/// maintenance. Such names are rejected instead.
void validateCollectionName(String collection) {
  if (collection.isEmpty) {
    throw const QueryException('Collection name must not be empty');
  }
  if (collection.contains(':') || collection.contains('\u0000')) {
    throw QueryException(
      'Collection name "$collection" must not contain ":" or NUL: document '
      'keys are "collection:id" and are split at the first ":"',
    );
  }
}

/// Reads the value at dotted [path] from [document].
///
/// Returns null when any segment is missing or a non-map is traversed. Both
/// the index writer and the query scanner use this extractor, so nested
/// fields behave identically on either path.
dynamic extractFieldValue(Map<String, dynamic> document, String path) {
  if (!path.contains('.')) return document[path];
  dynamic value = document;
  for (final String part in path.split('.')) {
    if (value is Map<String, dynamic>) {
      value = value[part];
    } else if (value is Map) {
      value = value[part];
    } else {
      return null;
    }
  }
  return value;
}

/// Order-preserving binary encoding for index component values.
///
/// Encodings are self-delimiting and compare bytewise exactly as
/// [compareValues] compares the source values.
final class IndexValueCodec {
  IndexValueCodec._();

  /// Component tag for the document id trailer.
  static const int docIdTag = 0x01;

  /// Tag for null / missing values (they are indexed, so `field == null`
  /// queries can be served by an index).
  static const int tagNull = 0x05;

  /// Tag for boolean false.
  static const int tagFalse = 0x10;

  /// Tag for boolean true.
  static const int tagTrue = 0x11;

  /// Tag for numbers (int and double share one comparable space).
  static const int tagNumber = 0x20;

  /// Tag for UTF-8 strings.
  static const int tagString = 0x30;

  /// Tag for lists.
  static const int tagList = 0x40;

  /// Tag for maps.
  static const int tagMap = 0x50;

  /// Tag for any other type, encoded via `toString()`.
  static const int tagOther = 0x60;

  /// Exclusive upper bound over all value tags (used to build range ends
  /// that exclude longer-string continuations, which start with 0xFF).
  static const int valueTagUpperBound = 0x61;

  /// Sentinel byte greater than every component tag but below the 0xFF
  /// escape continuation, used for inclusive/exclusive range boundaries.
  static const int boundarySentinel = 0xFE;

  /// Encodes [value] into its order-preserving byte form.
  static Uint8List encodeValue(dynamic value) {
    final BytesBuilder out = BytesBuilder(copy: false);
    writeValue(out, value);
    return out.toBytes();
  }

  /// Appends the encoding of [value] to [out].
  static void writeValue(BytesBuilder out, dynamic value) {
    if (value == null) {
      out.addByte(tagNull);
    } else if (value is bool) {
      out.addByte(value ? tagTrue : tagFalse);
    } else if (value is num) {
      out.addByte(tagNumber);
      final double number = value.toDouble();
      // -0.0 and 0.0 are one value; NaN keeps its own (largest) slot.
      out.add(_encodeNumber(number == 0 ? 0.0 : number));
    } else if (value is String) {
      out.addByte(tagString);
      _writeEscaped(out, utf8.encode(value));
      out.addByte(0x00);
    } else if (value is List) {
      out.addByte(tagList);
      for (final dynamic element in value) {
        writeValue(out, element);
      }
      out.addByte(0x00);
    } else if (value is Map) {
      out.addByte(tagMap);
      final List<String> keys =
          value.keys.map((dynamic k) => k.toString()).toList()..sort();
      for (final String key in keys) {
        writeValue(out, key);
        writeValue(out, value[key]);
      }
      out.addByte(0x00);
    } else {
      out.addByte(tagOther);
      _writeEscaped(out, utf8.encode(value.toString()));
      out.addByte(0x00);
    }
  }

  /// Appends the document id trailer for [docId] to [out].
  static void writeDocId(BytesBuilder out, String docId) {
    out.addByte(docIdTag);
    _writeEscaped(out, utf8.encode(docId));
    out.addByte(0x00);
  }

  /// Returns the offset just past the encoded value starting at [offset].
  ///
  /// Throws [CorruptionException] when the bytes are not a valid encoding.
  static int skipValue(Uint8List bytes, int offset) {
    if (offset >= bytes.length) {
      throw const CorruptionException('Truncated index value encoding');
    }
    final int tag = bytes[offset];
    switch (tag) {
      case tagNull:
      case tagFalse:
      case tagTrue:
        return offset + 1;
      case tagNumber:
        if (offset + 9 > bytes.length) {
          throw const CorruptionException('Truncated index number encoding');
        }
        return offset + 9;
      case tagString:
      case tagOther:
        return _skipEscaped(bytes, offset + 1);
      case tagList:
        int cursor = offset + 1;
        while (true) {
          if (cursor >= bytes.length) {
            throw const CorruptionException('Truncated index list encoding');
          }
          if (bytes[cursor] == 0x00) {
            return cursor + 1;
          }
          cursor = skipValue(bytes, cursor);
        }
      case tagMap:
        int cursor = offset + 1;
        while (true) {
          if (cursor >= bytes.length) {
            throw const CorruptionException('Truncated index map encoding');
          }
          if (bytes[cursor] == 0x00) {
            return cursor + 1;
          }
          cursor = skipValue(bytes, cursor);
        }
      default:
        throw CorruptionException(
          'Unknown index value tag 0x'
          '${tag.toRadixString(16)}',
        );
    }
  }

  /// Decodes the document id trailer that starts at [offset] in [key].
  static String decodeDocIdAt(Uint8List key, int offset) {
    if (offset >= key.length || key[offset] != docIdTag) {
      throw const CorruptionException('Missing document id in index key');
    }
    final BytesBuilder out = BytesBuilder(copy: false);
    int cursor = offset + 1;
    while (true) {
      if (cursor >= key.length) {
        throw const CorruptionException('Truncated document id in index key');
      }
      final int byte = key[cursor];
      if (byte == 0x00) {
        if (cursor + 1 < key.length && key[cursor + 1] == 0xFF) {
          out.addByte(0x00);
          cursor += 2;
          continue;
        }
        return utf8.decode(out.toBytes());
      }
      out.addByte(byte);
      cursor++;
    }
  }

  /// Encodes a double so that its big-endian bytes sort in numeric order.
  ///
  /// Positive values flip the sign bit; negative values invert all bits
  /// (the standard total-order transform for IEEE-754).
  static Uint8List _encodeNumber(double value) {
    final ByteData data = ByteData(8)..setFloat64(0, value, Endian.big);
    if (data.getUint8(0) & 0x80 != 0) {
      for (int i = 0; i < 8; i++) {
        data.setUint8(i, ~data.getUint8(i) & 0xFF);
      }
    } else {
      data.setUint8(0, data.getUint8(0) ^ 0x80);
    }
    return data.buffer.asUint8List();
  }

  /// Writes [bytes] escaping 0x00 as `0x00 0xFF` (order preserving).
  static void _writeEscaped(BytesBuilder out, List<int> bytes) {
    for (final int byte in bytes) {
      if (byte == 0x00) {
        out.addByte(0x00);
        out.addByte(0xFF);
      } else {
        out.addByte(byte);
      }
    }
  }

  static int _skipEscaped(Uint8List bytes, int offset) {
    int cursor = offset;
    while (true) {
      if (cursor >= bytes.length) {
        throw const CorruptionException('Truncated escaped index bytes');
      }
      if (bytes[cursor] == 0x00) {
        if (cursor + 1 < bytes.length && bytes[cursor + 1] == 0xFF) {
          cursor += 2;
          continue;
        }
        return cursor + 1;
      }
      cursor++;
    }
  }
}

/// Compares [a] and [b] under the documented total order.
///
/// Defined as an unsigned bytewise comparison of the two encodings, so this
/// comparator and the on-disk index ordering can never diverge.
int compareValues(dynamic a, dynamic b) {
  if (identical(a, b)) return 0;
  // Scalars are compared directly, with the same result as the encodings
  // and without building them: queries compare once per document.
  final int? scalar = _compareScalars(a, b);
  if (scalar != null) return scalar;
  final int encoded = compareBytes(
    IndexValueCodec.encodeValue(a),
    IndexValueCodec.encodeValue(b),
  );
  if (encoded != 0) return encoded;
  // Equal encodings are equal values, except for integers beyond 2^53
  // that round to the same double; break those ties exactly, inside lists
  // and maps too.
  return _compareExactTies(a, b);
}

/// [compareValues] for null, bools, numbers and strings, or null for any
/// other value (and for NaN), which go through the encodings.
///
/// Mirrors [IndexValueCodec]: the type tags order null < bool < number <
/// string; numbers compare as doubles with exact ties broken like
/// [_compareNumbersExactly]; strings compare by code point, the order of
/// their escaped UTF-8 bytes.
int? _compareScalars(dynamic a, dynamic b) {
  final int rankA = _scalarRank(a);
  final int rankB = _scalarRank(b);
  if (rankA < 0 || rankB < 0) return null;
  if (rankA != rankB) return rankA - rankB;
  switch (rankA) {
    case 0:
      return 0;
    case 1:
      return (a as bool ? 1 : 0) - (b as bool ? 1 : 0);
    case 2:
      final num x = a as num;
      final num y = b as num;
      // Rounding to double is monotonic, so ints compare exactly as ints.
      if (x is int && y is int) return x.compareTo(y);
      final double dx = x.toDouble();
      final double dy = y.toDouble();
      if (dx.isNaN || dy.isNaN) return null;
      if (dx < dy) return -1;
      if (dx > dy) return 1;
      return _compareNumbersExactly(x, y);
    default:
      return compareCodePoints(a as String, b as String);
  }
}

int _scalarRank(dynamic value) {
  if (value == null) return 0;
  if (value is bool) return 1;
  if (value is num) return 2;
  if (value is String) return 3;
  return -1;
}

int _compareExactTies(dynamic a, dynamic b) {
  if (a is num && b is num) return _compareNumbersExactly(a, b);
  if (a is List && b is List) {
    for (int i = 0; i < a.length && i < b.length; i++) {
      final int c = _compareExactTies(a[i], b[i]);
      if (c != 0) return c;
    }
    return 0;
  }
  if (a is Map && b is Map) {
    final List<String> keys = a.keys.map((dynamic k) => k.toString()).toList()
      ..sort();
    for (final String key in keys) {
      final int c = _compareExactTies(a[key], b[key]);
      if (c != 0) return c;
    }
  }
  return 0;
}

/// Compares two numbers whose double encodings are equal, exactly.
int _compareNumbersExactly(num a, num b) {
  if (a is int && b is int) return a.compareTo(b);
  if (a is double && b is double) return 0;
  // One int, one double with the same rounded value: the double is
  // integral and finite, so the comparison is exact in BigInt.
  final double d = (a is double ? a : b) as double;
  if (!d.isFinite) return 0;
  final BigInt asBig = BigInt.from(d);
  final int c = a is int
      ? BigInt.from(a).compareTo(asBig)
      : asBig.compareTo(BigInt.from(b as int));
  return c;
}

/// Whether the index encoding of [value] is exact, so a bound on it can be
/// used as written. Integers of magnitude 2^53 and above are not: 2^53
/// itself is representable, but 2^53 + 1 encodes the same.
bool isExactlyEncodable(dynamic value) {
  if (value is int) return value.abs() < _maxExactInt;
  if (value is List) return value.every(isExactlyEncodable);
  if (value is Map) return value.values.every(isExactlyEncodable);
  return true;
}

const int _maxExactInt = 9007199254740992; // 2^53

/// A string that is equal for any two values [valuesEqual] considers equal,
/// for grouping them in a hash map.
///
/// Different integers beyond 2^53 can share a bucket (their encodings
/// round alike), so values in one bucket must still be compared with
/// [valuesEqual].
String totalOrderBucket(dynamic value) =>
    String.fromCharCodes(IndexValueCodec.encodeValue(value));

/// Returns true when [a] and [b] are equal under the index total order
/// (structural equality for lists/maps, `5 == 5.0` for numbers).
bool valuesEqual(dynamic a, dynamic b) => compareValues(a, b) == 0;

/// Unsigned lexicographic comparison of two byte strings.
int compareBytes(Uint8List a, Uint8List b) {
  final int length = a.length < b.length ? a.length : b.length;
  for (int i = 0; i < length; i++) {
    final int diff = a[i] - b[i];
    if (diff != 0) return diff;
  }
  return a.length - b.length;
}

/// Builders for the reserved index key space.
final class IndexKeyCodec {
  IndexKeyCodec._();

  /// Key head shared by every entry of [definition]:
  /// prefix + collection + 0x00 + fieldSpec + 0x00.
  static Uint8List entryHead(IndexDefinition definition) {
    final BytesBuilder out = BytesBuilder(copy: false);
    out.add(indexEntryPrefix);
    out.add(utf8.encode(definition.collection));
    out.addByte(0x00);
    out.add(utf8.encode(definition.fieldSpec));
    out.addByte(0x00);
    return out.toBytes();
  }

  /// Full posting key for [values] (one per index field) and [docId].
  static Uint8List entryKey(
    IndexDefinition definition,
    List<dynamic> values,
    String docId,
  ) {
    if (values.length != definition.fields.length) {
      throw QueryException(
        'Index ${definition.name} expects ${definition.fields.length} values, '
        'got ${values.length}',
      );
    }
    final BytesBuilder out = BytesBuilder(copy: false);
    out.add(entryHead(definition));
    for (final dynamic value in values) {
      IndexValueCodec.writeValue(out, value);
    }
    IndexValueCodec.writeDocId(out, docId);
    return out.toBytes();
  }

  /// Metadata key that persists the existence of [definition].
  static Uint8List metaKey(IndexDefinition definition) {
    final BytesBuilder out = BytesBuilder(copy: false);
    out.add(indexMetaPrefix);
    out.add(utf8.encode(definition.collection));
    out.addByte(0x00);
    out.add(utf8.encode(definition.fieldSpec));
    return out.toBytes();
  }

  /// Extracts the document id from posting key [key] of [definition].
  static String docIdFromEntryKey(IndexDefinition definition, Uint8List key) {
    int offset = entryHead(definition).length;
    for (int i = 0; i < definition.fields.length; i++) {
      offset = IndexValueCodec.skipValue(key, offset);
    }
    return IndexValueCodec.decodeDocIdAt(key, offset);
  }

  /// Returns [bytes] plus one extra [suffix] byte.
  static Uint8List withSuffix(Uint8List bytes, int suffix) {
    final Uint8List out = Uint8List(bytes.length + 1);
    out.setAll(0, bytes);
    out[bytes.length] = suffix;
    return out;
  }

  /// Smallest key strictly greater than every key having [prefix].
  ///
  /// Returns null when no such key exists (all bytes are 0xFF).
  static Uint8List? prefixUpperBound(Uint8List prefix) {
    for (int i = prefix.length - 1; i >= 0; i--) {
      if (prefix[i] != 0xFF) {
        final Uint8List out = Uint8List(i + 1);
        out.setRange(0, i + 1, prefix);
        out[i] = prefix[i] + 1;
        return out;
      }
    }
    return null;
  }
}
