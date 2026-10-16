/// Binary value serialization and the stored-record envelope.
///
/// Two layers live here and both are used by exactly one place in the
/// codebase, the single write pipeline in `reaxdb.dart`:
///
/// - [ValueCodec] turns a Dart value into bytes and back. It never returns
///   null to signal failure: an undecodable value throws
///   [SerializationException].
/// - [RecordEnvelope] wraps encoded bytes in the stored-record envelope that
///   carries the optional expiry instant used by the TTL feature.
///
/// The envelope is the PLAINTEXT representation of a value: encryption is
/// applied to the envelope on the way to storage and removed on the way back,
/// and the cache stores envelopes (never ciphertext).
library;

import 'dart:convert';
import 'dart:typed_data';

import '../errors/exceptions.dart';

/// Self-describing binary encoding for user values.
///
/// Layout: `[type tag u8][payload]`. Strings, byte lists and JSON payloads
/// are length-implicit (they run to the end of the buffer), because a value
/// is always stored as a whole record.
///
/// Supported types: `null`, [String], [int], [double], [bool], `List<int>`
/// (stored verbatim as bytes) and anything `jsonEncode` accepts (maps, lists,
/// and nested combinations of the above). Anything else throws
/// [SerializationException] at encode time rather than being silently
/// mangled.
abstract final class ValueCodec {
  /// Tag for `null`.
  static const int tagNull = 0x00;

  /// Tag for [String] values, stored as UTF-8.
  static const int tagString = 0x01;

  /// Tag for [int] values, stored as a little-endian signed 64-bit integer.
  static const int tagInt = 0x02;

  /// Tag for [double] values, stored as a little-endian IEEE-754 double.
  static const int tagDouble = 0x03;

  /// Tag for `false`.
  static const int tagFalse = 0x04;

  /// Tag for `true`.
  static const int tagTrue = 0x05;

  /// Tag for raw byte payloads (`List<int>`).
  static const int tagBytes = 0x06;

  /// Tag for JSON-encoded structures (maps and heterogeneous lists).
  static const int tagJson = 0x07;

  /// Encodes [value].
  ///
  /// Throws [SerializationException] when [value] has no representation.
  static Uint8List encode(Object? value) {
    if (value == null) {
      return Uint8List.fromList(const <int>[tagNull]);
    }
    if (value is String) {
      return _tagged(tagString, utf8.encode(value));
    }
    if (value is int) {
      final ByteData data = ByteData(9)
        ..setUint8(0, tagInt)
        ..setInt64(1, value, Endian.little);
      return data.buffer.asUint8List();
    }
    if (value is double) {
      final ByteData data = ByteData(9)
        ..setUint8(0, tagDouble)
        ..setFloat64(1, value, Endian.little);
      return data.buffer.asUint8List();
    }
    if (value is bool) {
      return Uint8List.fromList(<int>[value ? tagTrue : tagFalse]);
    }
    if (value is Uint8List) {
      return _tagged(tagBytes, value);
    }
    if (value is List<int>) {
      return _tagged(tagBytes, value);
    }
    final String json;
    try {
      json = jsonEncode(value);
    } catch (error) {
      throw SerializationException(
        'Cannot encode a value of type ${value.runtimeType}: it is neither a '
        'primitive, a byte list, nor JSON-encodable',
        cause: error,
      );
    }
    return _tagged(tagJson, utf8.encode(json));
  }

  /// Decodes [bytes] produced by [encode].
  ///
  /// Throws [SerializationException] when the bytes are empty, carry an
  /// unknown tag, or are truncated. Never returns null to report a failure —
  /// `null` is returned only when `null` was stored.
  static Object? decode(Uint8List bytes) {
    if (bytes.isEmpty) {
      throw const SerializationException(
        'Cannot decode an empty value payload',
      );
    }
    final int tag = bytes[0];
    switch (tag) {
      case tagNull:
        return null;
      case tagString:
        try {
          return utf8.decode(Uint8List.sublistView(bytes, 1));
        } catch (error) {
          throw SerializationException(
            'Stored string value is not valid UTF-8',
            cause: error,
          );
        }
      case tagInt:
        _requireLength(bytes, 9, 'int');
        return ByteData.sublistView(bytes, 1, 9).getInt64(0, Endian.little);
      case tagDouble:
        _requireLength(bytes, 9, 'double');
        return ByteData.sublistView(bytes, 1, 9).getFloat64(0, Endian.little);
      case tagFalse:
        return false;
      case tagTrue:
        return true;
      case tagBytes:
        return Uint8List.fromList(Uint8List.sublistView(bytes, 1));
      case tagJson:
        try {
          return jsonDecode(utf8.decode(Uint8List.sublistView(bytes, 1)));
        } catch (error) {
          throw SerializationException(
            'Stored JSON value could not be decoded',
            cause: error,
          );
        }
      default:
        throw SerializationException(
          'Unknown value type tag 0x${tag.toRadixString(16)}',
        );
    }
  }

  static Uint8List _tagged(int tag, List<int> payload) {
    final Uint8List out = Uint8List(1 + payload.length);
    out[0] = tag;
    out.setRange(1, out.length, payload);
    return out;
  }

  static void _requireLength(Uint8List bytes, int needed, String what) {
    if (bytes.length < needed) {
      throw SerializationException(
        'Truncated $what value: ${bytes.length} bytes, expected $needed',
      );
    }
  }
}

/// A decoded stored record: the user value plus its optional expiry.
final class StoredRecord {
  /// Creates a record holding [value], expiring at [expiresAt] when set.
  const StoredRecord(this.value, this.expiresAt);

  /// The decoded user value.
  final Object? value;

  /// Absolute expiry instant, or null when the record never expires.
  final DateTime? expiresAt;

  /// Whether this record is expired at [now].
  bool isExpiredAt(DateTime now) {
    final DateTime? at = expiresAt;
    return at != null && !now.isBefore(at);
  }
}

/// Envelope that wraps an encoded value with its optional expiry.
///
/// Layout: `[format version u8][flags u8][expiry int64 LE, if flags bit 0]
/// [value bytes]`. The envelope is what the cache stores and what encryption
/// is applied to, so a single representation flows through the whole
/// pipeline.
abstract final class RecordEnvelope {
  /// Current envelope format version.
  static const int formatVersion = 0x01;

  /// Flag bit set when the record carries an expiry timestamp.
  static const int flagHasExpiry = 0x01;

  /// Wraps [value] in an envelope, optionally expiring at [expiresAt].
  static Uint8List encode(Object? value, {DateTime? expiresAt}) {
    final Uint8List payload = ValueCodec.encode(value);
    final int headerLength = expiresAt == null ? 2 : 10;
    final Uint8List out = Uint8List(headerLength + payload.length);
    out[0] = formatVersion;
    out[1] = expiresAt == null ? 0 : flagHasExpiry;
    if (expiresAt != null) {
      ByteData.sublistView(
        out,
        2,
        10,
      ).setInt64(0, expiresAt.millisecondsSinceEpoch, Endian.little);
    }
    out.setRange(headerLength, out.length, payload);
    return out;
  }

  /// Decodes an envelope produced by [encode].
  ///
  /// Throws [SerializationException] on an unknown version or truncation.
  static StoredRecord decode(Uint8List bytes) {
    final int offset = _payloadOffset(bytes);
    return StoredRecord(
      ValueCodec.decode(Uint8List.sublistView(bytes, offset)),
      _expiry(bytes),
    );
  }

  /// Reads only the expiry of an envelope, without decoding its value.
  static DateTime? peekExpiry(Uint8List bytes) {
    _payloadOffset(bytes);
    return _expiry(bytes);
  }

  /// Whether [bytes] is an envelope that has already expired at [now].
  static bool isExpired(Uint8List bytes, DateTime now) {
    final DateTime? at = peekExpiry(bytes);
    return at != null && !now.isBefore(at);
  }

  static DateTime? _expiry(Uint8List bytes) {
    if (bytes[1] & flagHasExpiry == 0) return null;
    return DateTime.fromMillisecondsSinceEpoch(
      ByteData.sublistView(bytes, 2, 10).getInt64(0, Endian.little),
    );
  }

  static int _payloadOffset(Uint8List bytes) {
    if (bytes.length < 2) {
      throw SerializationException(
        'Truncated record envelope (${bytes.length} bytes)',
      );
    }
    if (bytes[0] != formatVersion) {
      throw SerializationException(
        'Unsupported record envelope version ${bytes[0]} '
        '(supported: $formatVersion)',
      );
    }
    final bool hasExpiry = bytes[1] & flagHasExpiry != 0;
    final int offset = hasExpiry ? 10 : 2;
    if (bytes.length < offset) {
      throw const SerializationException(
        'Truncated record envelope: expiry header is incomplete',
      );
    }
    return offset;
  }
}
