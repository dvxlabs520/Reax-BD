import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

/// Redaction helpers for log messages.
///
/// ## No-PII logging policy
///
/// Database keys frequently contain emails, user ids, and other personal
/// data, and values are the user's data itself. ReaxDB therefore never logs
/// key contents, value contents, encryption keys, passphrases, or derived key
/// material at ANY log level. When a key must appear in a log message for
/// debugging, pass it through [Redaction.key] first: the output is a keyed,
/// non-reversible fingerprint that lets you correlate log lines about the
/// same key without revealing it.
abstract final class Redaction {
  /// HMAC-SHA256 keyed with 32 random bytes from `Random.secure()`,
  /// generated once per isolate and never logged or persisted.
  ///
  /// Without the key a fingerprint cannot be recomputed, so hashing a list
  /// of likely keys (email addresses, phone numbers) and comparing the
  /// results against a log — trivial with an unkeyed hash — does not work.
  /// Dart has no process-wide mutable state, so each isolate has its own
  /// key: fingerprints correlate within an isolate, not across isolates or
  /// runs.
  static final Hmac _hmac = Hmac(sha256, _randomKey());

  static List<int> _randomKey() {
    final Random random = Random.secure();
    return List<int>.generate(32, (_) => random.nextInt(256));
  }

  /// Redacts a database key to a non-reversible fingerprint such as
  /// `key#3f2a9c1d(len=17)`.
  ///
  /// The fingerprint is the first 8 hex characters of the HMAC-SHA256 of the
  /// UTF-8 key bytes under a random per-isolate key: the same key always
  /// yields the same fingerprint while the isolate lives, so log lines can be
  /// correlated, but it can neither be reversed nor brute-forced from a list
  /// of candidate keys. Fingerprints differ between runs.
  static String key(String key) => _fingerprint(utf8.encode(key), key.length);

  /// Redacts raw key bytes to a fingerprint, like [key] but for binary keys.
  ///
  /// Matches [key] for the UTF-8 encoding of an ASCII string.
  static String keyBytes(Uint8List bytes) => _fingerprint(bytes, bytes.length);

  static String _fingerprint(List<int> bytes, int length) {
    final String fingerprint = _hmac
        .convert(bytes)
        .bytes
        .take(4)
        .map((int b) => b.toRadixString(16).padLeft(2, '0'))
        .join();
    return 'key#$fingerprint(len=$length)';
  }

  /// Redacts a stored value, reporting only its runtime type and size.
  ///
  /// Never includes any part of the value's content.
  static String value(Object? value) {
    if (value == null) {
      return '<null>';
    }
    if (value is String) {
      return '<redacted String, ${value.length} chars>';
    }
    if (value is List<int>) {
      return '<redacted bytes, ${value.length} bytes>';
    }
    if (value is Map) {
      return '<redacted Map, ${value.length} entries>';
    }
    if (value is Iterable) {
      return '<redacted ${value.runtimeType}, ${value.length} items>';
    }
    return '<redacted ${value.runtimeType}>';
  }
}
