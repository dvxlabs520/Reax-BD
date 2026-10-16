import 'dart:convert';
import 'dart:isolate';
import 'dart:math';
import 'dart:typed_data';

import 'package:pointycastle/export.dart';

import '../errors/exceptions.dart';

/// Key derivation and secure random material for ReaxDB encryption.
///
/// Uses PBKDF2-HMAC-SHA256 (RFC 8018) from `package:pointycastle` — never a
/// homebrew construction. The salt MUST be random per database: it is
/// generated with [generateSalt] on first open and persisted in the database
/// metadata so the same passphrase derives the same key on subsequent opens,
/// while different databases (and different users) never share a salt.
abstract final class KeyDerivation {
  /// Default PBKDF2 iteration count.
  ///
  /// 210,000 follows the 2023+ OWASP recommendation for PBKDF2-HMAC-SHA256.
  /// Callers may raise it for higher security margins or lower it only for
  /// tests. Values below [minIterations] are rejected.
  static const int defaultIterations = 210000;

  /// Minimum accepted iteration count. Guards against accidental misuse
  /// (for example passing 0 or 1) that would defeat the KDF entirely.
  static const int minIterations = 1000;

  /// Length in bytes of an AES-256 key.
  static const int keyLength = 32;

  /// Recommended salt length in bytes.
  static const int saltLength = 16;

  /// Derives a [length]-byte key from [passphrase] and [salt] using
  /// PBKDF2-HMAC-SHA256 with [iterations] rounds.
  ///
  /// Deterministic for the same inputs. Throws [EncryptionException] on
  /// invalid parameters.
  static Uint8List deriveKey({
    required String passphrase,
    required Uint8List salt,
    int iterations = defaultIterations,
    int length = keyLength,
  }) {
    _validate(passphrase, salt, iterations, length);
    final PBKDF2KeyDerivator derivator = PBKDF2KeyDerivator(
      HMac(SHA256Digest(), 64),
    )..init(Pbkdf2Parameters(salt, iterations, length));
    return derivator.process(Uint8List.fromList(utf8.encode(passphrase)));
  }

  static void _validate(
    String passphrase,
    Uint8List salt,
    int iterations,
    int length,
  ) {
    if (passphrase.isEmpty) {
      throw const EncryptionException('Passphrase must not be empty');
    }
    if (salt.length < 8) {
      throw EncryptionException(
        'KDF salt must be at least 8 bytes (got ${salt.length})',
      );
    }
    if (iterations < minIterations) {
      throw EncryptionException(
        'PBKDF2 iteration count must be at least $minIterations '
        '(got $iterations)',
      );
    }
    if (length <= 0) {
      throw EncryptionException('Invalid derived key length: $length');
    }
  }

  /// Like [deriveKey], but runs PBKDF2 on a short-lived background isolate
  /// (`Isolate.run`) so the calling isolate — typically the UI isolate —
  /// stays responsive.
  ///
  /// At [defaultIterations] the pure-Dart PBKDF2 takes on the order of a
  /// second on a desktop and several seconds on a phone. Only the derivation
  /// runs off-isolate: [passphrase] and [salt] are copied to the worker and
  /// the derived key is copied back. Parameters are validated on the calling
  /// isolate first, so invalid input throws [EncryptionException] without
  /// spawning anything. The result is byte-for-byte identical to
  /// [deriveKey].
  static Future<Uint8List> deriveKeyInBackground({
    required String passphrase,
    required Uint8List salt,
    int iterations = defaultIterations,
    int length = keyLength,
  }) {
    _validate(passphrase, salt, iterations, length);
    return _runDerivation(
      passphrase,
      Uint8List.fromList(salt),
      iterations,
      length,
    );
  }

  // Kept separate so the closure handed to Isolate.run captures only the
  // four sendable arguments and nothing else from the caller's scope.
  static Future<Uint8List> _runDerivation(
    String passphrase,
    Uint8List salt,
    int iterations,
    int length,
  ) => Isolate.run<Uint8List>(
    () => deriveKey(
      passphrase: passphrase,
      salt: salt,
      iterations: iterations,
      length: length,
    ),
    debugName: 'reaxdb-pbkdf2',
  );

  /// Generates a cryptographically secure random salt of [length] bytes
  /// using `Random.secure()`.
  static Uint8List generateSalt({int length = saltLength}) {
    if (length < 8) {
      throw EncryptionException('Salt length must be at least 8 (got $length)');
    }
    final Random random = Random.secure();
    final Uint8List salt = Uint8List(length);
    for (int i = 0; i < length; i++) {
      salt[i] = random.nextInt(256);
    }
    return salt;
  }
}
