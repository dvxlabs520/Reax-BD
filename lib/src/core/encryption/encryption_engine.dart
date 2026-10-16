import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart' as crypto;
import 'package:pointycastle/export.dart';

import '../errors/exceptions.dart';
import 'encryption_config.dart';
import 'encryption_type.dart';
import 'key_derivation.dart';

/// Encrypts and decrypts database values according to an [EncryptionConfig].
///
/// ## Ciphertext envelope
///
/// Every non-plaintext value is wrapped in a self-describing envelope so the
/// algorithm of stored data is always detectable and future format changes
/// remain decryptable:
///
/// ```text
/// byte 0        envelope version (0x01 or 0x02)
/// byte 1        algorithm id (0x01 = XOR obfuscation, 0x02 = AES-256-GCM)
/// AES-256-GCM:  bytes 2..13  random 96-bit IV (Random.secure())
///               bytes 14..   ciphertext || 16-byte GCM authentication tag
/// obfuscation:  bytes 2..    XOR ciphertext
/// ```
///
/// The two versions differ only in the GCM additional authenticated data
/// (AAD):
///
/// * **Version 2** (written by every AES-256-GCM [encrypt] since 2.0.1):
///   AAD is the fixed domain tag [associatedDataDomain] (`reaxdb.v2`)
///   followed by the caller's `associatedData`. The database passes the
///   record's key bytes, so a ciphertext copied onto another key fails
///   authentication instead of decrypting.
/// * **Version 1** (written by ReaxDB 2.0.0): empty AAD. Still decrypted so
///   existing databases keep working; `associatedData` is ignored for these
///   envelopes, so a version 1 value is NOT bound to its key until it is
///   rewritten. Use [envelopeVersionOf] to find such values.
///
/// XOR obfuscation has no integrity protection at all, so binding it to
/// associated data would be meaningless: it keeps writing version 1 and
/// ignores `associatedData`.
///
/// Neither version detects replay of an OLDER ciphertext of the same key
/// (rolling a value back): that would need a per-write counter in the AAD.
///
/// [EncryptionType.none] performs no transformation and adds no envelope.
///
/// Decryption validates the envelope version and that the stored algorithm
/// matches the configured one, throwing [EncryptionException] otherwise —
/// including on GCM authentication failure (tampering, wrong key, or wrong
/// associated data). ReaxDB never silently downgrades: if the requested
/// cipher cannot run, engine construction throws instead of substituting a
/// weaker algorithm.
///
/// ## Key check
///
/// [createKeyCheck] and [verifyKeyCheck] let a database store an opaque
/// token in its header and reject a wrong key or passphrase at open time,
/// before anything is written with it.
///
/// Key material (keys, passphrases, derived keys) is never logged.
final class EncryptionEngine {
  /// Envelope version written by ReaxDB 2.0.0: AES-256-GCM with empty AAD.
  ///
  /// Still accepted by [decrypt]; also the version XOR obfuscation writes.
  static const int envelopeVersion1 = 0x01;

  /// Envelope version that binds AES-256-GCM ciphertext to
  /// [associatedDataDomain] plus the caller's associated data.
  static const int envelopeVersion2 = 0x02;

  /// Envelope version written for new AES-256-GCM values.
  static const int envelopeVersion = envelopeVersion2;

  /// Envelope algorithm id for XOR obfuscation.
  static const int algorithmIdObfuscation = 0x01;

  /// Envelope algorithm id for AES-256-GCM.
  static const int algorithmIdAes256Gcm = 0x02;

  /// Domain tag that prefixes the GCM AAD of every version 2 envelope.
  static const String associatedDataDomain = 'reaxdb.v2';

  /// Format version of the token produced by [createKeyCheck].
  static const int keyCheckVersion = 0x01;

  /// Length in bytes of a key-check token for an encrypting engine.
  static const int keyCheckLength = 2 + 32;

  static const int _headerLength = 2;
  static const int _ivLength = 12;
  static const int _tagLength = 16;
  static const int _expandedXorKeyLength = 512;

  static final Uint8List _emptyAad = Uint8List(0);
  static final Uint8List _domainBytes = Uint8List.fromList(
    utf8.encode(associatedDataDomain),
  );
  // A separate domain from record AAD ('reaxdb.v2' || key bytes), so no
  // record key can ever produce the key-check input.
  static final List<int> _keyCheckLabel = utf8.encode('reaxdb.keycheck.v1');

  final EncryptionType _type;
  final Uint8List? _expandedXorKey;
  final KeyParameter? _aesKeyParameter;
  final GCMBlockCipher? _gcm;
  final Random _random = Random.secure();

  EncryptionEngine._(
    this._type,
    this._expandedXorKey,
    this._aesKeyParameter,
    this._gcm,
  );

  /// Creates an engine for [config].
  ///
  /// For [EncryptionConfig.aes256FromPassphrase], [kdfSalt] is required: it is
  /// the per-database random salt generated with [KeyDerivation.generateSalt]
  /// on first open and persisted in the database metadata. Passing the same
  /// salt and passphrase always derives the same key.
  ///
  /// Throws [EncryptionException] on invalid key material or a missing salt.
  ///
  /// This constructor runs PBKDF2 synchronously on the calling isolate, which
  /// blocks it for up to several seconds at the default iteration count.
  /// Prefer [create] on a UI isolate.
  factory EncryptionEngine(EncryptionConfig config, {Uint8List? kdfSalt}) {
    switch (config) {
      case NoEncryptionConfig():
        return EncryptionEngine._(EncryptionType.none, null, null, null);
      case ObfuscationConfig(:final Uint8List key):
        return EncryptionEngine._(
          EncryptionType.obfuscation,
          _expandXorKey(key),
          null,
          null,
        );
      case Aes256KeyConfig(:final Uint8List key):
        return EncryptionEngine._forAesKey(key);
      case Aes256PassphraseConfig(
        :final String passphrase,
        :final int iterations,
      ):
        final Uint8List key = KeyDerivation.deriveKey(
          passphrase: passphrase,
          salt: _requireSalt(kdfSalt),
          iterations: iterations,
        );
        return EncryptionEngine._forAesKey(key);
    }
  }

  /// Creates an engine for [config] without blocking the calling isolate.
  ///
  /// Identical to the [EncryptionEngine] constructor — same arguments, same
  /// exceptions, and an engine that reads and writes exactly the same data —
  /// except that for [EncryptionConfig.aes256FromPassphrase] the PBKDF2 key
  /// derivation runs on a background isolate via
  /// [KeyDerivation.deriveKeyInBackground]. Only the derivation leaves the
  /// calling isolate; the engine itself is built here. Other configurations
  /// complete immediately.
  static Future<EncryptionEngine> create(
    EncryptionConfig config, {
    Uint8List? kdfSalt,
  }) async {
    if (config case Aes256PassphraseConfig(
      :final String passphrase,
      :final int iterations,
    )) {
      final Uint8List key = await KeyDerivation.deriveKeyInBackground(
        passphrase: passphrase,
        salt: _requireSalt(kdfSalt),
        iterations: iterations,
      );
      return EncryptionEngine._forAesKey(key);
    }
    return EncryptionEngine(config, kdfSalt: kdfSalt);
  }

  static Uint8List _requireSalt(Uint8List? kdfSalt) {
    if (kdfSalt == null) {
      throw const EncryptionException(
        'A per-database KDF salt is required for passphrase-based '
        'encryption. Generate one with KeyDerivation.generateSalt() on '
        'first open and persist it in the database metadata.',
      );
    }
    return kdfSalt;
  }

  factory EncryptionEngine._forAesKey(Uint8List key) {
    if (key.length != KeyDerivation.keyLength) {
      throw EncryptionException(
        'AES-256 requires a ${KeyDerivation.keyLength}-byte key '
        '(got ${key.length} bytes)',
      );
    }
    final GCMBlockCipher gcm;
    try {
      gcm = GCMBlockCipher(AESEngine());
    } catch (e) {
      // Never downgrade to a weaker cipher: fail loudly instead.
      throw EncryptionException(
        'AES-256-GCM is not available on this platform',
        cause: e,
      );
    }
    return EncryptionEngine._(
      EncryptionType.aes256,
      null,
      KeyParameter(Uint8List.fromList(key)),
      gcm,
    );
  }

  /// The effective algorithm this engine applies.
  EncryptionType get type => _type;

  /// Encrypts [data], returning the enveloped ciphertext.
  ///
  /// With AES-256-GCM the result is a version 2 envelope whose
  /// authentication covers [associatedData] (treated as empty when null):
  /// [decrypt] must be given the same bytes or it throws. The database
  /// passes the record's key bytes. [associatedData] is not stored in the
  /// envelope.
  ///
  /// For [EncryptionType.none] returns [data] unchanged; for
  /// [EncryptionType.obfuscation] [associatedData] is ignored.
  Uint8List encrypt(Uint8List data, {Uint8List? associatedData}) {
    switch (_type) {
      case EncryptionType.none:
        return data;
      case EncryptionType.obfuscation:
        return _obfuscate(data);
      case EncryptionType.aes256:
        return _encryptAes256(data, associatedData);
    }
  }

  /// Decrypts enveloped [data], validating the envelope and, for AES-256-GCM,
  /// the authentication tag.
  ///
  /// A version 2 envelope only authenticates with the [associatedData] it
  /// was encrypted with (null and empty are equivalent). A version 1
  /// envelope, written by ReaxDB 2.0.0, ignores [associatedData].
  ///
  /// Accepts sublist views: the input's `offsetInBytes` is respected.
  /// Throws [EncryptionException] on an unknown envelope version, an
  /// algorithm mismatch, truncated input, or authentication failure
  /// (tampered data, wrong key, or different associated data).
  Uint8List decrypt(Uint8List data, {Uint8List? associatedData}) {
    if (_type == EncryptionType.none) {
      return data;
    }
    if (data.length < _headerLength) {
      throw EncryptionException(
        'Encrypted value too short (${data.length} bytes)',
      );
    }
    final int version = data[0];
    final bool aes = _type == EncryptionType.aes256;
    final bool supported =
        version == envelopeVersion1 || (aes && version == envelopeVersion2);
    if (!supported) {
      final String accepted = aes
          ? '$envelopeVersion1, $envelopeVersion2'
          : '$envelopeVersion1';
      throw EncryptionException(
        'Unsupported ciphertext envelope version $version for ${_type.name} '
        '(supported: $accepted)',
      );
    }
    final int algorithm = data[1];
    final int expected = aes ? algorithmIdAes256Gcm : algorithmIdObfuscation;
    if (algorithm != expected) {
      throw EncryptionException(
        'Stored value uses algorithm id $algorithm but the database is '
        'configured for ${_type.name} (algorithm id $expected)',
      );
    }
    if (!aes) {
      return _deobfuscate(data);
    }
    return _decryptAes256(
      data,
      version == envelopeVersion2 ? _aadFor(associatedData) : _emptyAad,
    );
  }

  /// Returns the envelope version byte of a stored value (0x01 or 0x02 for a
  /// value this engine wrote), or null when [stored] is too short to carry
  /// an envelope.
  ///
  /// Lets a database find values still in the version 1 format, which are
  /// not bound to their key, and rewrite them. Only meaningful for values
  /// written by an engine whose [type] is not [EncryptionType.none]; the
  /// rest of the envelope is not validated.
  static int? envelopeVersionOf(Uint8List stored) =>
      stored.length < _headerLength ? null : stored[0];

  // --- key check -------------------------------------------------------

  /// Returns an opaque token that proves possession of this engine's key.
  ///
  /// Store it (for example base64-encoded in the database header) when the
  /// database is created, and pass it to [verifyKeyCheck] on every later open
  /// to detect a wrong key or passphrase before anything is read or written.
  ///
  /// For an encrypting engine the token is [keyCheckLength] bytes:
  ///
  /// ```text
  /// byte 0      key-check version (0x01)
  /// byte 1      algorithm id (0x01 = XOR obfuscation, 0x02 = AES-256-GCM)
  /// bytes 2..33 HMAC-SHA256(key, "reaxdb.keycheck.v1" || algorithm id)
  /// ```
  ///
  /// HMAC is a pseudo-random function, so the token reveals nothing about the
  /// key; the domain label keeps it unrelated to any record ciphertext. For
  /// AES the key is the 256-bit data key (after PBKDF2 for passphrases), for
  /// obfuscation it is the expanded XOR key. The token is deterministic: the
  /// same key always produces the same token.
  ///
  /// For [EncryptionType.none] the token is empty.
  Uint8List createKeyCheck() {
    final Uint8List? key = _keyCheckKey;
    if (key == null) return Uint8List(0);
    final int algorithm = _algorithmId;
    final Uint8List token = Uint8List(keyCheckLength);
    token[0] = keyCheckVersion;
    token[1] = algorithm;
    token.setRange(2, keyCheckLength, _keyCheckMac(key, algorithm));
    return token;
  }

  /// Whether [token], produced by [createKeyCheck], was created with this
  /// engine's key and algorithm.
  ///
  /// Returns false — never throws — for a wrong key or passphrase, or for a
  /// token created by an engine with a different algorithm. Throws
  /// [EncryptionException] only when [token] is malformed (wrong length,
  /// unknown key-check version or unknown algorithm id), which means the
  /// stored header is damaged rather than the key wrong.
  ///
  /// For [EncryptionType.none] returns true for the empty token and false for
  /// any other token (a non-empty token means the database was created with
  /// encryption). The comparison is constant-time.
  bool verifyKeyCheck(Uint8List token) {
    final Uint8List? key = _keyCheckKey;
    if (key == null) return token.isEmpty;
    if (token.length != keyCheckLength) {
      throw EncryptionException(
        'Malformed key-check token: expected $keyCheckLength bytes, got '
        '${token.length}',
      );
    }
    if (token[0] != keyCheckVersion) {
      throw EncryptionException(
        'Unsupported key-check token version ${token[0]} '
        '(supported: $keyCheckVersion)',
      );
    }
    final int algorithm = token[1];
    if (algorithm != algorithmIdAes256Gcm &&
        algorithm != algorithmIdObfuscation) {
      throw EncryptionException(
        'Malformed key-check token: unknown algorithm id $algorithm',
      );
    }
    if (algorithm != _algorithmId) return false;
    final List<int> expected = _keyCheckMac(key, algorithm);
    int difference = 0;
    for (int i = 0; i < expected.length; i++) {
      difference |= expected[i] ^ token[2 + i];
    }
    return difference == 0;
  }

  Uint8List? get _keyCheckKey => switch (_type) {
    EncryptionType.none => null,
    EncryptionType.obfuscation => _expandedXorKey,
    EncryptionType.aes256 => _aesKeyParameter!.key,
  };

  int get _algorithmId => _type == EncryptionType.aes256
      ? algorithmIdAes256Gcm
      : algorithmIdObfuscation;

  static List<int> _keyCheckMac(Uint8List key, int algorithm) => crypto.Hmac(
    crypto.sha256,
    key,
  ).convert(<int>[..._keyCheckLabel, algorithm]).bytes;

  /// Returns metadata describing the EFFECTIVE encryption configuration.
  ///
  /// The reported algorithm and security level always describe what this
  /// engine actually does — never an aspirational or requested value.
  Map<String, dynamic> getMetadata() {
    return <String, dynamic>{
      'enabled': _type != EncryptionType.none,
      'type': _type.name,
      'display_name': _type.displayName,
      'security_level': _type.securityLevel,
      'performance_impact': _type.performanceImpact,
      'is_authenticated': _type == EncryptionType.aes256,
      'is_secure': _type == EncryptionType.aes256,
      'envelope_version': switch (_type) {
        EncryptionType.none => null,
        EncryptionType.obfuscation => envelopeVersion1,
        EncryptionType.aes256 => envelopeVersion2,
      },
    };
  }

  // --- AES-256-GCM -----------------------------------------------------

  /// The GCM AAD of a version 2 envelope: domain tag || associated data.
  static Uint8List _aadFor(Uint8List? associatedData) {
    if (associatedData == null || associatedData.isEmpty) return _domainBytes;
    final Uint8List aad = Uint8List(
      _domainBytes.length + associatedData.length,
    );
    aad.setRange(0, _domainBytes.length, _domainBytes);
    aad.setRange(_domainBytes.length, aad.length, associatedData);
    return aad;
  }

  Uint8List _encryptAes256(Uint8List data, Uint8List? associatedData) {
    final GCMBlockCipher gcm = _gcm!;
    final Uint8List result = Uint8List(
      _headerLength + _ivLength + data.length + _tagLength,
    );
    result[0] = envelopeVersion2;
    result[1] = algorithmIdAes256Gcm;
    for (int i = 0; i < _ivLength; i++) {
      result[_headerLength + i] = _random.nextInt(256);
    }
    final Uint8List iv = Uint8List.sublistView(
      result,
      _headerLength,
      _headerLength + _ivLength,
    );
    gcm.init(
      true,
      AEADParameters(
        _aesKeyParameter!,
        _tagLength * 8,
        iv,
        _aadFor(associatedData),
      ),
    );
    const int ctOffset = _headerLength + _ivLength;
    int written = gcm.processBytes(data, 0, data.length, result, ctOffset);
    try {
      written += gcm.doFinal(result, ctOffset + written);
    } catch (e) {
      throw EncryptionException('AES-256-GCM encryption failed', cause: e);
    }
    if (ctOffset + written == result.length) {
      return result;
    }
    return Uint8List.sublistView(result, 0, ctOffset + written);
  }

  Uint8List _decryptAes256(Uint8List data, Uint8List aad) {
    const int ctOffset = _headerLength + _ivLength;
    if (data.length < ctOffset + _tagLength) {
      throw EncryptionException(
        'Encrypted value too short for AES-256-GCM (${data.length} bytes)',
      );
    }
    final GCMBlockCipher gcm = _gcm!;
    // sublistView respects offsetInBytes, unlike Uint8List.view(data.buffer).
    final Uint8List iv = Uint8List.sublistView(data, _headerLength, ctOffset);
    final Uint8List ciphertext = Uint8List.sublistView(data, ctOffset);
    gcm.init(false, AEADParameters(_aesKeyParameter!, _tagLength * 8, iv, aad));
    final Uint8List out = Uint8List(gcm.getOutputSize(ciphertext.length));
    int written = gcm.processBytes(ciphertext, 0, ciphertext.length, out, 0);
    try {
      written += gcm.doFinal(out, written);
    } catch (e) {
      throw EncryptionException(
        'AES-256-GCM authentication failed: the data was tampered with, the '
        'key is wrong, or the value belongs to a different key',
        cause: e,
      );
    }
    if (written == out.length) {
      return out;
    }
    return Uint8List.sublistView(out, 0, written);
  }

  // --- XOR obfuscation (NOT encryption) --------------------------------

  Uint8List _obfuscate(Uint8List data) {
    final Uint8List key = _expandedXorKey!;
    final int keyLen = key.length;
    final Uint8List result = Uint8List(_headerLength + data.length);
    result[0] = envelopeVersion1;
    result[1] = algorithmIdObfuscation;
    for (int i = 0; i < data.length; i++) {
      result[_headerLength + i] = data[i] ^ key[i % keyLen];
    }
    return result;
  }

  Uint8List _deobfuscate(Uint8List data) {
    final Uint8List key = _expandedXorKey!;
    final int keyLen = key.length;
    final int length = data.length - _headerLength;
    final Uint8List result = Uint8List(length);
    for (int i = 0; i < length; i++) {
      result[i] = data[_headerLength + i] ^ key[i % keyLen];
    }
    return result;
  }

  static Uint8List _expandXorKey(Uint8List key) {
    final Uint8List expanded = Uint8List(_expandedXorKeyLength);
    final int keyLen = key.length;
    for (int i = 0; i < _expandedXorKeyLength; i++) {
      expanded[i] = key[i % keyLen];
    }
    return expanded;
  }
}
