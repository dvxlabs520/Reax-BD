part of 'reaxdb.dart';

// Backup and restore archives for [ReaxDB.exportTo] and [ReaxDB.importFrom].
//
// Version 2 (written since 2.1), streamed so neither side holds the database
// in memory:
//
//   u32 magic "RXDB" | u16 version = 2 | u8 flags (bit 0: encrypted)
//   encrypted only:  u8 kdf (0 raw key, 1 PBKDF2) | PBKDF2 only: u8 salt
//                    length, salt, varint iterations | varint check length,
//                    key check token
//   varint metadata length | metadata JSON (schema version, indexes, ...)
//   records:         varint length (> 0) | record
//   varint 0         end of records
//   varint record count
//   32-byte SHA-256 of every byte before it
//
// A record is `varint key length | key | record envelope`; in an encrypted
// archive the whole record (key included) is one AES-256-GCM envelope. The
// checksum is verified over the whole file before anything is restored.
//
// Version 1 (2.0) held the whole body in one length-prefixed block and is
// still restored.

const int _archiveFlagEncrypted = 0x01;
const int _archiveKdfRawKey = 0;
const int _archiveKdfPbkdf2 = 1;
const int _archiveChunkBytes = 64 * 1024;
const int _archiveBatchSize = 500;
final Uint8List _archiveRecordAad = Uint8List.fromList(
  utf8.encode('reaxdb.backup.record'),
);

/// Collects the single digest a chunked SHA-256 conversion produces.
final class _DigestSink implements Sink<Digest> {
  Digest? value;

  @override
  void add(Digest data) => value = data;

  @override
  void close() {}
}

/// Buffered writer that hashes everything it writes.
final class _ArchiveWriter {
  _ArchiveWriter(this._sink);

  final IOSink _sink;
  final _DigestSink _digest = _DigestSink();
  BytesBuilder _buffer = BytesBuilder(copy: false);

  late final ByteConversionSink _hash = sha256.startChunkedConversion(_digest);

  void addBytes(List<int> bytes) {
    _buffer.add(bytes);
    if (_buffer.length >= _archiveChunkBytes) _drain();
  }

  void addByte(int byte) => _buffer.addByte(byte);

  void addVarint(int value) => Varint.write(_buffer, value);

  void _drain() {
    final Uint8List chunk = _buffer.takeBytes();
    _hash.add(chunk);
    _sink.add(chunk);
    _buffer = BytesBuilder(copy: false);
  }

  /// Writes the trailing digest and flushes.
  Future<void> finish() async {
    _drain();
    _hash.close();
    _sink.add(_digest.value!.bytes);
    await _sink.flush();
  }
}

/// Buffered reader over an archive file.
final class _ArchiveReader {
  _ArchiveReader(this._file, this._path, this._end);

  final RandomAccessFile _file;
  final String _path;

  /// Offset of the trailing digest: reading stops before it.
  final int _end;
  int _position = 0;
  Uint8List _buffer = Uint8List(0);
  int _cursor = 0;

  int get position => _position - (_buffer.length - _cursor);

  Future<void> _fill(int needed) async {
    while (_buffer.length - _cursor < needed) {
      final int remaining = _end - _position;
      if (remaining <= 0) {
        throw CorruptionException(
          'Archive is truncated',
          path: _path,
          offset: position,
        );
      }
      final Uint8List chunk = await _file.read(
        remaining < _archiveChunkBytes ? remaining : _archiveChunkBytes,
      );
      if (chunk.isEmpty) {
        throw CorruptionException(
          'Archive is truncated',
          path: _path,
          offset: position,
        );
      }
      _position += chunk.length;
      final BytesBuilder merged = BytesBuilder(copy: false)
        ..add(Uint8List.sublistView(_buffer, _cursor))
        ..add(chunk);
      _buffer = merged.takeBytes();
      _cursor = 0;
    }
  }

  Future<int> readByte() async {
    await _fill(1);
    return _buffer[_cursor++];
  }

  Future<Uint8List> readBytes(int length) async {
    await _fill(length);
    final Uint8List out = Uint8List.fromList(
      Uint8List.sublistView(_buffer, _cursor, _cursor + length),
    );
    _cursor += length;
    return out;
  }

  Future<int> readUint(int bytes) async {
    int value = 0;
    for (int i = 0; i < bytes; i++) {
      value = (value << 8) | await readByte();
    }
    return value;
  }

  Future<int> readVarint() async {
    int result = 0;
    int shift = 0;
    for (int i = 0; i < Varint.maxBytes; i++) {
      final int byte = await readByte();
      result |= (byte & 0x7f) << shift;
      if (byte < 0x80) return result;
      shift += 7;
    }
    throw CorruptionException(
      'Malformed length in archive',
      path: _path,
      offset: position,
    );
  }

  bool get atEnd => position >= _end;
}

/// The archive cipher, or null for a plaintext archive.
///
/// Only AES-256 is accepted: a backup is the copy most likely to leave the
/// device, so the obfuscation config is refused rather than offered.
EncryptionEngine? _archiveEngine(EncryptionConfig config, Uint8List? salt) =>
    switch (config) {
      NoEncryptionConfig() => null,
      Aes256KeyConfig() ||
      Aes256PassphraseConfig() => EncryptionEngine(config, kdfSalt: salt),
      ObfuscationConfig() => throw const EncryptionException(
        'Backups can be encrypted with AES-256 only. XOR obfuscation is not '
        'encryption; use EncryptionConfig.aes256(...) or '
        'EncryptionConfig.aes256FromPassphrase(...).',
      ),
    };

Future<int> _exportArchive(
  ReaxDB db,
  String archivePath,
  EncryptionConfig encryption,
) async {
  final Uint8List? salt = encryption is Aes256PassphraseConfig
      ? KeyDerivation.generateSalt()
      : null;
  final EncryptionEngine? cipher = _archiveEngine(encryption, salt);

  final File target = File(archivePath);
  await target.parent.create(recursive: true);
  final File temporary = File(
    '$archivePath.tmp-${DateTime.now().microsecondsSinceEpoch}',
  );
  final IOSink sink = temporary.openWrite();
  int entries = 0;
  try {
    final _ArchiveWriter out = _ArchiveWriter(sink);
    final ByteData head = ByteData(7)
      ..setUint32(0, ReaxDB.archiveMagic, Endian.big)
      ..setUint16(4, ReaxDB.archiveVersion, Endian.big)
      ..setUint8(6, cipher == null ? 0 : _archiveFlagEncrypted);
    out.addBytes(head.buffer.asUint8List());
    if (cipher != null) {
      if (encryption is Aes256PassphraseConfig) {
        out
          ..addByte(_archiveKdfPbkdf2)
          ..addByte(salt!.length)
          ..addBytes(salt)
          ..addVarint(encryption.iterations);
      } else {
        out.addByte(_archiveKdfRawKey);
      }
      final Uint8List check = cipher.createKeyCheck();
      out
        ..addVarint(check.length)
        ..addBytes(check);
    }

    final Uint8List metadata = Uint8List.fromList(
      utf8.encode(
        jsonEncode(<String, dynamic>{
          'schemaVersion': db._header.schemaVersion,
          'createdAtMs': DateTime.now().millisecondsSinceEpoch,
          'indexes': <Map<String, dynamic>>[
            for (final IndexDefinition d in db._indexes.listIndexes())
              <String, dynamic>{'collection': d.collection, 'fields': d.fields},
          ],
        }),
      ),
    );
    out
      ..addVarint(metadata.length)
      ..addBytes(metadata);

    await for (final KeyValue pair in db._storage.scan(
      startKey: ReaxDB._userKeySpaceStart,
    )) {
      final BytesBuilder record = BytesBuilder(copy: false);
      Varint.write(record, pair.key.length);
      record
        ..add(pair.key)
        ..add(db._toEnvelope(pair.value, pair.key));
      final Uint8List plain = record.takeBytes();
      final Uint8List stored = cipher == null
          ? plain
          : cipher.encrypt(plain, associatedData: _archiveRecordAad);
      out
        ..addVarint(stored.length)
        ..addBytes(stored);
      entries++;
    }
    out
      ..addVarint(0)
      ..addVarint(entries);
    await out.finish();
    await sink.close();
    await temporary.rename(target.path);
    syncDirectory(target.parent.path);
  } catch (_) {
    await sink.close().catchError((Object _) {});
    if (await temporary.exists()) await temporary.delete();
    rethrow;
  }
  await db._logger.info('Exported $entries entries to $archivePath');
  return entries;
}

/// Verifies the trailing SHA-256 of the archive at [file] by streaming it.
Future<void> _verifyArchiveDigest(File file, int length) async {
  final _DigestSink digest = _DigestSink();
  final ByteConversionSink hash = sha256.startChunkedConversion(digest);
  final RandomAccessFile handle = await file.open();
  try {
    int offset = 0;
    final int bodyEnd = length - 32;
    while (offset < bodyEnd) {
      final int size = bodyEnd - offset < _archiveChunkBytes
          ? bodyEnd - offset
          : _archiveChunkBytes;
      final Uint8List chunk = await handle.read(size);
      if (chunk.isEmpty) break;
      hash.add(chunk);
      offset += chunk.length;
    }
    hash.close();
    final Uint8List expected = await handle.read(32);
    final List<int> actual = digest.value!.bytes;
    for (int i = 0; i < 32; i++) {
      if (i >= expected.length || actual[i] != expected[i]) {
        throw CorruptionException(
          'Archive checksum mismatch: the file is damaged or truncated',
          path: file.path,
          offset: bodyEnd,
        );
      }
    }
  } finally {
    await handle.close();
  }
}

/// Moves a restored database from [staging] to [path], replacing what is
/// there only when [overwrite] allows it and the target is a ReaxDB
/// database (or empty), under that database's own lock.
Future<void> _installRestoredDatabase(
  Directory staging,
  String path,
  bool overwrite,
) async {
  final Directory target = Directory(path);
  if (!await target.exists()) {
    await staging.rename(path);
    syncDirectory(p.dirname(p.absolute(path)));
    return;
  }
  final bool empty = await target.list().isEmpty;
  if (!empty) {
    if (!overwrite) {
      throw DatabaseLockedException(
        'Target directory "$path" is not empty. Pass overwrite: true to '
        'replace the ReaxDB database in it.',
        path: path,
      );
    }
    if (!await File(p.join(path, ReaxDB.headerFileName)).exists()) {
      throw DatabaseLockedException(
        'Target directory "$path" is not empty and is not a ReaxDB database. '
        'importFrom only replaces ReaxDB databases; choose an empty or new '
        'directory.',
        path: path,
      );
    }
  }
  // Hold the existing database's lock while swapping it out, so an
  // instance open elsewhere is never replaced underneath.
  final _DatabaseLock lock = await _DatabaseLock.acquire(
    ReaxDB._canonicalize(path),
  );
  final String replaced =
      '$path.replaced-${DateTime.now().microsecondsSinceEpoch}';
  try {
    await target.rename(replaced);
  } finally {
    await lock.release();
  }
  await staging.rename(path);
  syncDirectory(p.dirname(p.absolute(path)));
  await Directory(replaced).delete(recursive: true);
}

Future<ReaxDB> _importArchive({
  required String archivePath,
  required String path,
  required EncryptionConfig encryption,
  required EncryptionConfig? archiveEncryption,
  required SyncMode syncMode,
  required bool overwrite,
}) async {
  final File file = File(archivePath);
  if (!await file.exists()) {
    throw CorruptionException(
      'Archive not found: $archivePath',
      path: archivePath,
    );
  }
  final int length = await file.length();
  if (length < 6 + 32) {
    throw CorruptionException(
      'Archive is too small to be a ReaxDB export',
      path: archivePath,
    );
  }
  final RandomAccessFile probe = await file.open();
  final int magic;
  final int version;
  try {
    final ByteData head = ByteData.sublistView(await probe.read(6));
    magic = head.getUint32(0, Endian.big);
    version = head.getUint16(4, Endian.big);
  } finally {
    await probe.close();
  }
  if (magic != ReaxDB.archiveMagic) {
    throw CorruptionException(
      'Not a ReaxDB export archive (bad magic number)',
      path: archivePath,
      offset: 0,
    );
  }
  if (version != 1 && version != ReaxDB.archiveVersion) {
    throw CorruptionException(
      'Unsupported archive format version $version '
      '(supported: 1, ${ReaxDB.archiveVersion})',
      path: archivePath,
      offset: 4,
    );
  }
  // Nothing is written before the whole file checks out.
  await _verifyArchiveDigest(file, length);

  // Restore into a sibling staging directory and move it into place only
  // once complete: a failure or a crash midway never leaves a half-restored
  // database at [path].
  final Directory staging = Directory(
    '${p.absolute(path)}.importing-${DateTime.now().microsecondsSinceEpoch}',
  );
  final RandomAccessFile handle = await file.open();
  ReaxDB? db;
  int schemaVersion = 1;
  try {
    final _ArchiveReader reader = _ArchiveReader(
      handle,
      archivePath,
      length - 32,
    );
    await reader.readBytes(6);
    EncryptionEngine? cipher;
    if (version >= 2) {
      final int flags = await reader.readByte();
      if (flags & _archiveFlagEncrypted != 0) {
        final int kdf = await reader.readByte();
        Uint8List? salt;
        if (kdf == _archiveKdfPbkdf2) {
          salt = await reader.readBytes(await reader.readByte());
          final int iterations = await reader.readVarint();
          final EncryptionConfig? given = archiveEncryption;
          if (given is Aes256PassphraseConfig &&
              given.iterations != iterations) {
            throw EncryptionException(
              'The archive was encrypted with $iterations PBKDF2 iterations '
              'but ${given.iterations} were given.',
            );
          }
        } else if (kdf != _archiveKdfRawKey) {
          throw CorruptionException(
            'Unknown archive key derivation $kdf',
            path: archivePath,
          );
        }
        final EncryptionConfig? given = archiveEncryption;
        if (given == null || given is NoEncryptionConfig) {
          throw const EncryptionException(
            'The archive is encrypted: pass the configuration it was exported '
            'with as archiveEncryption.',
          );
        }
        cipher = _archiveEngine(given, salt);
        final Uint8List check = await reader.readBytes(
          await reader.readVarint(),
        );
        if (!cipher!.verifyKeyCheck(check)) {
          throw const EncryptionException(
            'Wrong key or passphrase for this archive.',
          );
        }
      }
    }

    final Map<String, dynamic> meta =
        jsonDecode(
              utf8.decode(await reader.readBytes(await reader.readVarint())),
            )
            as Map<String, dynamic>;
    schemaVersion = (meta['schemaVersion'] as int?) ?? 1;

    db = await ReaxDB.open(
      path: staging.path,
      encryption: encryption,
      syncMode: syncMode,
      schemaVersion: schemaVersion,
    );
    final ReaxDB restored = db;

    List<_Mutation> batch = <_Mutation>[];
    Future<void> flushBatch() async {
      if (batch.isEmpty) return;
      final List<_Mutation> chunk = batch;
      batch = <_Mutation>[];
      await restored._applyMutations(chunk);
    }

    void addRecord(Uint8List plain) {
      final (int keyLength, int keyRead) = Varint.read(plain, 0);
      final String key = utf8.decode(
        Uint8List.sublistView(plain, keyRead, keyRead + keyLength),
      );
      final StoredRecord record = RecordEnvelope.decode(
        Uint8List.sublistView(plain, keyRead + keyLength),
      );
      batch.add(_Mutation.put(key, record.value, record.expiresAt));
    }

    if (version == 1) {
      // 2.0 archives: one length-prefixed body of `key, envelope` pairs.
      final int bodyLength = await reader.readVarint();
      final int bodyEnd = reader.position + bodyLength;
      while (reader.position < bodyEnd) {
        final int keyLength = await reader.readVarint();
        final Uint8List key = await reader.readBytes(keyLength);
        final Uint8List envelope = await reader.readBytes(
          await reader.readVarint(),
        );
        final BytesBuilder plain = BytesBuilder(copy: false);
        Varint.write(plain, keyLength);
        plain
          ..add(key)
          ..add(envelope);
        addRecord(plain.takeBytes());
        if (batch.length >= _archiveBatchSize) await flushBatch();
      }
    } else {
      while (true) {
        final int recordLength = await reader.readVarint();
        if (recordLength == 0) break;
        final Uint8List stored = await reader.readBytes(recordLength);
        addRecord(
          cipher == null
              ? stored
              : cipher.decrypt(stored, associatedData: _archiveRecordAad),
        );
        if (batch.length >= _archiveBatchSize) await flushBatch();
      }
      await reader.readVarint(); // record count, covered by the digest
    }
    await flushBatch();
    if (!reader.atEnd) {
      throw CorruptionException(
        'Archive trailer is malformed',
        path: archivePath,
        offset: reader.position,
      );
    }

    for (final Object? entry
        in (meta['indexes'] as List<dynamic>? ?? const <dynamic>[])) {
      final Map<String, dynamic> definition = entry as Map<String, dynamic>;
      await restored.createIndex(
        definition['collection'] as String,
        (definition['fields'] as List<dynamic>).cast<String>(),
      );
    }
    await restored.close();
    db = null;
    await _installRestoredDatabase(staging, path, overwrite);
  } catch (_) {
    await db?.close();
    if (await staging.exists()) await staging.delete(recursive: true);
    rethrow;
  } finally {
    await handle.close();
  }
  return ReaxDB.open(
    path: path,
    encryption: encryption,
    syncMode: syncMode,
    schemaVersion: schemaVersion,
  );
}
