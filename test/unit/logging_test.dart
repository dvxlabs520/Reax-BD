import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

import 'package:reaxdb_dart/src/core/encryption/encryption_config.dart';
import 'package:reaxdb_dart/src/core/encryption/encryption_engine.dart';
import 'package:reaxdb_dart/src/core/logging/file_log_output.dart';
import 'package:reaxdb_dart/src/core/logging/log_level.dart';
import 'package:reaxdb_dart/src/core/logging/log_output.dart';
import 'package:reaxdb_dart/src/core/logging/logger.dart';
import 'package:reaxdb_dart/src/core/logging/redaction.dart';
import 'package:test/test.dart';

void main() {
  group('per-instance configuration', () {
    test('two loggers have independent levels and outputs', () async {
      final MemoryLogOutput outA = MemoryLogOutput();
      final MemoryLogOutput outB = MemoryLogOutput();
      final ReaxLogger a = ReaxLogger(
        level: LogLevel.debug,
        outputs: <LogOutput>[outA],
        name: 'dbA',
      );
      final ReaxLogger b = ReaxLogger(
        level: LogLevel.error,
        outputs: <LogOutput>[outB],
        name: 'dbB',
      );

      await a.debug('debug message');
      await b.debug('debug message');
      await b.error('error message');

      expect(outA.logs, hasLength(1));
      expect(outA.logs.single.message, contains('[dbA]'));
      expect(outB.logs, hasLength(1));
      expect(outB.logs.single.level, LogLevel.error);
      expect(outB.logs.single.message, contains('[dbB]'));
    });

    test('level threshold is honored', () async {
      final MemoryLogOutput out = MemoryLogOutput();
      final ReaxLogger log = ReaxLogger(
        level: LogLevel.warning,
        outputs: <LogOutput>[out],
      );
      await log.debug('nope');
      await log.info('nope');
      await log.warning('yes');
      await log.error('yes');
      expect(out.logs.map((LogEntry e) => e.level), <LogLevel>[
        LogLevel.warning,
        LogLevel.error,
      ]);
    });

    test('configure replaces level, outputs and enabled at runtime', () async {
      final MemoryLogOutput first = MemoryLogOutput();
      final MemoryLogOutput second = MemoryLogOutput();
      final ReaxLogger log = ReaxLogger(
        level: LogLevel.info,
        outputs: <LogOutput>[first],
      );
      await log.info('to first');
      log.configure(level: LogLevel.debug, outputs: <LogOutput>[second]);
      await log.debug('to second');
      expect(first.logs, hasLength(1));
      expect(second.logs, hasLength(1));

      log.configure(enabled: false);
      await log.error('dropped');
      expect(second.logs, hasLength(1));
    });

    test('metadata and error details are forwarded', () async {
      final MemoryLogOutput out = MemoryLogOutput();
      final ReaxLogger log = ReaxLogger(
        level: LogLevel.error,
        outputs: <LogOutput>[out],
      );
      await log.error(
        'boom',
        error: StateError('inner'),
        metadata: <String, dynamic>{'code': 7},
      );
      final LogEntry entry = out.logs.single;
      expect(entry.metadata!['code'], 7);
      expect(entry.metadata!['error'], contains('inner'));
    });
  });

  group('lifecycle', () {
    test('close closes outputs and drops later writes', () async {
      final _ClosableOutput out = _ClosableOutput();
      final ReaxLogger log = ReaxLogger(
        level: LogLevel.debug,
        outputs: <LogOutput>[out],
      );
      await log.info('before close');
      await log.close();
      expect(out.closed, isTrue);
      await log.info('after close');
      expect(out.logs, hasLength(1));
      // close is idempotent.
      await log.close();
    });

    test('FileLogOutput writes, flushes and closes its sink', () async {
      final Directory dir = await Directory.systemTemp.createTemp('reax_log');
      addTearDown(() => dir.delete(recursive: true));
      final String path = '${dir.path}/db.log';
      final FileLogOutput fileOut = FileLogOutput(path);
      final ReaxLogger log = ReaxLogger(
        level: LogLevel.debug,
        outputs: <LogOutput>[fileOut],
      );
      await log.info('persisted line');
      await log.close();

      final String content = await File(path).readAsString();
      expect(content, contains('persisted line'));
      // Writing after close is a no-op, not an error.
      await fileOut.write(LogLevel.info, 'ignored');
      expect(await File(path).readAsString(), isNot(contains('ignored')));
    });

    test('a throwing output does not break logging to other outputs', () async {
      final MemoryLogOutput good = MemoryLogOutput();
      final ReaxLogger log = ReaxLogger(
        level: LogLevel.debug,
        outputs: <LogOutput>[_ThrowingOutput(), good],
      );
      await log.info('survives');
      expect(good.logs, hasLength(1));
    });
  });

  group('redaction', () {
    test('Redaction.key hides the key but stays correlatable', () {
      const String pii = 'user:davidvillegas15@gmail.com';
      final String redacted = Redaction.key(pii);
      expect(redacted, isNot(contains('gmail')));
      expect(redacted, isNot(contains('davidvillegas')));
      expect(redacted, startsWith('key#'));
      expect(redacted, contains('len=${pii.length}'));
      // Deterministic for correlation, distinct for different keys.
      expect(Redaction.key(pii), redacted);
      expect(Redaction.key('other-key'), isNot(redacted));
    });

    test('Redaction.key is keyed, not a plain SHA-256 prefix', () {
      // An unkeyed hash lets anyone hash candidate emails and match them
      // against a log. The fingerprint must not be recomputable that way.
      const String pii = 'user:davidvillegas15@gmail.com';
      final String plainSha = sha256
          .convert(utf8.encode(pii))
          .bytes
          .take(4)
          .map((int b) => b.toRadixString(16).padLeft(2, '0'))
          .join();
      final String redacted = Redaction.key(pii);
      expect(redacted, matches(RegExp(r'^key#[0-9a-f]{8}\(len=\d+\)$')));
      expect(redacted, isNot(contains(plainSha)));
      // Stable for the lifetime of the isolate.
      for (int i = 0; i < 100; i++) {
        expect(Redaction.key(pii), redacted);
      }
    });

    test('Redaction.key fingerprints are well distributed', () {
      final Set<String> fingerprints = <String>{
        for (int i = 0; i < 200; i++) Redaction.key('user:$i@example.com'),
      };
      expect(fingerprints, hasLength(200));
    });

    test('Redaction.keyBytes matches key for the same UTF-8 bytes', () {
      const String key = 'user:42';
      expect(
        Redaction.keyBytes(Uint8List.fromList(utf8.encode(key))),
        Redaction.key(key),
      );
    });

    test('Redaction.value reveals only type and size', () {
      expect(Redaction.value(null), '<null>');
      expect(Redaction.value('secret data'), isNot(contains('secret')));
      expect(Redaction.value('secret data'), contains('11 chars'));
      expect(
        Redaction.value(<String, dynamic>{'ssn': '123'}),
        isNot(contains('123')),
      );
      expect(Redaction.value(<int>[1, 2, 3]), contains('3 bytes'));
    });
  });

  group('no key material in logs', () {
    test('encryption operations never log key or passphrase bytes', () async {
      final MemoryLogOutput out = MemoryLogOutput();
      ReaxLogger.root.configure(
        level: LogLevel.debug,
        outputs: <LogOutput>[out],
      );
      addTearDown(() {
        ReaxLogger.root.configure(level: LogLevel.info, outputs: <LogOutput>[]);
      });

      const String passphrase = 'ultra-secret-passphrase-XYZZY';
      final Uint8List key = Uint8List.fromList(
        List<int>.generate(32, (int i) => 0xA5),
      );
      final Uint8List salt = Uint8List.fromList(
        List<int>.generate(16, (int i) => i),
      );

      final EncryptionEngine e1 = EncryptionEngine(
        EncryptionConfig.aes256(key: key),
      );
      final EncryptionEngine e2 = EncryptionEngine(
        EncryptionConfig.aes256FromPassphrase(
          passphrase: passphrase,
          iterations: 1000,
        ),
        kdfSalt: salt,
      );
      final Uint8List data = Uint8List.fromList(utf8.encode('hello'));
      e1.decrypt(e1.encrypt(data));
      e2.decrypt(e2.encrypt(data));
      e1.getMetadata();

      final String allLogs = out.logs
          .map((LogEntry e) => '${e.message} ${e.metadata ?? ''}')
          .join('\n');
      expect(allLogs, isNot(contains('XYZZY')));
      expect(allLogs, isNot(contains(passphrase)));
      // Hex or decimal renderings of the constant-byte key would contain
      // repeated a5/165 runs; the simplest guarantee is that nothing about
      // these operations was logged at all.
      expect(out.logs, isEmpty);
    });

    test('getMetadata contains no key material fields', () {
      final EncryptionEngine engine = EncryptionEngine(
        EncryptionConfig.aes256(
          key: Uint8List.fromList(List<int>.generate(32, (int i) => i)),
        ),
      );
      final String rendered = engine.getMetadata().toString().toLowerCase();
      expect(rendered, isNot(contains('key')));
      expect(rendered, isNot(contains('passphrase')));
      expect(rendered, isNot(contains('salt')));
    });
  });
}

class _ClosableOutput extends MemoryLogOutput {
  bool closed = false;

  @override
  Future<void> close() async {
    closed = true;
  }
}

class _ThrowingOutput extends LogOutput {
  @override
  Future<void> write(
    LogLevel level,
    String message, {
    Map<String, dynamic>? metadata,
  }) async {
    throw StateError('output failure');
  }
}
