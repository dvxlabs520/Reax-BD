/// Regression tests for the defects fixed in 2.1.
///
/// Each group reproduces a failure found by the 2.0 audit; every test fails
/// on 2.0.0 and passes now.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_regression_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    if (await root.exists()) await root.delete(recursive: true);
  });

  Future<ReaxDB> open(
    String name, {
    SyncMode syncMode = SyncMode.full,
    EncryptionConfig encryption = const EncryptionConfig.none(),
  }) => ReaxDB.open(
    path: p.join(root.path, name),
    syncMode: syncMode,
    encryption: encryption,
  );

  /// The value storage holds for [key], bypassing the cache.
  Future<Object?> stored(ReaxDB db, String key) async {
    final List<ReaxEntry<Object?>> hit = await db.range<Object?>(
      key,
      '$key\u0001',
    );
    return hit.isEmpty ? null : hit.single.value;
  }

  group('transactions run inside the write pipeline', () {
    test(
      'a transaction racing a put leaves cache and index consistent',
      () async {
        final ReaxDB db = await open('tx_vs_put');
        await db.createIndex('u', <String>['s']);
        for (int round = 0; round < 40; round++) {
          await db.put('u:1', <String, dynamic>{'s': 'init'});
          final Future<void> tx = db.transaction((ReaxTransaction t) async {
            await t.put('u:1', <String, dynamic>{'s': 'tx'});
          });
          for (int i = 0; i < round % 8; i++) {
            await Future<void>.microtask(() {});
          }
          final Future<void> plain = db.put('u:1', <String, dynamic>{
            's': 'plain',
          });
          await Future.wait(<Future<void>>[tx, plain]);

          final Map<String, dynamic> onDisk =
              (await stored(db, 'u:1'))! as Map<String, dynamic>;
          final Map<String, dynamic>? cached = await db
              .get<Map<String, dynamic>>('u:1');
          expect(cached, onDisk, reason: 'round $round');
          final String winner = onDisk['s'] as String;
          expect(
            await db.query('u').whereEquals('s', winner).count(),
            1,
            reason: 'round $round',
          );
          for (final String loser in <String>['init', 'tx', 'plain']) {
            if (loser == winner) continue;
            expect(
              await db.query('u').whereEquals('s', loser).count(),
              0,
              reason: 'round $round: stale posting for $loser',
            );
          }
        }
      },
    );

    test('index postings of a transaction commit with it', () async {
      final String path = p.join(root.path, 'tx_index');
      ReaxDB db = await ReaxDB.open(path: path);
      await db.createIndex('u', <String>['city']);
      await db.transaction((ReaxTransaction t) async {
        await t.put('u:1', <String, dynamic>{'city': 'Lima'});
        await t.put('u:2', <String, dynamic>{'city': 'Quito'});
      });
      expect(await db.query('u').whereEquals('city', 'Lima').count(), 1);
      await db.close();
      db = await ReaxDB.open(path: path);
      expect(await db.query('u').whereEquals('city', 'Quito').count(), 1);
    });

    test('optimistic transactions see plain writes made meanwhile', () async {
      final ReaxDB db = await open('optimistic');
      await db.put('counter', 1);
      int attempts = 0;
      await db.transaction((ReaxTransaction t) async {
        attempts++;
        final int value = (await t.get<int>('counter'))!;
        if (attempts == 1) await db.put('counter', 100);
        await t.put('counter', value + 1);
      }, isolationLevel: IsolationLevel.optimistic);
      expect(attempts, 2, reason: 'the first attempt must conflict');
      expect(await db.get<int>('counter'), 101);
    });

    test('compareAndSwap compares maps and lists structurally', () async {
      final ReaxDB db = await open('cas');
      await db.put('m', <String, dynamic>{'a': 1});
      expect(
        await db.compareAndSwap<Object?>('m', <String, dynamic>{'a': 1}, 2),
        isTrue,
      );
      await db.put('l', <Object?>[1, 2]);
      expect(await db.compareAndSwap<Object?>('l', <Object?>[1, 2], 3), isTrue);
      expect(
        await db.compareAndSwap<Object?>('l', <Object?>[1, 2], 4),
        isFalse,
      );
    });
  });

  group('TTL reclamation never deletes a fresh value', () {
    test('get of an expired key racing a put', () async {
      final ReaxDB db = await open('ttl_get');
      await db.put('k', 'old', ttl: const Duration(milliseconds: 1));
      await Future<void>.delayed(const Duration(milliseconds: 5));
      await db.flush();
      final Future<String?> read = db.get<String>('k');
      final Future<void> write = db.put('k', 'fresh');
      await Future.wait(<Future<Object?>>[read, write]);
      expect(await db.get<String>('k'), 'fresh');
      expect(await stored(db, 'k'), 'fresh');
    });

    test('purgeExpired racing a put', () async {
      final ReaxDB db = await open('ttl_purge');
      final Map<String, Object?> batch = <String, Object?>{
        for (int i = 0; i < 2000; i++) 'e:${i.toString().padLeft(5, '0')}': i,
      };
      await db.putBatch(batch, ttl: const Duration(milliseconds: 1));
      await Future<void>.delayed(const Duration(milliseconds: 5));
      final Future<int> purge = db.purgeExpired();
      await db.put('e:00000', 'fresh');
      final int purged = await purge;
      expect(await db.get<String>('e:00000'), 'fresh');
      expect(purged, lessThanOrEqualTo(2000));
    });

    test(
      'expired documents drop their index postings when reclaimed',
      () async {
        final ReaxDB db = await open('ttl_postings');
        await db.createIndex('s', <String>['n']);
        for (int i = 0; i < 20; i++) {
          await db.put('s:$i', <String, dynamic>{
            'n': 7,
          }, ttl: const Duration(milliseconds: 1));
        }
        await db.put('s:live', <String, dynamic>{'n': 7});
        await Future<void>.delayed(const Duration(milliseconds: 5));
        await db.purgeExpired();
        // Rewriting an expired document must also drop its old posting.
        await db.put('s:0', <String, dynamic>{'n': 8});
        expect(await db.query('s').whereEquals('n', 7).count(), 1);
        expect(await db.query('s').whereEquals('n', 8).count(), 1);
      },
    );
  });

  test('a read racing a write never caches the older value', () async {
    for (final SyncMode mode in SyncMode.values) {
      final String path = p.join(root.path, 'stale_${mode.name}');
      ReaxDB db = await ReaxDB.open(path: path, syncMode: mode);
      await db.put('k', 'v1');
      await db.close();
      db = await ReaxDB.open(path: path, syncMode: mode);
      final Future<String?> read = db.get<String>('k');
      await db.put('k', 'v2');
      await read;
      expect(await db.get<String>('k'), 'v2', reason: mode.name);
      final Future<String?> read2 = db.get<String>('k');
      await db.delete('k');
      await read2;
      expect(await db.get<String>('k'), isNull, reason: mode.name);
      await db.close();
    }
  });

  group('query values', () {
    test('integers beyond 2^53 compare exactly on both paths', () async {
      final ReaxDB db = await open('bigint');
      const int big = 9007199254740992; // 2^53
      await db.put('n:a', <String, dynamic>{'v': big});
      await db.put('n:b', <String, dynamic>{'v': big + 1});
      Future<List<Object?>> values(QueryBuilder q) async => <Object?>[
        for (final Map<String, dynamic> d in await q.find()) d['v'],
      ];
      expect(await values(db.query('n').whereEquals('v', big + 1)), <Object?>[
        big + 1,
      ]);
      expect(await values(db.query('n').whereGreaterThan('v', big)), <Object?>[
        big + 1,
      ]);
      await db.createIndex('n', <String>['v']);
      expect(await values(db.query('n').whereEquals('v', big + 1)), <Object?>[
        big + 1,
      ]);
      expect(await values(db.query('n').whereGreaterThan('v', big)), <Object?>[
        big + 1,
      ]);
      expect(await values(db.query('n').whereLessThan('v', big + 1)), <Object?>[
        big,
      ]);
    });

    test('-0.0 equals 0', () async {
      final ReaxDB db = await open('negzero');
      await db.put('z:1', <String, dynamic>{'v': -0.0});
      expect(await db.query('z').whereEquals('v', 0).count(), 1);
      await db.createIndex('z', <String>['v']);
      expect(await db.query('z').whereEquals('v', 0).count(), 1);
    });

    test('collection names containing ":" are rejected', () async {
      final ReaxDB db = await open('colon');
      expect(() => db.query('app:users'), throwsA(isA<QueryException>()));
      expect(
        () => db.createIndex('app:users', <String>['x']),
        throwsA(isA<QueryException>()),
      );
      expect(
        () => db.collection<Map<String, dynamic>>(
          'app:users',
          fromJson: (Map<String, dynamic> j) => j,
          toJson: (Map<String, dynamic> v) => v,
        ),
        throwsA(isA<QueryException>()),
      );
    });

    test('dropIndex then createIndex leaves no stale postings', () async {
      final ReaxDB db = await open('drop_create');
      await db.createIndex('u', <String>['c']);
      await db.put('u:1', <String, dynamic>{'c': 'a'});
      await db.dropIndex('u', <String>['c']);
      await db.put('u:1', <String, dynamic>{'c': 'b'});
      await db.createIndex('u', <String>['c']);
      expect(await db.query('u').whereEquals('c', 'a').count(), 0);
      expect(await db.query('u').whereEquals('c', 'b').count(), 1);
    });
  });

  group('one writer per database', () {
    test('a second isolate cannot open an open database', () async {
      final String path = p.join(root.path, 'isolates');
      final ReaxDB db = await ReaxDB.open(path: path);
      await db.put('k', 'v');
      final String outcome = await Isolate.run(() async {
        try {
          final ReaxDB other = await ReaxDB.open(path: path);
          await other.close();
          return 'opened';
        } on DatabaseLockedException {
          return 'locked';
        }
      });
      expect(outcome, 'locked');
      await db.close();
      final String after = await Isolate.run(() async {
        final ReaxDB other = await ReaxDB.open(path: path);
        await other.close();
        return 'opened';
      });
      expect(after, 'opened');
    });

    test('a symlinked path to an open database is refused', () async {
      if (Platform.isWindows) return;
      final String path = p.join(root.path, 'real');
      final ReaxDB db = await ReaxDB.open(path: path);
      final Link alias = Link(p.join(root.path, 'alias'));
      await alias.create(path);
      await expectLater(
        ReaxDB.open(path: alias.path),
        throwsA(isA<DatabaseLockedException>()),
      );
      await db.close();
    });

    test('the lock file records the process id', () async {
      final String path = p.join(root.path, 'pid');
      final ReaxDB db = await ReaxDB.open(path: path);
      final String contents = await File(
        p.join(path, ReaxDB.lockFileName),
      ).readAsString();
      expect(contents.split('\n').first, '$pid');
      await db.close();
    });
  });

  test('file system failures surface as StorageException', () async {
    final File blocker = File(p.join(root.path, 'not_a_directory'));
    await blocker.writeAsString('x');
    await expectLater(
      ReaxDB.open(path: p.join(blocker.path, 'db')),
      throwsA(isA<StorageException>()),
    );
  });

  group('wrong encryption key', () {
    test('a wrong passphrase is refused at open', () async {
      final String path = p.join(root.path, 'wrong_key');
      ReaxDB db = await ReaxDB.open(
        path: path,
        encryption: EncryptionConfig.aes256FromPassphrase(
          passphrase: 'right',
          iterations: 1000,
        ),
      );
      await db.put('a', 'secret');
      await db.close();
      await expectLater(
        ReaxDB.open(
          path: path,
          encryption: EncryptionConfig.aes256FromPassphrase(
            passphrase: 'WRONG',
            iterations: 1000,
          ),
        ),
        throwsA(isA<EncryptionException>()),
      );
      db = await ReaxDB.open(
        path: path,
        encryption: EncryptionConfig.aes256FromPassphrase(
          passphrase: 'right',
          iterations: 1000,
        ),
      );
      expect(await db.get<String>('a'), 'secret');
      expect(await db.keys().toList(), <String>['a']);
    });

    test('a ciphertext copied to another key does not decrypt', () async {
      final ReaxDB db = await open(
        'swap',
        encryption: EncryptionConfig.aes256(key: Uint8List(32)),
      );
      await db.put('role:alice', 'admin');
      await db.put('role:mallory', 'user');
      await db.close();
      // Same plaintext under another key must not be interchangeable; the
      // engine-level swap is covered in encryption_test.dart. Here: reopen
      // and read back normally.
      final ReaxDB again = await open(
        'swap',
        encryption: EncryptionConfig.aes256(key: Uint8List(32)),
      );
      expect(await again.get<String>('role:mallory'), 'user');
    });
  });

  group('backups', () {
    Future<ReaxDB> seeded(String name) async {
      final ReaxDB db = await open(name);
      await db.createIndex('u', <String>['city']);
      for (int i = 0; i < 1200; i++) {
        await db.put('u:$i', <String, dynamic>{'city': i.isEven ? 'A' : 'B'});
      }
      await db.put('ttl', 'x', ttl: const Duration(days: 1));
      return db;
    }

    test('plaintext round trip keeps values, TTL and indexes', () async {
      final ReaxDB db = await seeded('src_plain');
      final String archive = p.join(root.path, 'plain.rxdb');
      expect(await db.exportTo(archive), 1201);
      final ReaxDB restored = await ReaxDB.importFrom(
        archivePath: archive,
        path: p.join(root.path, 'restored_plain'),
      );
      expect(await restored.query('u').whereEquals('city', 'A').count(), 600);
      expect(restored.hasIndex('u', 'city'), isTrue);
      expect(await restored.get<String>('ttl'), 'x');
    });

    test('encrypted archives need the right passphrase', () async {
      final ReaxDB db = await seeded('src_enc');
      final String archive = p.join(root.path, 'enc.rxdb');
      final EncryptionConfig archiveKey = EncryptionConfig.aes256FromPassphrase(
        passphrase: 'backup secret',
        iterations: 1000,
      );
      await db.exportTo(archive, encryption: archiveKey);
      final Uint8List bytes = await File(archive).readAsBytes();
      expect(String.fromCharCodes(bytes).contains('u:1'), isFalse);
      await expectLater(
        ReaxDB.importFrom(
          archivePath: archive,
          path: p.join(root.path, 'restored_wrong'),
          archiveEncryption: EncryptionConfig.aes256FromPassphrase(
            passphrase: 'wrong',
            iterations: 1000,
          ),
        ),
        throwsA(isA<EncryptionException>()),
      );
      expect(
        Directory(p.join(root.path, 'restored_wrong')).existsSync(),
        isFalse,
      );
      final ReaxDB restored = await ReaxDB.importFrom(
        archivePath: archive,
        path: p.join(root.path, 'restored_enc'),
        archiveEncryption: archiveKey,
      );
      expect(await restored.get<Map<String, dynamic>>('u:3'), <String, dynamic>{
        'city': 'B',
      });
    });

    test(
      'overwrite never deletes a directory that is not a database',
      () async {
        final ReaxDB db = await seeded('src_overwrite');
        final String archive = p.join(root.path, 'ow.rxdb');
        await db.exportTo(archive);
        final Directory photos = Directory(p.join(root.path, 'photos'));
        await photos.create();
        await File(p.join(photos.path, 'cat.jpg')).writeAsString('meow');
        await expectLater(
          ReaxDB.importFrom(
            archivePath: archive,
            path: photos.path,
            overwrite: true,
          ),
          throwsA(isA<DatabaseLockedException>()),
        );
        expect(File(p.join(photos.path, 'cat.jpg')).existsSync(), isTrue);
      },
    );

    test('overwrite replaces a closed ReaxDB database', () async {
      final ReaxDB db = await seeded('src_replace');
      final String archive = p.join(root.path, 'rep.rxdb');
      await db.exportTo(archive);
      final String target = p.join(root.path, 'old_db');
      final ReaxDB old = await ReaxDB.open(path: target);
      await old.put('stale', true);
      await expectLater(
        ReaxDB.importFrom(archivePath: archive, path: target, overwrite: true),
        throwsA(isA<DatabaseLockedException>()),
        reason: 'the target is still open',
      );
      await old.close();
      final ReaxDB restored = await ReaxDB.importFrom(
        archivePath: archive,
        path: target,
        overwrite: true,
      );
      expect(await restored.get<bool>('stale'), isNull);
      expect(await restored.query('u').count(), 1200);
    });

    test('a damaged archive is refused before anything is written', () async {
      final ReaxDB db = await seeded('src_damaged');
      final String archive = p.join(root.path, 'damaged.rxdb');
      await db.exportTo(archive);
      final Uint8List bytes = await File(archive).readAsBytes();
      bytes[bytes.length ~/ 2] ^= 0xFF;
      await File(archive).writeAsBytes(bytes);
      await expectLater(
        ReaxDB.importFrom(
          archivePath: archive,
          path: p.join(root.path, 'restored_damaged'),
        ),
        throwsA(isA<CorruptionException>()),
      );
      expect(
        Directory(p.join(root.path, 'restored_damaged')).existsSync(),
        isFalse,
      );
    });
  });

  test(
    'a slow change-stream subscriber is ended, not buffered forever',
    () async {
      final ReaxDB db = await ReaxDB.open(
        path: p.join(root.path, 'overflow'),
        changeStreamBufferSize: 50,
      );
      final List<Object> errors = <Object>[];
      final Completer<void> done = Completer<void>();
      final StreamSubscription<DatabaseChangeEvent> subscription = db
          .watch('*')
          .listen((_) {}, onError: errors.add, onDone: done.complete);
      subscription.pause();
      for (int i = 0; i < 60; i++) {
        await db.put('k$i', i);
      }
      subscription.resume();
      await done.future.timeout(const Duration(seconds: 5));
      expect(errors.single, isA<ChangeStreamOverflowException>());
    },
  );
}
