# ReaxDB

A database for Flutter and Dart that does not lose your data, with queries,
transactions and encryption. Pure Dart, no native code, no code generation.

[![pub package](https://img.shields.io/pub/v/reaxdb_dart.svg)](https://pub.dev/packages/reaxdb_dart)
[![CI](https://github.com/dvxlabs520/Reax-BD/actions/workflows/ci.yml/badge.svg)](https://github.com/dvxlabs520/Reax-BD/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Start in 30 seconds

### In a Flutter app

```bash
flutter pub add reaxdb_flutter
```

```dart
import 'package:reaxdb_flutter/reaxdb_flutter.dart';

final db = await ReaxFlutter.open('app');

await db.put('user:1', {'name': 'Ada', 'age': 36});
final user = await db.get<Map<String, dynamic>>('user:1');
```

That is all. [`reaxdb_flutter`](https://pub.dev/packages/reaxdb_flutter)
puts the database in the app's private folder. Pass `encrypted: true` and it
also creates an encryption key and keeps it in the Keychain or Keystore.

### In Dart (server, command line)

```bash
dart pub add reaxdb_dart
```

```dart
import 'package:reaxdb_dart/reaxdb_dart.dart';

final db = await ReaxDB.open(path: '/absolute/path/to/data');
```

Use an absolute path. In Flutter without `reaxdb_flutter`, get one from
[`path_provider`](https://pub.dev/packages/path_provider): on iOS and Android
a relative path points to a read-only folder, and `open` fails with a
`StorageException`.

Needs Dart 3.12 or later (Flutter 3.44 or later). Runs on Android, iOS,
macOS, Windows, Linux and the Dart VM. It does not run on the web.

## What you get

- **Your data is really saved.** Every write goes to a log on disk and is
  flushed before `put` returns. A crash or a power cut does not lose it
  (with [one limit on Apple devices](ADVANCED.md#durability-contract)).
- **Queries.** Filter, sort and page documents, with indexes to make it
  fast.
- **Transactions.** Change several keys at once: all of the changes are
  saved, or none.
- **Encryption.** AES-256-GCM, which also detects tampering.
- **Live updates.** Watch a key, a prefix or a whole query and rebuild your
  UI when it changes.
- **Expiry, backups and migrations.** Entries can expire, a database can be
  exported to one file and restored, and a schema version runs your
  migration code.

## ReaxDB or Hive?

Both are pure Dart. This table compares documented features, with
`hive_ce` 2.22.0 (October 2026).

| | ReaxDB | Hive (`hive_ce`) |
| --- | --- | --- |
| Each write flushed to disk before it returns | Yes, by default | No, call `flush()` |
| Queries and indexes | Yes | No, key/value only |
| Transactions over several keys | Yes | Not in the documented API |
| Encryption | AES-256-GCM (authenticated) | AES-256-CBC |
| Entries that expire (TTL) | Yes | No |
| Typed objects | `toJson`/`fromJson`, no code generation | `TypeAdapter`s, usually generated |
| Reads | Cached, data on disk; or every value in memory with `keepInMemory: true` (2.5.0) | Normal boxes keep every value in memory |
| Background isolate | Yes, `background: true` (2.6.0) | Yes (`IsolatedHive`) |
| Web | No | Yes (IndexedDB) |

Speed, measured on the same machine with 100,000 documents (details and
smaller sizes in [BENCHMARKS.md](BENCHMARKS.md#reaxdb-and-hive)):

| | ReaxDB | ReaxDB, `keepInMemory` | Hive |
| --- | ---: | ---: | ---: |
| Durable write | 19.0k/s | 19.2k/s | 23.5k/s (`put` + `flush`) |
| Batch of 500 | 199.5k/s | | 309.1k/s |
| Read | 44.3k/s | 0.99M/s | 0.78M/s |
| Open | 11 ms | 177 ms | 261 ms |
| Query, 20,000 results | 449 ms | 24 ms | 3.9 ms (`values.where`) |

Choose Hive if you need the web, or the fastest possible iteration over
everything you store. Choose ReaxDB if losing a write is not acceptable, or
if you need queries, transactions or expiry.

## Common tasks

### Save, read and delete

```dart
await db.put('settings:theme', 'dark');
final theme = await db.get<String>('settings:theme');

await db.putBatch({'a': 1, 'b': 2}); // one atomic write
await db.delete('settings:theme');
final there = await db.exists('settings:theme'); // false
```

Keys are strings. Values can be strings, numbers, booleans, null, bytes, or
maps and lists of those.

### Go through many entries

```dart
await for (final entry in db.scanPrefix<Map<String, dynamic>>('users:')) {
  print('${entry.key}: ${entry.value}');
}
final firstPage = await db.range<int>('a', 'm', limit: 50);
```

### Documents and queries

A document is a map stored under `collection:id`.

```dart
await db.put('users:1', {'name': 'Ada', 'city': 'London', 'age': 36});
await db.put('users:2', {'name': 'Alan', 'city': 'London', 'age': 41});

await db.createIndex('users', ['city']); // optional, makes the query fast

final londoners = await db.query('users')
    .whereEquals('city', 'London')
    .orderBy('age', descending: true)
    .limit(20)
    .find();
```

### Typed objects

No code generation: give ReaxDB your `fromJson` and `toJson`.

```dart
class User {
  User({required this.name, required this.age});
  factory User.fromJson(Map<String, dynamic> json) =>
      User(name: json['name'] as String, age: json['age'] as int);
  final String name;
  final int age;
  Map<String, dynamic> toJson() => {'name': name, 'age': age};
}

final users = db.collection<User>(
  'users',
  fromJson: User.fromJson,
  toJson: (user) => user.toJson(),
);

await users.put('1', User(name: 'Ada', age: 36));
final ada = await users.get('1');
final adults = await users.where('age', QueryOperator.greaterThan, 18);
```

### Rebuild the UI when data changes

```dart
db.watch('users:*').listen((event) => print('${event.type} ${event.key}'));
```

With `reaxdb_flutter`, a query becomes a stream for `StreamBuilder`. It runs
again whenever the collection changes:

```dart
final stream = db.watchQuery(db.query('users').whereEquals('city', 'London'));
```

### Transactions

```dart
await db.transaction((tx) async {
  final from = await tx.get<Map<String, dynamic>>('account:1');
  final to = await tx.get<Map<String, dynamic>>('account:2');
  await tx.put('account:1', {...from!, 'balance': from['balance'] - 50});
  await tx.put('account:2', {...to!, 'balance': to['balance'] + 50});
}, isolationLevel: IsolationLevel.serializable);
```

Use `IsolationLevel.serializable` when you read a value and write it back,
as above. The default level is faster but can lose one of two concurrent
updates. [ADVANCED.md](ADVANCED.md#isolation-levels) explains every level.

### Encryption

In Flutter, `ReaxFlutter.open('app', encrypted: true)` handles the key for
you. In Dart, pass a 32-byte key or a passphrase:

```dart
final db = await ReaxDB.open(
  path: '/absolute/path/to/data',
  encryption: EncryptionConfig.aes256(key: key32Bytes),
  // or: EncryptionConfig.aes256FromPassphrase(passphrase: secret),
);
```

Values are encrypted. Keys and indexed fields are not, so do not put secrets
in keys and do not index a secret field. A wrong key fails at `open`.

To change the key (2.5.0), call `rekey`. It returns the database opened with
the new key:

```dart
db = await db.rekey(EncryptionConfig.aes256(key: newKey32Bytes));
// or: db = await db.changePassphrase('new secret');
```

### Entries that expire

```dart
await db.put('session:abc', token, ttl: const Duration(minutes: 30));
```

### Backup and restore

```dart
await db.exportTo('/backups/app.rxdb');

final restored = await ReaxDB.importFrom(
  archivePath: '/backups/app.rxdb',
  path: '/absolute/path/to/restored',
);
```

The backup file is not encrypted unless you pass `encryption:` to
`exportTo` (and the same to `importFrom` as `archiveEncryption:`).

### Schema migrations

```dart
final db = await ReaxDB.open(
  path: '/absolute/path/to/data',
  schemaVersion: 2,
  onUpgrade: (from, to, db) async {
    // Update your documents from version `from` to `to` here.
  },
);
```

### Keep everything in memory (2.5.0)

For data that fits in memory and is read far more than it is written, keep
every value decoded in memory, like a Hive box:

```dart
final db = await ReaxDB.open(path: '/absolute/path/to/data', keepInMemory: true);

final theme = db.getSync<String>('settings:theme'); // no await, e.g. in build()
```

Reads and queries then never touch the disk. Writes are still saved to disk
first, exactly as before. Opening takes longer, because it loads everything.
See [ADVANCED.md](ADVANCED.md#keeping-every-value-in-memory).

### Keep the UI isolate free (2.6.0)

```dart
final db = await ReaxDB.open(
  path: '/absolute/path/to/data',
  background: true,
  keepInMemory: true, // optional; getSync keeps working
);
```

The engine (write-ahead log, flushes and compactions, encryption, indexes)
then runs in an isolate of its own, so a long compaction never freezes the
UI. The API is the same; each call costs a few microseconds more. See
[ADVANCED.md](ADVANCED.md#running-the-engine-in-a-background-isolate).

### Faster writes when you can afford to lose the last ones

```dart
final db = await ReaxDB.open(path: '/absolute/path/to/cache', syncMode: SyncMode.os);
```

`SyncMode.full` (the default) survives power loss, `SyncMode.os` survives the
app being killed, and `SyncMode.none` is for throwaway data. Writing many
values with `putBatch` is fast in every mode.

## Good to know

- **One instance per database.** Opening a database that is already open,
  in this or another process, throws `DatabaseLockedException`. Close it
  with `await db.close()`.
- **Errors are typed.** Every failure is a `ReaxDbException`, never a silent
  `null`.
- **Upgrading from 1.x?** Your data migrates, encrypted data too. See
  [MIGRATION.md](MIGRATION.md).

## Documentation

- [ADVANCED.md](ADVANCED.md): durability, isolation levels, encryption
  details, tuning, errors and internals
- [BENCHMARKS.md](BENCHMARKS.md): measured performance and how it was
  measured
- [MIGRATION.md](MIGRATION.md): moving from 1.x
- [CHANGELOG.md](CHANGELOG.md)

## Contributing

Issues and pull requests are welcome at
<https://github.com/dvxlabs520/Reax-BD>. A bug fix should come with a test
that fails before it and passes after.

## License

MIT. See [LICENSE](LICENSE).
