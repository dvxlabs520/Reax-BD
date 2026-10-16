import 'dart:io';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

/// The permission bits of [path], as an octal string such as `700`.
String mode(String path) =>
    (FileStat.statSync(path).mode & 0x1FF).toRadixString(8);

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_permissions_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    await deleteTempDirectory(root);
  });

  test('the database directory and exports are owner-only', () async {
    final ReaxDB db = await ReaxDB.open(path: '${root.path}/db');
    await db.put('k', 'v');
    expect(mode('${root.path}/db'), '700');

    await db.exportTo('${root.path}/backup.rxdb');
    expect(mode('${root.path}/backup.rxdb'), '600');
  }, testOn: '!windows');
}
