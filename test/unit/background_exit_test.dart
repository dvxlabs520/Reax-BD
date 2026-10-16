// Runs a separate Dart process, which takes a while to compile on CI.
@Timeout(Duration(minutes: 2))
library;

import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

void main() {
  late Directory root;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_background_exit_');
  });

  tearDown(() async {
    await ReaxDB.closeAll();
    await deleteTempDirectory(root);
  });

  test(
    'a background database left open does not keep the process alive',
    () async {
      final String path = p.join(root.path, 'left');
      final ProcessResult result = await Process.run(
        Platform.resolvedExecutable,
        <String>['run', 'test/support/background_left_open.dart', path],
      );
      expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
      expect('${result.stdout}', contains('done'));
      final ReaxDB db = await ReaxDB.open(path: path);
      expect(await db.get<int>('a'), 1);
    },
  );
}
