// Opens a database in the background, writes, and returns from main without
// closing it. Run by test/unit/background_test.dart, which expects the
// process to exit by itself.
import 'package:reaxdb_dart/reaxdb_dart.dart';

Future<void> main(List<String> args) async {
  final ReaxDB db = await ReaxDB.open(path: args.single, background: true);
  await db.put('a', 1);
  print('done');
}
