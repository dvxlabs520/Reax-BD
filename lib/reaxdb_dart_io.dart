/// `dart:io`-dependent additions to ReaxDB.
///
/// The main `package:reaxdb_dart/reaxdb_dart.dart` barrel already depends
/// on `dart:io` (the storage engine writes files), so ReaxDB does not run on
/// the web. This entry point only adds [FileLogOutput], kept separate so the
/// main barrel's surface stays small.
library;

export 'src/core/logging/file_log_output.dart';
