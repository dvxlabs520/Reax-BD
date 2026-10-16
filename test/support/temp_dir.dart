/// Cleanup of temporary directories in tests that abandon an engine.
library;

import 'dart:io';

/// Deletes [dir] recursively.
///
/// Fault-injection tests abandon an engine without closing it, to simulate
/// a crash, so its files stay open until the test process exits. POSIX
/// deletes open files; Windows refuses with a sharing violation. There the
/// deletion is retried briefly and then left to the OS temp cleanup.
Future<void> deleteTempDirectory(Directory dir) async {
  for (int attempt = 1; ; attempt++) {
    try {
      if (await dir.exists()) await dir.delete(recursive: true);
      return;
    } on FileSystemException {
      if (!Platform.isWindows) rethrow;
      if (attempt >= 5) return;
      await Future<void>.delayed(Duration(milliseconds: 50 * attempt));
    }
  }
}
