/// Test seam for injecting I/O failures into the storage layer.
///
/// Production code never installs a hook; the guarded call sites then cost a
/// single null check. Tests set [IoFaults.hook] to simulate `ENOSPC`/`EIO`
/// at precise points (a partially written WAL frame, a manifest that cannot
/// be replaced, an SSTable whose rename fails) and must reset it afterwards.
library;

import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

/// Storage I/O operations that can be made to fail through [IoFaults.hook].
enum IoOp {
  /// Writing framed records to a WAL segment.
  walWrite,

  /// Fsyncing a WAL segment.
  walSync,

  /// Truncating a WAL segment back to its last good offset.
  walTruncate,

  /// Creating a new WAL segment (header write).
  walCreate,

  /// Writing an SSTable's temporary file.
  sstableWrite,

  /// Renaming an SSTable's temporary file into place.
  sstableRename,

  /// Writing the manifest's temporary file.
  manifestWrite,

  /// Renaming the manifest's temporary file into place.
  manifestRename,
}

/// Decides whether a guarded operation fails.
///
/// Called with the operation, the file it targets and, for writes, the
/// number of bytes about to be written. Return null to let the operation
/// proceed. Any non-null value makes it fail with an `ENOSPC`
/// [FileSystemException]; for write operations the value is the number of
/// leading bytes that reach the file first (a torn write). The hook may also
/// throw its own error.
typedef IoFaultHook = int? Function(IoOp op, String path, int length);

/// Global fault-injection switch for storage I/O. See the library docs.
abstract final class IoFaults {
  /// The installed hook, or null (the production default).
  static IoFaultHook? hook;

  static FileSystemException _failure(IoOp op, String path) =>
      FileSystemException(
        'Injected I/O failure (${op.name})',
        path,
        const OSError('No space left on device', 28),
      );

  /// Throws if the hook rejects [op] on [path].
  static void check(IoOp op, String path) {
    final h = hook;
    if (h != null && h(op, path, 0) != null) throw _failure(op, path);
  }

  /// Writes [bytes] to [raf], honoring the hook for [op].
  static Future<void> write(
    RandomAccessFile raf,
    Uint8List bytes,
    IoOp op,
    String path,
  ) async {
    final h = hook;
    if (h != null) {
      final allowed = h(op, path, bytes.length);
      if (allowed != null) {
        final n = math.min(math.max(allowed, 0), bytes.length);
        if (n > 0) await raf.writeFrom(bytes, 0, n);
        throw _failure(op, path);
      }
    }
    await raf.writeFrom(bytes);
  }
}
