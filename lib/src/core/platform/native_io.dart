/// The few POSIX calls `dart:io` does not expose: `flock(2)` and `fsync(2)`
/// on a directory.
///
/// Both are reached through `dart:ffi` against the C library that every
/// Dart process already links; no native code ships with the package. When
/// a symbol cannot be resolved (or on Windows, where neither call exists)
/// the helpers report that they are unavailable and callers fall back to
/// `dart:io`.
library;

import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

typedef _OpenC = Int32 Function(Pointer<Uint8> path, Int32 flags);
typedef _OpenDart = int Function(Pointer<Uint8> path, int flags);
typedef _IntIntC = Int32 Function(Int32 a, Int32 b);
typedef _IntIntDart = int Function(int a, int b);
typedef _IntC = Int32 Function(Int32 a);
typedef _IntDart = int Function(int a);
typedef _MallocC = Pointer<Uint8> Function(IntPtr size);
typedef _MallocDart = Pointer<Uint8> Function(int size);
typedef _FreeC = Void Function(Pointer<Uint8> pointer);
typedef _FreeDart = void Function(Pointer<Uint8> pointer);
typedef _ChmodC = Int32 Function(Pointer<Uint8> path, Uint32 mode);
typedef _ChmodDart = int Function(Pointer<Uint8> path, int mode);
typedef _ErrnoC = Pointer<Int32> Function();
typedef _ErrnoDart = Pointer<Int32> Function();

/// Thin bindings to the POSIX calls ReaxDB needs, or null when unavailable.
final class NativeIo {
  NativeIo._(
    this._open,
    this._close,
    this._flock,
    this._fsync,
    this._malloc,
    this._free,
    this._errno,
    this.closeFinalizer,
    this._chmod,
  );

  static const int _oRdonly = 0;
  static const int _oRdwr = 2;
  static const int _lockEx = 2;
  static const int _lockNb = 4;
  static const int _lockUn = 8;

  final _OpenDart _open;
  final _IntDart _close;
  final _IntIntDart _flock;
  final _IntDart _fsync;
  final _MallocDart _malloc;
  final _FreeDart _free;
  final _ErrnoDart? _errno;
  final _ChmodDart _chmod;

  /// Closes a file descriptor passed as the finalizer token, so a lock held
  /// by an isolate that dies without closing its database is released when
  /// that isolate's heap goes away.
  final NativeFinalizer closeFinalizer;

  /// The bindings for this process, or null on Windows or when a symbol is
  /// missing.
  static final NativeIo? instance = _load();

  static NativeIo? _load() {
    if (Platform.isWindows) return null;
    if (!(Platform.isLinux ||
        Platform.isAndroid ||
        Platform.isMacOS ||
        Platform.isIOS)) {
      return null;
    }
    try {
      final DynamicLibrary libc = DynamicLibrary.process();
      _ErrnoDart? errno;
      for (final String name in const <String>[
        '__error', // Apple
        '__errno_location', // glibc, musl
        '__errno', // bionic
      ]) {
        if (libc.providesSymbol(name)) {
          errno = libc.lookupFunction<_ErrnoC, _ErrnoDart>(name);
          break;
        }
      }
      return NativeIo._(
        libc.lookupFunction<_OpenC, _OpenDart>('open'),
        libc.lookupFunction<_IntC, _IntDart>('close'),
        libc.lookupFunction<_IntIntC, _IntIntDart>('flock'),
        libc.lookupFunction<_IntC, _IntDart>('fsync'),
        libc.lookupFunction<_MallocC, _MallocDart>('malloc'),
        libc.lookupFunction<_FreeC, _FreeDart>('free'),
        errno,
        NativeFinalizer(libc.lookup<NativeFinalizerFunction>('close')),
        libc.lookupFunction<_ChmodC, _ChmodDart>('chmod'),
      );
    } catch (_) {
      return null;
    }
  }

  /// `O_CLOEXEC`, so a child process started with `Process.run` never
  /// inherits (and keeps holding) a database lock.
  static int get _oCloexec =>
      Platform.isMacOS || Platform.isIOS ? 0x01000000 : 0x00080000;

  /// `EWOULDBLOCK` / `EAGAIN`.
  static int get _eWouldBlock => Platform.isMacOS || Platform.isIOS ? 35 : 11;

  int get _lastErrno {
    final _ErrnoDart? errno = _errno;
    return errno == null ? -1 : errno().value;
  }

  int _openPath(String path, int flags) {
    final List<int> bytes = utf8.encode(path);
    final Pointer<Uint8> native = _malloc(bytes.length + 1);
    if (native.address == 0) return -1;
    try {
      final Uint8List view = native.asTypedList(bytes.length + 1);
      view.setAll(0, bytes);
      view[bytes.length] = 0;
      return _open(native, flags | _oCloexec);
    } finally {
      _free(native);
    }
  }

  /// Takes an exclusive, non-blocking `flock` on the existing file [path].
  ///
  /// Returns the file descriptor holding the lock, [lockBusy] when another
  /// open file description holds it (another process, another isolate, or
  /// another open of the same directory in this isolate), or [lockUnsupported]
  /// when the file system does not support `flock`.
  int tryLockExclusive(String path) {
    final int fd = _openPath(path, _oRdwr);
    if (fd < 0) return lockUnsupported;
    if (_flock(fd, _lockEx | _lockNb) == 0) return fd;
    final int errno = _lastErrno;
    _close(fd);
    return errno == _eWouldBlock ? lockBusy : lockUnsupported;
  }

  /// Releases the lock taken by [tryLockExclusive] and closes [fd].
  void unlock(int fd) {
    _flock(fd, _lockUn);
    _close(fd);
  }

  /// Sets the permission bits of [path] to [mode]. Returns false on failure.
  bool chmod(String path, int mode) {
    final List<int> bytes = utf8.encode(path);
    final Pointer<Uint8> native = _malloc(bytes.length + 1);
    if (native.address == 0) return false;
    try {
      final Uint8List view = native.asTypedList(bytes.length + 1);
      view.setAll(0, bytes);
      view[bytes.length] = 0;
      return _chmod(native, mode) == 0;
    } finally {
      _free(native);
    }
  }

  /// fsyncs the directory [path], making renames, creations and deletions
  /// inside it durable. Returns false when the directory cannot be opened.
  bool syncDirectory(String path) {
    final int fd = _openPath(path, _oRdonly);
    if (fd < 0) return false;
    try {
      return _fsync(fd) == 0;
    } finally {
      _close(fd);
    }
  }

  /// [tryLockExclusive] result: the lock is held by someone else.
  static const int lockBusy = -2;

  /// [tryLockExclusive] result: `flock` cannot be used on this file.
  static const int lockUnsupported = -3;
}

/// fsyncs the directory [path] when the platform allows it.
///
/// POSIX only makes a rename or a file creation durable once its parent
/// directory is fsynced; `dart:io` cannot open a directory, so this goes
/// through [NativeIo]. A no-op where unavailable (Windows, whose NTFS
/// journals metadata itself).
void syncDirectory(String path) {
  NativeIo.instance?.syncDirectory(path);
}

/// Makes [path] readable and writable by its owner only: `0700` for a
/// directory, `0600` for a file. Best effort, and a no-op on Windows, which
/// has no POSIX permission bits.
///
/// Files ReaxDB creates inside a database directory get the process's
/// default permissions, often world-readable; a `0700` database directory
/// keeps other users out of all of them.
void restrictToOwner(String path, {required bool directory}) {
  NativeIo.instance?.chmod(path, directory ? 0x1C0 : 0x180);
}
