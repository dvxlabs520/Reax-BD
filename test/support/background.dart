/// Helpers for tests that also run with the engine in a background isolate
/// (`test/background_suite_test.dart`).
library;

import 'package:reaxdb_dart/src/reaxdb.dart' show debugOpenInBackground;

/// Why a test is skipped when the suite runs in background mode, or null.
///
/// I/O fault hooks are static state of the test isolate; with
/// `background: true` the engine runs in another isolate and never sees
/// them.
String? get skipFaultInjectionInBackground => debugOpenInBackground
    ? 'I/O fault hooks do not reach a background engine isolate'
    : null;
