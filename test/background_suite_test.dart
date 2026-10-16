// The facade tests again, with every database opened in a background
// isolate (`ReaxDB.open(background: true)`).
//
// Tests that inject I/O faults are skipped here: the fault hooks are static
// state of the test isolate, and the engine runs in another one.
@Tags(<String>['background'])
library;

import 'package:reaxdb_dart/src/reaxdb.dart' show debugOpenInBackground;
import 'package:test/test.dart';

import 'integration_test.dart' as integration;
import 'legacy/legacy_migration_safety_test.dart' as legacy_safety;
import 'legacy/legacy_migration_test.dart' as legacy_migration;
import 'persistence_test.dart' as persistence;
import 'simple_api_test.dart' as simple_api;
import 'stress/compaction_stress_test.dart' as compaction_stress;
import 'stress/concurrency_stress_test.dart' as concurrency_stress;
import 'stress/lifecycle_stress_test.dart' as lifecycle_stress;
import 'transaction_api_test.dart' as transaction_api;
import 'unit/compat_test.dart' as compat;
import 'unit/error_handling_test.dart' as error_handling;
import 'unit/keep_in_memory_test.dart' as keep_in_memory;
import 'unit/permissions_test.dart' as permissions;
import 'unit/query_batch_test.dart' as query_batch;
import 'unit/reaxdb_test.dart' as reaxdb;
import 'unit/regression_2_1_test.dart' as regression_2_1;
import 'unit/rekey_test.dart' as rekey;

void main() {
  debugOpenInBackground = true;
  group('integration', integration.main);
  group('persistence', persistence.main);
  group('simple api', simple_api.main);
  group('transaction api', transaction_api.main);
  group('compat', compat.main);
  group('error handling', error_handling.main);
  group('keepInMemory', keep_in_memory.main);
  group('permissions', permissions.main);
  group('query batch', query_batch.main);
  group('reaxdb', reaxdb.main);
  group('regression 2.1', regression_2_1.main);
  group('rekey', rekey.main);
  group('compaction stress', compaction_stress.main);
  group('concurrency stress', concurrency_stress.main);
  group('lifecycle stress', lifecycle_stress.main);
  group('legacy migration', legacy_migration.main);
  group('legacy migration safety', legacy_safety.main);
}
