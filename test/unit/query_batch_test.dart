import 'dart:io';

import 'package:reaxdb_dart/reaxdb_dart.dart';
import 'package:reaxdb_dart/src/core/storage/io_faults.dart';
import 'package:test/test.dart';

import '../support/temp_dir.dart';

void main() {
  late Directory root;
  late ReaxDB db;

  setUp(() async {
    root = await Directory.systemTemp.createTemp('reaxdb_query_batch_');
    db = await ReaxDB.open(path: '${root.path}/db');
    await db.putBatch(<String, Object?>{
      for (int i = 0; i < 20; i++) 'items:$i': {'n': i, 'group': i % 3},
    });
  });

  tearDown(() async {
    IoFaults.hook = null;
    await ReaxDB.closeAll();
    await deleteTempDirectory(root);
  });

  Future<int> countWalWrites(Future<void> Function() action) async {
    int writes = 0;
    IoFaults.hook = (IoOp op, String _, int _) {
      if (op == IoOp.walWrite) writes++;
      return null;
    };
    try {
      await action();
    } finally {
      IoFaults.hook = null;
    }
    return writes;
  }

  test('update writes every matching document in one batch', () async {
    final int writes = await countWalWrites(() async {
      final int updated = await db.query('items').whereLessThan('n', 10).update(
        <String, dynamic>{'flag': true},
      );
      expect(updated, 10);
    });
    expect(writes, 1);
    expect(await db.query('items').whereEquals('flag', true).count(), 10);
  });

  test('delete removes every matching document in one batch', () async {
    final int writes = await countWalWrites(() async {
      expect(await db.query('items').whereEquals('group', 0).delete(), 7);
    });
    expect(writes, 1);
    expect(await db.query('items').count(), 13);
  });

  test('distinct and groupBy keep large integers apart', () async {
    const int big = 9007199254740992; // 2^53
    await db.put('big:1', {'v': big});
    await db.put('big:2', {'v': big + 1});
    await db.put('big:3', {'v': big + 1});
    await db.put('big:4', {'v': 1.0});
    await db.put('big:5', {'v': 1});

    final List<dynamic> values = await db.query('big').distinct('v');
    expect(values, hasLength(3));
    expect(values.where((dynamic v) => v == big + 1), hasLength(1));

    final List<GroupByResult> groups =
        await db
                .query('big')
                .aggregate((AggregationBuilder a) => a.groupBy('v').count())
                .executeAggregation()
            as List<GroupByResult>;
    expect(groups, hasLength(3));
    expect(
      groups.firstWhere((GroupByResult g) => g.groupKey == big + 1).documents,
      hasLength(2),
    );
  });
}
