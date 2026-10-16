import 'dart:math';

import 'package:reaxdb_dart/src/core/indexing/secondary_index.dart';
import 'package:test/test.dart';

/// The definition of the total order: the encodings compared bytewise, with
/// integers beyond 2^53 that encode alike compared exactly.
int reference(Object? a, Object? b) {
  final int encoded = compareBytes(
    IndexValueCodec.encodeValue(a),
    IndexValueCodec.encodeValue(b),
  );
  if (encoded != 0) return encoded.sign;
  if (a is int && b is int) return a.compareTo(b).sign;
  if (a is num && b is num) {
    final double d = (a is double ? a : b) as double;
    if (a is double && b is double) return 0;
    if (!d.isFinite) return 0;
    return a is int
        ? BigInt.from(a).compareTo(BigInt.from(d)).sign
        : BigInt.from(d).compareTo(BigInt.from(b as int)).sign;
  }
  return 0;
}

void main() {
  final Random random = Random(42);
  const int big = 9007199254740992; // 2^53

  Object? randomScalar() {
    switch (random.nextInt(9)) {
      case 0:
        return null;
      case 1:
        return random.nextBool();
      case 2:
        return random.nextInt(7) - 3;
      case 3:
        return <double>[
          0.0,
          -0.0,
          1.0,
          -1.5,
          2.5,
          double.infinity,
          double.negativeInfinity,
          double.nan,
          big.toDouble(),
        ][random.nextInt(9)];
      case 4:
        return big + random.nextInt(4) - 1;
      case 5:
        return -big - random.nextInt(3);
      case 6:
        return String.fromCharCodes(<int>[
          for (int i = 0; i < random.nextInt(4); i++)
            <int>[0x00, 0x41, 0x62, 0xE9, 0xE000, 0xFFFD, 0x1F600][random
                .nextInt(7)],
        ]);
      case 7:
        return <Object?>[random.nextInt(3), 'x'];
      default:
        return <String, Object?>{'k': random.nextInt(3)};
    }
  }

  test('compareValues matches the encoded order for every scalar pair', () {
    for (int i = 0; i < 20000; i++) {
      final Object? a = randomScalar();
      final Object? b = randomScalar();
      expect(compareValues(a, b).sign, reference(a, b), reason: '$a vs $b');
      expect(valuesEqual(a, b), reference(a, b) == 0, reason: '$a == $b');
    }
  });

  test('a few cases written out', () {
    expect(compareValues(null, false), lessThan(0));
    expect(compareValues(true, 0), lessThan(0));
    expect(compareValues(1, '1'), lessThan(0));
    expect(compareValues(1, 1.0), 0);
    expect(compareValues(-0.0, 0.0), 0);
    expect(compareValues(big, big + 1), lessThan(0));
    expect(compareValues(big.toDouble(), big + 1), lessThan(0));
    expect(compareValues('�', '😀'), lessThan(0));
    expect(compareValues('a', 'a\u0000'), lessThan(0));
    expect(compareValues(double.nan, double.infinity), greaterThan(0));
  });

  test('extractFieldValue reads plain and dotted paths', () {
    final Map<String, dynamic> doc = <String, dynamic>{
      'a': 1,
      'b': <String, dynamic>{'c': 2},
      'b.c': 3,
    };
    expect(extractFieldValue(doc, 'a'), 1);
    expect(extractFieldValue(doc, 'b.c'), 2);
    expect(extractFieldValue(doc, 'missing'), isNull);
    expect(extractFieldValue(doc, 'a.x'), isNull);
  });
}
