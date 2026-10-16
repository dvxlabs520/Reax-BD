/// String comparison in code point order.
library;

/// Compares [a] and [b] by code point, which is the order of their UTF-8
/// bytes, and so the order the storage engine and the index encoding use.
///
/// Plain `String.compareTo` compares UTF-16 code units, which puts
/// characters above U+FFFF (stored as surrogate pairs) before U+E000 to
/// U+FFFF. Remapping both ranges when two units differ fixes that.
int compareCodePoints(String a, String b) {
  final int length = a.length < b.length ? a.length : b.length;
  for (int i = 0; i < length; i++) {
    int x = a.codeUnitAt(i);
    int y = b.codeUnitAt(i);
    if (x == y) continue;
    if (x >= 0xD800 && y >= 0xD800) {
      x = x >= 0xE000 ? x - 0x800 : x + 0x2000;
      y = y >= 0xE000 ? y - 0x800 : y + 0x2000;
    }
    return x - y;
  }
  return a.length - b.length;
}
