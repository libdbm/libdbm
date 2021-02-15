import 'dart:typed_data';

int align(int size, int alignment) {
  size = size.abs();
  final padding = alignment - (size % alignment);
  return size + padding;
}

bool matches(Uint8List a, Uint8List b) {
  if(a == null || b == null) return false;
  if (a.length != b.length) return false;

  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

int hash(Uint8List data) {
  var value = 0x238f13af * data.length;
  for (var i = 0; i < data.length; i++) {
    value = (value + (data[i] << (i * 5 % 24))) & 0x7fffffff;
  }
  return (1103515243 * value + 12345) & 0x7fffffff;
}

int crc32(Uint8List message) {
  int crc, mask;

  crc = 0xffffffff;
  for (var byte in message) {
    crc = crc ^ byte;
    for (var j = 7; j >= 0; j--) {
      mask = -(crc & 1);
      crc = (crc >> 1) ^ (0xedb88320 & mask);
    }
  }
  return ~crc & 0xffffffff;
}
