import 'dart:typed_data';

/// Given a size, adjust the size to fit into the specified alignment by
/// rounding up as needed.
int align(int size, int alignment) {
  size = size.abs();
  final padding = alignment - (size % alignment);
  return size + padding;
}

/// Check to see if two lists match exactly. This is done as a function to
/// allow updates/changes to the matching as needed
bool matches(Uint8List a, Uint8List b) {
  if (identical(a, b)) {
    return true;
  }

  if (a.length != b.length) {
    return false;
  }

  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) {
      return false;
    }
  }

  return true;
}

/// Hash the given list and return a hash code. This function is based on other
/// DBM implementations
int hash(Uint8List data) {
  var value = 0x238f13af * data.length;
  for (var i = 0; i < data.length; i++) {
    value = (value + (data[i] << (i * 5 % 24))) & 0x7fffffff;
  }
  return (1103515243 * value + 12345) & 0x7fffffff;
}

/// Calculate a 32 bit CRC for the given message
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
