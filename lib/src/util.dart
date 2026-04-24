import 'dart:typed_data';

/// Given a size, adjust the size to fit into the specified alignment by
/// rounding up as needed.
int align(int size, int alignment) {
  if (alignment <= 0) {
    throw ArgumentError.value(alignment, 'alignment', 'must be > 0');
  }
  size = size.abs();
  final padding = (alignment - (size % alignment)) % alignment;
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
  var shift = 0;
  for (var i = 0; i < data.length; i++) {
    value = (value + (data[i] << shift)) & 0x7fffffff;
    shift += 5;
    if (shift >= 24) shift -= 24;
  }
  return (1103515243 * value + 12345) & 0x7fffffff;
}

/// Lexicographic comparison of two byte lists. Returns negative if [a] < [b],
/// zero if equal, positive if [a] > [b].
int compare(Uint8List a, Uint8List b) {
  final length = a.length < b.length ? a.length : b.length;
  for (var i = 0; i < length; i++) {
    final diff = a[i] - b[i];
    if (diff != 0) return diff;
  }
  return a.length - b.length;
}

final List<int> _crc32Table = () {
  final table = List<int>.filled(256, 0);
  for (var i = 0; i < 256; i++) {
    var c = i;
    for (var k = 0; k < 8; k++) {
      c = (c & 1) != 0 ? (0xedb88320 ^ (c >> 1)) : (c >> 1);
    }
    table[i] = c;
  }
  return table;
}();

/// Calculate a 32 bit CRC for the given message
int crc32(final Uint8List message) {
  var crc = 0xffffffff;
  for (var i = 0; i < message.length; i++) {
    crc = _crc32Table[(crc ^ message[i]) & 0xff] ^ (crc >> 8);
  }
  return ~crc & 0xffffffff;
}
