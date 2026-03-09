import 'dart:io';
import 'dart:typed_data';

import '../dbm.dart';
import 'constants.dart';
import 'io.dart';
import 'util.dart';

/// On-disk format for a set of key-value changes (a delta).
///
/// Layout:
/// ```
/// +0x00 (8b)  magic
/// +0x08 (8b)  version number
/// +0x10 (8b)  timestamp (ms since epoch)
/// +0x18 (4b)  entry count
/// +0x1C (4b)  flags (reserved)
/// +0x20..     entries[], each:
///               key_length   (4b)
///               value_length (4b) — 0xFFFFFFFF = tombstone
///               key_data     (key_length bytes)
///               value_data   (value_length bytes)
/// ```
class DeltaBlock {
  /// Magic number for delta blocks.
  // ignore: constant_identifier_names
  static const int MAGIC = DBMConstants.DELTA_BLOCK_MAGIC;

  // ignore: non_constant_identifier_names
  static final int _MAGIC_OFFSET = 0;
  // ignore: non_constant_identifier_names
  static final int _VERSION_OFFSET = _MAGIC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _TIMESTAMP_OFFSET = _VERSION_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _COUNT_OFFSET = _TIMESTAMP_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _FLAGS_OFFSET = _COUNT_OFFSET + 4;
  // ignore: non_constant_identifier_names
  static final int _ENTRIES_OFFSET = _FLAGS_OFFSET + 4;

  /// Tombstone marker in value_length field.
  // ignore: constant_identifier_names
  static const int TOMBSTONE_MARKER = 0xFFFFFFFF;

  /// The underlying block on disk.
  final Block block;

  /// Construct from an existing block read from disk.
  DeltaBlock._(this.block);

  ByteData get _data => block.data;

  /// The version this delta represents.
  int get version => _data.getUint64(_VERSION_OFFSET);

  /// The timestamp when this delta was created.
  int get timestamp => _data.getUint64(_TIMESTAMP_OFFSET);

  /// Number of entries in this delta.
  int get count => _data.getUint32(_COUNT_OFFSET);

  /// Decode all entries from this delta block.
  Map<BytesKey, Uint8List?> decode() {
    final result = <BytesKey, Uint8List?>{};
    final buffer = block.buffer;
    final length = buffer.length;
    var offset = _ENTRIES_OFFSET;
    final n = count;

    for (var i = 0; i < n; i++) {
      if (offset + 8 > length) {
        throw DBMException(
            500,
            'DeltaBlock corrupt: entry header at $offset '
            'exceeds block size $length');
      }
      final klen = _data.getUint32(offset);
      final vlen = _data.getUint32(offset + 4);
      offset += 8;

      if (offset + klen > length) {
        throw DBMException(
            500,
            'DeltaBlock corrupt: key at $offset '
            'exceeds block size $length');
      }
      final key = Uint8List.fromList(buffer.sublist(offset, offset + klen));
      offset += klen;

      if (vlen == TOMBSTONE_MARKER) {
        result[BytesKey(key)] = null;
      } else {
        if (offset + vlen > length) {
          throw DBMException(
              500,
              'DeltaBlock corrupt: value at $offset '
              'exceeds block size $length');
        }
        final value = Uint8List.fromList(buffer.sublist(offset, offset + vlen));
        offset += vlen;
        result[BytesKey(key)] = value;
      }
    }
    return result;
  }

  /// Look up a single key in this delta. Returns the value if found,
  /// [tombstone] if deleted, or null if the key is not in this delta.
  Uint8List? lookup(final Uint8List key) {
    final buffer = block.buffer;
    final length = buffer.length;
    var offset = _ENTRIES_OFFSET;
    final n = count;

    for (var i = 0; i < n; i++) {
      if (offset + 8 > length) {
        throw DBMException(
            500,
            'DeltaBlock corrupt: entry header at $offset '
            'exceeds block size $length');
      }
      final klen = _data.getUint32(offset);
      final vlen = _data.getUint32(offset + 4);
      offset += 8;

      if (offset + klen > length) {
        throw DBMException(
            500,
            'DeltaBlock corrupt: key at $offset '
            'exceeds block size $length');
      }
      final k = buffer.sublist(offset, offset + klen);
      offset += klen;

      if (matches(key, k)) {
        if (vlen == TOMBSTONE_MARKER) return tombstone;
        if (offset + vlen > length) {
          throw DBMException(
              500,
              'DeltaBlock corrupt: value at $offset '
              'exceeds block size $length');
        }
        return Uint8List.fromList(buffer.sublist(offset, offset + vlen));
      }
      if (vlen != TOMBSTONE_MARKER) offset += vlen;
    }
    return null;
  }

  /// Calculate the required buffer size for the given entries.
  static int required(final Map<BytesKey, Uint8List?> entries) {
    var size = _ENTRIES_OFFSET;
    for (final entry in entries.entries) {
      size += 8 + entry.key.bytes.length;
      if (entry.value != null) size += entry.value!.length;
    }
    return size;
  }

  /// Serialize entries into a new delta block buffer.
  static DeltaBlock encode(final int version, final int timestamp,
      final Map<BytesKey, Uint8List?> entries, final Pointer pointer) {
    final buffer = Uint8List(pointer.length);
    final data = ByteData.view(buffer.buffer);

    data.setUint64(_MAGIC_OFFSET, MAGIC);
    data.setUint64(_VERSION_OFFSET, version);
    data.setUint64(_TIMESTAMP_OFFSET, timestamp);
    data.setUint32(_COUNT_OFFSET, entries.length);
    data.setUint32(_FLAGS_OFFSET, 0);

    var offset = _ENTRIES_OFFSET;
    for (final entry in entries.entries) {
      final key = entry.key.bytes;
      final value = entry.value;
      data.setUint32(offset, key.length);
      data.setUint32(
          offset + 4, value == null ? TOMBSTONE_MARKER : value.length);
      offset += 8;
      buffer.setRange(offset, offset + key.length, key);
      offset += key.length;
      if (value != null) {
        buffer.setRange(offset, offset + value.length, value);
        offset += value.length;
      }
    }

    final block = Block(pointer, buffer);
    return DeltaBlock._(block);
  }

  /// Read a delta block from disk.
  static DeltaBlock read(final RandomAccessFile file, final Pointer pointer) {
    final block = Block(pointer, Uint8List(pointer.length));
    block.read(file);
    final magic = block.data.getUint64(_MAGIC_OFFSET);
    if (magic != MAGIC) {
      throw DBMException(500, 'DeltaBlock magic mismatch: $magic');
    }
    return DeltaBlock._(block);
  }
}
