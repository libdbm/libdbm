import 'dart:typed_data';

import 'constants.dart';

/// Magic number for B+tree nodes.
// ignore: constant_identifier_names
const int _NODE_MAGIC = DBMConstants.BTREE_NODE_MAGIC;

/// Type byte for internal nodes.
// ignore: constant_identifier_names
const int _INTERNAL = 0x01;

/// Type byte for leaf nodes.
// ignore: constant_identifier_names
const int _LEAF = 0x02;

/// Header size shared by both node types: [magic:8][type:1][count:2][pad:5]
// ignore: constant_identifier_names
const int _HEADER = 16;

/// A leaf node in the B+tree. Stores key-value pairs and next/previous
/// pointers for a doubly-linked leaf chain.
///
/// Layout:
/// ```
/// [magic:8][type:1=0x02][count:2][pad:5] = 16 byte header
/// [next:8][previous:8]                   = 16 bytes leaf pointers
/// entries[]:
///   [key_len:4][val_len:4][key_data][val_data]  per entry
/// ```
class LeafNode {
  /// The node ID (sequential uint64 used as key in the underlying store).
  final int id;

  /// Keys stored in sorted order.
  final List<Uint8List> keys;

  /// Values corresponding to each key.
  final List<Uint8List> values;

  /// Pointer (node ID) to the next leaf, or 0 if none.
  int next;

  /// Pointer (node ID) to the previous leaf, or 0 if none.
  int previous;

  // Retains the backing buffer so zero-copy views remain valid.
  // ignore: unused_field
  final Uint8List? _source;

  /// Constructor.
  LeafNode(this.id, this.keys, this.values,
      {this.next = 0, this.previous = 0, Uint8List? source})
      : _source = source;

  /// Serialize this leaf node to bytes.
  Uint8List encode() {
    var size = _HEADER + 16; // header + next/previous
    for (var i = 0; i < keys.length; i++) {
      size += 8 + keys[i].length + values[i].length;
    }
    final buffer = Uint8List(size);
    final data = ByteData.view(buffer.buffer);

    data.setUint64(0, _NODE_MAGIC);
    data.setUint8(8, _LEAF);
    data.setUint16(9, keys.length);
    // pad bytes 11..15 are zero

    data.setUint64(_HEADER, next);
    data.setUint64(_HEADER + 8, previous);

    var offset = _HEADER + 16;
    for (var i = 0; i < keys.length; i++) {
      data.setUint32(offset, keys[i].length);
      data.setUint32(offset + 4, values[i].length);
      offset += 8;
      buffer.setRange(offset, offset + keys[i].length, keys[i]);
      offset += keys[i].length;
      buffer.setRange(offset, offset + values[i].length, values[i]);
      offset += values[i].length;
    }

    return buffer;
  }

  /// Decode a leaf node from bytes.
  static LeafNode decode(final int id, final Uint8List buffer) {
    final data = ByteData.view(buffer.buffer, buffer.offsetInBytes);
    final magic = data.getUint64(0);
    if (magic != _NODE_MAGIC) {
      throw StateError('LeafNode magic mismatch: $magic');
    }
    final type = data.getUint8(8);
    if (type != _LEAF) {
      throw StateError('Expected leaf node type $_LEAF, got $type');
    }
    final count = data.getUint16(9);
    final next = data.getUint64(_HEADER);
    final previous = data.getUint64(_HEADER + 8);

    final keys = <Uint8List>[];
    final values = <Uint8List>[];
    final base = buffer.offsetInBytes;
    var offset = _HEADER + 16;
    for (var i = 0; i < count; i++) {
      final klen = data.getUint32(offset);
      final vlen = data.getUint32(offset + 4);
      offset += 8;
      keys.add(buffer.buffer.asUint8List(base + offset, klen));
      offset += klen;
      values.add(buffer.buffer.asUint8List(base + offset, vlen));
      offset += vlen;
    }

    return LeafNode(id, keys, values,
        next: next, previous: previous, source: buffer);
  }
}

/// An internal (non-leaf) node in the B+tree. Stores separator keys and
/// child pointers.
///
/// Layout:
/// ```
/// [magic:8][type:1=0x01][count:2][pad:5] = 16 byte header
/// [child_0:8]                            = first child pointer
/// entries[]:
///   [key_len:4][key_data][child_{i+1}:8]  per entry
/// ```
class InternalNode {
  /// The node ID.
  final int id;

  /// Separator keys. `children.length == keys.length + 1`.
  final List<Uint8List> keys;

  /// Child node IDs. First child is before keys[0], last child is after
  /// keys[last].
  final List<int> children;

  // Retains the backing buffer so zero-copy views remain valid.
  // ignore: unused_field
  final Uint8List? _source;

  /// Constructor.
  InternalNode(this.id, this.keys, this.children, {Uint8List? source})
      : _source = source;

  /// Serialize this internal node to bytes.
  Uint8List encode() {
    var size = _HEADER + 8; // header + child_0
    for (var i = 0; i < keys.length; i++) {
      size += 4 + keys[i].length + 8; // key_len + key_data + child_{i+1}
    }
    final buffer = Uint8List(size);
    final data = ByteData.view(buffer.buffer);

    data.setUint64(0, _NODE_MAGIC);
    data.setUint8(8, _INTERNAL);
    data.setUint16(9, keys.length);
    // pad bytes 11..15 are zero

    data.setUint64(_HEADER, children[0]);

    var offset = _HEADER + 8;
    for (var i = 0; i < keys.length; i++) {
      data.setUint32(offset, keys[i].length);
      offset += 4;
      buffer.setRange(offset, offset + keys[i].length, keys[i]);
      offset += keys[i].length;
      data.setUint64(offset, children[i + 1]);
      offset += 8;
    }

    return buffer;
  }

  /// Decode an internal node from bytes.
  static InternalNode decode(final int id, final Uint8List buffer) {
    final data = ByteData.view(buffer.buffer, buffer.offsetInBytes);
    final magic = data.getUint64(0);
    if (magic != _NODE_MAGIC) {
      throw StateError('InternalNode magic mismatch: $magic');
    }
    final type = data.getUint8(8);
    if (type != _INTERNAL) {
      throw StateError('Expected internal node type $_INTERNAL, got $type');
    }
    final count = data.getUint16(9);
    final children = <int>[data.getUint64(_HEADER)];
    final keys = <Uint8List>[];

    final base = buffer.offsetInBytes;
    var offset = _HEADER + 8;
    for (var i = 0; i < count; i++) {
      final klen = data.getUint32(offset);
      offset += 4;
      keys.add(buffer.buffer.asUint8List(base + offset, klen));
      offset += klen;
      children.add(data.getUint64(offset));
      offset += 8;
    }

    return InternalNode(id, keys, children, source: buffer);
  }
}

/// Determine whether a serialized node buffer represents a leaf.
bool isLeaf(final Uint8List buffer) {
  final data = ByteData.view(buffer.buffer, buffer.offsetInBytes);
  return data.getUint8(8) == _LEAF;
}
