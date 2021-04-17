import 'dart:io';
import 'dart:typed_data';

/// Pointer into the underlying storage. Used to simplify tracking elsewhere
/// and to enforce the limits
class Pointer {
  /// Number of bytes used to store offset
  // ignore: non_constant_identifier_names
  static final int OFFSET_WIDTH = 8;

  /// Number of bytes used to store length
  // ignore: non_constant_identifier_names
  static final int LENGTH_WIDTH = 8;

  /// Total width of a pointer
  // ignore: non_constant_identifier_names
  static final int WIDTH = OFFSET_WIDTH + LENGTH_WIDTH;

  /// Mask applied to offsets to avoid overflow etc.
  // ignore: non_constant_identifier_names
  static final int OFFSET_MASK = 0x0fffffffffffffff;

  /// Mask applied to length to avoid overflow etc.
  // ignore: non_constant_identifier_names
  static final int LENGTH_MASK = 0x00000000ffffffff;

  final int _offset;
  final int _length;

  /// Constructor
  Pointer(int offset, int length)
      : _offset = offset & OFFSET_MASK,
        _length = length & LENGTH_MASK;

  /// Get the length of the pointer
  int get length => _length;

  /// Get the offset of the pointer
  int get offset => _offset;

  /// Get the start of the pointer (same as offset)
  int get start => _offset;

  /// Get the end of the pointer. Used to simplify code elsewhere.
  int get end => _offset + _length - 1; // starting is 0

  /// Check if a pointer is empty
  bool get isEmpty => _length == 0;

  /// Check if a pointer is not empty
  bool get isNotEmpty => !isEmpty;

  /// Empty pointer
  // ignore: non_constant_identifier_names
  static Pointer NIL = Pointer(0, 0);
}

/// A block of storage in the underlying file
class Block {
  final Pointer _pointer;
  final Uint8List _buffer;
  final ByteData _data;

  /// Get the position of the end of the block
  int get end => pointer.end;

  /// Get the offset/start of a lock
  int get offset => pointer.offset;

  /// Get the length of a block
  int get length => pointer.length;

  /// Get the data associated with a block in a convenient form
  ByteData get data => _data;

  /// Get the raw buffer holding block data
  Uint8List get buffer => _buffer;

  /// Get the underlying pointer for the block
  Pointer get pointer => _pointer;

  /// Create a block at a given position using the given data. Here [pointer]
  /// length and the size of [buffer] should be the same.
  Block(Pointer pointer, Uint8List buffer)
      : _buffer = buffer,
        _pointer = pointer,
        _data = ByteData.view(buffer.buffer);

  /// Read this block of data in
  void read(RandomAccessFile file) {
    file.setPositionSync(offset);
    file.readIntoSync(buffer, 0, length);
  }

  /// Write this block of data out
  void write(RandomAccessFile file) {
    file.setPositionSync(offset);
    file.writeFromSync(buffer, 0, length);
  }
}

/// A block of pointers. Useful as shorthand for setting values directly.
class PointerBlock extends Block {
  late final Uint64List _view;

  /// Constructor
  PointerBlock(Pointer pointer) : super(pointer, Uint8List(pointer.length)) {
    _view = buffer.buffer.asUint64List();
  }

  /// Get the number of pointers held in a block
  int get count => (_view.length / 2).floor();

  /// Access to pointers held in the block
  Pointer operator [](int i) => Pointer(_view[i * 2], _view[(i * 2) + 1]);

  /// Set the value of a pointer within the block
  void operator []=(int i, Pointer value) {
    _view[i * 2] = value.offset;
    _view[(i * 2) + 1] = value.length;
  } // set
}
