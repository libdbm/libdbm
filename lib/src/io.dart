import 'dart:io';
import 'dart:typed_data';

/// Pointer into the underlying storage. Used to simplify tracking elsewhere
/// and to enforce the limits
class Pointer {
  static int OFFSET_WIDTH = 8;
  static int LENGTH_WIDTH = 8;
  static int WIDTH = OFFSET_WIDTH + LENGTH_WIDTH;

  static int OFFSET_MASK = 0x0fffffffffffffff;
  static int LENGTH_MASK = 0x00000000ffffffff;

  final int _offset;
  final int _length;

  Pointer(int offset, int length)
      : _offset = offset & OFFSET_MASK,
        _length = length & LENGTH_MASK;

  int get length => _length;
  int get offset => _offset;
  int get start => _offset;
  int get end => _offset + _length - 1; // starting is 0

  bool get isEmpty => _length == 0;
  bool get isNotEmpty => !isEmpty;

  /// Empty pointer
  static Pointer NIL = Pointer(0, 0);
}

/// A block of storage in the underlying file
class Block {
  final Pointer _pointer;
  final Uint8List _buffer;
  final ByteData _data;

  int get end => pointer.end;
  int get offset => pointer.offset;
  int get length => pointer.length;

  ByteData get data => _data;
  Uint8List get buffer => _buffer;
  Pointer get pointer => _pointer;

  Block(Pointer pointer, Uint8List buffer)
      : _buffer = buffer,
        _pointer = pointer,
        _data = ByteData.view(buffer.buffer);

  /// Read this block of data in
  void read(RandomAccessFile file) {
    file.setPositionSync(offset);
    file.readIntoSync(buffer, 0, length);
  }

  // Write this block of data out
  void write(RandomAccessFile file) {
    file.setPositionSync(offset);
    file.writeFromSync(buffer, 0, length);
  }
}

/// A block of pointers. Useful as shorthand for setting values directly.
class PointerBlock extends Block {
  late final Uint64List _view;

  PointerBlock(Pointer pointer) : super(pointer, Uint8List(pointer.length)) {
    _view = buffer.buffer.asUint64List();
  }

  int get count => (_view.length / 2).floor();

  Pointer operator [](int i) => Pointer(_view[i * 2], _view[(i * 2) + 1]);
  operator []=(int i, Pointer value) {
    _view[i * 2] = value.offset;
    _view[(i * 2) + 1] = value.length;
  } // set
}
