import 'dart:io';
import 'dart:typed_data';

import 'package:libdbm/libdbm.dart';
import 'io.dart';
import 'util.dart';

/// Header for the memory pool
class MemoryPoolHeader extends Block {
  static int MAGIC = 0xa0c0a1e5ced0da7a;
  static int HEADER_SIZE = 128;

  static int MAGIC_OFFSET = 0;
  static int PAGE_START_OFFSET = MAGIC_OFFSET + 8;
  static int PAGE_LENGTH_OFFSET = PAGE_START_OFFSET + 8;

  MemoryPoolHeader(int offset)
      : super(Pointer(offset, HEADER_SIZE), Uint8List(HEADER_SIZE)) {
    magic = MAGIC;
    page = Pointer.NIL;
  }

  int get magic => data.getUint64(MAGIC_OFFSET);
  set magic(int v) => data.setUint64(MAGIC_OFFSET, v);

  Pointer get page => Pointer(
      data.getUint64(PAGE_START_OFFSET), data.getUint64(PAGE_LENGTH_OFFSET));

  set page(Pointer pointer) {
    data.setUint64(PAGE_START_OFFSET, pointer.offset);
    data.setUint64(PAGE_LENGTH_OFFSET, pointer.length);
  }
}

/// Class for managing blocks of storage on persistent media. This could be
/// abstracted/extracted out as an interface
class MemoryPool {
  static int ALIGNMENT = 128;
  static int SPLIT_THRESHOLD = 512;
  static int SIZE = MemoryPoolHeader.HEADER_SIZE;
  final MemoryPoolHeader _header;
  final RandomAccessFile _file;
  final _pointers = <Pointer>[];

  MemoryPool(this._file, offset) : _header = MemoryPoolHeader(offset) {
    final length = _file.lengthSync();
    if (length < _header.end) {
      _header.write(_file);
    } else {
      _header.read(_file);
      if (_header.magic != MemoryPoolHeader.MAGIC) {
        throw DBMException(
            500, 'MemoryPoolHeader magic mismatch: ${_header.magic}');
      }
    }

    // If we already have some data, load it in
    if (_header.page.isNotEmpty) {
      _read();
    }
  }

  int get end => _header.end;
  int get length => _pointers.length;
  Pointer operator [](int index) => _pointers[index];

  /// Remove all pointers and clear the pointer page
  void clear() {
    _pointers.clear();
    _header.page = Pointer.NIL;
  }

  /// Write everything to dick
  void flush() {
    _write();
  }

  /// Allocate a block of data. This will look in the list of existing blocks
  /// first, and if a suitable block is finished, the data will be reused. If
  /// a large block is found, it is split. Block are aligned to 128 byte
  /// boundaries.
  Pointer allocate(int size) {
    size = align(size, ALIGNMENT);
    for (var i = 0; i < _pointers.length; i++) {
      if (_pointers[i].length >= size) {
        var ptr = _pointers.removeAt(i);
        // Split a big block to help with data reuse
        if (ptr.length - size > SPLIT_THRESHOLD) {
          free(Pointer(ptr.offset + size, ptr.length - size));
          ptr = Pointer(ptr.offset, size);
        }
        return ptr;
      }
    }

    return Pointer(_file.lengthSync(), align(size, ALIGNMENT));
  }

  /// Free a pointer. If adjacent block are free, they are merged into larger
  /// blocks to encourage reuse.
  void free(Pointer pointer) {
    _pointers.add(pointer);

    // Sort by offset ascending
    _pointers.sort((a, b) => a.offset - b.offset);

    // Merge adjacent blocks
    for (var i = 0; i < _pointers.length - 1;) {
      if (_pointers[i].end == _pointers[i + 1].start - 1) {
        var a = _pointers.removeAt(i);
        var b = _pointers.removeAt(i);
        final p = Pointer(a.offset, a.length + b.length);
        _pointers.insert(i, p);
      } else {
        i += 1;
      }
    }

    // Sort by length ascending
    _pointers.sort((a, b) => a.length - b.length);
  }

  /// Flush everything out to disk
  void _write() {
    if (_pointers.isNotEmpty) {
      var block = PointerBlock(_header.page);
      // If we need to allocate/reallocate the pointer block
      if (block.count < _pointers.length) {
        if (_header.page.isNotEmpty) free(_header.page);
        final required = align(_pointers.length, 256) * Pointer.WIDTH;
        _header.page = allocate(required);
        block = PointerBlock(_header.page);
      }

      // Write all the pointers into the block, including empty pointers
      for (var i = 0; i < _pointers.length; i++) {
        block[i] = _pointers[i];
      }

      // Write the block to the file
      block.write(_file);
    }
    _header.write(_file);
  }

  /// read in the block of pointers.
  void _read() {
    _pointers.clear();
    if (_header.page.isEmpty) return;
    final block = PointerBlock(_header.page);
    block.read(_file);
    for (var i = 0; i < block.count; i++) {
      final ptr = block[i];
      // Only add non-empty pointers
      if (ptr.isNotEmpty) _pointers.add(ptr);
    }
  }
}
