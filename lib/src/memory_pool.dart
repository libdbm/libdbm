import 'dart:io';
import 'dart:typed_data';

import '../libdbm.dart';
import 'constants.dart';
import 'io.dart';
import 'util.dart';

/// Header for the memory pool
class MemoryPoolHeader extends Block {
  /// Magic number of the memory pool
  // ignore: constant_identifier_names
  static const int MAGIC = DBMConstants.MEMORY_POOL_MAGIC;

  /// Size of the header for the memory pool
  // ignore: non_constant_identifier_names
  static final int HEADER_SIZE = 128;

  // ignore: non_constant_identifier_names
  static final int _MAGIC_OFFSET = 0;
  // ignore: non_constant_identifier_names
  static final int _PAGE_START_OFFSET = _MAGIC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _PAGE_LENGTH_OFFSET = _PAGE_START_OFFSET + 8;

  /// Create a header loaded from the given offset
  MemoryPoolHeader(int offset)
      : super(Pointer(offset, HEADER_SIZE), Uint8List(HEADER_SIZE)) {
    magic = MAGIC;
    page = Pointer.NIL;
  }

  /// Access to the magic number of the header
  int get magic => data.getUint64(_MAGIC_OFFSET);
  set magic(int v) => data.setUint64(_MAGIC_OFFSET, v);

  /// Access to the page pointer for the memory pool. Note that this can be
  /// changed, thereby allowing reallocation of a memory pool.
  Pointer get page => Pointer(
      data.getUint64(_PAGE_START_OFFSET), data.getUint64(_PAGE_LENGTH_OFFSET));
  set page(Pointer pointer) {
    data.setUint64(_PAGE_START_OFFSET, pointer.offset);
    data.setUint64(_PAGE_LENGTH_OFFSET, pointer.length);
  }
}

/// Class for managing blocks of storage on persistent media. This could be
/// abstracted/extracted out as an interface
class MemoryPool {
  /// Byte alignment of pages in the memory pool. All units will scale to
  /// fall on boundaries of this size.
  // ignore: non_constant_identifier_names
  static final int ALIGNMENT = 128;

  /// Size of the memory pool
  // ignore: non_constant_identifier_names
  static final int SIZE = MemoryPoolHeader.HEADER_SIZE;

  /// Number of extra bytes allocated which will force a block split.
  // ignore: non_constant_identifier_names
  static final int _SPLIT_THRESHOLD = 512;

  final MemoryPoolHeader _header;
  final RandomAccessFile _file;
  final _pointers = <Pointer>[];

  // -1 when clean; otherwise the lowest slot index whose on-disk value may
  // differ from [_pointers]. Mutations to the ordered free list always shift
  // slots from some index to the end, so a single lower bound is enough.
  int _firstDirty = -1;
  // Number of slots last persisted to the page, used to NIL-out trailing
  // entries when the list shrinks.
  int _lastWrittenCount = 0;
  bool _headerDirty = false;

  /// Create a memory pool
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
    _lastWrittenCount = _pointers.length;
  }

  void _markDirty(final int from) {
    if (_firstDirty == -1 || from < _firstDirty) _firstDirty = from;
  }

  /// Get the end of the header/memory pool
  int get end => _header.end;

  /// Get the number of pointers
  int get length => _pointers.length;

  /// Get a given pointer
  Pointer operator [](int index) => _pointers[index];

  /// Remove all pointers and clear the pointer page
  void clear() {
    if (_pointers.isNotEmpty || _header.page.isNotEmpty) {
      _markDirty(0);
      _headerDirty = true;
    }
    _pointers.clear();
    _header.page = Pointer.NIL;
  }

  /// Write everything to disk
  void flush() {
    _write();
  }

  /// Truncate trailing free blocks at end-of-file and return bytes reclaimed
  int compact() {
    if (_pointers.isEmpty) return 0;

    // List is already sorted by offset ascending
    final length = _file.lengthSync();
    var reclaimed = 0;

    while (_pointers.isNotEmpty) {
      final last = _pointers.last;
      if (last.end + 1 >= length - reclaimed) {
        reclaimed += last.length;
        _pointers.removeLast();
      } else {
        break;
      }
    }

    if (reclaimed > 0) {
      _markDirty(_pointers.length);
      _file.truncateSync(length - reclaimed);
    }

    return reclaimed;
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
        _markDirty(i);
        // Split a big block to help with data reuse
        if (ptr.length - size > _SPLIT_THRESHOLD) {
          free(Pointer(ptr.offset + size, ptr.length - size));
          ptr = Pointer(ptr.offset, size);
        }
        return ptr;
      }
    }

    return Pointer(_file.lengthSync(), align(size, ALIGNMENT));
  }

  /// Free a pointer. If adjacent blocks are free, they are merged into larger
  /// blocks to encourage reuse.
  void free(Pointer pointer) {
    // Insert in offset-sorted position via binary search
    var lo = 0;
    var hi = _pointers.length;
    while (lo < hi) {
      final mid = (lo + hi) >> 1;
      if (_pointers[mid].offset < pointer.offset) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    _pointers.insert(lo, pointer);
    _markDirty(lo);

    // Merge with right neighbour
    if (lo + 1 < _pointers.length &&
        _pointers[lo].end == _pointers[lo + 1].start - 1) {
      final a = _pointers.removeAt(lo);
      final b = _pointers.removeAt(lo);
      _pointers.insert(lo, Pointer(a.offset, a.length + b.length));
    }

    // Merge with left neighbour
    if (lo > 0 && _pointers[lo - 1].end == _pointers[lo].start - 1) {
      final a = _pointers.removeAt(lo - 1);
      final b = _pointers.removeAt(lo - 1);
      _pointers.insert(lo - 1, Pointer(a.offset, a.length + b.length));
      _markDirty(lo - 1);
    }
  }

  /// Flush everything out to disk. Skips I/O entirely when nothing has
  /// mutated since the last flush, and otherwise writes only the slot range
  /// that changed (plus the trailing NIL-out range when the list shrank).
  void _write() {
    // A freshly (re)allocated page may land on disk bytes that previously
    // held record data — those bytes must be overwritten with NIL in full,
    // or `_read()` on reopen will resurrect garbage into the free list.
    var fullRewrite = false;

    if (_header.page.isEmpty && _pointers.isNotEmpty) {
      final required = align(_pointers.length, 256) * Pointer.WIDTH;
      _header.page = allocate(required);
      _headerDirty = true;
      fullRewrite = true;
    }

    if (_header.page.isNotEmpty) {
      var block = PointerBlock(_header.page);

      // If we need to grow the pointer block, allocate a new page.
      if (block.count < _pointers.length) {
        free(_header.page);
        final required = align(_pointers.length, 256) * Pointer.WIDTH;
        _header.page = allocate(required);
        _headerDirty = true;
        block = PointerBlock(_header.page);
        fullRewrite = true;
      }

      if (fullRewrite) {
        for (var i = 0; i < block.count; i++) {
          block[i] = i < _pointers.length ? _pointers[i] : Pointer.NIL;
        }
        block.write(_file);
        _firstDirty = -1;
        _lastWrittenCount = _pointers.length;
      } else if (_firstDirty >= 0) {
        final touched = _pointers.length > _lastWrittenCount
            ? _pointers.length
            : _lastWrittenCount;
        final end = touched < block.count ? touched : block.count;
        for (var i = _firstDirty; i < end; i++) {
          block[i] = i < _pointers.length ? _pointers[i] : Pointer.NIL;
        }
        if (end > _firstDirty) {
          final byteStart = _firstDirty * Pointer.WIDTH;
          final byteEnd = end * Pointer.WIDTH;
          _file.setPositionSync(_header.page.offset + byteStart);
          _file.writeFromSync(block.buffer, byteStart, byteEnd);
        }
        _firstDirty = -1;
        _lastWrittenCount = _pointers.length;
      }
    }

    if (_headerDirty) {
      _header.write(_file);
      _headerDirty = false;
    }
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
