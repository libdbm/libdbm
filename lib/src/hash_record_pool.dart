import 'dart:io';
import 'dart:typed_data';

import '../dbm.dart';
import 'constants.dart';
import 'io.dart';
import 'memory_pool.dart';
import 'record_pool.dart';
import 'util.dart';

/// Header for the hash pool
class HashRecordPoolHeader extends Block {
  /// Magic number for the hashed record pool
  // ignore: constant_identifier_names
  static const int MAGIC = DBMConstants.HASH_RECORD_POOL_MAGIC;

  /// Size of the header
  // ignore: non_constant_identifier_names
  static final int HEADER_SIZE = 128;

  // ignore: non_constant_identifier_names
  static final int _MAGIC_OFFSET = 0;
  // ignore: non_constant_identifier_names
  static final int _PAGE_START_OFFSET = _MAGIC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _PAGE_LENGTH_OFFSET = _PAGE_START_OFFSET + 8;

  /// Create a header located at a given offset from data passed in
  HashRecordPoolHeader(int offset)
      : super(Pointer(offset, HEADER_SIZE), Uint8List(HEADER_SIZE)) {
    magic = MAGIC;
    page = Pointer(0, 0);
  }

  /// Get the magic number.
  int get magic => data.getUint64(_MAGIC_OFFSET);
  set magic(int v) => data.setUint64(_MAGIC_OFFSET, v);

  /// Get the page the record pool starts at
  Pointer get page => Pointer(
      data.getUint64(_PAGE_START_OFFSET), data.getUint64(_PAGE_LENGTH_OFFSET));
  set page(Pointer pointer) {
    data.setUint64(_PAGE_START_OFFSET, pointer.offset);
    data.setUint64(_PAGE_LENGTH_OFFSET, pointer.length);
  }
}

/// A block holding a key-value pair
class RecordBlock extends Block implements Record {
  /// Magic number of a block
  // ignore: constant_identifier_names
  static const int MAGIC = DBMConstants.RECORD_BLOCK_MAGIC;

  // ignore: non_constant_identifier_names
  static final int _MAGIC_OFFSET = 0;
  // ignore: non_constant_identifier_names
  static final int _CRC_OFFSET = _MAGIC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _NEXT_RECORD_OFFSET = _CRC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _NEXT_RECORD_LENGTH_OFFSET = _NEXT_RECORD_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _DATA_OFFSET = _NEXT_RECORD_LENGTH_OFFSET + 8;

  /// Calculate the number of bytes required for a key+value pair.
  static int required(Uint8List key, Uint8List value) {
    return key.length + value.length + _DATA_OFFSET + 16;
  }

  // Note, this is used to shortcut the semantics of `putIfAbsent`
  bool _isNew = false;

  /// The old value bytes, captured before an overwrite. Transient field used
  /// to communicate overwrite results without a second chain walk.
  Uint8List? replaced;

  /// The old block's allocated size before an overwrite. Transient field.
  int prior = 0;

  /// Create a block from the given data, located at a given offset within
  /// a file.
  RecordBlock(super.pointer, super.buffer) {
    magic = MAGIC;
    next = Pointer(0, 0);
    keyLength = 0;
    valueLength = 0;
  }

  @override
  int get size => pointer.length;

  @override
  Uint8List get key => buffer.buffer.asUint8List(keyOffset, keyLength);
  set key(Uint8List v) {
    keyLength = v.length;
    buffer.setRange(keyOffset, keyOffset + keyLength, v);
  }

  @override
  Uint8List get value => buffer.buffer.asUint8List(valueOffset, valueLength);
  set value(Uint8List v) {
    valueLength = v.length;
    buffer.setRange(valueOffset, valueOffset + valueLength, v);
  }

  /// A flag indicating if the record is newly added.
  // ignore: unnecessary_getters_setters
  bool get isNew => _isNew;
  // ignore: unnecessary_getters_setters
  set isNew(bool v) => _isNew = v;

  /// The CRC of the record
  int get crc => data.getUint32(_CRC_OFFSET);

  /// Calculate and set the CRC of the underlying buffer
  void setCRC() {
    data.setUint32(_CRC_OFFSET, 0);
    data.setUint32(_CRC_OFFSET, crc32(buffer));
  }

  /// The offset to the key, in bytes
  int get keyOffset => _DATA_OFFSET + 8;

  /// The length of the key, in bytes
  int get keyLength => data.getUint64(_DATA_OFFSET);
  set keyLength(int i) => data.setUint64(_DATA_OFFSET, i);

  /// The offset to the value, in bytes
  int get valueOffset => keyOffset + keyLength + 8;

  /// The length of the value, in bytes
  int get valueLength => data.getUint64(keyOffset + keyLength);
  set valueLength(int i) => data.setUint64(keyOffset + keyLength, i);

  /// Access to the next record in the chain associated with a bucket
  Pointer get next => Pointer(data.getUint64(_NEXT_RECORD_OFFSET),
      data.getUint64(_NEXT_RECORD_LENGTH_OFFSET));
  set next(Pointer pointer) => {
        data.setUint64(_NEXT_RECORD_OFFSET, pointer.offset),
        data.setUint64(_NEXT_RECORD_LENGTH_OFFSET, pointer.length)
      };

  /// Access to the record magic number
  int get magic => data.getUint64(_MAGIC_OFFSET);
  set magic(int v) => data.setUint64(_MAGIC_OFFSET, v);
}

/// Iterator over the hash table.
class HashRecordPoolIterator
    implements Iterator<MapEntry<Uint8List, Uint8List>> {
  final Function(Pointer) _fetcher;
  final PointerBlock _buckets;
  int _index;
  RecordBlock? _current;

  /// Constructor
  HashRecordPoolIterator(this._fetcher, this._buckets)
      : _index = 0,
        _current = null;

  @override
  MapEntry<Uint8List, Uint8List> get current {
    if (_current == null) throw DBMException(400, 'current() is not valid');
    return MapEntry(_current!.key, _current!.value);
  }

  @override
  bool moveNext() {
    final next = _current?.next;
    if (next != null && next.isNotEmpty) {
      _current = _fetcher(next);
      return true;
    }

    var ptr = Pointer.NIL;
    while (_index < _buckets.count && (ptr = _buckets[_index++]).isEmpty) {}
    if (ptr.isNotEmpty) {
      _current = _fetcher(ptr);
      return true;
    }

    _current = null;
    return false;
  }
}

/// The heart of the hashed data storage. This managed an underlying
/// hashtable, and associated data. The data is managed via [MemoryPool] rather
/// than directly.
class HashRecordPool implements RecordPool {
  final RandomAccessFile _file;
  final HashRecordPoolHeader _header;
  final MemoryPool _memoryPool;
  final bool _checkCRC;
  late final PointerBlock _buckets;
  bool _headerDirty = false;

  /// Create a new record pool. [memoryPool] is used to actually allocate
  /// records
  HashRecordPool(this._file, int offset, this._memoryPool, int buckets,
      {bool crc = false})
      : _header = HashRecordPoolHeader(offset),
        _checkCRC = crc {
    final length = _file.lengthSync();
    if (length < _header.end) {
      _header.write(_file);
    } else {
      _header.read(_file);
      if (_header.magic != HashRecordPoolHeader.MAGIC) {
        throw DBMException(
            500, 'HashRecordPool header magic mismatch: ${_header.magic}');
      }
    }

    if (_header.page.isEmpty) {
      _header.page = _memoryPool.allocate(buckets * Pointer.WIDTH);
      _buckets = PointerBlock(_header.page);
      _buckets.write(_file);
      _header.write(_file);
    } else {
      _buckets = PointerBlock(_header.page);
      _buckets.read(_file);
    }
  }

  @override
  Iterator<MapEntry<Uint8List, Uint8List>> entries() {
    return HashRecordPoolIterator((ptr) => _fetch(_file, ptr), _buckets);
  }

  @override
  Record put(Uint8List key, Uint8List value, bool overwrite) {
    return _insertAtTail(key, value, overwrite);
  }

  @override
  void free(Record record) {
    final block = record as RecordBlock;
    final bucket = hash(block.key) % _buckets.count;
    var ptr = _buckets[bucket];
    RecordBlock? last;
    while (ptr.isNotEmpty) {
      var block = _fetch(_file, ptr);
      if (matches(block.key, record.key)) {
        if (last == null) {
          // Head of the chain
          _buckets[bucket] = block.next;
          _buckets.writeAt(_file, bucket);
        } else {
          // Middle or end of the chain
          last.next = block.next;
          if (_checkCRC) last.setCRC();
          last.write(_file);
        }
        _memoryPool.free(block.pointer);
        return;
      }
      last = block;
      ptr = block.next;
    }
  }

  @override
  Record? remove(Uint8List key) {
    final bucket = hash(key) % _buckets.count;
    var ptr = _buckets[bucket];
    RecordBlock? last;
    while (ptr.isNotEmpty) {
      var block = _fetch(_file, ptr);
      if (matches(block.key, key)) {
        if (last == null) {
          _buckets[bucket] = block.next;
          _buckets.writeAt(_file, bucket);
        } else {
          last.next = block.next;
          if (_checkCRC) last.setCRC();
          last.write(_file);
        }
        _memoryPool.free(block.pointer);
        return block;
      }
      last = block;
      ptr = block.next;
    }
    return null;
  }

  @override
  Record? get(Uint8List key) {
    final bucket = hash(key) % _buckets.count;
    var ptr = _buckets[bucket];
    while (ptr.isNotEmpty) {
      var block = _fetch(_file, ptr);
      if (matches(block.key, key)) {
        return block;
      }
      ptr = block.next;
    }
    return null;
  }

  @override
  void clear() {
    for (var i = 0; i < _buckets.count; i++) {
      var ptr = _buckets[i];
      while (ptr.isNotEmpty) {
        var block = _fetch(_file, ptr);
        _memoryPool.free(ptr);
        ptr = block.next;
      }
      _buckets[i] = Pointer.NIL;
    }
    _buckets.write(_file);
  }

  @override
  void flush() {
    if (_headerDirty) {
      _header.write(_file);
      _headerDirty = false;
    }
    // Bucket mutations are already persisted via writeAt() on every put /
    // remove, so there's nothing further to do here.
  }

  RecordBlock _insertAtTail(Uint8List key, Uint8List value, bool overwrite) {
    // Find the block that matches, or the end of the chain
    final bucket = hash(key) % _buckets.count;
    var ptr = _buckets[bucket];

    // Simple case where the bucket is empty
    if (ptr.isEmpty) {
      var ret = _create(key, value);
      if (_checkCRC) ret.setCRC();
      ret.write(_file);
      _buckets[bucket] = ret.pointer;
      _buckets.writeAt(_file, bucket);
      return ret;
    }

    RecordBlock? previous;
    while (ptr.isNotEmpty) {
      var current = _fetch(_file, ptr);
      if (matches(current.key, key)) {
        if (overwrite == false || matches(current.value, value)) {
          return current;
        }
        // Capture old value and size before any overwrite.
        final old = Uint8List.fromList(current.value);
        final oldSize = current.size;

        // Try in-place overwrite if new data fits.
        final needed = RecordBlock.required(key, value);
        if (needed <= current.pointer.length) {
          current.value = value;
          if (_checkCRC) current.setCRC();
          current.write(_file);
          current.isNew = false;
          current.replaced = old;
          current.prior = oldSize;
          return current;
        }

        // Need a new block — allocate and link in place of current.
        var ret = _create(key, value);
        ret.next = current.next;
        ret.isNew = false;
        ret.replaced = old;
        ret.prior = oldSize;

        if (previous == null) {
          // Head of the chain.
          if (_checkCRC) ret.setCRC();
          ret.write(_file);
          _buckets[bucket] = ret.pointer;
          _buckets.writeAt(_file, bucket);
        } else {
          // Middle of the chain.
          previous.next = ret.pointer;
          if (_checkCRC) ret.setCRC();
          ret.write(_file);
          if (_checkCRC) previous.setCRC();
          previous.write(_file);
        }
        _memoryPool.free(current.pointer);
        return ret;
      }
      previous = current;
      ptr = current.next;
    }

    // End of the chain — append new record.
    var ret = _create(key, value);
    if (_checkCRC) ret.setCRC();
    ret.write(_file);

    previous!.next = ret.pointer;
    if (_checkCRC) previous.setCRC();
    previous.write(_file);
    return ret;
  }

  /// Create a new record block with the associated key and value
  /// but do not write the block to disk
  RecordBlock _create(Uint8List key, Uint8List value) {
    final size = RecordBlock.required(key, value);
    final ptr = _memoryPool.allocate(size);
    final ret = RecordBlock(ptr, Uint8List(ptr.length));

    ret.key = key;
    ret.value = value;
    ret.isNew = true;
    if (_checkCRC) ret.setCRC();

    return ret;
  }

  /// Fetch a record from the underlying storage
  RecordBlock _fetch(RandomAccessFile file, Pointer pointer) {
    var ret = RecordBlock(pointer, Uint8List(pointer.length));
    ret.read(file);

    if (ret.magic != RecordBlock.MAGIC) {
      throw DBMException(500, 'Invalid RecordBlock magic ${ret.magic}');
    }

    final crc = ret.crc;
    if (_checkCRC) ret.setCRC();
    if (_checkCRC && ret.crc != crc) {
      throw DBMException(
          500, 'Invalid RecordBlock CRC at ${ret.pointer.offset}');
    }

    ret.isNew = false;
    return ret;
  }
}
