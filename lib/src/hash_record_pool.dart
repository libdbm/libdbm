import 'dart:typed_data';
import 'dart:io';

import '../dbm.dart';
import 'io.dart';
import 'util.dart';
import 'memory_pool.dart';
import 'record_pool.dart';

/// Header for the hash pool
class HashRecordPoolHeader extends Block {
  static int MAGIC = 0xa0ba51c0da7aba5e;
  static int HEADER_SIZE = 128;

  static int MAGIC_OFFSET = 0;
  static int PAGE_START_OFFSET = MAGIC_OFFSET + 8;
  static int PAGE_LENGTH_OFFSET = PAGE_START_OFFSET + 8;

  HashRecordPoolHeader(int offset)
      : super(Pointer(offset, HEADER_SIZE), Uint8List(HEADER_SIZE)) {
    magic = MAGIC;
    page = Pointer(0, 0);
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

/// A block holding a key-value pair
class RecordBlock extends Block implements Record {
  static int MAGIC = 0xa0c011ec7ed01eaf;
  static int MAGIC_OFFSET = 0;
  static int CRC_OFFSET = MAGIC_OFFSET + 8;
  static int NEXT_RECORD_OFFSET = CRC_OFFSET + 8;
  static int NEXT_RECORD_LENGTH_OFFSET = NEXT_RECORD_OFFSET + 8;
  static int DATA_OFFSET = NEXT_RECORD_LENGTH_OFFSET + 8;

  static int required(Uint8List key, Uint8List value) {
    return key.length + value.length + DATA_OFFSET + 8;
  }

  // Note, this is used to shortcut the semantics of `putIfAbsent`
  bool _isNew;

  RecordBlock(Pointer pointer, Uint8List buffer) : super(pointer, buffer) {
    magic = MAGIC;
    next = Pointer(0, 0);
    keyLength = 0;
    valueLength = 0;
    isNew = false;
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

  bool get isNew => _isNew;
  set isNew(bool v) => _isNew = v;

  int get crc => data.getUint32(CRC_OFFSET);
  void setCRC() {
    data.setUint32(CRC_OFFSET, 0);
    data.setUint32(CRC_OFFSET, crc32(buffer));
  }

  int get keyOffset => DATA_OFFSET + 8;
  int get keyLength => data.getUint64(DATA_OFFSET);
  set keyLength(int i) => data.setUint64(DATA_OFFSET, i);

  int get valueOffset => keyOffset + keyLength + 8;
  int get valueLength => data.getUint64(keyOffset + keyLength);
  set valueLength(int i) => data.setUint64(keyOffset + keyLength, i);

  Pointer get next => Pointer(data.getUint64(NEXT_RECORD_OFFSET),
      data.getUint64(NEXT_RECORD_LENGTH_OFFSET));

  set next(Pointer pointer) => {
        data.setUint64(NEXT_RECORD_OFFSET, pointer.offset),
        data.setUint64(NEXT_RECORD_LENGTH_OFFSET, pointer.length)
      };

  int get magic => data.getUint64(MAGIC_OFFSET);
  set magic(int v) => data.setUint64(MAGIC_OFFSET, v);
}

/// Iterator over the hash table.
class HashRecordPoolIterator
    implements Iterator<MapEntry<Uint8List, Uint8List>> {
  final Function(Pointer) _fetcher;
  final PointerBlock _buckets;
  int _index;
  Pointer _ptr;
  RecordBlock _current;

  HashRecordPoolIterator(this._fetcher, this._buckets)
      : _index = 0,
        _ptr = Pointer.NIL,
        _current = null;

  @override
  MapEntry<Uint8List, Uint8List> get current {
    if (_current == null) throw DBMException(400, 'current() is not valid');
    return MapEntry(_current.key, _current.value);
  }

  @override
  bool moveNext() {
    _ptr = _current?.next ?? Pointer.NIL;
    if (_ptr.isEmpty) {
      while (_index < _buckets.count && (_ptr = _buckets[_index++]).isEmpty) {}
      if (_ptr.isNotEmpty) {
        _current = _fetcher(_ptr);
        return true;
      }
    }
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
  PointerBlock _buckets;
  final bool _checkCRC;

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
    if (block == null) return null;
    final bucket = hash(block.key) % _buckets.count;
    var ptr = _buckets[bucket];
    RecordBlock last;
    while (ptr.isNotEmpty) {
      var block = _fetch(_file, ptr);
      if (matches(block.key, record.key)) {
        if (last == null) {
          // Head of the chain
          _buckets[bucket] = block.next;
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
  Record get(Uint8List key) {
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
  }

  @override
  void flush() {
    _header.write(_file);
    _buckets.write(_file);
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
      return ret;
    }

    RecordBlock previous;
    while (ptr.isNotEmpty) {
      var current = _fetch(_file, ptr);
      if (matches(current.key, key)) {
        if (overwrite == false || matches(current.value, value)) return current;
        if (previous == null) {
          // Found at the head of the chain
          var ret = _create(key, value);
          ret.next = current.next;

          if (_checkCRC) ret.setCRC();
          ret.write(_file);

          _buckets[bucket] = ret.pointer;
          _memoryPool.free(current.pointer);

          // Inserted record is a replacement
          current.isNew = false;
          return current;
        } else {
          // Found in the middle of the chain
          var ret = _create(key, value);
          previous.next = ret.pointer;
          ret.next = current.next;

          if (_checkCRC) ret.setCRC();
          ret.write(_file);

          if (_checkCRC) previous.setCRC();
          previous.write(_file);

          _memoryPool.free(current.pointer);
          // Inserted record is a replacement
          current.isNew = false;
          return current;
        }
      }
      previous = current;
      ptr = current.next;
    }

    // If we got here, we are at the end of the chain
    var ret = _create(key, value);
    if (_checkCRC) ret.setCRC();
    ret.write(_file);

    previous.next = ret.pointer;
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
      print('$crc vs ${ret.crc}');
      throw DBMException(
          500, 'Invalid RecordBlock CRC at ${ret.pointer.offset}');
    }

    ret.isNew = false;
    return ret;
  }
}
