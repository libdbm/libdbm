import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import '../dbm.dart';
import 'hash_record_pool.dart';
import 'io.dart';
import 'memory_pool.dart';
import 'record_pool.dart';

/// Header block for a hashed DBM implementation
class HashHeader extends Block {
  /// Magic number
  // ignore: non_constant_identifier_names
  static final int MAGIC = 0xda7aba5eda7afeed;

  /// Version number
  // ignore: non_constant_identifier_names
  static final int VERSION = 0x00010008;

  /// Header size
  // ignore: non_constant_identifier_names
  static final int SIZE = 256;

  // ignore: non_constant_identifier_names
  static final int _MAGIC_OFFSET = 0;
  // ignore: non_constant_identifier_names
  static final int _VERSION_OFFSET = _MAGIC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _BUCKET_COUNT_OFFSET = _VERSION_OFFSET + 4;
  // ignore: non_constant_identifier_names
  static final int _RECORD_COUNT_OFFSET = _BUCKET_COUNT_OFFSET + 4;
  // ignore: non_constant_identifier_names
  static final int _BYTE_COUNT_OFFSET = _RECORD_COUNT_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _MODIFIED_OFFSET = _BYTE_COUNT_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _MEMPOOL_OFFSET = _MODIFIED_OFFSET + 8;

  /// Constructor
  HashHeader(int size) : super(Pointer(0, SIZE), Uint8List(SIZE)) {
    magic = MAGIC;
    version = VERSION;
    numBuckets = size;
    numRecords = 0;
    numBytes = 0;
    memPoolOffset = SIZE;
    modified = DateTime.now().millisecondsSinceEpoch;
  }

  /// Access to the underlying magic number
  int get magic => data.getUint64(_MAGIC_OFFSET);
  set magic(int v) => data.setUint64(_MAGIC_OFFSET, v);

  /// Access to the underlying version number
  int get version => data.getUint32(_VERSION_OFFSET);
  set version(int v) => data.setUint32(_VERSION_OFFSET, v);

  /// Access to the last modification date
  int get modified => data.getUint64(_MODIFIED_OFFSET);
  set modified(int v) => data.setUint64(_MODIFIED_OFFSET, v);

  /// Access to the number of buckets used for hashing ids to records
  int get numBuckets => data.getUint32(_BUCKET_COUNT_OFFSET);
  set numBuckets(int v) => data.setUint32(_BUCKET_COUNT_OFFSET, v);

  /// Access to the number of bytes used by records in the database. This is not
  /// 100% accurate
  int get numBytes => data.getUint64(_BYTE_COUNT_OFFSET);
  set numBytes(int v) => data.setUint64(_BYTE_COUNT_OFFSET, max(0, v));

  /// Access to the number of records stored in the database.
  int get numRecords => data.getUint64(_RECORD_COUNT_OFFSET);
  set numRecords(int v) => data.setUint64(_RECORD_COUNT_OFFSET, v);

  /// Access to the offset to the memory pool data. The records start
  /// immediately after this.
  int get memPoolOffset => data.getUint64(_MEMPOOL_OFFSET);
  set memPoolOffset(int v) => data.setUint64(_MEMPOOL_OFFSET, v);
}

/// Hash-based implementation of [DBM]
class HashDBM implements DBM {
  /// Version of the database
  // ignore: non_constant_identifier_names
  static final int VERSION = HashHeader.VERSION;

  final RandomAccessFile _file;
  final HashHeader _header;
  final bool _flush;

  late final RecordPool _recordPool;
  late final MemoryPool _memoryPool;

  /// Open a new database. Optional parameters are: [buckets] which sets the
  /// number of hash buckets to use, [flush] which when set to true will force
  /// data to disk every time it is changed, and [crc] which will enable CRC
  /// checks on underlying records if set to true. The defaults are generally
  /// good enough.
  HashDBM(RandomAccessFile file,
      {int buckets = 10007, bool flush = true, bool crc = false})
      : _file = file,
        _flush = flush,
        _header = HashHeader(buckets) {
    final length = file.lengthSync();
    if (length > _header.length) {
      _header.read(_file);
    }
    if (_header.magic != HashHeader.MAGIC) {
      throw DBMException(500, 'HashHeader magic mismatch: ${_header.magic}');
    }

    // Update with current time
    _header.modified = DateTime.now().millisecondsSinceEpoch;
    _header.write(_file);

    // Create the memory pool
    _memoryPool = MemoryPool(_file, _header.memPoolOffset);

    // Create the record pool
    _recordPool = HashRecordPool(
        _file, _memoryPool.end + 1, _memoryPool, _header.numBuckets,
        crc: crc);
  }

  /// Get the underlying file
  RandomAccessFile get file => _file;

  /// Get the number of buckets in the hash table.
  int get hashTableSize => _header.numBuckets;

  @override
  int size() {
    return _header.numBytes;
  }

  @override
  DateTime modified() =>
      DateTime.fromMillisecondsSinceEpoch(_header.modified, isUtc: true);

  @override
  int version() => _header.version;

  @override
  void clear() {
    _recordPool.clear();
    _memoryPool.clear();
    _header.numRecords = 0;
    if (_flush) {
      flush();
    }
  }

  @override
  void close() {
    flush();
    _file.closeSync();
  }

  @override
  int count() {
    return _header.numRecords;
  }

  @override
  Uint8List? get(Uint8List key) {
    return _recordPool.get(key)?.value;
  }

  @override
  Uint8List putIfAbsent(Uint8List key, Uint8List value) {
    var record = _recordPool.put(key, value, false) as RecordBlock;
    if (record.isNew) {
      _header.numBytes += record.size;
      _header.numRecords += 1;
    } else {
      _header.numBytes += record.value.length - value.length;
    }
    if (_flush) flush();
    return record.value;
  }

  @override
  Uint8List? put(Uint8List key, Uint8List value) {
    final record = _recordPool.put(key, value, true) as RecordBlock;
    if (record.isNew) {
      _header.numBytes += record.size;
      _header.numRecords += 1;
    } else {
      _header.numBytes += record.value.length - value.length;
    }
    if (_flush) flush();
    return record.value;
  }

  @override
  Uint8List? remove(Uint8List key) {
    final record = _recordPool.get(key);
    if (record != null) _recordPool.free(record);
    _header.numRecords -= record == null ? 0 : 1;
    _header.numBytes -= record == null ? 0 : record.size;
    if (_flush) flush();
    return record?.value;
  }

  @override
  void flush() {
    _memoryPool.flush();
    _recordPool.flush();
    _header.write(_file);
    _file.flushSync();
  }

  @override
  Iterator<MapEntry<Uint8List, Uint8List>> entries() {
    return _recordPool.entries();
  }
}
