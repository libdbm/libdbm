import 'dart:io';
import 'dart:typed_data';

import '../dbm.dart';
import 'constants.dart';
import 'delta_block.dart';
import 'io.dart';
import 'memory_pool.dart';
import 'util.dart';

/// A single version entry in the version list.
class VersionEntry {
  /// The version number.
  final int version;

  /// Timestamp when this version was committed.
  final int timestamp;

  /// Pointer to the delta block on disk.
  final Pointer delta;

  /// Constructor.
  VersionEntry(this.version, this.timestamp, this.delta);
}

/// Manages the version list block on disk and provides version resolution.
///
/// Version list block layout:
/// ```
/// +0x00 (8b)  magic
/// +0x08 (8b)  base version (oldest available)
/// +0x10 (4b)  entry count
/// +0x14..     entries[], each:
///               version    (8b)
///               timestamp  (8b)
///               delta_ptr  (16b) — Pointer(offset, length)
/// ```
class VersionStore {
  /// Magic number for the version list block.
  // ignore: constant_identifier_names
  static const int MAGIC = DBMConstants.VERSION_LIST_MAGIC;

  // ignore: non_constant_identifier_names
  static final int _MAGIC_OFFSET = 0;
  // ignore: non_constant_identifier_names
  static final int _BASE_VERSION_OFFSET = _MAGIC_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _COUNT_OFFSET = _BASE_VERSION_OFFSET + 8;
  // ignore: non_constant_identifier_names
  static final int _ENTRIES_OFFSET = _COUNT_OFFSET + 4;
  // ignore: non_constant_identifier_names
  static final int _ENTRY_SIZE = 32; // 8 + 8 + 16

  final RandomAccessFile _file;
  final MemoryPool _pool;

  /// The base version (oldest available, deltas folded into base table).
  int base = 0;

  /// In-memory list of version entries (sorted by version ascending).
  final List<VersionEntry> _entries = [];

  /// Constructor. Loads existing version list if [pointer] is non-nil.
  VersionStore(this._file, this._pool, final Pointer pointer) {
    if (pointer.isNotEmpty) {
      _read(pointer);
    }
  }

  /// Number of delta versions stored.
  int get length => _entries.length;

  /// All version numbers available (base + deltas).
  List<int> get versions {
    final result = <int>[base];
    for (final entry in _entries) {
      result.add(entry.version);
    }
    return result;
  }

  /// The latest version number.
  int get current => _entries.isEmpty ? base : _entries.last.version;

  /// Get entries for versions from [base]+1 through [version], newest first.
  List<VersionEntry> through(final int version) {
    final result = <VersionEntry>[];
    for (var i = _entries.length - 1; i >= 0; i--) {
      final entry = _entries[i];
      if (entry.version <= version && entry.version > base) {
        result.add(entry);
      }
    }
    return result;
  }

  /// Reset to initial empty state.
  void reset() {
    _entries.clear();
    base = 0;
  }

  /// Append a new version entry.
  void append(final VersionEntry entry) {
    _entries.add(entry);
  }

  /// Remove entries through [version] (used during compaction).
  /// Returns the removed entries so their delta blocks can be freed.
  List<VersionEntry> removeTo(final int version) {
    final removed = <VersionEntry>[];
    while (_entries.isNotEmpty && _entries.first.version <= version) {
      removed.add(_entries.removeAt(0));
    }
    base = version;
    return removed;
  }

  /// Write the version list to disk, returning the new pointer.
  /// The caller is responsible for freeing [old] after the header
  /// that references the returned pointer has been durably flushed.
  Pointer write(final Pointer old) {
    final needed = align(
        _ENTRIES_OFFSET + _entries.length * _ENTRY_SIZE, MemoryPool.ALIGNMENT);

    Pointer pointer;
    if (old.isNotEmpty && old.length >= needed) {
      pointer = old;
    } else {
      pointer = _pool.allocate(needed);
    }

    final buffer = Uint8List(pointer.length);
    final data = ByteData.view(buffer.buffer);

    data.setUint64(_MAGIC_OFFSET, MAGIC);
    data.setUint64(_BASE_VERSION_OFFSET, base);
    data.setUint32(_COUNT_OFFSET, _entries.length);

    var offset = _ENTRIES_OFFSET;
    for (final entry in _entries) {
      data.setUint64(offset, entry.version);
      data.setUint64(offset + 8, entry.timestamp);
      data.setUint64(offset + 16, entry.delta.offset);
      data.setUint64(offset + 24, entry.delta.length);
      offset += _ENTRY_SIZE;
    }

    // Zero remaining bytes
    for (var i = offset; i < buffer.length; i++) {
      buffer[i] = 0;
    }

    final block = Block(pointer, buffer);
    block.write(_file);
    return pointer;
  }

  void _read(final Pointer pointer) {
    final block = Block(pointer, Uint8List(pointer.length));
    block.read(_file);
    final data = block.data;

    final magic = data.getUint64(_MAGIC_OFFSET);
    if (magic != MAGIC) {
      throw DBMException(500, 'VersionStore magic mismatch: $magic');
    }

    base = data.getUint64(_BASE_VERSION_OFFSET);
    final count = data.getUint32(_COUNT_OFFSET);
    final required = _ENTRIES_OFFSET + count * _ENTRY_SIZE;
    if (required > pointer.length) {
      throw DBMException(
          500, 'VersionStore corrupt: $required bytes needed but '
          'block is ${pointer.length}');
    }

    _entries.clear();
    var offset = _ENTRIES_OFFSET;
    for (var i = 0; i < count; i++) {
      final version = data.getUint64(offset);
      final timestamp = data.getUint64(offset + 8);
      final doff = data.getUint64(offset + 16);
      final dlen = data.getUint64(offset + 24);
      _entries.add(VersionEntry(version, timestamp, Pointer(doff, dlen)));
      offset += _ENTRY_SIZE;
    }
  }

  /// Read a delta block at the given version.
  DeltaBlock delta(final int version) {
    for (final entry in _entries) {
      if (entry.version == version) {
        return DeltaBlock.read(_file, entry.delta);
      }
    }
    throw DBMException(404, 'Version $version not found');
  }
}
