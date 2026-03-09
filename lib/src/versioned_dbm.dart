import 'dart:io';
import 'dart:typed_data';

import '../dbm.dart';
import 'delta_block.dart';
import 'hash_dbm.dart';
import 'io.dart';
import 'transaction.dart';
import 'version_store.dart';

/// A [VersionedDBM] implementation that wraps [HashDBM] with delta overlay
/// transactions and version history.
class VersionedHashDBM implements VersionedDBM {
  final HashDBM _dbm;
  late final VersionStore _store;

  /// Open a versioned database. The [file] should be opened with the same
  /// mode semantics as [HashDBM]. Optional parameters match [HashDBM].
  VersionedHashDBM(final RandomAccessFile file,
      {final int buckets = 10007,
      final bool flush = true,
      final bool crc = false,
      final bool readonly = false})
      : _dbm = HashDBM(file,
            buckets: buckets,
            flush: flush,
            crc: crc,
            readonly: readonly,
            versioned: true) {
    _store = VersionStore(_dbm.file, _dbm.pool, _dbm.header.list);
  }

  @override
  Transaction begin() {
    _guardReadonly();
    return HashTransaction(this, _store.current);
  }

  @override
  int get current => _store.current;

  @override
  List<int> get versions => _store.versions;

  @override
  DBM at(final int version) {
    final available = _store.versions;
    if (!available.contains(version)) {
      throw DBMException(404, 'Version $version not available');
    }
    return _SnapshotDBM(this, version);
  }

  @override
  void merge({final int? through}) {
    _guardReadonly();
    final target = through ?? _store.current;
    if (target <= _store.base) return;
    if (target > _store.current) {
      throw DBMException(
          400, 'Cannot merge through $target: current is ${_store.current}');
    }

    // Suppress per-op flushing during bulk merge
    _dbm.batch = true;

    // Apply deltas to base table in order
    final entries = _store.through(target);
    // Reverse so we apply oldest first
    for (final entry in entries.reversed) {
      final delta = DeltaBlock.read(_dbm.file, entry.delta);
      final decoded = delta.decode();
      for (final kv in decoded.entries) {
        if (kv.value == null) {
          _dbm.remove(kv.key.bytes);
        } else {
          _dbm.put(kv.key.bytes, kv.value!);
        }
      }
    }

    _dbm.batch = false;

    // Free delta blocks and remove version entries
    final removed = _store.removeTo(target);
    for (final entry in removed) {
      _dbm.pool.free(entry.delta);
    }

    // Persist version list — write new list, flush header, then free old
    final old = _dbm.header.list;
    final pointer = _store.write(old);
    _dbm.header.list = pointer;
    _dbm.header.counter = _store.current;
    _flush();
    if (old.isNotEmpty && old != pointer) {
      _dbm.pool.free(old);
    }
  }

  @override
  void flatten() {
    _guardReadonly();
    merge();
    _store.reset();
    _dbm.header.counter = 0;
    _dbm.header.list = Pointer.NIL;
    _dbm.header.version = HashHeader.VERSION_PLAIN;
    _flush();
  }

  @override
  Uint8List? get(final Uint8List key) => resolve(key, _store.current);

  @override
  Uint8List? put(final Uint8List key, final Uint8List value) {
    _guardReadonly();
    final old = get(key);
    final transaction = begin();
    transaction.put(key, value);
    transaction.commit();
    return old;
  }

  @override
  Uint8List? remove(final Uint8List key) {
    _guardReadonly();
    final old = get(key);
    if (old == null) return null;
    final transaction = begin();
    transaction.remove(key);
    transaction.commit();
    return old;
  }

  @override
  Uint8List putIfAbsent(final Uint8List key, final Uint8List value) {
    _guardReadonly();
    final existing = get(key);
    if (existing != null) return existing;
    final transaction = begin();
    transaction.put(key, value);
    transaction.commit();
    return value;
  }

  @override
  Iterator<MapEntry<Uint8List, Uint8List>> entries() =>
      entriesAt(_store.current);

  @override
  DateTime modified() => _dbm.modified();

  @override
  int version() => _dbm.version();

  @override
  int size() => _dbm.size();

  @override
  int count() {
    var n = 0;
    final iter = entries();
    while (iter.moveNext()) {
      n++;
    }
    return n;
  }

  @override
  void clear() {
    _guardReadonly();
    _dbm.clear();
    _store.reset();
    _dbm.header.counter = 0;
    _dbm.header.list = Pointer.NIL;
    _flush();
  }

  @override
  int compact() => _dbm.compact();

  @override
  void flush() => _flush();

  @override
  void close() => _dbm.close();

  /// Resolve a key at a specific version by scanning deltas then base.
  Uint8List? resolve(final Uint8List key, final int version) {
    // Walk deltas from newest to oldest for this version
    final deltas = _store.through(version);
    for (final entry in deltas) {
      final delta = DeltaBlock.read(_dbm.file, entry.delta);
      final found = delta.lookup(key);
      if (found != null) {
        return isTombstone(found) ? null : found;
      }
    }
    // Fall through to base table
    return _dbm.get(key);
  }

  /// Apply a set of pending changes as a new delta version.
  void apply(final Map<BytesKey, Uint8List?> pending) {
    _guardReadonly();
    final version = _store.current + 1;
    final timestamp = DateTime.now().millisecondsSinceEpoch;

    // Serialize delta
    final needed = DeltaBlock.required(pending);
    final pointer = _dbm.pool.allocate(needed);
    final delta = DeltaBlock.encode(version, timestamp, pending, pointer);
    delta.block.write(_dbm.file);

    // Append to version list
    _store.append(VersionEntry(version, timestamp, pointer));

    // Write version list — flush header before freeing old block
    final old = _dbm.header.list;
    final listPtr = _store.write(old);
    _dbm.header.list = listPtr;
    _dbm.header.counter = version;
    _flush();
    if (old.isNotEmpty && old != listPtr) {
      _dbm.pool.free(old);
    }
  }

  /// Iterate entries at a specific version, merging base + deltas.
  Iterator<MapEntry<Uint8List, Uint8List>> entriesAt(final int version) {
    // Build merged overlay from all deltas up to version
    final overlay = <BytesKey, Uint8List?>{};
    final deltas = _store.through(version);
    // Apply oldest first so newest wins
    for (final entry in deltas.reversed) {
      final delta = DeltaBlock.read(_dbm.file, entry.delta);
      final decoded = delta.decode();
      overlay.addAll(decoded);
    }

    return _MergedIterator(_dbm.entries(), overlay);
  }

  void _guardReadonly() {
    if (_dbm.readonly) {
      throw DBMException(403, 'Database is opened in readonly mode');
    }
  }

  void _flush() {
    _dbm.flush();
  }
}

/// A read-only snapshot view at a specific version.
class _SnapshotDBM implements DBM {
  final VersionedHashDBM _owner;
  final int _version;

  _SnapshotDBM(this._owner, this._version);

  @override
  Uint8List? get(final Uint8List key) => _owner.resolve(key, _version);

  @override
  Iterator<MapEntry<Uint8List, Uint8List>> entries() =>
      _owner.entriesAt(_version);

  @override
  int count() {
    var n = 0;
    final iter = entries();
    while (iter.moveNext()) {
      n++;
    }
    return n;
  }

  @override
  int size() => _owner.size();

  @override
  DateTime modified() => _owner.modified();

  @override
  int version() => _version;

  @override
  Uint8List? put(final Uint8List key, final Uint8List value) =>
      throw DBMException(403, 'Snapshot is read-only');

  @override
  Uint8List putIfAbsent(final Uint8List key, final Uint8List value) =>
      throw DBMException(403, 'Snapshot is read-only');

  @override
  Uint8List? remove(final Uint8List key) =>
      throw DBMException(403, 'Snapshot is read-only');

  @override
  void clear() => throw DBMException(403, 'Snapshot is read-only');

  @override
  int compact() => throw DBMException(403, 'Snapshot is read-only');

  @override
  void flush() {}

  @override
  void close() {}
}

/// Iterator that merges base table entries with a delta overlay.
class _MergedIterator implements Iterator<MapEntry<Uint8List, Uint8List>> {
  final Iterator<MapEntry<Uint8List, Uint8List>> _base;
  final Map<BytesKey, Uint8List?> _overlay;
  final Set<BytesKey> _emitted = {};
  late Iterator<MapEntry<BytesKey, Uint8List?>> _overlayIter;
  bool _baseExhausted = false;
  bool _inOverlayPhase = false;
  MapEntry<Uint8List, Uint8List>? _current;

  _MergedIterator(this._base, this._overlay) {
    _overlayIter = _overlay.entries.iterator;
  }

  @override
  MapEntry<Uint8List, Uint8List> get current => _current!;

  @override
  bool moveNext() {
    // Phase 1: iterate base, applying overlay overrides
    while (!_baseExhausted && !_inOverlayPhase) {
      if (!_base.moveNext()) {
        _baseExhausted = true;
        _inOverlayPhase = true;
        break;
      }
      final entry = _base.current;
      final k = BytesKey(entry.key);
      _emitted.add(k);

      if (_overlay.containsKey(k)) {
        final value = _overlay[k];
        if (value == null) continue; // tombstone, skip
        _current = MapEntry(entry.key, value);
        return true;
      }
      _current = entry;
      return true;
    }

    // Phase 2: emit overlay-only keys (inserts not in base)
    while (_overlayIter.moveNext()) {
      final entry = _overlayIter.current;
      if (_emitted.contains(entry.key)) continue;
      if (entry.value == null) continue; // tombstone for non-existent key
      _current = MapEntry(entry.key.bytes, entry.value!);
      return true;
    }

    return false;
  }
}
