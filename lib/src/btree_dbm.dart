import 'dart:io';
import 'dart:typed_data';

import '../dbm.dart';
import 'btree_node.dart';
import 'constants.dart';
import 'hash_dbm.dart';
import 'sorted_dbm.dart';
import 'util.dart';

/// Meta record magic number.
// ignore: constant_identifier_names
const int _META_MAGIC = DBMConstants.BTREE_META_MAGIC;

/// Meta record size in bytes.
// ignore: constant_identifier_names
const int _META_SIZE = 48;

/// Meta record field offsets.
// ignore: constant_identifier_names
const int _META_MAGIC_OFF = 0;
// ignore: constant_identifier_names
const int _META_ROOT_OFF = 8;
// ignore: constant_identifier_names
const int _META_HEIGHT_OFF = 16;
// ignore: constant_identifier_names
const int _META_COUNTER_OFF = 20;
// ignore: constant_identifier_names
const int _META_RECORDS_OFF = 28;
// ignore: constant_identifier_names
const int _META_ORDER_OFF = 36;

/// B+tree implementation of [SortedDBM]. Stores tree nodes as entries in an
/// underlying [DBM] (typically [HashDBM]).
///
/// Node IDs are sequential uint64s encoded as 8-byte big-endian keys. ID 0
/// is reserved for the meta record.
class BTreeDBM implements SortedDBM {
  final DBM _store;
  final bool _owned;
  final KeyComparator _compare;
  final int _order;
  final int _capacity;

  int _root = 0;
  int _height = 0;
  int _counter = 0;
  int _records = 0;
  bool _dirty = false;

  // Reusable buffer for encoding node IDs as 8-byte keys.
  final Uint8List _buf = Uint8List(8);
  late final ByteData _bufView = ByteData.view(_buf.buffer);

  // Reusable buffer for meta record serialization.
  final Uint8List _meta = Uint8List(_META_SIZE);
  late final ByteData _metaView = ByteData.view(_meta.buffer);

  // LRU node cache: Map preserves insertion order, most recent at end.
  final Map<int, Object> _cache = {};

  /// Create a new [BTreeDBM] that owns a [HashDBM] built from [file].
  BTreeDBM(final RandomAccessFile file,
      {final int order = 128,
      final int buckets = 49999,
      final bool flush = true,
      final bool crc = false,
      final int cache = 256,
      final KeyComparator? comparator})
      : _store = HashDBM(file,
            buckets: buckets, flush: flush,
            crc: crc, cache: cache),
        _owned = true,
        _compare = comparator ?? compare,
        _order = order,
        _capacity = cache {
    _load();
  }

  /// Wrap an existing [DBM] as storage. The caller retains ownership
  /// of [store] and is responsible for closing it.
  BTreeDBM.wrap(final DBM store,
      {final int order = 128,
      final int cache = 256,
      final KeyComparator? comparator})
      : _store = store,
        _owned = false,
        _compare = comparator ?? compare,
        _order = order,
        _capacity = cache {
    _load();
  }

  // -- Meta record ----------------------------------------------------------

  /// Encode a node ID as 8 big-endian bytes. Returns the shared buffer
  /// — safe because HashDBM copies key bytes into its record block
  /// immediately and does not retain a reference to this buffer.
  Uint8List _key(final int value) {
    _bufView.setUint64(0, value);
    return _buf;
  }

  void _load() {
    final raw = _store.get(_key(0));
    if (raw == null) {
      _root = 0;
      _height = 0;
      _counter = 0;
      _records = 0;
      _save();
      return;
    }
    final data = ByteData.view(raw.buffer, raw.offsetInBytes);
    final magic = data.getUint64(_META_MAGIC_OFF);
    if (magic != _META_MAGIC) {
      throw DBMException(500, 'BTreeDBM meta magic mismatch: $magic');
    }
    _root = data.getUint64(_META_ROOT_OFF);
    _height = data.getUint32(_META_HEIGHT_OFF);
    _counter = data.getUint64(_META_COUNTER_OFF);
    _records = data.getUint64(_META_RECORDS_OFF);
  }

  void _save() {
    _metaView.setUint64(_META_MAGIC_OFF, _META_MAGIC);
    _metaView.setUint64(_META_ROOT_OFF, _root);
    _metaView.setUint32(_META_HEIGHT_OFF, _height);
    _metaView.setUint64(_META_COUNTER_OFF, _counter);
    _metaView.setUint64(_META_RECORDS_OFF, _records);
    _metaView.setUint32(_META_ORDER_OFF, _order);
    _store.put(_key(0), _meta);
    _dirty = false;
  }

  int _next() => ++_counter;

  // -- Node cache -----------------------------------------------------------

  void _evict() {
    while (_cache.length > _capacity) {
      _cache.remove(_cache.keys.first);
    }
  }

  void _cached(final int id, final Object node) {
    _cache.remove(id);
    _cache[id] = node;
    _evict();
  }

  void _invalidate(final int id) {
    _cache.remove(id);
  }

  // -- Node I/O -------------------------------------------------------------

  void _write(final int id, final Uint8List encoded) {
    _invalidate(id);
    _store.put(_key(id), encoded);
  }

  Uint8List _read(final int id) {
    final raw = _store.get(_key(id));
    if (raw == null) {
      throw DBMException(500, 'BTreeDBM: missing node $id');
    }
    return raw;
  }

  void _delete(final int id) {
    _invalidate(id);
    _store.remove(_key(id));
  }

  LeafNode _leaf(final int id) {
    final hit = _cache[id];
    if (hit is LeafNode) {
      _cache.remove(id);
      _cache[id] = hit;
      return hit;
    }
    final node = LeafNode.decode(id, _read(id));
    _cached(id, node);
    return node;
  }

  InternalNode _internal(final int id) {
    final hit = _cache[id];
    if (hit is InternalNode) {
      _cache.remove(id);
      _cache[id] = hit;
      return hit;
    }
    final node = InternalNode.decode(id, _read(id));
    _cached(id, node);
    return node;
  }

  // -- Binary search --------------------------------------------------------

  /// Find insertion point in a sorted key list. Returns the index of the first
  /// key that is >= [target]. If all keys are less, returns keys.length.
  int _search(final List<Uint8List> keys, final Uint8List target) {
    var lo = 0;
    var hi = keys.length;
    while (lo < hi) {
      final mid = (lo + hi) >> 1;
      if (_compare(keys[mid], target) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  /// Find exact match index in a sorted key list, or -1.
  int _exact(final List<Uint8List> keys, final Uint8List target) {
    final idx = _search(keys, target);
    if (idx < keys.length && _compare(keys[idx], target) == 0) return idx;
    return -1;
  }

  /// Find child index for descent in an internal node.
  int _child(final InternalNode node, final Uint8List key) {
    var lo = 0;
    var hi = node.keys.length;
    while (lo < hi) {
      final mid = (lo + hi) >> 1;
      if (_compare(node.keys[mid], key) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  // -- Batch mode -----------------------------------------------------------

  /// Enter batch mode for multi-write operations (splits, deletes with
  /// unlinking). Suppresses per-write flushing on HashDBM.
  void _begin() {
    if (_store is HashDBM) {
      (_store as HashDBM).batch = true;
    }
  }

  /// Exit batch mode, save meta, and flush once.
  void _end() {
    _save();
    if (_store is HashDBM) {
      (_store as HashDBM).batch = false;
    }
    _store.flush();
  }

  // -- Traversal helpers ----------------------------------------------------

  /// Navigate from root to the leaf that should contain [key], recording
  /// the path of (internal node, child index) pairs.
  LeafNode _traverse(final Uint8List key,
      [final List<_PathEntry>? path]) {
    var id = _root;
    for (var level = _height - 1; level > 0; level--) {
      final node = _internal(id);
      final idx = _child(node, key);
      path?.add(_PathEntry(node, idx));
      id = node.children[idx];
    }
    return _leaf(id);
  }

  /// Find the leftmost leaf by always going to children[0].
  LeafNode? _leftmost() {
    if (_root == 0) return null;
    var id = _root;
    for (var level = _height - 1; level > 0; level--) {
      final node = _internal(id);
      id = node.children[0];
    }
    return _leaf(id);
  }

  /// Find the rightmost leaf by always going to children[last].
  LeafNode? _rightmost() {
    if (_root == 0) return null;
    var id = _root;
    for (var level = _height - 1; level > 0; level--) {
      final node = _internal(id);
      id = node.children.last;
    }
    return _leaf(id);
  }

  // -- DBM interface --------------------------------------------------------

  @override
  Uint8List? get(final Uint8List key) {
    if (_root == 0) return null;
    final leaf = _traverse(key);
    final idx = _exact(leaf.keys, key);
    if (idx < 0) return null;
    return leaf.values[idx];
  }

  @override
  Uint8List? put(final Uint8List key, final Uint8List value) {
    if (_root == 0) {
      // Empty tree — create first leaf as root.
      final id = _next();
      final leaf = LeafNode(id, [Uint8List.fromList(key)],
          [Uint8List.fromList(value)]);
      _write(id, leaf.encode());
      _root = id;
      _height = 1;
      _records = 1;
      _dirty = true;
      return null;
    }

    final path = <_PathEntry>[];
    final leaf = _traverse(key, path);
    final idx = _exact(leaf.keys, key);

    if (idx >= 0) {
      // Overwrite existing key.
      final old = leaf.values[idx];
      leaf.values[idx] = Uint8List.fromList(value);
      _write(leaf.id, leaf.encode());
      return old;
    }

    // Insert into leaf at sorted position.
    final pos = _search(leaf.keys, key);
    leaf.keys.insert(pos, Uint8List.fromList(key));
    leaf.values.insert(pos, Uint8List.fromList(value));
    _records++;
    _dirty = true;

    if (leaf.keys.length < _order) {
      _write(leaf.id, leaf.encode());
      return null;
    }

    // Leaf is full — split upward (multi-write, use batch).
    _begin();
    try {
      var promoted = _splitLeaf(leaf);
      var left = leaf.id;
      var right = promoted.right;

      while (path.isNotEmpty) {
        final entry = path.removeLast();
        final parent = entry.node;
        parent.keys.insert(entry.index, promoted.key);
        parent.children.insert(entry.index + 1, right);

        if (parent.keys.length < _order) {
          _write(parent.id, parent.encode());
          return null;
        }

        promoted = _splitInternal(parent);
        left = parent.id;
        right = promoted.right;
      }

      // Split reached root — create new root.
      final id = _next();
      final root = InternalNode(id, [promoted.key], [left, right]);
      _write(id, root.encode());
      _root = id;
      _height++;
    } finally {
      _end();
    }
    return null;
  }

  @override
  Uint8List putIfAbsent(final Uint8List key, final Uint8List value) {
    final existing = get(key);
    if (existing != null) return existing;
    put(key, value);
    return value;
  }

  @override
  Uint8List? remove(final Uint8List key) {
    if (_root == 0) return null;

    final path = <_PathEntry>[];
    final leaf = _traverse(key, path);
    final idx = _exact(leaf.keys, key);
    if (idx < 0) return null;

    final old = leaf.values[idx];
    leaf.keys.removeAt(idx);
    leaf.values.removeAt(idx);
    _records--;
    _dirty = true;

    if (leaf.keys.isNotEmpty) {
      _write(leaf.id, leaf.encode());
      return old;
    }

    // Leaf is empty — unlink and remove (multi-write, use batch).
    _begin();
    try {
      _unlinkLeaf(leaf);
      _delete(leaf.id);

      if (path.isEmpty) {
        // Was the root leaf.
        _root = 0;
        _height = 0;
        return old;
      }

      // Remove child pointer from parent.
      var entry = path.removeLast();
      entry.node.children.removeAt(entry.index);
      if (entry.index > 0) {
        entry.node.keys.removeAt(entry.index - 1);
      } else if (entry.node.keys.isNotEmpty) {
        entry.node.keys.removeAt(0);
      }

      if (entry.node.children.isNotEmpty) {
        _write(entry.node.id, entry.node.encode());
      } else {
        // Internal node is empty too — collapse upward.
        _delete(entry.node.id);
        while (path.isNotEmpty) {
          entry = path.removeLast();
          entry.node.children.removeAt(entry.index);
          if (entry.index > 0) {
            entry.node.keys.removeAt(entry.index - 1);
          } else if (entry.node.keys.isNotEmpty) {
            entry.node.keys.removeAt(0);
          }
          if (entry.node.children.isNotEmpty) {
            _write(entry.node.id, entry.node.encode());
            break;
          }
          _delete(entry.node.id);
        }
        if (path.isEmpty) {
          // Check if root became empty.
          final raw = _store.get(_key(_root));
          if (raw == null) {
            _root = 0;
            _height = 0;
          }
        }
      }
    } finally {
      _end();
    }
    return old;
  }

  void _unlinkLeaf(final LeafNode leaf) {
    if (leaf.previous != 0) {
      final prev = _leaf(leaf.previous);
      prev.next = leaf.next;
      _write(prev.id, prev.encode());
    }
    if (leaf.next != 0) {
      final nxt = _leaf(leaf.next);
      nxt.previous = leaf.previous;
      _write(nxt.id, nxt.encode());
    }
  }

  @override
  Iterator<MapEntry<Uint8List, Uint8List>> entries() {
    return _LeafIterator(this);
  }

  @override
  void clear() {
    _cache.clear();
    _store.clear();
    _root = 0;
    _height = 0;
    _counter = 0;
    _records = 0;
    _save();
  }

  @override
  int compact() {
    _cache.clear();
    if (_root == 0) return _store.compact();

    // Rebuild tree from leaf iteration.
    final all = <MapEntry<Uint8List, Uint8List>>[];
    final iter = entries();
    while (iter.moveNext()) {
      all.add(iter.current);
    }

    _store.clear();
    _root = 0;
    _height = 0;
    _counter = 0;
    _records = 0;
    _save();

    for (final entry in all) {
      put(entry.key, entry.value);
    }

    return _store.compact();
  }

  @override
  int count() => _records;

  @override
  int size() => _store.size();

  @override
  DateTime modified() => _store.modified();

  @override
  int version() => _store.version();

  @override
  void flush() {
    if (_dirty) _save();
    _store.flush();
  }

  @override
  void close() {
    if (_owned) {
      if (_dirty) _save();
      _store.close();
    }
  }

  // -- Split helpers --------------------------------------------------------

  _Split _splitLeaf(final LeafNode leaf) {
    final mid = leaf.keys.length >> 1;
    final rid = _next();

    final right = LeafNode(
        rid,
        List<Uint8List>.from(leaf.keys.sublist(mid)),
        List<Uint8List>.from(leaf.values.sublist(mid)),
        next: leaf.next,
        previous: leaf.id);

    if (leaf.next != 0) {
      final old = _leaf(leaf.next);
      old.previous = rid;
      _write(old.id, old.encode());
    }

    leaf.keys.removeRange(mid, leaf.keys.length);
    leaf.values.removeRange(mid, leaf.values.length);
    leaf.next = rid;

    _write(leaf.id, leaf.encode());
    _write(rid, right.encode());

    return _Split(Uint8List.fromList(right.keys[0]), rid);
  }

  _Split _splitInternal(final InternalNode node) {
    final mid = node.keys.length >> 1;
    final promoted = Uint8List.fromList(node.keys[mid]);
    final rid = _next();

    final right = InternalNode(
        rid,
        List<Uint8List>.from(node.keys.sublist(mid + 1)),
        List<int>.from(node.children.sublist(mid + 1)));

    node.keys.removeRange(mid, node.keys.length);
    node.children.removeRange(mid + 1, node.children.length);

    _write(node.id, node.encode());
    _write(rid, right.encode());

    return _Split(promoted, rid);
  }

  // -- SortedDBM interface --------------------------------------------------

  @override
  MapEntry<Uint8List, Uint8List>? first() {
    final leaf = _leftmost();
    if (leaf == null || leaf.keys.isEmpty) return null;
    return MapEntry(leaf.keys[0], leaf.values[0]);
  }

  @override
  MapEntry<Uint8List, Uint8List>? last() {
    final leaf = _rightmost();
    if (leaf == null || leaf.keys.isEmpty) return null;
    return MapEntry(leaf.keys.last, leaf.values.last);
  }

  @override
  Iterator<MapEntry<Uint8List, Uint8List>> range(
      {final Uint8List? start, final Uint8List? end}) {
    return _RangeIterator(this, start, end);
  }

  @override
  MapEntry<Uint8List, Uint8List>? floor(final Uint8List key) {
    if (_root == 0) return null;
    final leaf = _traverse(key);
    final idx = _search(leaf.keys, key);

    // Exact match.
    if (idx < leaf.keys.length && _compare(leaf.keys[idx], key) == 0) {
      return MapEntry(leaf.keys[idx], leaf.values[idx]);
    }
    // Greatest key less than target in this leaf.
    if (idx > 0) {
      return MapEntry(leaf.keys[idx - 1], leaf.values[idx - 1]);
    }
    // Go to previous leaf.
    if (leaf.previous != 0) {
      final prev = _leaf(leaf.previous);
      if (prev.keys.isNotEmpty) {
        return MapEntry(prev.keys.last, prev.values.last);
      }
    }
    return null;
  }

  @override
  MapEntry<Uint8List, Uint8List>? ceiling(final Uint8List key) {
    if (_root == 0) return null;
    final leaf = _traverse(key);
    final idx = _search(leaf.keys, key);

    if (idx < leaf.keys.length) {
      return MapEntry(leaf.keys[idx], leaf.values[idx]);
    }
    // Go to next leaf.
    if (leaf.next != 0) {
      final nxt = _leaf(leaf.next);
      if (nxt.keys.isNotEmpty) {
        return MapEntry(nxt.keys[0], nxt.values[0]);
      }
    }
    return null;
  }
}

// -- Private helpers --------------------------------------------------------

class _PathEntry {
  final InternalNode node;
  final int index;
  _PathEntry(this.node, this.index);
}

class _Split {
  final Uint8List key;
  final int right;
  _Split(this.key, this.right);
}

/// Iterator that walks the leaf chain from left to right.
class _LeafIterator implements Iterator<MapEntry<Uint8List, Uint8List>> {
  final BTreeDBM _tree;
  LeafNode? _leaf;
  int _index = -1;
  bool _started = false;
  MapEntry<Uint8List, Uint8List>? _current;

  _LeafIterator(this._tree);

  @override
  MapEntry<Uint8List, Uint8List> get current => _current!;

  @override
  bool moveNext() {
    if (!_started) {
      _started = true;
      _leaf = _tree._leftmost();
      _index = 0;
    } else {
      _index++;
    }

    while (_leaf != null) {
      if (_index < _leaf!.keys.length) {
        _current = MapEntry(_leaf!.keys[_index], _leaf!.values[_index]);
        return true;
      }
      if (_leaf!.next == 0) return false;
      _leaf = _tree._leaf(_leaf!.next);
      _index = 0;
    }
    return false;
  }
}

/// Iterator for range queries with optional start/end bounds.
class _RangeIterator implements Iterator<MapEntry<Uint8List, Uint8List>> {
  final BTreeDBM _tree;
  final Uint8List? _end;
  LeafNode? _leaf;
  int _index = -1;
  bool _started = false;
  MapEntry<Uint8List, Uint8List>? _current;

  _RangeIterator(this._tree, final Uint8List? start, this._end) {
    if (_tree._root == 0) return;
    if (start != null) {
      _leaf = _tree._traverse(start);
      var idx = _tree._search(_leaf!.keys, start);
      // If index is past end of leaf, advance to next.
      if (idx >= _leaf!.keys.length) {
        if (_leaf!.next != 0) {
          _leaf = _tree._leaf(_leaf!.next);
          idx = 0;
        } else {
          _leaf = null;
          return;
        }
      }
      _index = idx - 1; // compensate for first moveNext() increment
    } else {
      _leaf = _tree._leftmost();
      _index = -1; // compensate for first moveNext() increment
    }
    _started = true;
  }

  @override
  MapEntry<Uint8List, Uint8List> get current => _current!;

  @override
  bool moveNext() {
    if (!_started) {
      _started = true;
    } else {
      _index++;
    }

    while (_leaf != null) {
      if (_index < _leaf!.keys.length) {
        if (_end != null && _tree._compare(_leaf!.keys[_index], _end!) >= 0) {
          return false;
        }
        _current = MapEntry(_leaf!.keys[_index], _leaf!.values[_index]);
        return true;
      }
      if (_leaf!.next == 0) return false;
      _leaf = _tree._leaf(_leaf!.next);
      _index = 0;
    }
    return false;
  }
}
