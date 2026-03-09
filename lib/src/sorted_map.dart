import 'dart:convert' as convert;
import 'dart:io';
import 'dart:typed_data';

import 'btree_dbm.dart';
import 'persistent_map.dart';
import 'sorted_dbm.dart';

/// A [PersistentMap] with sorted operations backed by a [SortedDBM].
class SortedPersistentMap<K, V> extends PersistentMap<K, V> {
  final SortedDBM _sorted;
  final Uint8List Function(K) _ks;
  final K Function(Uint8List) _kd;
  final V Function(Uint8List) _vd;

  /// Create a sorted persistent map over a [SortedDBM].
  // ignore: use_super_parameters
  SortedPersistentMap(
    final SortedDBM dbm,
    final Uint8List Function(K) keySerializer,
    final K Function(Uint8List) keyDeserializer,
    final Uint8List Function(V) valueSerializer,
    final V Function(Uint8List) valueDeserializer,
  )   : _sorted = dbm,
        _ks = keySerializer,
        _kd = keyDeserializer,
        _vd = valueDeserializer,
        super(dbm, keySerializer, keyDeserializer, valueSerializer,
            valueDeserializer);

  /// Return the first (smallest) entry, or null if empty.
  MapEntry<K, V>? first() {
    final entry = _sorted.first();
    if (entry == null) return null;
    return MapEntry(_kd(entry.key), _vd(entry.value));
  }

  /// Return the last (largest) entry, or null if empty.
  MapEntry<K, V>? last() {
    final entry = _sorted.last();
    if (entry == null) return null;
    return MapEntry(_kd(entry.key), _vd(entry.value));
  }

  /// Iterate entries in sorted order from [start] to [end] (exclusive).
  Iterable<MapEntry<K, V>> range({final K? start, final K? end}) {
    final s = start != null ? _ks(start) : null;
    final e = end != null ? _ks(end) : null;
    return _SortedIterable(() => _sorted.range(start: s, end: e), _kd, _vd);
  }

  /// Return the greatest entry with key <= [key], or null.
  MapEntry<K, V>? floor(final K key) {
    final entry = _sorted.floor(_ks(key));
    if (entry == null) return null;
    return MapEntry(_kd(entry.key), _vd(entry.value));
  }

  /// Return the smallest entry with key >= [key], or null.
  MapEntry<K, V>? ceiling(final K key) {
    final entry = _sorted.ceiling(_ks(key));
    if (entry == null) return null;
    return MapEntry(_kd(entry.key), _vd(entry.value));
  }

  /// Create a sorted persistent map with String keys and String values.
  static SortedPersistentMap<String, String> strings(final File file,
      {final bool create = false,
      final int order = 128,
      final int buckets = 10007}) {
    late final RandomAccessFile random;
    if (file.existsSync()) {
      random = file.openSync(mode: FileMode.append);
    } else if (create) {
      file.createSync(recursive: true);
      random = file.openSync(mode: FileMode.write);
    } else {
      throw StateError('File does not exist and create flag not specified.');
    }
    final dbm = BTreeDBM(random, order: order, buckets: buckets);
    return SortedPersistentMap<String, String>(
      dbm,
      (final key) => convert.utf8.encoder.convert(key),
      (final bytes) => convert.utf8.decode(bytes),
      (final value) => convert.utf8.encoder.convert(value),
      (final bytes) => convert.utf8.decode(bytes),
    );
  }
}

class _SortedIterable<K, V> extends Iterable<MapEntry<K, V>> {
  final Iterator<MapEntry<Uint8List, Uint8List>> Function() _generator;
  final K Function(Uint8List) _kd;
  final V Function(Uint8List) _vd;

  _SortedIterable(this._generator, this._kd, this._vd);

  @override
  Iterator<MapEntry<K, V>> get iterator =>
      _MappingIterator(_generator(), _kd, _vd);
}

class _MappingIterator<K, V> implements Iterator<MapEntry<K, V>> {
  final Iterator<MapEntry<Uint8List, Uint8List>> _inner;
  final K Function(Uint8List) _kd;
  final V Function(Uint8List) _vd;
  MapEntry<K, V>? _current;

  _MappingIterator(this._inner, this._kd, this._vd);

  @override
  MapEntry<K, V> get current => _current!;

  @override
  bool moveNext() {
    if (!_inner.moveNext()) return false;
    _current = MapEntry(_kd(_inner.current.key), _vd(_inner.current.value));
    return true;
  }
}
