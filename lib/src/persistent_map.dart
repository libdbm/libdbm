import 'dart:io';
import 'dart:convert' as convert;
import 'dart:typed_data';
import 'package:libdbm/libdbm.dart';

// Hidden iterable for persistent maps
class _Iterable<T> extends Iterable<T> {
  final Iterator<T> Function() _generator;

  _Iterable(this._generator);

  @override
  Iterator<T> get iterator => _generator();
}

// Iterator over entries
class _EntryIterator<K, V> implements Iterator<MapEntry<K, V>> {
  final Iterator<MapEntry<Uint8List, Uint8List>> _iterator;
  final K Function(Uint8List) _keyDeserializer;
  final V Function(Uint8List) _valueDeserializer;
  late MapEntry<K, V> _current;

  _EntryIterator(
      this._iterator, this._keyDeserializer, this._valueDeserializer);

  @override
  MapEntry<K, V> get current => _current;

  @override
  bool moveNext() {
    final ret = _iterator.moveNext();
    if (ret) {
      _current = MapEntry(_keyDeserializer(_iterator.current.key),
          _valueDeserializer(_iterator.current.value));
    }
    return ret;
  }
}

// Iterator over keys
class _KeyIterator<K> implements Iterator<K> {
  final Iterator<MapEntry<Uint8List, Uint8List>> _iterator;
  final K Function(Uint8List) _keyDeserializer;
  late K _current;

  _KeyIterator(this._iterator, this._keyDeserializer);

  @override
  K get current => _current;

  @override
  bool moveNext() {
    final ret = _iterator.moveNext();
    if (ret) {
      _current = _keyDeserializer(_iterator.current.key);
    }
    return ret;
  }
}

// Iterator over values
class _ValueIterator<V> implements Iterator<V> {
  final Iterator<MapEntry<Uint8List, Uint8List>> _iterator;
  final V Function(Uint8List) _valueDeserializer;
  late V _current;

  _ValueIterator(this._iterator, this._valueDeserializer);

  @override
  V get current => _current;

  @override
  bool moveNext() {
    final ret = _iterator.moveNext();
    if (ret) {
      _current = _valueDeserializer(_iterator.current.value);
    }
    return ret;
  }
}

/// PersistentMap used a DBM database to provide an implementation of the
/// dart Map interface which transparently saves the Map to disk
class PersistentMap<K, V> implements Map<K, V> {
  final DBM _dbm;
  final Uint8List Function(K) _keySerializer;
  final K Function(Uint8List) _keyDeserializer;
  final Uint8List Function(V) _valueSerializer;
  final V Function(Uint8List) _valueDeserializer;

  bool Function(V, V) _valueComparator = (a, b) => a == b;

  /// Create a new PersistentMap, with the underlying dbm database, key
  /// and value serialization functions, and an optional value comparator.
  PersistentMap(this._dbm, this._keySerializer, this._keyDeserializer,
      this._valueSerializer, this._valueDeserializer,
      {bool Function(V, V)? valueComparator}) {
    if (valueComparator != null) _valueComparator = valueComparator;
  }

  /// Create a PersistentMap with strings for keys and values,
  /// with [file] being the database file, and [create] specifying
  /// whether to create a new file if it doesn't exist. If [file]
  /// already exists, the file will not be overwritten, but rather opened.
  /// If [file] contains and incompatible database, behavior is unspecified.
  static PersistentMap<String, String> withStringValue(final File file,
      {bool create = false}) {
    return make<String, String>(
        file,
        (key) => convert.utf8.encoder.convert(key),
        (bytes) => convert.utf8.decode(bytes),
        (value) => convert.utf8.encoder.convert(value),
        (bytes) => convert.utf8.decode(bytes),
        create: create);
  }

  /// Create a PersistentMap with strings for keys and maps for values,
  /// with [file] being the database file, and [create] specifying
  /// whether to create a new file if it doesn't exist. If [file]
  /// already exists, the file will not be overwritten, but rather opened.
  /// If [file] contains and incompatible database, behavior is unspecified.
  /// Note that the value must be convertible using the json functions in
  /// dart:convert
  static PersistentMap<String, Map<String, dynamic>> withMapValue(
      final File file,
      {bool create = false,
      bool Function(Map<String, dynamic>, Map<String, dynamic>)? comparator}) {
    return make<String, Map<String, dynamic>>(
        file,
        (key) => convert.utf8.encoder.convert(key),
        (bytes) => convert.utf8.decode(bytes),
        (value) => convert.utf8.encoder.convert(convert.json.encode(value)),
        (bytes) => convert.json.decode(convert.utf8.decode(bytes)),
        create: create,
        comparator: comparator);
  }

  /// Create a PersistentMap with [K1] for keys and [V1] for values,
  /// with [file] being the database file, and [create] specifying
  /// whether to create a new file if it doesn't exist. If [file]
  /// already exists, the file will not be overwritten, but rather opened.
  /// The serializers for the keys and values must be provided when calling
  /// this function. If [file] contains and incompatible database, behavior
  /// is unspecified.
  static PersistentMap<K1, V1> make<K1, V1>(
      final File file,
      final Uint8List Function(K1) keySerializer,
      final K1 Function(Uint8List) keyDeserializer,
      final Uint8List Function(V1) valueSerializer,
      final V1 Function(Uint8List) valueDeserializer,
      {bool create = false,
      bool Function(V1, V1)? comparator}) {
    late final RandomAccessFile _random;
    if (file.existsSync()) {
      _random = file.openSync(mode: FileMode.append);
    } else if (create) {
      file.createSync(recursive: true);
      _random = file.openSync(mode: FileMode.write);
    } else {
      throw StateError('File does not exist and create flag not specified.');
    }
    DBM dbm = HashDBM(_random);
    return PersistentMap<K1, V1>(
        dbm, keySerializer, keyDeserializer, valueSerializer, valueDeserializer,
        valueComparator: comparator);
  }

  void close() {
    _dbm.close();
  }

  @override
  V? operator [](Object? key) {
    assert(key != null);
    final ret = _dbm.get(_keySerializer(key as K));
    return ret != null ? _valueDeserializer(ret) : null;
  }

  @override
  void operator []=(K key, V value) {
    assert(key != null);
    assert(value != null);
    _dbm.put(_keySerializer(key), _valueSerializer(value));
  }

  @override
  void addAll(Map<K, V> other) {
    addEntries(other.entries);
  }

  @override
  void addEntries(Iterable<MapEntry<K, V>> newEntries) {
    for (var e in newEntries) {
      _dbm.put(_keySerializer(e.key), _valueSerializer(e.value));
    }
  }

  /// This method is not implemented by this map implementation and will
  /// result in an exception being thrown.
  @override
  Map<RK, RV> cast<RK, RV>() {
    throw UnimplementedError();
  }

  @override
  void clear() {
    _dbm.clear();
  }

  @override
  bool containsKey(dynamic key) {
    assert(key != null);
    return _dbm.get(_keySerializer(key)) != null;
  }

  /// Checks to see if the map contains a value. The result of this is dictated
  /// by the comparator function passed when creating the PersistentMap and
  /// uses it to check every entry value until a match is found. The function
  /// should therefore be capable of deep comparisons for structured types.
  /// Given that this function will check _every_ value until a match is found
  /// it can potentially have significant performance impact.
  @override
  bool containsValue(dynamic value) {
    assert(value != null);
    for (var v in values) {
      if (_valueComparator(v, value)) {
        return true;
      }
    }
    return false;
  }

  @override
  Iterable<MapEntry<K, V>> get entries => _Iterable(() =>
      _EntryIterator(_dbm.entries(), _keyDeserializer, _valueDeserializer));

  @override
  void forEach(void Function(K key, V value) action) {
    entries.forEach((e) => action(e.key, e.value));
  }

  @override
  bool get isEmpty => _dbm.count() == 0;

  @override
  bool get isNotEmpty => _dbm.count() != 0;

  @override
  Iterable<K> get keys =>
      _Iterable(() => _KeyIterator(_dbm.entries(), _keyDeserializer));

  @override
  int get length => _dbm.count();

  @override
  V putIfAbsent(K key, V Function() ifAbsent) {
    assert(key != null);
    final value = ifAbsent();
    assert(value != null, 'value from isAbsent() is null');
    final ret = _dbm.putIfAbsent(_keySerializer(key), _valueSerializer(value));
    return _valueDeserializer(ret);
  }

  @override
  V? remove(Object? key) {
    assert(key != null);
    final ret = _dbm.remove(_keySerializer(key as K));
    return ret != null ? _valueDeserializer(ret) : null;
  }

  @override
  void removeWhere(bool test(K key, V value)) {
    final remove = [];
    entries.forEach((e) {
      if (test(e.key, e.value)) remove.add(e.key);
    });
    for (var k in remove) {
      _dbm.remove(_keySerializer(k));
    }
  }

  @override
  V update(K key, V ifPresent(V value), {V ifAbsent()?}) {
    assert(key != null);
    final k = _keySerializer(key);
    final tmp = _dbm.get(k);
    var ret;
    if (tmp != null) {
      ret = ifPresent(_valueDeserializer(tmp));
      _dbm.put(k, _valueSerializer(ret));
    } else {
      assert(ifAbsent != null);
      ret = ifAbsent!();
      _dbm.put(k, _valueSerializer(ret));
    }
    return ret;
  }

  @override
  void updateAll(V update(K key, V value)) {
    for (var key in keys.toList()) {
      final k = _keySerializer(key);
      final v = update(key, _valueDeserializer(_dbm.get(k)!));
      _dbm.put(_keySerializer(key), _valueSerializer(v));
    }
  }

  @override
  Iterable<V> get values =>
      _Iterable(() => _ValueIterator(_dbm.entries(), _valueDeserializer));

  /// Map from a PersistentMap to a memory-resident map created by applying
  /// a mapping function to each key-value pair.
  @override
  Map<K2, V2> map<K2, V2>(MapEntry<K2, V2> Function(K key, V value) convert) {
    Map<K2, V2> result = {};
    entries.forEach((e) {
      final entry = convert(e.key, e.value);
      result[entry.key] = entry.value;
    });
    return result;
  }
}
