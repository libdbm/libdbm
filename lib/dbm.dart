import 'dart:typed_data';

/// DBM specific exception.
class DBMException implements Exception {
  final int code;
  final String message;

  DBMException(this.code, this.message);

  @override
  String toString() {
    return 'DBMException{code: $code, message: $message}';
  }
}

/// Interface to an underlying DBM implementation.
abstract class DBM {
  /// Get a value from the database using [key], or else return `null`
  /// ```dart
  /// get(utf8.encode('foo');
  /// ```
  Uint8List get(Uint8List key);

  /// Remove a value from the database using [key]. If the key had
  /// an associated value, return the value or else return `null`
  /// ```dart
  /// remove(utf8.encode('foo');
  /// ```
  Uint8List remove(Uint8List key);

  /// Add a [key], [value] pair to the underlying store, overwriting any
  /// existing values. Returns the old value if one existed.
  /// ```dart
  /// put(utf8.encode('foo',utf8.encode('bar');
  /// put(utf8.encode('foo',utf8.encode('baz'); // returns 'bar'
  /// get(utf8.encode('foo'); // returns 'baz'
  /// ```
  Uint8List put(Uint8List key, Uint8List value);

  /// Add a [key], [value] pair to the underlying store if not key is in
  /// the underlying store. Returns the old value if [key] existed, otherwise
  /// [value] is returned.
  /// ```dart
  /// putIfAbsent(utf8.encode('foo',utf8.encode('bar')
  /// putIfAbsent(utf8.encode('foo',utf8.encode('baz') // returns 'bar'
  /// get(utf8.encode('foo'); // returns 'bar'
  /// ```
  Uint8List putIfAbsent(Uint8List key, Uint8List value);

  /// Iterate over all keys and values in the database
  Iterator<MapEntry<Uint8List, Uint8List>> entries();

  /// Get the last time the database was opened or modified.
  DateTime modified();

  /// Get the version of the underlying implementation as a 32bit integer.
  int version();

  /// Get the size, in bytes, of data stored. This is intended to track
  /// approximate data size and will differ from the size of the file itself.
  int size();

  /// Get the number of records stored in the database as a `Uint64`
  int count();

  /// Erases all data and resets the internal structure, but does not reclaim
  /// storage.
  void clear();

  /// Flush all data to external storage.
  void flush();

  // [flush()] and close the underlying file.
  void close();
}
