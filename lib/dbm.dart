import 'dart:typed_data';

import 'src/util.dart';

/// DBM specific exception.
class DBMException implements Exception {
  /// The code associated with the exception
  final int code;

  /// The message associated with the exception
  final String message;

  /// Constructor
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
  Uint8List? get(Uint8List key);

  /// Remove a value from the database using [key]. If the key had
  /// an associated value, return the value or else return `null`
  /// ```dart
  /// remove(utf8.encode('foo');
  /// ```
  Uint8List? remove(Uint8List key);

  /// Add a [key], [value] pair to the underlying store, overwriting any
  /// existing values. Returns the old value if one existed, otherwise
  /// returns the new value.
  /// ```dart
  /// put(utf8.encode('foo'), utf8.encode('bar')); // returns 'bar' (new)
  /// put(utf8.encode('foo'), utf8.encode('baz')); // returns 'bar' (old)
  /// get(utf8.encode('foo')); // returns 'baz'
  /// ```
  Uint8List? put(Uint8List key, Uint8List value);

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

  /// Truncate trailing free blocks at end-of-file and return bytes reclaimed.
  int compact();

  /// Flush all data to external storage.
  void flush();

  /// [flush()] and close the underlying file.
  void close();
}

/// A [DBM] with delta overlay transactions and version history.
abstract class VersionedDBM implements DBM {
  /// Start a new transaction.
  Transaction begin();

  /// Current (latest) version number.
  int get current;

  /// List of available versions (base..current).
  List<int> get versions;

  /// Read-only view at a specific version.
  DBM at(final int version);

  /// Merge deltas through the given version into the base table.
  /// If [through] is null, merges all deltas through [current].
  void merge({final int? through});

  /// Merge all deltas into the base table and convert to plain format.
  /// After flattening, the file can be reopened with plain [HashDBM].
  void flatten();
}

/// A transaction that accumulates changes and commits them atomically.
abstract class Transaction {
  /// Read a key (sees own uncommitted writes + snapshot).
  Uint8List? get(final Uint8List key);

  /// Stage a write.
  void put(final Uint8List key, final Uint8List value);

  /// Stage a delete.
  void remove(final Uint8List key);

  /// Atomically commit all staged changes as a new version.
  void commit();

  /// Discard all staged changes.
  void rollback();
}

final Uint8List _tombstone = Uint8List(0);

/// Sentinel value used internally to represent a deleted key in a delta.
Uint8List get tombstone => _tombstone;

/// Check whether a value represents a tombstone (deletion marker).
bool isTombstone(final Uint8List? value) =>
    value != null && identical(value, _tombstone);

/// A wrapper that compares [Uint8List] keys by content for use in maps.
class BytesKey {
  /// The underlying bytes.
  final Uint8List bytes;

  /// Constructor.
  BytesKey(this.bytes);

  @override
  // ignore: avoid_equals_and_hash_code_on_mutable_classes
  bool operator ==(Object other) =>
      other is BytesKey && matches(bytes, other.bytes);

  @override
  // ignore: avoid_equals_and_hash_code_on_mutable_classes
  int get hashCode => hash(bytes);
}
