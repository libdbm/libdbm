import 'dart:typed_data';

/// Interface to a record stored in the database
abstract class Record {
  /// The key for the record
  Uint8List get key;

  /// The value for the report
  Uint8List get value;

  /// Get the total size, in bytes, of the record
  int get size;
}

/// Interface to a RecordPool
abstract class RecordPool {
  /// Clear all records from the pool
  void clear();

  /// Flush data to storage
  void flush();

  /// Free the given record
  void free(Record record);

  /// Find the record for the given key, possible returning null
  Record? get(Uint8List key);

  /// Store a record, and if a record with the key already exists, return
  /// the previous record.
  // ignore: avoid_positional_boolean_parameters
  Record? put(Uint8List key, final Uint8List value, bool overwrite);

  /// Get an iterator over all records
  Iterator<MapEntry<Uint8List, Uint8List>> entries();
}
