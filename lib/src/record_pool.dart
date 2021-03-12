import 'dart:typed_data';

abstract class Record {
  Uint8List get key;
  Uint8List get value;
  int get size;
}

/// Interface to a RecordPool
abstract class RecordPool {
  void clear();
  void flush();
  void free(Record record);
  Record? get(Uint8List key);
  Record? put(Uint8List key, final Uint8List value, bool overwrite);

  Iterator<MapEntry<Uint8List, Uint8List>> entries();
}
