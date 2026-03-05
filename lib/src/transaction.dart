import 'dart:typed_data';

import '../dbm.dart';
import 'versioned_dbm.dart';

/// Implementation of [Transaction] that accumulates changes in memory
/// and commits them atomically as a delta block.
class HashTransaction implements Transaction {
  final VersionedHashDBM _dbm;
  final int _snapshot;
  final Map<BytesKey, Uint8List?> _pending = {};
  bool _done = false;

  /// Constructor. [snapshot] is the version at which reads are resolved.
  HashTransaction(this._dbm, this._snapshot);

  @override
  Uint8List? get(final Uint8List key) {
    _guard();
    final k = BytesKey(key);
    if (_pending.containsKey(k)) {
      final value = _pending[k];
      return value == null ? null : Uint8List.fromList(value);
    }
    return _dbm.resolve(key, _snapshot);
  }

  @override
  void put(final Uint8List key, final Uint8List value) {
    _guard();
    _pending[BytesKey(Uint8List.fromList(key))] = Uint8List.fromList(value);
  }

  @override
  void remove(final Uint8List key) {
    _guard();
    _pending[BytesKey(Uint8List.fromList(key))] = null;
  }

  @override
  void commit() {
    _guard();
    _done = true;
    if (_pending.isEmpty) return;
    _dbm.apply(_pending);
  }

  @override
  void rollback() {
    _guard();
    _done = true;
    _pending.clear();
  }

  void _guard() {
    if (_done) {
      throw DBMException(400, 'Transaction already completed');
    }
  }
}
