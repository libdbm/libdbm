import 'dart:typed_data';

import '../dbm.dart';

/// Comparator for raw byte keys. Returns negative if a < b,
/// zero if equal, positive if a > b.
typedef KeyComparator = int Function(Uint8List a, Uint8List b);

/// A [DBM] that maintains keys in sorted order and supports range queries.
abstract class SortedDBM implements DBM {
  /// Return the first (smallest) entry, or null if empty.
  MapEntry<Uint8List, Uint8List>? first();

  /// Return the last (largest) entry, or null if empty.
  MapEntry<Uint8List, Uint8List>? last();

  /// Iterate entries in sorted order from [start] (inclusive) to [end]
  /// (exclusive). If [start] is null, iteration begins at the first key.
  /// If [end] is null, iteration continues through the last key.
  Iterator<MapEntry<Uint8List, Uint8List>> range(
      {final Uint8List? start, final Uint8List? end});

  /// Return the greatest entry with key less than or equal to [key],
  /// or null if no such entry exists.
  MapEntry<Uint8List, Uint8List>? floor(final Uint8List key);

  /// Return the smallest entry with key greater than or equal to [key],
  /// or null if no such entry exists.
  MapEntry<Uint8List, Uint8List>? ceiling(final Uint8List key);
}
