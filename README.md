# libdbm

A fast, zero-dependency, disk-based key-value store for Dart.

- **HashDBM** — hash table storage for fast unordered access
- **BTreeDBM** — B+tree storage with sorted iteration, range queries, floor/ceiling
- **VersionedHashDBM** — delta-overlay transactions with point-in-time snapshots
- **PersistentMap** / **SortedPersistentMap** — familiar `Map<K, V>` API backed by disk

## Getting Started

### PersistentMap

The simplest way to use libdbm. Works like a regular `Map`, but data persists to disk.

```dart
import 'dart:io';
import 'package:libdbm/libdbm.dart';

void main() {
  final file = File('my.db');
  final db = PersistentMap.withStringValue(file, create: true);

  db['foo'] = 'bar';
  print(db['foo']); // bar

  db.remove('foo');
  db.close();
  file.deleteSync();
}
```

### HashDBM

The low-level hash table API. Keys and values are `Uint8List`.

```dart
import 'dart:convert' show utf8;
import 'dart:io';
import 'package:libdbm/libdbm.dart';

void main() {
  final file = File('hash.db');
  final db = HashDBM(file.openSync(mode: FileMode.write));

  db.put(utf8.encode('key'), utf8.encode('value'));
  final result = db.get(utf8.encode('key'));
  print(utf8.decode(result!)); // value

  db.close();
  file.deleteSync();
}
```

Use `FileMode.append` to reopen an existing database without truncating.

### BTreeDBM

Sorted key-value store with range queries and ordered iteration.

```dart
import 'dart:convert' show utf8;
import 'dart:io';
import 'package:libdbm/libdbm.dart';

void main() {
  final file = File('btree.db');
  final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 64);

  for (final name in ['delta', 'alpha', 'echo', 'bravo', 'charlie']) {
    db.put(utf8.encode(name), utf8.encode('val_$name'));
  }

  // Sorted iteration
  final iter = db.entries();
  while (iter.moveNext()) {
    print(utf8.decode(iter.current.key));
  }
  // alpha, bravo, charlie, delta, echo

  // Range query [bravo, delta)
  final range = db.range(
      start: utf8.encode('bravo'), end: utf8.encode('delta'));
  while (range.moveNext()) {
    print(utf8.decode(range.current.key));
  }
  // bravo, charlie

  // Floor and ceiling
  print(utf8.decode(db.floor(utf8.encode('d'))!.key));   // charlie
  print(utf8.decode(db.ceiling(utf8.encode('d'))!.key)); // delta

  db.close();
  file.deleteSync();
}
```

Or use `SortedPersistentMap` for a typed `Map` interface:

```dart
final map = SortedPersistentMap.strings(file, create: true, order: 64);
map['zebra'] = 'z';
map['apple'] = 'a';
for (final entry in map.entries) {
  print('${entry.key} = ${entry.value}');
}
// apple = a, zebra = z
map.close();
```

### VersionedHashDBM

Transactional storage with version history and point-in-time queries.

```dart
import 'dart:convert' show utf8;
import 'dart:io';
import 'package:libdbm/libdbm.dart';

void main() {
  final file = File('versioned.db');
  final db = VersionedHashDBM(file.openSync(mode: FileMode.write));

  var tx = db.begin();
  tx.put(utf8.encode('user'), utf8.encode('alice'));
  tx.commit(); // version 1

  tx = db.begin();
  tx.put(utf8.encode('user'), utf8.encode('bob'));
  tx.commit(); // version 2

  // Point-in-time query
  final v1 = db.at(1);
  print(utf8.decode(v1.get(utf8.encode('user'))!)); // alice

  // Merge and flatten
  db.flatten();
  db.close();
  file.deleteSync();
}
```

## Performance

HashDBM with 50,000 entries (flush=false, 10007 buckets):

| Operation | Time | Per-op |
|:----------|:-----|:-------|
| Insert | 0.54s | 10.8 µs |
| Random read | 0.37s | 7.5 µs |
| Overwrite | 0.47s | 9.5 µs |
| Delete 25K | 0.16s | 6.6 µs |
| Iteration | 0.03s | 1.1 µs |

BTreeDBM with 50,000 entries (order=128, flush=false):

| Operation | Time | Per-op |
|:----------|:-----|:-------|
| Insert | 0.54s | 10.9 µs |
| Random read | 0.16s | 3.2 µs |
| Range query (10K) | 0.8ms | 0.1 µs |
| Sorted iteration | 3.6ms | 0.1 µs |

Key factors affecting performance:
- **Hash table size** (buckets) — use a large prime for HashDBM
- **B+tree order** — higher order means fewer levels but larger nodes
- **flush mode** — `flush=true` is safest but slower (~5x)
- **CRC checking** — optional, roughly halves throughput

## Architecture

Layered design with no runtime dependencies:

1. **I/O layer** — `Pointer`, `Block`, `PointerBlock` over `RandomAccessFile`
2. **Memory pool** — block allocator with 128-byte alignment and free-list merging
3. **Hash record pool** — hash table with separate chaining
4. **HashDBM** — hash-based `DBM` implementation
5. **BTreeDBM** — B+tree layered over HashDBM for sorted access
6. **PersistentMap** / **SortedPersistentMap** — typed `Map` wrappers

## Limitations

- No WAL or full transaction support for HashDBM (use `flush=true` for safety)
- Hash table size is fixed at creation time
- `PersistentMap` does not support `cast()`

## API

```dart
abstract class DBM {
  Uint8List? get(Uint8List key);
  Uint8List? put(Uint8List key, Uint8List value);
  Uint8List putIfAbsent(Uint8List key, Uint8List value);
  Uint8List? remove(Uint8List key);
  Iterator<MapEntry<Uint8List, Uint8List>> entries();
  int count();
  int size();
  DateTime modified();
  int version();
  void clear();
  int compact();
  void flush();
  void close();
}

abstract class SortedDBM implements DBM {
  MapEntry<Uint8List, Uint8List>? first();
  MapEntry<Uint8List, Uint8List>? last();
  Iterator<MapEntry<Uint8List, Uint8List>> range({Uint8List? start, Uint8List? end});
  MapEntry<Uint8List, Uint8List>? floor(Uint8List key);
  MapEntry<Uint8List, Uint8List>? ceiling(Uint8List key);
}
```

## Licence

```
Copyright 2021 Gavin Nicol

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
