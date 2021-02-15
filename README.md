# Introduction
This is __libdbm.dart__, a dart implementation of a `dbm` like database. Like many other
`dbm` based systems, it uses a hashing approach to provide a very fast key-value store. It is
purposefully intended to be very minimalistic and to have no dependencies.

__This is an early preview__. There will be additional capabilities added, including the
ability to maintain multiple indexes over the keys, and support for `IndexedDB` APIs.

## Getting Started
The API is deliberately extremely simple. In order to use this library, import
the package, open a database, and store/fetch values.

```dart
import 'dart:io';
import 'dart:convert' show utf8;
import 'package:libdbm/libdbm.dart';

final key = utf8.encode('A key');
final value = utf8.encode('A value');

final file = File('dummy.db');
final db = HashDBM(file.openSync(mode: FileMode.write));
db.put(key, value);
var result = db.get(key);
db.remove(key);
db.get(key); // will return null
db.close();
```
Note that to open an already closed database, use `FileMode.append` otherwise the old
data will be overwritten (this is a simple way to truncate the database).

## Performance

Benchmarks are notoriously difficult, but as a general guideline, `libdbm` hash storage is
capable of reading and writing 1000s of key-value pairs per second. The following numbers are
taken from the test suite for 10,000 pairs.

| buckets | op     | time           |
|:-------:|:------:|:--------------:|
| 103     | insert | 0:00:05.483791 |
| 103     | fetch  | 0:00:02.668405 |
| 1009    | insert | 0:00:03.275287 |
| 1009    | fetch: | 0:00:01.393490 |
| 10007   | insert | 0:00:00.371533 |
| 10007   | fetch  | 0:00:00.126720 |
| 100003  | insert | 0:00:00.126404 |
| 100003  | fetch  | 0:00:00.059652 |

As can be seen, one of the main factors in performance is how large the internal hash table is.
This is persisted to external storage when `flush()` or `close()` is called, and will generally
consume `16*num` bytes. As a general rule, having this be a largish prime number is good.

Other major factors are whether `flush` is set, which will force memory-based data structures to
disk whenever they are modified. It is also possible to add a CRC check to records, which will 
roughly halve the throughput (i.e. operations will take twice as long);

## Space and memory usage

The database file format has some fixed and dynamic sized overheads. As a general rule, the 
static overhead is < 1k. The dynamic overhead is whatever size is needed for the hash table and
memory pool (roughly 16 bytes per entry each), and then a per-record overhead of about 32 bytes.
As such, the overhead for storing many tiny values will be fairly high, so it is better to aggregate
such values into a single record. 

## Limitations

The biggest current limitations are related to robustness. The library doesn't (yet) support
transactions and while care has been taken to ensure reliability, the library doesn't use a WAL
so in extreme cases, there is a small chance of corruption. The best way to mitigate this
is to have `flush` turned on. Further tests need to be/will be written to handle bad input etc.

Currently the hash table size is fixed, though the file format supports rehashing/reallocating the
hash table. In the future, this capability will be used to optimize performance automatically.

## Exposed API

The interface to the underlying storage engine is basically that of a simple map from
`Uint8List` to `Uint8List`.

```dart
abstract class DBM {
  Uint8List get(Uint8List key);
  Uint8List remove(Uint8List key);
  Uint8List put(Uint8List key, Uint8List value);
  Uint8List putIfAbsent(Uint8List key, Uint8List value);

  Iterator<MapEntry<Uint8List,Uint8List>> entries();

  DateTime modified();
  int version();
  int size();
  int count();
  void clear();
  void flush();
  void close();
}
```

### Licence

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