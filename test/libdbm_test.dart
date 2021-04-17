import 'dart:convert' show utf8;
import 'dart:io';

import 'package:faker/faker.dart';
import 'package:libdbm/libdbm.dart';
import 'package:test/test.dart';

// ignore: constant_identifier_names
const COUNT = 10000;
void main() {
  final file = File('dummy.bin');
  final faker = Faker();
  int writeRecords(HashDBM db, int count) {
    var size = 0;
    for (var i = 0; i < count; i++) {
      final key = utf8.encoder.convert('key: $i');
      final data = utf8.encoder.convert('value: $i');
      size += key.length;
      size += data.length;
      db.put(key, data);
      expect(db.get(key), equals(data));
    }
    return size;
  }

  void deleteRecords(HashDBM db, int count) {
    for (var i = 0; i < count; i++) {
      final key = utf8.encoder.convert('key: $i');
      final data = utf8.encoder.convert('value: $i');
      expect(db.remove(key), equals(data));
      expect(db.get(key), isNull);
    }
  }

  void readRecords(HashDBM db, int count) {
    for (var i = 0; i < count; i++) {
      final key = utf8.encoder.convert('key: $i');
      final data = utf8.encoder.convert('value: $i');
      expect(db.get(key), equals(data));
    }
  }

  setUp(() async {
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
      } catch (e) {} finally {}
    }
  });
  tearDown(() async {
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
      } catch (e) {} finally {}
    }
  });
  test('Create and retrieve with timing', () {
    final count = 10000;
    for (var size in [103, 1009, 10007, 100003]) {
      var db = HashDBM(file.openSync(mode: FileMode.write),
          buckets: size, flush: false, crc: false);
      var s = Stopwatch();

      s.start();
      writeRecords(db, count);
      print('$size insert: ${s.elapsed}');

      s.reset();
      db.flush();
      print('$size flush: ${s.elapsed}');

      s.reset();
      readRecords(db, count);
      print('$size fetch: ${s.elapsed}');

      s.stop();
      db.close();

      print('');
    }
  });
  test('Closing database preserves state.', () {
    final first = HashDBM(file.openSync(mode: FileMode.write));
    var diff = DateTime.now().difference(first.modified());
    expect(diff.inMilliseconds, lessThan(1000));
    expect(first.version(), equals(HashDBM.VERSION));
    writeRecords(first, COUNT);
    first.close();

    final second = HashDBM(file.openSync(mode: FileMode.append));
    diff = DateTime.now().difference(second.modified());
    expect(diff.inMilliseconds, lessThan(1000));
    expect(second.version(), equals(HashDBM.VERSION));
    readRecords(second, COUNT);
    second.close();
  });
  test('Hash table size is loaded from disk.', () {
    final first = HashDBM(file.openSync(mode: FileMode.write), buckets: 1009);
    writeRecords(first, COUNT);
    first.close();

    final second = HashDBM(file.openSync(mode: FileMode.append), buckets: 103);
    readRecords(second, COUNT);
    expect(second.hashTableSize, equals(1009));
    second.close();
  });
  test('Deleting records.', () {
    final db = HashDBM(file.openSync(mode: FileMode.write));
    final size = writeRecords(db, COUNT);
    expect(db.count(), equals(COUNT));
    expect(db.size(), greaterThan(size));

    deleteRecords(db, COUNT);
    expect(db.count(), equals(0));
    expect(db.size(), equals(0));

    db.close();
  });
  test('Using CRC check on records.', () {
    final db =
        HashDBM(file.openSync(mode: FileMode.write), flush: true, crc: true);
    final size = writeRecords(db, COUNT);
    expect(db.count(), equals(COUNT));
    expect(db.size(), greaterThan(size));

    deleteRecords(db, COUNT);
    expect(db.count(), equals(0));
    expect(db.size(), equals(0));

    writeRecords(db, COUNT);
    expect(db.count(), equals(COUNT));
    expect(db.size(), greaterThan(size));

    db.close();
  });
  test('Space is reused if data is the same size.', () {
    // NOTE: If we enable flushing, the memory pool will affect size
    final first = HashDBM(file.openSync(mode: FileMode.write), flush: true);
    final start = file.lengthSync();

    writeRecords(first, COUNT);
    var size = file.lengthSync();
    expect(size, greaterThan(start));

    deleteRecords(first, COUNT);
    size = file.lengthSync();

    writeRecords(first, COUNT);
    expect(file.lengthSync(), equals(size));

    first.close();
  });
  test('Iteration over keys and values.', () {
    final db = HashDBM(file.openSync(mode: FileMode.write));
    final keys = [];
    final data = [];
    for (var i = 0; i < 100; i++) {
      final key = utf8.encoder.convert('key: $i');
      final data = utf8.encoder.convert('value: $i');
      expect(db.put(key, data), equals(data));
    }
    for (var i = db.entries(); i.moveNext();) {
      final e = i.current;
      keys.add(utf8.decode(e.key));
      data.add(utf8.decode(e.value));
    }
    expect(keys.length, equals(100));
    expect(data.length, equals(100));
    for (var i = 0; i < 100; i++) {
      final key = 'key: $i';
      final data = 'value: $i';
      expect(keys.contains(key), equals(true));
      expect(data.contains(data), equals(true));
    }
    db.close();
  });
  test('Calling clear() results in empty database.', () {
    var db = HashDBM(file.openSync(mode: FileMode.write));
    for (var i = 0; i < 100; i++) {
      final key = utf8.encoder.convert('key: $i');
      final data = utf8.encoder.convert('value: $i');
      expect(db.put(key, data), equals(data));
      expect(db.get(key), equals(data));
    }
    db.clear();
    db.close();
    db = HashDBM(file.openSync(mode: FileMode.append));
    for (var i = 0; i < 100; i++) {
      final key = utf8.encoder.convert('key: $i');
      expect(db.get(key), isNull);
    }
    var i = db.entries();
    expect(i.moveNext(), equals(false));
    db.close();
  });
  test('Iterator on empty database is null', () {
    var db = HashDBM(file.openSync(mode: FileMode.write));
    var i = db.entries();
    expect(i.moveNext(), equals(false));
    db.close();
  });
  test('Large keys and records.', () {
    final db = HashDBM(file.openSync(mode: FileMode.write));
    final keys = [];
    final data = [];
    for (var i = 0; i < 100; i++) {
      keys.add(utf8.encoder.convert(faker.lorem.sentences(1000).join(' ')));
      data.add(utf8.encoder.convert(faker.lorem.sentences(1000).join(' ')));
      expect(db.put(keys[i], data[i]), equals(data[i]));
    }
    expect(db.count(), equals(100));
    for (var i = 0; i < 100; i++) {
      expect(db.remove(keys[i]), equals(data[i]));
    }
    expect(db.count(), equals(0));
    expect(db.size(), equals(0));

    db.close();
  });
  test('Stress mempool by deleting large numbers of records.', () {
    // ignore: constant_identifier_names
    const MAX = 500;
    final db = HashDBM(file.openSync(mode: FileMode.write), flush: false);
    for (var i = 0; i < MAX; i++) {
      final key = utf8.encoder.convert('key:$i');
      final data = utf8.encoder.convert('value:$i');
      expect(db.put(key, data), equals(data));
    }
    for (var i = 0; i < MAX; i++) {
      final key = utf8.encoder.convert('key:$i');
      final data = utf8.encoder.convert('a value:$i');
      db.put(key, data);
    }
    for (var i = 150; i < MAX; i++) {
      final key = utf8.encoder.convert('key:$i');
      final data = utf8.encoder.convert('a value:$i');
      expect(db.remove(key), equals(data));
    }
    for (var i = 0; i < 150; i++) {
      final key = utf8.encoder.convert('key:$i');
      final data = utf8.encoder.convert('a value:$i');
      expect(db.remove(key), equals(data));
    }
    db.flush();
    expect(db.count(), equals(0));

    var size = MAX;
    for (var count = 1; count < 5; count++) {
      size *= count;
      for (var i = 0; i < size; i++) {
        final key = utf8.encoder.convert('key:$i');
        final data = utf8.encoder.convert('value:$i');
        expect(db.put(key, data), equals(data));
      }
      db.flush();
      for (var i = 0; i < size; i += 2) {
        final key = utf8.encoder.convert('key:$i');
        final data = utf8.encoder.convert('value:$i');
        expect(db.remove(key), equals(data));
      }
      db.flush();
    }
    for (var i = 0; i < 50000; i++) {
      final key = utf8.encoder.convert('key:$i');
      final data = utf8.encoder.convert('value:$i');
      expect(db.put(key, data), equals(data));
    }
    for (var i = 0; i < 50000; i++) {
      final key = utf8.encoder.convert('key:$i');
      final data = utf8.encoder.convert('value:$i');
      expect(db.remove(key), equals(data));
    }
    db.flush();
    expect(db.count(), equals(0));
    expect(db.size(), equals(0));

    db.close();
  });
  test('Verify putIfAbsent behavior', () {
    final db = HashDBM(file.openSync(mode: FileMode.write), buckets: 109);
    final key = utf8.encoder.convert('key');
    final first = utf8.encoder.convert('first');
    final second = utf8.encoder.convert('second');

    // Insert a new value, returns the new value
    expect(db.putIfAbsent(key, first), equals(first));
    expect(db.get(key), equals(first));

    // Try insert a new value, returns the old value and does not replace
    expect(db.putIfAbsent(key, second), equals(first));
    expect(db.get(key), equals(first));

    // Insert a new value, returns the old value, but we can get the new value
    expect(db.put(key, second), equals(first));
    expect(db.get(key), equals(second));
    expect(db.putIfAbsent(key, first), equals(second));

    // Only 1 record (key) was ever inserted, but with different values
    expect(db.count(), equals(1));

    db.close();
  });
  test('Stress test with small hash table.', () {
    // ignore: constant_identifier_names
    const CYCLES = 10;
    // ignore: constant_identifier_names
    const MAX = 10000;

    final db = HashDBM(file.openSync(mode: FileMode.write),
        buckets: 109, flush: true, crc: true);
    for (var cycle = 0; cycle < CYCLES; cycle++) {
      for (var i = 0; i < MAX; i++) {
        final key = utf8.encoder.convert('key: $i');
        final data = utf8.encoder.convert(faker.lorem.sentence());
        db.put(key, data);
        expect(db.get(key), equals(data));
      }
    }
    expect(db.count(), equals(MAX));
    db.close();

    for (var cycle = 0; cycle < CYCLES; cycle++) {
      final db = HashDBM(file.openSync(mode: FileMode.append));
      for (var i in faker.randomGenerator.numbers(MAX, 100)) {
        final key = utf8.encoder.convert('key: $i');
        final data = utf8.encoder.convert(faker.lorem.sentence());
        expect(db.remove(key), isNotNull);
        expect(db.put(key, data), equals(data));
      }
      db.close();
    }
  });
  // TODO: Negative tests
}
