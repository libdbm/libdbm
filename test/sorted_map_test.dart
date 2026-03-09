import 'dart:io';

import 'package:libdbm/src/sorted_map.dart';
import 'package:test/test.dart';

void main() {
  final file = File('sorted_map_test.bin');

  setUp(() {
    if (file.existsSync()) file.deleteSync();
  });

  tearDown(() {
    if (file.existsSync()) file.deleteSync();
  });

  group('SortedPersistentMap', () {
    test('strings factory: insert and sorted iteration', () {
      final map = SortedPersistentMap.strings(file, create: true, order: 8);
      map['delta'] = 'd';
      map['bravo'] = 'b';
      map['echo'] = 'e';
      map['alpha'] = 'a';
      map['charlie'] = 'c';

      expect(map.length, equals(5));
      final sorted = map.entries.map((final e) => e.key).toList();
      expect(sorted, equals(['alpha', 'bravo', 'charlie', 'delta', 'echo']));
      map.close();
    });

    test('first and last', () {
      final map = SortedPersistentMap.strings(file, create: true, order: 8);
      map['charlie'] = '3';
      map['alpha'] = '1';
      map['echo'] = '5';

      expect(map.first()!.key, equals('alpha'));
      expect(map.last()!.key, equals('echo'));
      map.close();
    });

    test('first and last on empty', () {
      final map = SortedPersistentMap.strings(file, create: true, order: 8);
      expect(map.first(), isNull);
      expect(map.last(), isNull);
      map.close();
    });

    test('range queries', () {
      final map = SortedPersistentMap.strings(file, create: true, order: 8);
      for (var i = 0; i < 20; i++) {
        map['key${i.toString().padLeft(3, '0')}'] = 'v$i';
      }

      final result = map
          .range(start: 'key005', end: 'key010')
          .map((final e) => e.key)
          .toList();
      expect(result.length, equals(5));
      expect(result.first, equals('key005'));
      expect(result.last, equals('key009'));
      map.close();
    });

    test('floor and ceiling', () {
      final map = SortedPersistentMap.strings(file, create: true, order: 8);
      map['b'] = '2';
      map['d'] = '4';
      map['f'] = '6';

      // Exact match.
      expect(map.floor('d')!.key, equals('d'));
      expect(map.ceiling('d')!.key, equals('d'));

      // Between keys.
      expect(map.floor('c')!.key, equals('b'));
      expect(map.ceiling('c')!.key, equals('d'));

      // Before all.
      expect(map.floor('a'), isNull);
      expect(map.ceiling('a')!.key, equals('b'));

      // After all.
      expect(map.floor('z')!.key, equals('f'));
      expect(map.ceiling('z'), isNull);

      map.close();
    });

    test('close and reopen preserves sorted order', () {
      var map = SortedPersistentMap.strings(file, create: true, order: 16);
      for (var i = 0; i < 100; i++) {
        map['key${i.toString().padLeft(3, '0')}'] = 'v$i';
      }
      map.close();

      map = SortedPersistentMap.strings(file, order: 16);
      expect(map.length, equals(100));
      final sorted = map.entries.map((final e) => e.key).toList();
      for (var i = 1; i < sorted.length; i++) {
        expect(sorted[i].compareTo(sorted[i - 1]), greaterThan(0));
      }
      map.close();
    });

    test('Map interface operations', () {
      final map = SortedPersistentMap.strings(file, create: true, order: 8);
      map['a'] = '1';
      map['b'] = '2';
      map['c'] = '3';

      expect(map.containsKey('b'), isTrue);
      expect(map.containsKey('z'), isFalse);
      expect(map['b'], equals('2'));

      map.remove('b');
      expect(map.containsKey('b'), isFalse);
      expect(map.length, equals(2));

      map.clear();
      expect(map.isEmpty, isTrue);
      map.close();
    });

    test('SortedPersistentMap performance',
        skip: 'Benchmark: run explicitly with --run-skipped', () {
      final n = 50000;
      final s = Stopwatch();
      final map = SortedPersistentMap.strings(file,
          create: true, order: 128, buckets: 10007);

      // Insert
      s.start();
      for (var i = 0; i < n; i++) {
        final padded = i.toString().padLeft(6, '0');
        map['key$padded'] = 'val$padded';
      }
      s.stop();
      print('SortedPersistentMap insert $n: ${s.elapsed}');

      // Random read
      s.reset();
      s.start();
      for (var i = 0; i < n; i++) {
        final padded = i.toString().padLeft(6, '0');
        map['key$padded'];
      }
      s.stop();
      print('SortedPersistentMap read $n: ${s.elapsed}');

      // Sorted iteration via entries
      s.reset();
      s.start();
      var count = 0;
      for (final _ in map.entries) {
        count++;
      }
      s.stop();
      expect(count, equals(n));
      print('SortedPersistentMap iterate $n: ${s.elapsed}');

      // Range query (10%)
      s.reset();
      s.start();
      count = 0;
      for (final _ in map.range(
          start: 'key010000', end: 'key015000')) {
        count++;
      }
      s.stop();
      expect(count, equals(5000));
      print('SortedPersistentMap range 5000: ${s.elapsed}');

      // floor/ceiling
      s.reset();
      s.start();
      for (var i = 0; i < 1000; i++) {
        map.floor('key025000');
        map.ceiling('key025000');
      }
      s.stop();
      print('SortedPersistentMap floor/ceiling 1000: '
          '${s.elapsed}');

      // first/last
      s.reset();
      s.start();
      for (var i = 0; i < 10000; i++) {
        map.first();
        map.last();
      }
      s.stop();
      print('SortedPersistentMap first/last 10000: '
          '${s.elapsed}');

      map.close();
    });
  });
}
