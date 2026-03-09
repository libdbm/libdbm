import 'dart:convert' show utf8;
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:libdbm/libdbm.dart';
import 'package:test/test.dart';

Uint8List _key(final String value) => Uint8List.fromList(utf8.encode(value));
Uint8List _val(final String value) => Uint8List.fromList(utf8.encode(value));
String _str(final Uint8List bytes) => utf8.decode(bytes);

void main() {
  final file = File('btree_test.bin');

  setUp(() {
    if (file.existsSync()) file.deleteSync();
  });

  tearDown(() {
    if (file.existsSync()) file.deleteSync();
  });

  group('BTreeDBM', () {
    test('empty tree', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      expect(db.get(_key('anything')), isNull);
      expect(db.count(), equals(0));
      expect(db.first(), isNull);
      expect(db.last(), isNull);
      final iter = db.entries();
      expect(iter.moveNext(), isFalse);
      db.close();
    });

    test('single insert and get', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      final result = db.put(_key('foo'), _val('bar'));
      expect(result, isNull); // first insert returns null
      expect(_str(db.get(_key('foo'))!), equals('bar'));
      expect(db.count(), equals(1));
      db.close();
    });

    test('overwrite returns old value', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      db.put(_key('foo'), _val('bar'));
      final old = db.put(_key('foo'), _val('baz'));
      expect(_str(old!), equals('bar'));
      expect(_str(db.get(_key('foo'))!), equals('baz'));
      expect(db.count(), equals(1));
      db.close();
    });

    test('sequential inserts', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 1000; i++) {
        final padded = i.toString().padLeft(4, '0');
        db.put(_key('key$padded'), _val('val$i'));
      }
      expect(db.count(), equals(1000));
      for (var i = 0; i < 1000; i++) {
        final padded = i.toString().padLeft(4, '0');
        expect(_str(db.get(_key('key$padded'))!), equals('val$i'));
      }
      db.close();
    });

    test('random inserts', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 16);
      final rng = Random(42);
      final map = <String, String>{};
      for (var i = 0; i < 1000; i++) {
        final k = 'k${rng.nextInt(100000)}';
        final v = 'v$i';
        map[k] = v;
        db.put(_key(k), _val(v));
      }
      expect(db.count(), equals(map.length));
      for (final entry in map.entries) {
        expect(_str(db.get(_key(entry.key))!), equals(entry.value));
      }
      db.close();
    });

    test('sorted iteration order', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      final keys = ['delta', 'bravo', 'echo', 'alpha', 'charlie'];
      for (final k in keys) {
        db.put(_key(k), _val('v_$k'));
      }
      final sorted = <String>[];
      final iter = db.entries();
      while (iter.moveNext()) {
        sorted.add(_str(iter.current.key));
      }
      expect(sorted, equals(['alpha', 'bravo', 'charlie', 'delta', 'echo']));
      db.close();
    });

    test('delete existing', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      db.put(_key('a'), _val('1'));
      db.put(_key('b'), _val('2'));
      db.put(_key('c'), _val('3'));
      final old = db.remove(_key('b'));
      expect(_str(old!), equals('2'));
      expect(db.get(_key('b')), isNull);
      expect(db.count(), equals(2));
      expect(_str(db.get(_key('a'))!), equals('1'));
      expect(_str(db.get(_key('c'))!), equals('3'));
      db.close();
    });

    test('delete nonexistent', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      db.put(_key('a'), _val('1'));
      expect(db.remove(_key('z')), isNull);
      expect(db.count(), equals(1));
      db.close();
    });

    test('delete from empty', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      expect(db.remove(_key('a')), isNull);
      expect(db.count(), equals(0));
      db.close();
    });

    test('delete all keys', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 100; i++) {
        db.put(_key('key${i.toString().padLeft(3, '0')}'), _val('v$i'));
      }
      expect(db.count(), equals(100));
      for (var i = 0; i < 100; i++) {
        db.remove(_key('key${i.toString().padLeft(3, '0')}'));
      }
      expect(db.count(), equals(0));
      expect(db.entries().moveNext(), isFalse);
      expect(db.first(), isNull);
      expect(db.last(), isNull);
      db.close();
    });

    test('clear resets to empty', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 50; i++) {
        db.put(_key('k$i'), _val('v$i'));
      }
      db.clear();
      expect(db.count(), equals(0));
      expect(db.get(_key('k0')), isNull);
      expect(db.entries().moveNext(), isFalse);
      // Can insert after clear.
      db.put(_key('new'), _val('value'));
      expect(db.count(), equals(1));
      db.close();
    });

    test('close and reopen persistence', () {
      var db = BTreeDBM(file.openSync(mode: FileMode.write), order: 16);
      for (var i = 0; i < 500; i++) {
        final padded = i.toString().padLeft(4, '0');
        db.put(_key('key$padded'), _val('val$i'));
      }
      db.close();

      db = BTreeDBM(file.openSync(mode: FileMode.append), order: 16);
      expect(db.count(), equals(500));
      for (var i = 0; i < 500; i++) {
        final padded = i.toString().padLeft(4, '0');
        expect(_str(db.get(_key('key$padded'))!), equals('val$i'));
      }
      // Verify sorted order persists.
      final iter = db.entries();
      String? previous;
      while (iter.moveNext()) {
        final k = _str(iter.current.key);
        if (previous != null) {
          expect(k.compareTo(previous), greaterThan(0));
        }
        previous = k;
      }
      db.close();
    });

    test('split stress with small order', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 4);
      for (var i = 0; i < 200; i++) {
        final padded = i.toString().padLeft(4, '0');
        db.put(_key('k$padded'), _val('v$i'));
      }
      expect(db.count(), equals(200));

      // Verify all present and sorted.
      final sorted = <String>[];
      final iter = db.entries();
      while (iter.moveNext()) {
        sorted.add(_str(iter.current.key));
      }
      expect(sorted.length, equals(200));
      for (var i = 1; i < sorted.length; i++) {
        expect(sorted[i].compareTo(sorted[i - 1]), greaterThan(0));
      }
      db.close();
    });

    test('first and last', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      db.put(_key('charlie'), _val('3'));
      db.put(_key('alpha'), _val('1'));
      db.put(_key('echo'), _val('5'));
      db.put(_key('bravo'), _val('2'));
      db.put(_key('delta'), _val('4'));

      expect(_str(db.first()!.key), equals('alpha'));
      expect(_str(db.last()!.key), equals('echo'));
      db.close();
    });

    test('floor and ceiling', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 10; i++) {
        db.put(_key('key${(i * 10).toString().padLeft(3, '0')}'),
            _val('v$i'));
      }

      // Exact match.
      expect(
          _str(db.floor(_key('key050'))!.key), equals('key050'));
      expect(
          _str(db.ceiling(_key('key050'))!.key), equals('key050'));

      // Between keys.
      expect(
          _str(db.floor(_key('key055'))!.key), equals('key050'));
      expect(
          _str(db.ceiling(_key('key055'))!.key), equals('key060'));

      // Before all keys.
      expect(db.floor(_key('a')), isNull);
      expect(
          _str(db.ceiling(_key('a'))!.key), equals('key000'));

      // After all keys.
      expect(
          _str(db.floor(_key('z'))!.key), equals('key090'));
      expect(db.ceiling(_key('z')), isNull);

      db.close();
    });

    test('range bounded', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 100; i++) {
        db.put(
            _key('k${i.toString().padLeft(3, '0')}'), _val('v$i'));
      }

      final result = <String>[];
      final iter = db.range(start: _key('k010'), end: _key('k020'));
      while (iter.moveNext()) {
        result.add(_str(iter.current.key));
      }
      expect(result.length, equals(10)); // k010..k019
      expect(result.first, equals('k010'));
      expect(result.last, equals('k019'));
      db.close();
    });

    test('range unbounded start', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 20; i++) {
        db.put(
            _key('k${i.toString().padLeft(3, '0')}'), _val('v$i'));
      }

      final result = <String>[];
      final iter = db.range(end: _key('k005'));
      while (iter.moveNext()) {
        result.add(_str(iter.current.key));
      }
      expect(result.length, equals(5)); // k000..k004
      db.close();
    });

    test('range unbounded end', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 20; i++) {
        db.put(
            _key('k${i.toString().padLeft(3, '0')}'), _val('v$i'));
      }

      final result = <String>[];
      final iter = db.range(start: _key('k015'));
      while (iter.moveNext()) {
        result.add(_str(iter.current.key));
      }
      expect(result.length, equals(5)); // k015..k019
      db.close();
    });

    test('range fully unbounded equals entries', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 50; i++) {
        db.put(_key('k${i.toString().padLeft(3, '0')}'), _val('v$i'));
      }
      final all = <String>[];
      final iter = db.range();
      while (iter.moveNext()) {
        all.add(_str(iter.current.key));
      }
      expect(all.length, equals(50));
      db.close();
    });

    test('putIfAbsent semantics', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      final result = db.putIfAbsent(_key('a'), _val('1'));
      expect(_str(result), equals('1'));
      expect(db.count(), equals(1));

      final existing = db.putIfAbsent(_key('a'), _val('2'));
      expect(_str(existing), equals('1'));
      expect(_str(db.get(_key('a'))!), equals('1'));
      expect(db.count(), equals(1));
      db.close();
    });

    test('large keys and values', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      final large = 'x' * 10000;
      db.put(_key(large), _val(large));
      expect(_str(db.get(_key(large))!), equals(large));
      db.close();
    });

    test('wrap constructor with existing HashDBM', () {
      final hash = HashDBM(file.openSync(mode: FileMode.write));
      final db = BTreeDBM.wrap(hash, order: 8);
      db.put(_key('a'), _val('1'));
      db.put(_key('b'), _val('2'));
      expect(db.count(), equals(2));
      expect(_str(db.get(_key('a'))!), equals('1'));
      db.close();
      hash.close();
    });

    test('custom comparator: reverse ordering', () {
      int reverse(final Uint8List a, final Uint8List b) =>
          -utf8.decode(a).compareTo(utf8.decode(b));

      final db = BTreeDBM(file.openSync(mode: FileMode.write),
          order: 8, comparator: reverse);
      db.put(_key('a'), _val('1'));
      db.put(_key('b'), _val('2'));
      db.put(_key('c'), _val('3'));

      final sorted = <String>[];
      final iter = db.entries();
      while (iter.moveNext()) {
        sorted.add(_str(iter.current.key));
      }
      expect(sorted, equals(['c', 'b', 'a']));
      db.close();
    });

    test('stress: many inserts and deletes with small order', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 6);
      final rng = Random(123);
      final live = <String>{};

      for (var i = 0; i < 5000; i++) {
        final k = 'k${rng.nextInt(2000).toString().padLeft(4, '0')}';
        final v = 'v$i';
        if (rng.nextBool() && live.contains(k)) {
          db.remove(_key(k));
          live.remove(k);
        } else {
          db.put(_key(k), _val(v));
          live.add(k);
        }
      }

      expect(db.count(), equals(live.length));

      // Verify sorted order.
      final sorted = <String>[];
      final iter = db.entries();
      while (iter.moveNext()) {
        sorted.add(_str(iter.current.key));
      }
      expect(sorted.length, equals(live.length));
      for (var i = 1; i < sorted.length; i++) {
        expect(sorted[i].compareTo(sorted[i - 1]), greaterThan(0));
      }
      db.close();
    });

    test('wrap with VersionedHashDBM', () {
      final vdb = VersionedHashDBM(file.openSync(mode: FileMode.write),
          buckets: 1009);
      final db = BTreeDBM.wrap(vdb, order: 8);
      db.put(_key('x'), _val('1'));
      db.put(_key('y'), _val('2'));
      expect(db.count(), equals(2));
      expect(_str(db.get(_key('x'))!), equals('1'));
      db.close();
      vdb.close();
    });

    test('BTreeDBM performance',
        skip: 'Benchmark: run explicitly with --run-skipped', () {
      final s = Stopwatch();

      for (final order in [16, 64, 128]) {
        final n = 100000;
        if (file.existsSync()) file.deleteSync();
        final db = BTreeDBM(file.openSync(mode: FileMode.write),
            order: order, flush: false);

        // Sequential insert
        s.start();
        for (var i = 0; i < n; i++) {
          final padded = i.toString().padLeft(6, '0');
          db.put(_key('key$padded'), _val('val$padded'));
        }
        db.flush();
        s.stop();
        print('order=$order sequential insert $n: ${s.elapsed}');

        // Random read
        final rng = Random(99);
        final indices = List.generate(n, (final _) => rng.nextInt(n));
        s.reset();
        s.start();
        for (final i in indices) {
          final padded = i.toString().padLeft(6, '0');
          db.get(_key('key$padded'));
        }
        s.stop();
        print('order=$order random read $n: ${s.elapsed}');

        // Sorted iteration
        s.reset();
        s.start();
        var count = 0;
        final iter = db.entries();
        while (iter.moveNext()) {
          count++;
        }
        s.stop();
        expect(count, equals(n));
        print('order=$order sorted iteration $n: ${s.elapsed}');

        // Range query (10% of keys)
        s.reset();
        s.start();
        count = 0;
        final range = db.range(
            start: _key('key020000'), end: _key('key030000'));
        while (range.moveNext()) {
          count++;
        }
        s.stop();
        expect(count, equals(10000));
        print('order=$order range query 10000: ${s.elapsed}');

        // floor/ceiling (1000 lookups)
        s.reset();
        s.start();
        for (var i = 0; i < 1000; i++) {
          final k = rng.nextInt(n).toString().padLeft(6, '0');
          db.floor(_key('key$k'));
          db.ceiling(_key('key$k'));
        }
        s.stop();
        print('order=$order floor/ceiling 1000: ${s.elapsed}');

        // Delete half
        s.reset();
        s.start();
        for (var i = 0; i < n; i += 2) {
          final padded = i.toString().padLeft(6, '0');
          db.remove(_key('key$padded'));
        }
        db.flush();
        s.stop();
        print('order=$order delete ${n ~/ 2}: ${s.elapsed}');

        db.close();
        print('');
      }
    });

    test('BTreeDBM vs HashDBM comparison',
        skip: 'Benchmark: run explicitly with --run-skipped', () {
      final n = 50000;
      final s = Stopwatch();

      // HashDBM baseline
      if (file.existsSync()) file.deleteSync();
      final hash = HashDBM(file.openSync(mode: FileMode.write),
          flush: false);
      s.start();
      for (var i = 0; i < n; i++) {
        final padded = i.toString().padLeft(6, '0');
        hash.put(_key('key$padded'), _val('val$padded'));
      }
      hash.flush();
      s.stop();
      print('HashDBM insert $n: ${s.elapsed}');

      s.reset();
      s.start();
      for (var i = 0; i < n; i++) {
        final padded = i.toString().padLeft(6, '0');
        hash.get(_key('key$padded'));
      }
      s.stop();
      print('HashDBM read $n: ${s.elapsed}');
      hash.close();

      // BTreeDBM
      if (file.existsSync()) file.deleteSync();
      final btree = BTreeDBM(file.openSync(mode: FileMode.write),
          order: 128, flush: false);
      s.reset();
      s.start();
      for (var i = 0; i < n; i++) {
        final padded = i.toString().padLeft(6, '0');
        btree.put(_key('key$padded'), _val('val$padded'));
      }
      btree.flush();
      s.stop();
      print('BTreeDBM insert $n: ${s.elapsed}');

      s.reset();
      s.start();
      for (var i = 0; i < n; i++) {
        final padded = i.toString().padLeft(6, '0');
        btree.get(_key('key$padded'));
      }
      s.stop();
      print('BTreeDBM read $n: ${s.elapsed}');
      btree.close();
    });

    test('BTreeDBM random insert performance',
        skip: 'Benchmark: run explicitly with --run-skipped', () {
      final n = 100000;
      final s = Stopwatch();
      final rng = Random(77);

      for (final order in [16, 64, 128]) {
        if (file.existsSync()) file.deleteSync();
        final db = BTreeDBM(file.openSync(mode: FileMode.write),
            order: order, flush: false);

        final keys = List.generate(
            n, (final _) => rng.nextInt(999999).toString().padLeft(6, '0'));

        s.reset();
        s.start();
        for (final k in keys) {
          db.put(_key('key$k'), _val('val$k'));
        }
        db.flush();
        s.stop();
        print('order=$order random insert $n: ${s.elapsed} '
            'count=${db.count()}');

        db.close();
      }
    });

    test('compact rebuilds tree', () {
      final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 8);
      for (var i = 0; i < 100; i++) {
        db.put(
            _key('k${i.toString().padLeft(3, '0')}'), _val('v$i'));
      }
      // Delete half.
      for (var i = 0; i < 50; i++) {
        db.remove(_key('k${i.toString().padLeft(3, '0')}'));
      }
      expect(db.count(), equals(50));
      db.compact();
      expect(db.count(), equals(50));

      // Verify sorted iteration after compact.
      final sorted = <String>[];
      final iter = db.entries();
      while (iter.moveNext()) {
        sorted.add(_str(iter.current.key));
      }
      expect(sorted.length, equals(50));
      expect(sorted.first, equals('k050'));
      expect(sorted.last, equals('k099'));
      db.close();
    });
  });
}
