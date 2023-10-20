import 'dart:io';
import 'package:faker/faker.dart';
import 'package:libdbm/libdbm.dart';
import 'package:test/test.dart';

void main() {
  var file = File('dummy.bin');
  var map = <String, String>{};
  final faker = Faker();
  final keys = faker.lorem
      .words(10)
      .map((e) => e + faker.randomGenerator.integer(10000).toString())
      .toList();
  final values = faker.lorem.sentences(10);
  setUpAll(() async {
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
      } catch (e) {
      } finally {}
    }
    file.createSync(recursive: true);
    map = PersistentMap.withStringValue(file);
  });
  tearDownAll(() async {
    (map as PersistentMap).close();
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
      } catch (e) {
      } finally {}
    }
  });
  group('String map', () {
    test('Test insertion', () {
      for (var i = 0; i < keys.length; i++) {
        map[keys[i]] = values[i];
        expect(map[keys[i]], equals(values[i]));
        expect(map.containsKey(keys[i]), isTrue);
        expect(map.containsValue(values[i]), isTrue);
      }
      expect(map.length, equals(keys.length));
      expect(map.isEmpty, isFalse);
      expect(map.isNotEmpty, isTrue);
    });
    test('Test map function', () {
      final local = map.map((key, value) => MapEntry(key, value));
      local.forEach((key, value) {
        expect(keys.contains(key), isTrue);
        expect(values.contains(value), isTrue);
      });
    });
    test('Test insertion from map', () {
      var other = <String, String>{};
      faker.lorem
          .words(10)
          .map((e) => '$e::${faker.lorem.word()}')
          .forEach((e) {
        other[e] = faker.lorem.sentence();
      });
      map.addAll(other);
      for (var e in other.entries) {
        expect(map[e.key], equals(e.value));
      }
      for (var k in other.keys) {
        map.remove(k);
      }
    });
    test('Test iteration over values', () {
      map.forEach((key, value) {
        expect(keys.contains(key), isTrue);
        expect(values.contains(value), isTrue);
      });
    });
    test('Test iteration keys and values', () {
      for (var key in map.keys) {
        expect(keys.contains(key), isTrue);
      }
      ;
      for (var value in map.values) {
        expect(values.contains(value), isTrue);
      }
      ;
    });
    test('Test removing keys', () {
      map['foobar'] = 'qux';
      expect(map['foobar'], equals('qux'));
      expect(map.containsKey('foobar'), isTrue);
      expect(map.containsValue('qux'), isTrue);

      map.remove('foobar');
      expect(map['foobar'], isNull);
      expect(map.containsKey('foobar'), isFalse);
      expect(map.containsValue('qux'), isFalse);
    });
    test('Test removing with filter', () {
      final newKeys =
          faker.lorem.words(10).map((e) => '$e-${faker.lorem.word()}').toList();
      for (var key in newKeys) {
        map[key] = key;
      }
      for (var key in newKeys) {
        expect(map.containsKey(key), isTrue);
      }
      for (var key in keys) {
        expect(map.containsKey(key), isTrue);
      }
      map.removeWhere((key, value) => newKeys.contains(key));
      for (var key in newKeys) {
        expect(map.containsKey(key), isFalse);
      }
      for (var key in keys) {
        expect(map.containsKey(key), isTrue);
      }
    });
    test('Test putIfAbsent', () {
      final newKeys =
          faker.lorem.words(10).map((e) => '$e-${faker.lorem.word()}').toList();
      for (var key in keys) {
        map.putIfAbsent(key, () => 'should not happen');
        expect(map[key] != 'this should not happen', isTrue);
      }
      for (var key in newKeys) {
        map.putIfAbsent(key, () => key);
      }
      for (var key in newKeys) {
        expect(map[key], equals(key));
      }
      for (var key in newKeys) {
        map.remove(key);
      }
    });
    test('Test update of values by key', () {
      var ret = map.update('foobar', (v) => 'baz', ifAbsent: () => 'bar');
      expect(ret, equals('bar'));
      ret = map.update('foobar', (v) => 'baz', ifAbsent: () => 'bar');
      expect(ret, equals('baz'));
      map.remove('foobar');
    });
    test('Test update all values', () {
      map.updateAll((key, value) => '$key- updated');
      map.forEach((key, value) {
        expect(value, equals('$key- updated'));
      });
    });
    test('Test clearing map', () {
      map.clear();
      expect(map.isEmpty, isTrue);
      expect(map.isNotEmpty, isFalse);
      for (var i = 0; i < keys.length; i++) {
        expect(map[keys[i]], isNull);
        expect(map.containsKey(keys[i]), isFalse);
        expect(map.containsValue(values[i]), isFalse);
      }
    });
  });
}
