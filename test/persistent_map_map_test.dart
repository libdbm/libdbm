import 'dart:io';
import 'package:collection/collection.dart';
import 'package:faker/faker.dart';
import 'package:libdbm/libdbm.dart';
import 'package:test/test.dart';

void main() {
  var file = File('dummy.bin');
  var map = {};
  final faker = Faker();
  final keys = faker.lorem
      .words(10)
      .map((e) => e + faker.randomGenerator.integer(10000).toString())
      .toList();
  final values = faker.lorem
      .sentences(10)
      .map((e) => {
            's': e,
            'i': faker.randomGenerator.integer(1000000),
            'f': faker.randomGenerator.decimal(),
            'b': faker.randomGenerator.boolean(),
            'l': faker.randomGenerator.numbers(10000, 5),
            'm': {
              'g': faker.guid.guid(),
              'n': faker.person.name(),
            }
          })
      .toList();
  bool valuesContains(Map<String, dynamic> value) {
    for (var v in values) {
      if (DeepCollectionEquality().equals(v, value)) return true;
    }
    return false;
  }

  setUpAll(() async {
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
      } catch (e) {
      } finally {}
    }
    file.createSync(recursive: true);
    map = PersistentMap.withMapValue(file,
        comparator: (a, b) => DeepCollectionEquality().equals(a, b));
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
  group('Map value map', () {
    test('Test insertion', () {
      for (var i = 0; i < keys.length; i++) {
        map[keys[i]] = values[i];
        expect(map[keys[i]], equals(values[i]));
      }
      expect(map.length, equals(keys.length));
      expect(map.isEmpty, isFalse);
      expect(map.isNotEmpty, isTrue);
    });
    test('Test key/value contains', () {
      for (var key in keys) {
        expect(map.containsKey(key), isTrue);
      }
      for (var value in values) {
        expect(map.containsValue(value), isTrue);
      }
    });
    test('Test insertion from map', () {
      var other = <String, Map<String, dynamic>>{};
      faker.lorem
          .words(10)
          .map((e) => '$e::${faker.lorem.word()}')
          .forEach((e) {
        other[e] = {
          's': faker.lorem.sentence(),
        };
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
        expect(valuesContains(value), isTrue);
      });
    });
    test('Test iteration keys and values', () {
      for (var key in map.keys) {
        expect(keys.contains(key), isTrue);
      }
      ;
      for (var value in map.values) {
        expect(valuesContains(value), isTrue);
      }
      ;
    });
    test('Test removing keys', () {
      final value = {'qux': 1234};
      map['foobar'] = value;
      expect(map['foobar'], equals(value));
      expect(map.containsKey('foobar'), isTrue);
      expect(map.containsValue(value), isTrue);

      map.remove('foobar');
      expect(map['foobar'], isNull);
      expect(map.containsKey('foobar'), isFalse);
      expect(map.containsValue(value), isFalse);
    });
    test('Test removing with filter', () {
      final newKeys =
          faker.lorem.words(10).map((e) => '$e-${faker.lorem.word()}').toList();
      for (var key in newKeys) {
        map[key] = {'k': key};
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
        map.putIfAbsent(key, () => {'k': 'should not happen'});
        expect(map[key] != 'this should not happen', isTrue);
      }
      for (var key in newKeys) {
        map.putIfAbsent(key, () => {'k': key});
      }
      for (var key in newKeys) {
        expect(map[key], equals({'k': key}));
      }
      for (var key in newKeys) {
        map.remove(key);
      }
    });
    test('Test update of values by key', () {
      var ret = map.update('foobar', (v) => {'k': 'baz'},
          ifAbsent: () => {'k': 'bar'});
      expect(ret, equals({'k': 'bar'}));
      ret = map.update('foobar', (v) => {'k': 'baz'},
          ifAbsent: () => {'k': 'bar'});
      expect(ret, equals({'k': 'baz'}));
      map.remove('foobar');
    });
    test('Test update all values', () {
      map.updateAll((key, value) => {'k': '$key- updated'});
      map.forEach((key, value) {
        expect(value, equals({'k': '$key- updated'}));
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
