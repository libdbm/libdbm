import 'dart:io';
import 'package:test/test.dart';
import 'package:faker/faker.dart';
import 'package:collection/collection.dart';

import 'package:libdbm/libdbm.dart';

void main() {
  var file = File('dummy.bin');
  setUp(() async {
    if (file.existsSync()) file.deleteSync(recursive: true);
  });
  tearDown(() async {
    if (file.existsSync()) file.deleteSync(recursive: true);
  });
  test('Test create flag', () {
    expect(() => PersistentMap.withStringValue(file), throwsException);

    var map = PersistentMap.withStringValue(file, create: true);
    expect(map.length, equals(0));
    map.close();
  });
  test('Test open/close preserves state', () {
    var map = PersistentMap.withStringValue(file, create: true);
    expect(map.length, equals(0));
    map['foo'] = 'bar';
    expect(map.length, equals(1));
    map.close();

    map = PersistentMap.withStringValue(file);
    expect(map['foo'], equals('bar'));
    map.close();
  });
  test('Test asserts', () {
    var map = PersistentMap.withStringValue(file, create: true);

    expect(
        () => map.putIfAbsent(null, () => ''), throwsA(isA<AssertionError>()));
    expect(() => map.putIfAbsent('foo', () => null),
        throwsA(isA<AssertionError>()));
    expect(() => map[null], throwsA(isA<AssertionError>()));
    expect(() => map['foo'] = null, throwsA(isA<AssertionError>()));
    expect(() => map.remove(null), throwsA(isA<AssertionError>()));
    expect(() => map.removeWhere(null), throwsA(isA<AssertionError>()));
    expect(() => map.addAll(null), throwsA(isA<AssertionError>()));
    expect(() => map.addEntries(null), throwsA(isA<AssertionError>()));
    expect(() => map.containsKey(null), throwsA(isA<AssertionError>()));
    expect(() => map.containsValue(null), throwsA(isA<AssertionError>()));
    expect(() => map.forEach(null), throwsA(isA<AssertionError>()));
    expect(() => map.update(null, null, ifAbsent: null),
        throwsA(isA<AssertionError>()));
    expect(() => map.update('foo', null, ifAbsent: null),
        throwsA(isA<AssertionError>()));
    expect(() => map.update('foo', (v) => 'bar', ifAbsent: null),
        throwsA(isA<AssertionError>()));
    expect(() => map.updateAll(null), throwsA(isA<AssertionError>()));
    expect(() => map.map(null), throwsA(isA<AssertionError>()));
    map.close();
  });
}
