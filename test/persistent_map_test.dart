import 'dart:io';
import 'package:test/test.dart';

import 'package:libdbm/libdbm.dart';

void main() {
  var file = File('dummy.bin');
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
  test('Test create flag', () {
    expect(() => PersistentMap.withStringValue(file), throwsStateError);

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

    expect(() => map[null], throwsA(isA<AssertionError>()));
    expect(() => map.remove(null), throwsA(isA<AssertionError>()));
    expect(() => map.containsKey(null), throwsA(isA<AssertionError>()));
    expect(() => map.containsValue(null), throwsA(isA<AssertionError>()));
    expect(() => map.update('foo', (v) => 'bar', ifAbsent: null),
        throwsA(isA<AssertionError>()));
    map.close();
  });
}
