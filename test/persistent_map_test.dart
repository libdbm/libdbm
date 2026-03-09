import 'dart:io';
import 'package:libdbm/libdbm.dart';
import 'package:test/test.dart';

void main() {
  var file = File('dummy.persistent_map.bin');
  setUp(() async {
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
        // ignore: empty_catches
      } catch (e) {
      } finally {}
    }
  });
  tearDown(() async {
    if (file.existsSync()) {
      try {
        file.deleteSync(recursive: true);
        // ignore: avoid_catches_without_on_clauses
        // ignore: empty_catches
      } catch (e) {
      } finally {}
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
  test('wrong key type returns null or false', () {
    var map = PersistentMap.withStringValue(file, create: true);

    // ignore: collection_methods_unrelated_type
    expect(map[null], isNull);
    // ignore: collection_methods_unrelated_type
    expect(map[123], isNull);
    // ignore: collection_methods_unrelated_type
    expect(map.remove(null), isNull);
    // ignore: collection_methods_unrelated_type
    expect(map.remove(123), isNull);
    // ignore: collection_methods_unrelated_type
    expect(map.containsKey(null), isFalse);
    // ignore: collection_methods_unrelated_type
    expect(map.containsKey(123), isFalse);
    expect(() => map.containsValue(null), throwsA(isA<AssertionError>()));
    expect(() => map.update('foo', (v) => 'bar', ifAbsent: null),
        throwsA(isA<AssertionError>()));
    map.close();
  });
  test('putIfAbsent callback is lazy', () {
    final map = PersistentMap.withStringValue(file, create: true);
    map['foo'] = 'bar';

    var calls = 0;
    final value = map.putIfAbsent('foo', () {
      calls += 1;
      return 'baz';
    });

    expect(value, equals('bar'));
    expect(calls, equals(0));
    expect(map['foo'], equals('bar'));
    map.close();
  });
}
