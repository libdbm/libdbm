import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libdbm/dbm.dart';
import 'package:libdbm/src/hash_dbm.dart';
import 'package:libdbm/src/versioned_dbm.dart';
import 'package:test/test.dart';

Uint8List key(final String value) => Uint8List.fromList(utf8.encode(value));
Uint8List val(final String value) => Uint8List.fromList(utf8.encode(value));
String str(final Uint8List? bytes) => bytes == null ? '' : utf8.decode(bytes);

void main() {
  final path = 'dummy.versioned_dbm.bin';

  late VersionedHashDBM db;

  void setup() {
    final f = File(path);
    if (f.existsSync()) f.deleteSync();
    final file = File(path).openSync(mode: FileMode.write);
    db = VersionedHashDBM(file, buckets: 1009);
  }

  void teardown() {
    db.close();
    final f = File(path);
    if (f.existsSync()) f.deleteSync();
  }

  group('VersionedHashDBM', () {
    setUp(setup);
    tearDown(teardown);

    test('initial state has version 0', () {
      expect(db.current, 0);
      expect(db.versions, [0]);
    });

    test('commit creates a new version', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('hello'));
      transaction.commit();

      expect(db.current, 1);
      expect(db.versions, [0, 1]);
    });

    test('committed data readable at version', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('hello'));
      transaction.commit();

      final snapshot = db.at(1);
      expect(str(snapshot.get(key('a'))), 'hello');
    });

    test('put creates a version and is readable', () {
      db.put(key('x'), val('base'));
      expect(db.current, 1);
      expect(str(db.get(key('x'))), 'base');
    });

    test('transaction sees own writes', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('one'));
      expect(str(transaction.get(key('a'))), 'one');
    });

    test('transaction sees previously committed data', () {
      db.put(key('x'), val('base'));
      final transaction = db.begin();
      expect(str(transaction.get(key('x'))), 'base');
    });

    test('rollback discards changes', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('temp'));
      transaction.rollback();

      expect(db.current, 0);
      expect(db.versions, [0]);
    });

    test('multiple transactions build version history', () {
      var transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('a'), val('v2'));
      transaction.put(key('b'), val('new'));
      transaction.commit();

      transaction = db.begin();
      transaction.remove(key('a'));
      transaction.commit();

      expect(db.current, 3);
      expect(db.versions, [0, 1, 2, 3]);

      // Point-in-time queries
      expect(str(db.at(1).get(key('a'))), 'v1');
      expect(db.at(1).get(key('b')), isNull);

      expect(str(db.at(2).get(key('a'))), 'v2');
      expect(str(db.at(2).get(key('b'))), 'new');

      expect(db.at(3).get(key('a')), isNull);
      expect(str(db.at(3).get(key('b'))), 'new');
    });

    test('tombstone hides entry at current version', () {
      db.put(key('x'), val('base')); // v1

      final transaction = db.begin();
      transaction.remove(key('x'));
      transaction.commit(); // v2

      // At v1 the key exists
      expect(str(db.at(1).get(key('x'))), 'base');

      // At v2 it is tombstoned
      expect(db.at(2).get(key('x')), isNull);

      // Current version sees tombstone
      expect(db.get(key('x')), isNull);
    });

    test('snapshot is read-only', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      final snapshot = db.at(1);
      expect(() => snapshot.put(key('b'), val('fail')),
          throwsA(isA<DBMException>()));
      expect(() => snapshot.remove(key('a')), throwsA(isA<DBMException>()));
      expect(snapshot.clear, throwsA(isA<DBMException>()));
    });

    test('completed transaction throws on reuse', () {
      final transaction = db.begin();
      transaction.commit();

      expect(() => transaction.put(key('a'), val('fail')),
          throwsA(isA<DBMException>()));
      expect(transaction.commit, throwsA(isA<DBMException>()));
    });

    test('invalid version throws', () {
      expect(() => db.at(99), throwsA(isA<DBMException>()));
    });

    test('merge folds deltas into base', () {
      var transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('b'), val('v2'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('a'), val('v3'));
      transaction.commit();

      // Merge through version 2
      db.merge(through: 2);

      // Current resolves through remaining delta 3
      expect(str(db.get(key('a'))), 'v3');
      expect(str(db.get(key('b'))), 'v2');

      // Version 3 still queryable (delta 3 has a=v3)
      expect(str(db.at(3).get(key('a'))), 'v3');

      // Base is now 2, so version 1 is gone
      expect(db.versions.contains(1), isFalse);
      expect(db.versions, [2, 3]);
    });

    test('merge with tombstone removes from base', () {
      db.put(key('x'), val('base')); // v1

      final transaction = db.begin();
      transaction.remove(key('x'));
      transaction.commit(); // v2

      db.merge(through: 2);

      expect(db.get(key('x')), isNull);
      expect(db.count(), 0);
    });

    test('entries at version includes delta inserts', () {
      db.put(key('a'), val('base')); // v1

      final transaction = db.begin();
      transaction.put(key('b'), val('delta'));
      transaction.commit(); // v2

      final snapshot = db.at(2);
      final map = <String, String>{};
      final iter = snapshot.entries();
      while (iter.moveNext()) {
        map[str(iter.current.key)] = str(iter.current.value);
      }

      expect(map['a'], 'base');
      expect(map['b'], 'delta');
      expect(map.length, 2);
    });

    test('entries at version respects tombstones', () {
      db.put(key('a'), val('base')); // v1
      db.put(key('b'), val('keep')); // v2

      final transaction = db.begin();
      transaction.remove(key('a'));
      transaction.commit(); // v3

      final snapshot = db.at(3);
      final map = <String, String>{};
      final iter = snapshot.entries();
      while (iter.moveNext()) {
        map[str(iter.current.key)] = str(iter.current.value);
      }

      expect(map.containsKey('a'), isFalse);
      expect(map['b'], 'keep');
      expect(map.length, 1);
    });

    test('entries at version respects value overrides', () {
      db.put(key('a'), val('old')); // v1

      final transaction = db.begin();
      transaction.put(key('a'), val('new'));
      transaction.commit(); // v2

      final snapshot = db.at(2);
      final map = <String, String>{};
      final iter = snapshot.entries();
      while (iter.moveNext()) {
        map[str(iter.current.key)] = str(iter.current.value);
      }

      expect(map['a'], 'new');
      expect(map.length, 1);
    });

    test('snapshot count reflects overlay', () {
      db.put(key('a'), val('base')); // v1
      db.put(key('b'), val('base')); // v2

      var transaction = db.begin();
      transaction.remove(key('a'));
      transaction.put(key('c'), val('new'));
      transaction.commit(); // v3

      final snapshot = db.at(3);
      expect(snapshot.count(), 2); // b + c (a deleted)
    });

    test('crash recovery: uncommitted has no effect', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('uncommitted'));
      // Don't commit — just let it go

      expect(db.current, 0);
      expect(db.versions, [0]);
    });

    test('reopen preserves version history', () {
      var transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('b'), val('v2'));
      transaction.commit();

      db.close();

      // Reopen
      final file = File(path).openSync(mode: FileMode.append);
      db = VersionedHashDBM(file, buckets: 1009);

      expect(db.current, 2);
      expect(db.versions, [0, 1, 2]);
      expect(str(db.at(1).get(key('a'))), 'v1');
      expect(str(db.at(2).get(key('b'))), 'v2');
    });

    test('empty commit is a no-op', () {
      final transaction = db.begin();
      transaction.commit();

      expect(db.current, 0);
    });

    test('many small transactions', () {
      for (var i = 0; i < 50; i++) {
        final transaction = db.begin();
        transaction.put(key('k$i'), val('v$i'));
        transaction.commit();
      }

      expect(db.current, 50);
      expect(db.versions.length, 51); // 0..50

      for (var i = 0; i < 50; i++) {
        final snapshot = db.at(i + 1);
        expect(str(snapshot.get(key('k$i'))), 'v$i');
      }
    });

    test('transaction reads snapshot not latest', () {
      var transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      // Start a transaction at version 1
      final reading = db.begin();

      // Commit another version
      transaction = db.begin();
      transaction.put(key('a'), val('v2'));
      transaction.commit();

      // The reading transaction should see v1, not v2
      expect(str(reading.get(key('a'))), 'v1');
      reading.rollback();
    });

    // ---- Regression tests ----

    test('merge beyond current throws', () {
      final transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      expect(() => db.merge(through: 99), throwsA(isA<DBMException>()));
    });

    test('merge persists across reopen', () {
      var transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('b'), val('v2'));
      transaction.commit();

      db.merge(through: 1);
      db.close();

      final file = File(path).openSync(mode: FileMode.append);
      db = VersionedHashDBM(file, buckets: 1009);

      // Base is now 1, only delta 2 remains
      expect(db.versions, [1, 2]);
      expect(db.current, 2);
      expect(str(db.get(key('a'))), 'v1');
      expect(str(db.get(key('b'))), 'v2');
    });

    test('empty value is not confused with tombstone', () {
      final transaction = db.begin();
      transaction.put(key('empty'), Uint8List(0));
      transaction.commit();

      expect(db.get(key('empty')), isNotNull);
      expect(db.get(key('empty'))!.isEmpty, isTrue);
      expect(isTombstone(db.get(key('empty'))), isFalse);
    });

    test('buffer aliasing does not corrupt staged data', () {
      final k = Uint8List.fromList([1, 2, 3]);
      final v = Uint8List.fromList([4, 5, 6]);

      final transaction = db.begin();
      transaction.put(k, v);

      // Mutate original buffers
      k[0] = 99;
      v[0] = 99;

      transaction.commit();

      // Data should reflect original values, not mutated
      final result = db.get(Uint8List.fromList([1, 2, 3]));
      expect(result, Uint8List.fromList([4, 5, 6]));
    });

    test('clear resets version state', () {
      db.put(key('a'), val('v1'));
      db.put(key('b'), val('v2'));

      db.clear();

      expect(db.current, 0);
      expect(db.versions, [0]);
      expect(db.get(key('a')), isNull);
      expect(db.count(), 0);
    });

    test('putIfAbsent does not overwrite existing', () {
      db.put(key('a'), val('first'));
      final result = db.putIfAbsent(key('a'), val('second'));
      expect(str(result), 'first');
      expect(str(db.get(key('a'))), 'first');
    });

    test('remove returns old value', () {
      db.put(key('a'), val('hello'));
      final old = db.remove(key('a'));
      expect(str(old), 'hello');
      expect(db.get(key('a')), isNull);
    });

    test('remove of missing key returns null', () {
      final old = db.remove(key('missing'));
      expect(old, isNull);
    });

    test('merge with no args compacts all deltas', () {
      var transaction = db.begin();
      transaction.put(key('a'), val('v1'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('b'), val('v2'));
      transaction.commit();

      transaction = db.begin();
      transaction.put(key('a'), val('v3'));
      transaction.commit();

      db.merge();

      expect(str(db.get(key('a'))), 'v3');
      expect(str(db.get(key('b'))), 'v2');
      expect(db.versions, [3]);
    });

    test('flatten converts to plain format', () {
      db.put(key('a'), val('v1'));
      db.put(key('b'), val('v2'));
      db.put(key('a'), val('v3'));
      db.close();

      // Reopen as plain HashDBM — should succeed after flatten
      var file = File(path).openSync(mode: FileMode.append);
      final versioned = VersionedHashDBM(file, buckets: 1009);
      versioned.flatten();
      versioned.close();

      file = File(path).openSync(mode: FileMode.append);
      final plain = HashDBM(file, buckets: 1009);
      expect(str(plain.get(key('a'))), 'v3');
      expect(str(plain.get(key('b'))), 'v2');
      plain.close();
    });

    test('flatten on fresh db is no-op', () {
      db.flatten();
      db.close();

      final file = File(path).openSync(mode: FileMode.append);
      final plain = HashDBM(file, buckets: 1009);
      expect(plain.count(), 0);
      plain.close();

      // Reassign db for teardown
      final file2 = File(path).openSync(mode: FileMode.append);
      db = VersionedHashDBM(file2, buckets: 1009);
    });

    test('flatten then reopen with VersionedHashDBM re-upgrades', () {
      db.put(key('a'), val('hello'));
      db.put(key('b'), val('world'));
      db.flatten();
      db.close();

      final file = File(path).openSync(mode: FileMode.append);
      db = VersionedHashDBM(file, buckets: 1009);
      expect(str(db.get(key('a'))), 'hello');
      expect(str(db.get(key('b'))), 'world');
      expect(db.current, 0);
      expect(db.versions, [0]);

      // Can create new versions after re-upgrade
      db.put(key('c'), val('new'));
      expect(db.current, 1);
    });
  });

  group('Format version migration', () {
    final migrationPath = 'dummy.migration.bin';

    tearDown(() {
      final f = File(migrationPath);
      if (f.existsSync()) f.deleteSync();
    });

    test('plain file opened with VersionedHashDBM upgrades cleanly', () {
      // Create a plain HashDBM file
      final f = File(migrationPath);
      if (f.existsSync()) f.deleteSync();
      var file = f.openSync(mode: FileMode.write);
      var plain = HashDBM(file, buckets: 1009);
      plain.put(key('a'), val('hello'));
      plain.put(key('b'), val('world'));
      plain.close();

      // Reopen with VersionedHashDBM — should upgrade
      file = File(migrationPath).openSync(mode: FileMode.append);
      final versioned = VersionedHashDBM(file, buckets: 1009);
      expect(str(versioned.get(key('a'))), 'hello');
      expect(str(versioned.get(key('b'))), 'world');
      expect(versioned.current, 0);
      versioned.close();
    });

    test('reopening upgraded file with VersionedHashDBM works', () {
      // Create plain, upgrade, close
      final f = File(migrationPath);
      if (f.existsSync()) f.deleteSync();
      var file = f.openSync(mode: FileMode.write);
      var plain = HashDBM(file, buckets: 1009);
      plain.put(key('x'), val('data'));
      plain.close();

      file = File(migrationPath).openSync(mode: FileMode.append);
      var versioned = VersionedHashDBM(file, buckets: 1009);
      versioned.put(key('y'), val('more'));
      versioned.close();

      // Reopen again — already versioned format
      file = File(migrationPath).openSync(mode: FileMode.append);
      versioned = VersionedHashDBM(file, buckets: 1009);
      expect(str(versioned.get(key('x'))), 'data');
      expect(str(versioned.get(key('y'))), 'more');
      versioned.close();
    });

    test('opening versioned file with plain HashDBM throws', () {
      // Create a versioned file
      final f = File(migrationPath);
      if (f.existsSync()) f.deleteSync();
      var file = f.openSync(mode: FileMode.write);
      final versioned = VersionedHashDBM(file, buckets: 1009);
      versioned.put(key('a'), val('v1'));
      versioned.close();

      // Try to open with plain HashDBM
      file = File(migrationPath).openSync(mode: FileMode.append);
      expect(
        () => HashDBM(file, buckets: 1009),
        throwsA(isA<DBMException>().having(
            (final e) => e.code, 'code', 403)),
      );
      file.closeSync();
    });

    test('plain files still open normally with HashDBM', () {
      final f = File(migrationPath);
      if (f.existsSync()) f.deleteSync();
      var file = f.openSync(mode: FileMode.write);
      var plain = HashDBM(file, buckets: 1009);
      plain.put(key('a'), val('hello'));
      plain.close();

      // Reopen with plain HashDBM — should work fine
      file = File(migrationPath).openSync(mode: FileMode.append);
      plain = HashDBM(file, buckets: 1009);
      expect(str(plain.get(key('a'))), 'hello');
      plain.close();
    });
  });
}
