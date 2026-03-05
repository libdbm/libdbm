import 'dart:convert' show utf8;
import 'dart:io';
import 'dart:typed_data';

import 'package:libdbm/libdbm.dart';

String? decode(final Uint8List? bytes) =>
    bytes == null ? null : utf8.decode(bytes);

Uint8List encode(final String value) => utf8.encoder.convert(value);

void main() {
  final file = File('dummy_versioned.db');
  final db = VersionedHashDBM(file.openSync(mode: FileMode.write));

  // --- Version 1: seed some users ---
  var tx = db.begin();
  tx.put(encode('user:alice'), encode('admin'));
  tx.put(encode('user:bob'), encode('editor'));
  tx.put(encode('user:carol'), encode('viewer'));
  tx.commit();
  print('After v1: ${db.current} versions=${db.versions}');

  // --- Version 2: promote bob, remove carol ---
  tx = db.begin();
  tx.put(encode('user:bob'), encode('admin'));
  tx.remove(encode('user:carol'));
  tx.commit();
  print('After v2: ${db.current} versions=${db.versions}');

  // --- Version 3: add dave, demote alice ---
  tx = db.begin();
  tx.put(encode('user:dave'), encode('editor'));
  tx.put(encode('user:alice'), encode('viewer'));
  tx.commit();
  print('After v3: ${db.current} versions=${db.versions}');

  // --- Point-in-time queries ---
  print('\n--- Snapshots ---');
  for (final v in db.versions) {
    final snap = db.at(v);
    final alice = decode(snap.get(encode('user:alice')));
    final bob = decode(snap.get(encode('user:bob')));
    final carol = decode(snap.get(encode('user:carol')));
    final dave = decode(snap.get(encode('user:dave')));
    print('v$v: alice=$alice bob=$bob carol=$carol dave=$dave');
  }

  // --- Iterate all entries at version 2 ---
  print('\n--- All entries at v2 ---');
  final snap = db.at(2);
  final iter = snap.entries();
  while (iter.moveNext()) {
    print('  ${decode(iter.current.key)} = ${decode(iter.current.value)}');
  }

  // --- Transaction isolation: reads see snapshot, not later commits ---
  print('\n--- Isolation ---');
  final reading = db.begin();
  tx = db.begin();
  tx.put(encode('user:alice'), encode('superadmin'));
  tx.commit();
  final concurrent = decode(reading.get(encode('user:alice')));
  print('Concurrent read sees alice=$concurrent');
  final latest = decode(db.at(db.current).get(encode('user:alice')));
  print('Latest version sees alice=$latest');
  reading.rollback();

  // --- Merge old versions into base ---
  print('\n--- Merge through v2 ---');
  db.merge(through: 2);
  print('Versions after merge: ${db.versions}');
  print('Base table alice=${decode(db.get(encode("user:alice")))}');
  print('v3 alice=${decode(db.at(3).get(encode("user:alice")))}');
  print('v4 alice=${decode(db.at(4).get(encode("user:alice")))}');

  // --- Flatten: merge all deltas and convert to plain format ---
  print('\n--- Flatten ---');
  db.flatten();
  print('Versions after flatten: ${db.versions}');
  print('alice=${decode(db.get(encode("user:alice")))}');
  print('bob=${decode(db.get(encode("user:bob")))}');
  db.close();

  // Reopen as plain HashDBM — works because flatten reset the format
  print('\n--- Reopen as plain HashDBM ---');
  final plain = HashDBM(file.openSync(mode: FileMode.append));
  print('alice=${decode(plain.get(encode("user:alice")))}');
  print('bob=${decode(plain.get(encode("user:bob")))}');
  print('dave=${decode(plain.get(encode("user:dave")))}');
  print('carol=${decode(plain.get(encode("user:carol")))}');
  plain.close();

  file.deleteSync();
  print('\nDone.');
}
