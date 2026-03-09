import 'dart:convert' show utf8;
import 'dart:io';

import 'package:libdbm/libdbm.dart';

void main() {
  final file = File('btree_example.db');

  // Create a B+tree with sorted keys.
  final db = BTreeDBM(file.openSync(mode: FileMode.write), order: 32);

  // Insert some entries.
  final names = ['delta', 'alpha', 'echo', 'bravo', 'charlie'];
  for (final name in names) {
    db.put(utf8.encode(name), utf8.encode('value_$name'));
  }

  // Sorted iteration.
  print('All entries (sorted):');
  final iter = db.entries();
  while (iter.moveNext()) {
    print('  ${utf8.decode(iter.current.key)} = '
        '${utf8.decode(iter.current.value)}');
  }

  // First and last.
  print('\nFirst: ${utf8.decode(db.first()!.key)}');
  print('Last:  ${utf8.decode(db.last()!.key)}');

  // Range query.
  print('\nRange [bravo, delta):');
  final range = db.range(
      start: utf8.encode('bravo'), end: utf8.encode('delta'));
  while (range.moveNext()) {
    print('  ${utf8.decode(range.current.key)}');
  }

  // Floor and ceiling.
  print('\nFloor("d"):   ${utf8.decode(db.floor(utf8.encode('d'))!.key)}');
  print('Ceiling("d"): ${utf8.decode(db.ceiling(utf8.encode('d'))!.key)}');

  db.close();

  // Typed sorted map.
  final map = SortedPersistentMap.strings(file, create: true, order: 32);
  map['zebra'] = 'z';
  map['apple'] = 'a';
  map['mango'] = 'm';

  print('\nSorted map entries:');
  for (final entry in map.entries) {
    print('  ${entry.key} = ${entry.value}');
  }
  print('First: ${map.first()!.key}');
  print('Last:  ${map.last()!.key}');

  map.close();
  file.deleteSync();
}
