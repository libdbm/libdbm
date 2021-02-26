import 'dart:io';
import 'package:libdbm/libdbm.dart';

void main() {
  final key = 'key';
  final value = 'A value';

  // open the database/map
  final file = File('dummy.db');
  var db = PersistentMap.withStringValue(file, create: true);

  // standard map operation
  db[key] = value;
  var result = db[key];
  print('$result');
  for (var i in db.entries) {
    print('${i.key}');
    print('${i.value}');
  }
  db.remove(key);
  db[key]; // will return null
  db.close();

  // open the saved database
  db = PersistentMap.withStringValue(file);
  result = db[key];
  print('$result');
  db.close();

  file.delete();
}
