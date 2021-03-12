import 'dart:io';
import 'dart:convert' show utf8;
import 'package:libdbm/libdbm.dart';

void main() {
  final key = utf8.encoder.convert('A key');
  final value = utf8.encoder.convert('A value');

  final file = File('dummy.db');
  final db = HashDBM(file.openSync(mode: FileMode.write));
  db.put(key, value);
  var result = db.get(key);
  print('${utf8.decode(result!.toList())}');
  for (var i = db.entries(); i.moveNext();) {
    print('${utf8.decode(i.current.key)}');
    print('${utf8.decode(i.current.value)}');
  }
  db.remove(key);
  db.get(key); // will return null
  db.close();
  file.delete();
}
