import 'dart:io';
import 'package:test/test.dart';

import 'package:libdbm/src/memory_pool.dart';
import 'package:libdbm/src/io.dart';
import 'package:libdbm/src/util.dart';

void main() {
  File file = File('dummy.bin');
  setUp(() async {
    if (file.existsSync())
      try {
        file.deleteSync(recursive: true);
      } finally {}
  });
  tearDown(() async {
    if (file.existsSync())
      try {
        file.deleteSync(recursive: true);
      } finally {}
  });
  test('Test alignment', () {
    expect(align(123, 128), equals(128));
    expect(align(150, 128), equals(256));
    expect(align(253, 128), equals(256));
  });
  test('Verify pointer merging', () {
    final r = file.openSync(mode: FileMode.write);
    final pool = MemoryPool(r, 0);
    pool.free(Pointer(0, 100));
    pool.free(Pointer(200, 100));
    pool.free(Pointer(350, 100));

    expect(pool.length, equals(3));

    pool.free(Pointer(100, 100));

    expect(pool.length, equals(2));
    expect(pool[0].offset, equals(350));
    expect(pool[0].length, equals(100));
    expect(pool[1].offset, equals(0));
    expect(pool[1].length, equals(300));

    r.closeSync();
  });
}
