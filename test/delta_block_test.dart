import 'dart:io';
import 'dart:typed_data';

import 'package:libdbm/dbm.dart';
import 'package:libdbm/src/delta_block.dart';
import 'package:libdbm/src/io.dart';
import 'package:libdbm/src/memory_pool.dart';
import 'package:test/test.dart';

void main() {
  final path = 'dummy.delta_test.bin';

  late RandomAccessFile file;

  void setup() {
    final f = File(path);
    if (f.existsSync()) f.deleteSync();
    file = File(path).openSync(mode: FileMode.write);
  }

  void teardown() {
    file.closeSync();
    final f = File(path);
    if (f.existsSync()) f.deleteSync();
  }

  group('DeltaBlock', () {
    setUp(setup);
    tearDown(teardown);

    test('encode and decode round-trip', () {
      final entries = <BytesKey, Uint8List?>{
        BytesKey(Uint8List.fromList([1, 2, 3])):
            Uint8List.fromList([10, 20, 30]),
        BytesKey(Uint8List.fromList([4, 5])):
            Uint8List.fromList([40, 50, 60, 70]),
      };

      final needed = DeltaBlock.required(entries);
      final pointer = Pointer(0, align(needed, MemoryPool.ALIGNMENT));
      final delta = DeltaBlock.encode(1, 1000, entries, pointer);
      delta.block.write(file);

      final loaded = DeltaBlock.read(file, pointer);
      expect(loaded.version, 1);
      expect(loaded.timestamp, 1000);
      expect(loaded.count, 2);

      final decoded = loaded.decode();
      expect(decoded.length, 2);
      expect(decoded[BytesKey(Uint8List.fromList([1, 2, 3]))],
          Uint8List.fromList([10, 20, 30]));
      expect(decoded[BytesKey(Uint8List.fromList([4, 5]))],
          Uint8List.fromList([40, 50, 60, 70]));
    });

    test('tombstone entries round-trip', () {
      final entries = <BytesKey, Uint8List?>{
        BytesKey(Uint8List.fromList([1, 2])): null, // tombstone
        BytesKey(Uint8List.fromList([3, 4])): Uint8List.fromList([30]),
      };

      final needed = DeltaBlock.required(entries);
      final pointer = Pointer(0, align(needed, MemoryPool.ALIGNMENT));
      final delta = DeltaBlock.encode(2, 2000, entries, pointer);
      delta.block.write(file);

      final loaded = DeltaBlock.read(file, pointer);
      expect(loaded.count, 2);

      final decoded = loaded.decode();
      expect(decoded[BytesKey(Uint8List.fromList([1, 2]))], isNull);
      expect(decoded[BytesKey(Uint8List.fromList([3, 4]))],
          Uint8List.fromList([30]));
    });

    test('lookup finds existing key', () {
      final entries = <BytesKey, Uint8List?>{
        BytesKey(Uint8List.fromList([1])): Uint8List.fromList([10]),
        BytesKey(Uint8List.fromList([2])): Uint8List.fromList([20]),
      };

      final needed = DeltaBlock.required(entries);
      final pointer = Pointer(0, align(needed, MemoryPool.ALIGNMENT));
      final delta = DeltaBlock.encode(1, 1000, entries, pointer);
      delta.block.write(file);

      final loaded = DeltaBlock.read(file, pointer);
      expect(loaded.lookup(Uint8List.fromList([1])), Uint8List.fromList([10]));
      expect(loaded.lookup(Uint8List.fromList([2])), Uint8List.fromList([20]));
    });

    test('lookup returns tombstone for deleted key', () {
      final entries = <BytesKey, Uint8List?>{
        BytesKey(Uint8List.fromList([1])): null,
      };

      final needed = DeltaBlock.required(entries);
      final pointer = Pointer(0, align(needed, MemoryPool.ALIGNMENT));
      final delta = DeltaBlock.encode(1, 1000, entries, pointer);
      delta.block.write(file);

      final loaded = DeltaBlock.read(file, pointer);
      final result = loaded.lookup(Uint8List.fromList([1]));
      expect(result, isNotNull);
      expect(isTombstone(result), isTrue);
    });

    test('lookup returns null for missing key', () {
      final entries = <BytesKey, Uint8List?>{
        BytesKey(Uint8List.fromList([1])): Uint8List.fromList([10]),
      };

      final needed = DeltaBlock.required(entries);
      final pointer = Pointer(0, align(needed, MemoryPool.ALIGNMENT));
      final delta = DeltaBlock.encode(1, 1000, entries, pointer);
      delta.block.write(file);

      final loaded = DeltaBlock.read(file, pointer);
      expect(loaded.lookup(Uint8List.fromList([99])), isNull);
    });

    test('empty delta round-trip', () {
      final entries = <BytesKey, Uint8List?>{};

      final needed = DeltaBlock.required(entries);
      final pointer = Pointer(0, align(needed, MemoryPool.ALIGNMENT));
      final delta = DeltaBlock.encode(5, 5000, entries, pointer);
      delta.block.write(file);

      final loaded = DeltaBlock.read(file, pointer);
      expect(loaded.version, 5);
      expect(loaded.count, 0);
      expect(loaded.decode(), isEmpty);
    });

    test('invalid magic throws', () {
      // Write garbage
      file.setPositionSync(0);
      file.writeFromSync(Uint8List(256));

      expect(() => DeltaBlock.read(file, Pointer(0, 256)),
          throwsA(isA<DBMException>()));
    });
  });
}

int align(final int size, final int alignment) {
  final padding = (alignment - (size % alignment)) % alignment;
  return size + padding;
}
