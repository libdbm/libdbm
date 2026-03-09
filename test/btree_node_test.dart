import 'dart:convert' show utf8;
import 'dart:typed_data';

import 'package:libdbm/src/btree_node.dart';
import 'package:test/test.dart';

void main() {
  group('LeafNode', () {
    test('encode/decode empty leaf', () {
      final node = LeafNode(1, [], []);
      final encoded = node.encode();
      final decoded = LeafNode.decode(1, encoded);
      expect(decoded.id, equals(1));
      expect(decoded.keys, isEmpty);
      expect(decoded.values, isEmpty);
      expect(decoded.next, equals(0));
      expect(decoded.previous, equals(0));
    });

    test('encode/decode single entry', () {
      final key = utf8.encode('hello');
      final value = utf8.encode('world');
      final node = LeafNode(
          42, [Uint8List.fromList(key)], [Uint8List.fromList(value)],
          next: 10, previous: 5);
      final encoded = node.encode();
      final decoded = LeafNode.decode(42, encoded);
      expect(decoded.id, equals(42));
      expect(decoded.keys.length, equals(1));
      expect(utf8.decode(decoded.keys[0]), equals('hello'));
      expect(utf8.decode(decoded.values[0]), equals('world'));
      expect(decoded.next, equals(10));
      expect(decoded.previous, equals(5));
    });

    test('encode/decode many entries', () {
      final keys = <Uint8List>[];
      final values = <Uint8List>[];
      for (var i = 0; i < 100; i++) {
        keys.add(Uint8List.fromList(utf8.encode('key$i')));
        values.add(Uint8List.fromList(utf8.encode('value$i')));
      }
      final node = LeafNode(7, keys, values, next: 8, previous: 6);
      final decoded = LeafNode.decode(7, node.encode());
      expect(decoded.keys.length, equals(100));
      expect(decoded.values.length, equals(100));
      for (var i = 0; i < 100; i++) {
        expect(utf8.decode(decoded.keys[i]), equals('key$i'));
        expect(utf8.decode(decoded.values[i]), equals('value$i'));
      }
      expect(decoded.next, equals(8));
      expect(decoded.previous, equals(6));
    });

    test('encode/decode empty keys and values', () {
      final node = LeafNode(1, [Uint8List(0)], [Uint8List(0)]);
      final decoded = LeafNode.decode(1, node.encode());
      expect(decoded.keys.length, equals(1));
      expect(decoded.keys[0].length, equals(0));
      expect(decoded.values[0].length, equals(0));
    });

    test('encode/decode large keys and values', () {
      final key = Uint8List(10000);
      final value = Uint8List(20000);
      for (var i = 0; i < key.length; i++) {
        key[i] = i % 256;
      }
      for (var i = 0; i < value.length; i++) {
        value[i] = (i * 7) % 256;
      }
      final node = LeafNode(99, [key], [value]);
      final decoded = LeafNode.decode(99, node.encode());
      expect(decoded.keys[0], equals(key));
      expect(decoded.values[0], equals(value));
    });

    test('magic validation on corrupt data', () {
      final buffer = Uint8List(32);
      // No valid magic
      expect(
          () => LeafNode.decode(1, buffer), throwsA(isA<StateError>()));
    });
  });

  group('InternalNode', () {
    test('encode/decode single key', () {
      final key = Uint8List.fromList(utf8.encode('separator'));
      final node = InternalNode(10, [key], [100, 200]);
      final decoded = InternalNode.decode(10, node.encode());
      expect(decoded.id, equals(10));
      expect(decoded.keys.length, equals(1));
      expect(utf8.decode(decoded.keys[0]), equals('separator'));
      expect(decoded.children, equals([100, 200]));
    });

    test('encode/decode many keys', () {
      final keys = <Uint8List>[];
      final children = <int>[0];
      for (var i = 0; i < 50; i++) {
        keys.add(Uint8List.fromList(utf8.encode('key$i')));
        children.add(i + 1);
      }
      final node = InternalNode(5, keys, children);
      final decoded = InternalNode.decode(5, node.encode());
      expect(decoded.keys.length, equals(50));
      expect(decoded.children.length, equals(51));
      for (var i = 0; i < 50; i++) {
        expect(utf8.decode(decoded.keys[i]), equals('key$i'));
        expect(decoded.children[i + 1], equals(i + 1));
      }
      expect(decoded.children[0], equals(0));
    });

    test('children count equals keys plus one', () {
      final keys = <Uint8List>[];
      final children = <int>[42];
      for (var i = 0; i < 10; i++) {
        keys.add(Uint8List.fromList(utf8.encode('k$i')));
        children.add(100 + i);
      }
      final node = InternalNode(1, keys, children);
      final decoded = InternalNode.decode(1, node.encode());
      expect(decoded.children.length, equals(decoded.keys.length + 1));
    });

    test('magic validation on corrupt data', () {
      final buffer = Uint8List(32);
      expect(
          () => InternalNode.decode(1, buffer), throwsA(isA<StateError>()));
    });
  });

  group('isLeaf', () {
    test('detects leaf nodes', () {
      final leaf = LeafNode(1, [], []);
      expect(isLeaf(leaf.encode()), isTrue);
    });

    test('detects internal nodes', () {
      final internal =
          InternalNode(1, [Uint8List.fromList([1])], [10, 20]);
      expect(isLeaf(internal.encode()), isFalse);
    });
  });
}
