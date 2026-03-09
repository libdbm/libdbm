import 'dart:convert' show Encoding, utf8;
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:libdbm/libdbm.dart';
import 'package:libdbm/src/btree_node.dart';
import 'package:libdbm/src/util.dart';

// ─── Instrumented RandomAccessFile ─────────────────────────────────

int ioReads = 0;
int ioWrites = 0;
int ioFlushes = 0;
int ioSeeks = 0;
int ioBytesRead = 0;
int ioBytesWritten = 0;
Duration ioReadTime = Duration.zero;
Duration ioWriteTime = Duration.zero;
Duration ioFlushTime = Duration.zero;

void resetIO() {
  ioReads = 0;
  ioWrites = 0;
  ioFlushes = 0;
  ioSeeks = 0;
  ioBytesRead = 0;
  ioBytesWritten = 0;
  ioReadTime = Duration.zero;
  ioWriteTime = Duration.zero;
  ioFlushTime = Duration.zero;
}

void reportIO(final String label) {
  print('  [$label I/O]');
  if (ioSeeks > 0) print('    seeks:     $ioSeeks');
  if (ioReads > 0) {
    print('    reads:     $ioReads '
        '($ioBytesRead bytes, ${ioReadTime.inMicroseconds}us)');
  }
  if (ioWrites > 0) {
    print('    writes:    $ioWrites '
        '($ioBytesWritten bytes, ${ioWriteTime.inMicroseconds}us)');
  }
  if (ioFlushes > 0) {
    print('    flushes:   $ioFlushes '
        '(${ioFlushTime.inMicroseconds}us)');
  }
}

/// Wraps a [RandomAccessFile] to count and time every I/O call.
class InstrumentedFile implements RandomAccessFile {
  final RandomAccessFile _inner;
  InstrumentedFile(this._inner);

  @override
  void setPositionSync(final int position) {
    ioSeeks++;
    _inner.setPositionSync(position);
  }

  @override
  int readIntoSync(final List<int> buffer,
      [final int start = 0, final int? end]) {
    ioReads++;
    final s = Stopwatch()..start();
    final n = _inner.readIntoSync(buffer, start, end);
    s.stop();
    ioBytesRead += n;
    ioReadTime += s.elapsed;
    return n;
  }

  @override
  void writeFromSync(final List<int> buffer,
      [final int start = 0, final int? end]) {
    ioWrites++;
    final s = Stopwatch()..start();
    _inner.writeFromSync(buffer, start, end);
    s.stop();
    final count = (end ?? buffer.length) - start;
    ioBytesWritten += count;
    ioWriteTime += s.elapsed;
  }

  @override
  void flushSync() {
    ioFlushes++;
    final s = Stopwatch()..start();
    _inner.flushSync();
    s.stop();
    ioFlushTime += s.elapsed;
  }

  // ── Delegate everything else ──
  @override
  void closeSync() => _inner.closeSync();
  @override
  int lengthSync() => _inner.lengthSync();
  @override
  int positionSync() => _inner.positionSync();
  @override
  Uint8List readSync(final int count) => _inner.readSync(count);
  @override
  int readByteSync() => _inner.readByteSync();
  @override
  void lockSync(
          [final FileLock mode = FileLock.exclusive,
          final int start = 0,
          final int end = -1]) =>
      _inner.lockSync(mode, start, end);
  @override
  void unlockSync([final int start = 0, final int end = -1]) =>
      _inner.unlockSync(start, end);
  @override
  void truncateSync(final int length) => _inner.truncateSync(length);
  @override
  int writeByteSync(final int value) => _inner.writeByteSync(value);
  @override
  void writeStringSync(final String string,
          {final Encoding encoding = systemEncoding}) =>
      _inner.writeStringSync(string, encoding: encoding);
  @override
  String get path => _inner.path;
  @override
  Future<void> close() => _inner.close();
  @override
  Future<RandomAccessFile> flush() => _inner.flush();
  @override
  Future<int> length() => _inner.length();
  @override
  Future<RandomAccessFile> lock(
          [final FileLock mode = FileLock.exclusive,
          final int start = 0,
          final int end = -1]) =>
      _inner.lock(mode, start, end);
  @override
  Future<int> position() => _inner.position();
  @override
  Future<Uint8List> read(final int count) => _inner.read(count);
  @override
  Future<int> readByte() => _inner.readByte();
  @override
  Future<int> readInto(final List<int> buffer,
          [final int start = 0, final int? end]) =>
      _inner.readInto(buffer, start, end);
  @override
  Future<RandomAccessFile> setPosition(final int position) =>
      _inner.setPosition(position);
  @override
  Future<RandomAccessFile> truncate(final int length) =>
      _inner.truncate(length);
  @override
  Future<RandomAccessFile> unlock([final int start = 0, final int end = -1]) =>
      _inner.unlock(start, end);
  @override
  Future<RandomAccessFile> writeByte(final int value) =>
      _inner.writeByte(value);
  @override
  Future<RandomAccessFile> writeFrom(final List<int> buffer,
          [final int start = 0, final int? end]) =>
      _inner.writeFrom(buffer, start, end);
  @override
  Future<RandomAccessFile> writeString(final String string,
          {final Encoding encoding = systemEncoding}) =>
      _inner.writeString(string, encoding: encoding);
}

// ─── Helpers ───────────────────────────────────────────────────────

Uint8List key(final String value) => Uint8List.fromList(utf8.encode(value));
Uint8List val(final String value) => Uint8List.fromList(utf8.encode(value));

String padded(final int i, [final int width = 6]) =>
    i.toString().padLeft(width, '0');

void header(final String title) {
  print('\n${"=" * 60}');
  print(' $title');
  print('${"=" * 60}\n');
}

void result(final String label, final int ops, final Duration elapsed) {
  final us = elapsed.inMicroseconds;
  final per = ops > 0 ? (us / ops).toStringAsFixed(1) : '0';
  print('  $label: $elapsed '
      '($ops ops, ${per}us/op)');
}

// ─── HashDBM profiling ────────────────────────────────────────────

void profileHash(final int n) {
  header('HashDBM ($n operations)');
  final file = File('profile_hash.bin');
  if (file.existsSync()) file.deleteSync();
  final s = Stopwatch();

  // ── flush=false (in-memory buffered) ──
  print('--- flush=false ---');
  var raf = InstrumentedFile(file.openSync(mode: FileMode.write));
  var db = HashDBM(raf, flush: false);

  // Sequential insert
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  db.flush();
  s.stop();
  result('Sequential insert', n, s.elapsed);
  reportIO('insert');
  final insertSize = file.lengthSync();
  print('    file size: ${(insertSize / 1024).round()} KB');

  // Random read
  resetIO();
  final rng = Random(42);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.get(key('key${padded(rng.nextInt(n))}'));
  }
  s.stop();
  result('Random read', n, s.elapsed);
  reportIO('read');

  // Overwrite
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('new${padded(i)}'));
  }
  db.flush();
  s.stop();
  result('Overwrite', n, s.elapsed);
  reportIO('overwrite');

  // Delete half
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i += 2) {
    db.remove(key('key${padded(i)}'));
  }
  db.flush();
  s.stop();
  result('Delete ${n ~/ 2}', n ~/ 2, s.elapsed);
  reportIO('delete');

  // Iteration
  resetIO();
  s
    ..reset()
    ..start();
  var count = 0;
  final iter = db.entries();
  while (iter.moveNext()) {
    count++;
  }
  s.stop();
  result('Iteration ($count entries)', count, s.elapsed);
  reportIO('iteration');

  db.close();
  if (file.existsSync()) file.deleteSync();

  // ── flush=true ──
  print('\n--- flush=true ---');
  raf = InstrumentedFile(file.openSync(mode: FileMode.write));
  db = HashDBM(raf, flush: true);

  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  s.stop();
  result('Sequential insert', n, s.elapsed);
  reportIO('insert');

  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.get(key('key${padded(rng.nextInt(n))}'));
  }
  s.stop();
  result('Random read', n, s.elapsed);
  reportIO('read');

  db.close();
  if (file.existsSync()) file.deleteSync();
}

// ─── BTreeDBM profiling ───────────────────────────────────────────

void profileBTree(final int n, final int order) {
  header('BTreeDBM (n=$n, order=$order)');
  final file = File('profile_btree.bin');
  if (file.existsSync()) file.deleteSync();
  final s = Stopwatch();
  final raf = InstrumentedFile(file.openSync(mode: FileMode.write));
  final db = BTreeDBM(raf, order: order, flush: false);

  // Sequential insert
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  db.flush();
  s.stop();
  result('Sequential insert', n, s.elapsed);
  reportIO('insert');
  final size = file.lengthSync();
  print('    file size: ${(size / 1024).round()} KB');

  // Random read
  resetIO();
  final rng = Random(42);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.get(key('key${padded(rng.nextInt(n))}'));
  }
  s.stop();
  result('Random read', n, s.elapsed);
  reportIO('read');

  // Sorted iteration
  resetIO();
  s
    ..reset()
    ..start();
  var count = 0;
  final iter = db.entries();
  while (iter.moveNext()) {
    count++;
  }
  s.stop();
  result('Sorted iteration', count, s.elapsed);
  reportIO('iteration');

  // Range query (10%)
  resetIO();
  s
    ..reset()
    ..start();
  count = 0;
  final range = db.range(start: key('key010000'), end: key('key020000'));
  while (range.moveNext()) {
    count++;
  }
  s.stop();
  result('Range query ($count entries)', count, s.elapsed);
  reportIO('range');

  // floor/ceiling (1000 lookups)
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < 1000; i++) {
    db.floor(key('key${padded(rng.nextInt(n))}'));
    db.ceiling(key('key${padded(rng.nextInt(n))}'));
  }
  s.stop();
  result('floor/ceiling (2000 lookups)', 2000, s.elapsed);
  reportIO('floor/ceiling');

  // Delete every other key
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i += 2) {
    db.remove(key('key${padded(i)}'));
  }
  db.flush();
  s.stop();
  result('Delete ${n ~/ 2}', n ~/ 2, s.elapsed);
  reportIO('delete');

  db.close();
  if (file.existsSync()) file.deleteSync();
}

// ─── Micro-benchmarks ──────────────────────────────────────────────

void microBenchmarks(final int n) {
  header('Micro-benchmarks ($n iterations)');
  final s = Stopwatch();

  // 1. Uint8List allocation
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    final buf = Uint8List(8);
    ByteData.view(buf.buffer).setUint64(0, i);
    // ignore: unnecessary_statements
    Uint8List.fromList(buf);
  }
  s.stop();
  result('Uint8List(8) + fromList copy', n, s.elapsed);

  // 2. ByteData.view creation
  final source = Uint8List(256);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    ByteData.view(source.buffer, 0);
  }
  s.stop();
  result('ByteData.view() creation', n, s.elapsed);

  // 3. Lexicographic compare
  final a = key('key025000');
  final b = key('key025001');
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    compare(a, b);
  }
  s.stop();
  result('compare() 9 bytes', n, s.elapsed);

  // 4. hash() function
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    hash(a);
  }
  s.stop();
  result('hash() 9 bytes', n, s.elapsed);

  // 5. matches() function
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    matches(a, b);
  }
  s.stop();
  result('matches() 9 bytes (differ)', n, s.elapsed);

  final c = Uint8List.fromList(a);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    matches(a, c);
  }
  s.stop();
  result('matches() 9 bytes (equal)', n, s.elapsed);

  // 6. LeafNode encode/decode
  final keys = List.generate(64, (final i) => key('key${padded(i)}'));
  final values = List.generate(64, (final i) => val('val${padded(i)}'));
  final leaf = LeafNode(1, keys, values, next: 2, previous: 0);
  s
    ..reset()
    ..start();
  late Uint8List encoded;
  for (var i = 0; i < n; i++) {
    encoded = leaf.encode();
  }
  s.stop();
  result(
      'LeafNode.encode() (64 entries, '
      '${encoded.length}B)',
      n,
      s.elapsed);

  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    LeafNode.decode(1, encoded);
  }
  s.stop();
  result('LeafNode.decode() (64 entries)', n, s.elapsed);

  // 7. InternalNode encode/decode
  final ikeys = List.generate(64, (final i) => key('key${padded(i)}'));
  final children = List.generate(65, (final i) => i + 1);
  final internal = InternalNode(1, ikeys, children);
  s
    ..reset()
    ..start();
  late Uint8List iencoded;
  for (var i = 0; i < n; i++) {
    iencoded = internal.encode();
  }
  s.stop();
  result(
      'InternalNode.encode() (64 keys, '
      '${iencoded.length}B)',
      n,
      s.elapsed);

  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    InternalNode.decode(1, iencoded);
  }
  s.stop();
  result('InternalNode.decode() (64 keys)', n, s.elapsed);

  // 8. Binary search over 64 keys
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    _bsearch(keys, key('key000032'));
  }
  s.stop();
  result('Binary search (64 keys)', n, s.elapsed);

  // 9. List.insert (simulating leaf insert)
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    final list = List<int>.from(List.generate(64, (final j) => j));
    list.insert(32, 999);
  }
  s.stop();
  result('List.insert at middle (64 items)', n, s.elapsed);

  // 10. File I/O: single seek+read vs seek+write
  final file = File('profile_micro_io.bin');
  final raf = file.openSync(mode: FileMode.write);
  raf.writeFromSync(Uint8List(4096));
  final buf = Uint8List(256);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    raf.setPositionSync(0);
    raf.readIntoSync(buf);
  }
  s.stop();
  result('File seek+read 256B', n, s.elapsed);

  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    raf.setPositionSync(0);
    raf.writeFromSync(buf);
  }
  s.stop();
  result('File seek+write 256B', n, s.elapsed);

  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    raf.flushSync();
  }
  s.stop();
  result('File flushSync()', n, s.elapsed);

  raf.closeSync();
  file.deleteSync();
}

// ─── HashDBM internal analysis ─────────────────────────────────────

void hashAnalysis(final int n) {
  header('HashDBM internal analysis ($n entries)');

  final file = File('profile_analysis.bin');
  if (file.existsSync()) file.deleteSync();
  final raf = file.openSync(mode: FileMode.write);
  final db = HashDBM(raf, flush: false);

  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  db.flush();

  final iter = db.entries();
  var count = 0;
  while (iter.moveNext()) {
    count++;
  }

  final s = Stopwatch();

  // Cold reads (sequential)
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.get(key('key${padded(i)}'));
  }
  s.stop();
  result('Sequential read', n, s.elapsed);

  // Hot reads (same key)
  final hot = key('key${padded(n ~/ 2)}');
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.get(hot);
  }
  s.stop();
  result('Hot read (same key)', n, s.elapsed);

  // flush() cost
  s
    ..reset()
    ..start();
  for (var i = 0; i < 100; i++) {
    db.flush();
  }
  s.stop();
  result('flush() only', 100, s.elapsed);

  print('  count: $count');
  print('  file size: ${(file.lengthSync() / 1024).round()} KB');
  print('  hash table size: ${db.hashTableSize}');

  db.close();
  if (file.existsSync()) file.deleteSync();
}

// ─── HashDBM put() breakdown ──────────────────────────────────────

void putBreakdown(final int n) {
  header('HashDBM put() breakdown (flush=false, $n ops)');
  final file = File('profile_put.bin');
  if (file.existsSync()) file.deleteSync();
  final s = Stopwatch();

  // Measure put with instrumented file
  final raf = InstrumentedFile(file.openSync(mode: FileMode.write));
  final db = HashDBM(raf, flush: false);

  // Insert n keys, measure total
  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  s.stop();
  print('  Insert $n (no flush): ${s.elapsed}');
  print('    IO reads:  $ioReads ($ioBytesRead B)');
  print('    IO writes: $ioWrites ($ioBytesWritten B)');
  print('    IO seeks:  $ioSeeks');
  print('    IO flushes: $ioFlushes');
  final noFlush = s.elapsed;

  // Now measure a single flush
  resetIO();
  s
    ..reset()
    ..start();
  db.flush();
  s.stop();
  print('  Single flush(): ${s.elapsed}');
  print('    IO writes: $ioWrites ($ioBytesWritten B)');
  print('    IO flushes: $ioFlushes');

  // Per-op breakdown
  final us = noFlush.inMicroseconds;
  final perOp = us / n;
  final readUs = ioReadTime.inMicroseconds;
  final writeUs = ioWriteTime.inMicroseconds;
  print('\n  Per insert: ${perOp.toStringAsFixed(1)}us');
  print('    seek+read portion: '
      '${(readUs / n).toStringAsFixed(1)}us/op');
  print('    write portion:     '
      '${(writeUs / n).toStringAsFixed(1)}us/op');

  db.close();
  if (file.existsSync()) file.deleteSync();
}

// ─── BTreeDBM node decode cost ────────────────────────────────────

void btreeDecodeProfile(final int n) {
  header('BTreeDBM decode cost analysis (n=$n)');
  final file = File('profile_decode.bin');
  if (file.existsSync()) file.deleteSync();
  final raf = InstrumentedFile(file.openSync(mode: FileMode.write));
  final db = BTreeDBM(raf, order: 128, flush: false);

  // Insert n keys
  for (var i = 0; i < n; i++) {
    db.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  db.flush();

  // Now profile reads only (tree is stable)
  final rng = Random(42);
  final s = Stopwatch();

  resetIO();
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    db.get(key('key${padded(rng.nextInt(n))}'));
  }
  s.stop();
  final total = s.elapsed;
  final reads = ioReads;
  final bytes = ioBytesRead;
  final readTime = ioReadTime;

  print('  Random read $n keys:');
  result('Total', n, total);
  print('    File reads:  $reads '
      '(${(reads / n).toStringAsFixed(1)}/op)');
  print('    Bytes read:  $bytes '
      '(${(bytes / reads).round()} avg/read)');
  print('    File I/O:    ${readTime.inMicroseconds}us '
      '(${(readTime.inMicroseconds / n).toStringAsFixed(1)}us/op)');
  final cpu = total.inMicroseconds - readTime.inMicroseconds;
  print('    CPU (decode+search): ${cpu}us '
      '(${(cpu / n).toStringAsFixed(1)}us/op)');

  db.close();
  if (file.existsSync()) file.deleteSync();
}

// ─── Comparison ───────────────────────────────────────────────────

void comparison(final int n) {
  header('Head-to-head comparison ($n ops)');
  final file = File('profile_compare.bin');
  final s = Stopwatch();

  // HashDBM flush=false
  if (file.existsSync()) file.deleteSync();
  var raf = file.openSync(mode: FileMode.write);
  var hash = HashDBM(raf, flush: false);

  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    hash.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  hash.flush();
  s.stop();
  final hashInsert = s.elapsed;

  final rng = Random(42);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    hash.get(key('key${padded(rng.nextInt(n))}'));
  }
  s.stop();
  final hashRead = s.elapsed;

  hash.close();

  // BTreeDBM (order 128)
  if (file.existsSync()) file.deleteSync();
  raf = file.openSync(mode: FileMode.write);
  final btree = BTreeDBM(raf, order: 128, flush: false);

  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    btree.put(key('key${padded(i)}'), val('val${padded(i)}'));
  }
  btree.flush();
  s.stop();
  final btreeInsert = s.elapsed;

  final rng2 = Random(42);
  s
    ..reset()
    ..start();
  for (var i = 0; i < n; i++) {
    btree.get(key('key${padded(rng2.nextInt(n))}'));
  }
  s.stop();
  final btreeRead = s.elapsed;

  // BTree sorted iteration
  s
    ..reset()
    ..start();
  final iter = btree.entries();
  while (iter.moveNext()) {}
  s.stop();
  final btreeIter = s.elapsed;

  btree.close();
  if (file.existsSync()) file.deleteSync();

  print('  Operation           HashDBM       BTreeDBM     '
      'Ratio');
  print('  ${"─" * 58}');

  final hi = hashInsert.inMicroseconds / n;
  final bi = btreeInsert.inMicroseconds / n;
  print('  Insert (us/op)      '
      '${hi.toStringAsFixed(1).padLeft(8)}      '
      '${bi.toStringAsFixed(1).padLeft(8)}     '
      '${(bi / hi).toStringAsFixed(2)}x');

  final hr = hashRead.inMicroseconds / n;
  final br = btreeRead.inMicroseconds / n;
  print('  Random read (us/op) '
      '${hr.toStringAsFixed(1).padLeft(8)}      '
      '${br.toStringAsFixed(1).padLeft(8)}     '
      '${(br / hr).toStringAsFixed(2)}x');

  final biter = btreeIter.inMicroseconds / n;
  print('  Sorted iter (us/op)         n/a      '
      '${biter.toStringAsFixed(1).padLeft(8)}');
}

// ─── Binary search helper for micro-bench ─────────────────────────

int _bsearch(final List<Uint8List> keys, final Uint8List target) {
  var lo = 0;
  var hi = keys.length;
  while (lo < hi) {
    final mid = (lo + hi) >> 1;
    if (compare(keys[mid], target) < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// ─── Main ─────────────────────────────────────────────────────────

void main() {
  final n = 50000;
  print('═══ libdbm Profiling Suite ═══');
  print('Operations: $n');
  print('Platform: ${Platform.operatingSystem} '
      '${Platform.operatingSystemVersion}');
  print('Dart: ${Platform.version}');

  microBenchmarks(100000);
  hashAnalysis(n);
  putBreakdown(n);
  profileHash(n);
  btreeDecodeProfile(n);
  profileBTree(n, 64);
  profileBTree(n, 128);
  comparison(n);

  // Cleanup
  for (final name in [
    'profile_hash.bin',
    'profile_btree.bin',
    'profile_analysis.bin',
    'profile_put.bin',
    'profile_decode.bin',
    'profile_compare.bin',
    'profile_micro_io.bin',
  ]) {
    final f = File(name);
    if (f.existsSync()) f.deleteSync();
  }
}
