// Focused write-path benchmark used to measure the effect of the
// memory-pool flush / batch-mode changes. Run with:
//   dart run example/bench_writes.dart
//
// Each scenario is run against a fresh file so numbers are comparable.

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libdbm/libdbm.dart';

Uint8List b(final String s) => Uint8List.fromList(utf8.encode(s));

String pad(final int i) => i.toString().padLeft(7, '0');

void bench(final String label, final int ops, final void Function() body) {
  final sw = Stopwatch()..start();
  body();
  sw.stop();
  final us = sw.elapsedMicroseconds;
  final perOp = ops > 0 ? (us / ops).toStringAsFixed(2) : '-';
  final rate = ops > 0 ? (ops * 1e6 / us).toStringAsFixed(0) : '-';
  print('  ${label.padRight(42)} ${ops.toString().padLeft(7)} ops  '
      '${sw.elapsed.toString().padLeft(14)}  '
      '${perOp.padLeft(9)} us/op  $rate ops/s');
}

File tempFile(final String name) {
  final file = File('${Directory.systemTemp.path}/$name');
  if (file.existsSync()) file.deleteSync();
  return file;
}

HashDBM open(final File file,
    {required final bool flush, final int buckets = 10007}) {
  final raf = file.openSync(mode: FileMode.write);
  return HashDBM(raf, flush: flush, buckets: buckets);
}

void scenario(final String title, final int n, final Function() fn) {
  print('\n── $title (n=$n) ──');
  fn();
}

void main() {
  print('dart:       ${Platform.version}');
  print('platform:   ${Platform.operatingSystem}');
  print('tmp dir:    ${Directory.systemTemp.path}\n');

  for (final n in [2000, 10000]) {
    scenario('sequential put', n, () {
      // flush=true — default, fsync per put
      {
        final file = tempFile('bench_put_flush.bin');
        final db = open(file, flush: true);
        bench('put, flush=true', n, () {
          for (var i = 0; i < n; i++) {
            db.put(b('key${pad(i)}'), b('val${pad(i)}'));
          }
        });
        db.close();
        file.deleteSync();
      }

      // flush=true + batch=true (supposed to behave like flush=false)
      {
        final file = tempFile('bench_put_batch.bin');
        final db = open(file, flush: true);
        db.batch = true;
        bench('put, flush=true + batch=true', n, () {
          for (var i = 0; i < n; i++) {
            db.put(b('key${pad(i)}'), b('val${pad(i)}'));
          }
          db.flush();
        });
        db.close();
        file.deleteSync();
      }

      // flush=false (manual flush at end)
      {
        final file = tempFile('bench_put_noflush.bin');
        final db = open(file, flush: false);
        bench('put, flush=false', n, () {
          for (var i = 0; i < n; i++) {
            db.put(b('key${pad(i)}'), b('val${pad(i)}'));
          }
          db.flush();
        });
        db.close();
        file.deleteSync();
      }
    });

    scenario('putIfAbsent (all new keys)', n, () {
      // putIfAbsent currently ignores batch — we want to see this
      {
        final file = tempFile('bench_pia_flush.bin');
        final db = open(file, flush: true);
        bench('putIfAbsent, flush=true', n, () {
          for (var i = 0; i < n; i++) {
            db.putIfAbsent(b('key${pad(i)}'), b('val${pad(i)}'));
          }
        });
        db.close();
        file.deleteSync();
      }
      {
        final file = tempFile('bench_pia_batch.bin');
        final db = open(file, flush: true);
        db.batch = true;
        bench('putIfAbsent, flush=true + batch=true', n, () {
          for (var i = 0; i < n; i++) {
            db.putIfAbsent(b('key${pad(i)}'), b('val${pad(i)}'));
          }
          db.flush();
        });
        db.close();
        file.deleteSync();
      }
    });

    scenario('overwrite (same-size values)', n, () {
      final file = tempFile('bench_overwrite.bin');
      final db = open(file, flush: true);
      db.batch = true;
      for (var i = 0; i < n; i++) {
        db.put(b('key${pad(i)}'), b('val${pad(i)}'));
      }
      db.flush();
      db.batch = false;
      bench('overwrite, flush=true', n, () {
        for (var i = 0; i < n; i++) {
          db.put(b('key${pad(i)}'), b('new${pad(i)}'));
        }
      });
      db.close();
      file.deleteSync();
    });

    scenario('remove', n, () {
      final file = tempFile('bench_remove.bin');
      final db = open(file, flush: true);
      db.batch = true;
      for (var i = 0; i < n; i++) {
        db.put(b('key${pad(i)}'), b('val${pad(i)}'));
      }
      db.flush();
      db.batch = false;
      bench('remove, flush=true', n, () {
        for (var i = 0; i < n; i++) {
          db.remove(b('key${pad(i)}'));
        }
      });
      db.close();
      file.deleteSync();
    });
  }

  // Bigger DB to expose pool-page-rewrite scaling with free-list growth.
  scenario('churn (put + remove then put) flush=true', 5000, () {
    final file = tempFile('bench_churn.bin');
    final db = open(file, flush: true);
    db.batch = true;
    // seed 20k keys so the free list gets exercised.
    for (var i = 0; i < 20000; i++) {
      db.put(b('key${pad(i)}'), b('val${pad(i)}'));
    }
    db.flush();
    db.batch = false;
    bench('remove+put churn (flush=true)', 5000, () {
      for (var i = 0; i < 5000; i++) {
        db.remove(b('key${pad(i)}'));
        db.put(b('key${pad(i + 100000)}'), b('val${pad(i)}'));
      }
    });
    db.close();
    file.deleteSync();
  });
}
