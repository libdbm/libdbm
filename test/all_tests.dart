library libdbm.all_tests;

import 'delta_block_test.dart' as delta;
import 'libdbm_test.dart' as libdbm;
import 'memory_pool_test.dart' as mempool;
import 'persistent_map_map_test.dart' as mapmap;
import 'persistent_map_test.dart' as map;
import 'persistent_string_map_test.dart' as stringmap;
import 'versioned_dbm_test.dart' as versioned;

void main() {
  mempool.main();
  libdbm.main();
  map.main();
  stringmap.main();
  mapmap.main();
  delta.main();
  versioned.main();
}
