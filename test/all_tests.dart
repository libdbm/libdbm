// ignore_for_file: unnecessary_library_name
library libdbm.all_tests;

import 'btree_dbm_test.dart' as btree;
import 'btree_node_test.dart' as nodes;
import 'delta_block_test.dart' as delta;
import 'libdbm_test.dart' as libdbm;
import 'memory_pool_test.dart' as mempool;
import 'persistent_map_map_test.dart' as mapmap;
import 'persistent_map_test.dart' as map;
import 'persistent_string_map_test.dart' as stringmap;
import 'sorted_map_test.dart' as sorted;
import 'versioned_dbm_test.dart' as versioned;

void main() {
  mempool.main();
  libdbm.main();
  map.main();
  stringmap.main();
  mapmap.main();
  delta.main();
  versioned.main();
  nodes.main();
  btree.main();
  sorted.main();
}
