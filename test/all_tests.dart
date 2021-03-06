library libdbm.all_tests;

import 'libdbm_test.dart' as libdbm;
import 'memory_pool_test.dart' as mempool;
import 'persistent_map_map_test.dart' as mapmap;
import 'persistent_map_test.dart' as map;
import 'persistent_string_map_test.dart' as stringmap;

void main() {
  mempool.main();
  libdbm.main();
  map.main();
  stringmap.main();
  mapmap.main();
}
