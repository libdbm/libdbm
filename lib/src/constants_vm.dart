// ignore_for_file: constant_identifier_names, public_member_api_docs

/// Native platform constants (full 64-bit integers)
class DBMConstants {
  // Hash DBM Magic Number - Full 64-bit
  static const int HASH_DBM_MAGIC = 0xda7aba5eda7afeed;

  // Hash Record Pool Header Magic Number - Full 64-bit
  static const int HASH_RECORD_POOL_MAGIC = 0xa0ba51c0da7aba5e;

  // Record Block Magic Number - Full 64-bit
  static const int RECORD_BLOCK_MAGIC = 0xa0c011ec7ed01eaf;

  // Memory Pool Header Magic Number - Full 64-bit
  static const int MEMORY_POOL_MAGIC = 0xa0c0a1e5ced0da7a;

  // Delta Block Magic Number - Full 64-bit
  static const int DELTA_BLOCK_MAGIC = 0xde17ab10c0da7a01;

  // Version List Block Magic Number - Full 64-bit
  static const int VERSION_LIST_MAGIC = 0x0e5510911570da7a;

  // B+Tree Node Magic Number - Full 64-bit
  static const int BTREE_NODE_MAGIC = 0xb7ee40de50da7a01;

  // B+Tree Meta Magic Number - Full 64-bit
  static const int BTREE_META_MAGIC = 0xb7ee4e7a50da7a02;

  // Offset Mask - Full 64-bit
  static const int OFFSET_MASK = 0x0fffffffffffffff;

  // Length Mask - 32-bit
  static const int LENGTH_MASK = 0x00000000ffffffff;
}
