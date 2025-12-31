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
  
  // Offset Mask - Full 64-bit
  static const int OFFSET_MASK = 0x0fffffffffffffff;
  
  // Length Mask - 32-bit
  static const int LENGTH_MASK = 0x00000000ffffffff;
}
