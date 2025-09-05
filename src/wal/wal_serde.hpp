#pragma once

#include <cstdint>
#include <string>

struct lean_wal_record;

namespace leanstore {

/// WAL serialization and deserialization utilities
class WalSerde {
public:
  /// Convert the WAL record to a JSON string.
  static std::string ToJson(const lean_wal_record* record);

  /// Compute the masked CRC32 checksum of the given WAL record.
  static uint32_t Crc32Masked(const lean_wal_record* record);
};

} // namespace leanstore