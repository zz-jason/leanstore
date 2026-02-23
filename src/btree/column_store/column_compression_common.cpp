#include "btree/column_store/column_compression_internal.hpp"
#include "leanstore/base/error.hpp"

#include <cstring>

namespace leanstore::column_store {

uint8_t WidthForRange(uint64_t range) {
  if (range <= 0xFF) {
    return 1;
  }
  if (range <= 0xFFFF) {
    return 2;
  }
  if (range <= 0xFFFFFFFF) {
    return 4;
  }
  return 8;
}

void AppendBytes(std::vector<uint8_t>& buffer, const void* data, size_t size) {
  const auto* ptr = reinterpret_cast<const uint8_t*>(data);
  buffer.insert(buffer.end(), ptr, ptr + size);
}

int CompareBytesByLength(const char* lhs, size_t lhs_len, const char* rhs, size_t rhs_len) {
  if (lhs_len != rhs_len) {
    return lhs_len < rhs_len ? -1 : 1;
  }
  if (lhs_len == 0) {
    return 0;
  }
  return std::memcmp(lhs, rhs, lhs_len);
}

int64_t LoadSigned(const uint8_t* ptr, uint8_t width) {
  int64_t value = 0;
  std::memcpy(&value, ptr, width);
  return value;
}

uint64_t LoadUnsigned(const uint8_t* ptr, uint8_t width) {
  uint64_t value = 0;
  std::memcpy(&value, ptr, width);
  return value;
}

uint8_t PhysicalTypeWidth(ColumnType type) {
  switch (type) {
  case ColumnType::kBool:
    return 1;
  case ColumnType::kInt32:
  case ColumnType::kFloat32:
    return 4;
  case ColumnType::kInt64:
  case ColumnType::kUInt64:
  case ColumnType::kFloat64:
    return 8;
  case ColumnType::kBinary:
  case ColumnType::kString:
    return 0;
  }
  return 0;
}

std::string_view GetInputString(const ColumnCompressionInput& input, uint32_t row_idx) {
  const uint32_t len = input.string_lengths_[row_idx];
  if (len == 0) {
    return {};
  }
  const uint32_t offset = input.string_offsets_[row_idx];
  return {reinterpret_cast<const char*>(input.string_data_.data() + offset), len};
}

Result<void> LoadDictionaryEntry(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                                 uint32_t idx, uint32_t& out_off, uint32_t& out_len) {
  if (idx >= meta.dict_entries_) {
    return Error::General("dictionary index out of range");
  }
  const size_t offsets_base = meta.dict_offset_;
  const size_t lengths_base = static_cast<size_t>(meta.dict_offset_) +
                              static_cast<size_t>(meta.dict_entries_) * sizeof(uint32_t);
  std::memcpy(&out_off, storage.data() + offsets_base + static_cast<size_t>(idx) * sizeof(uint32_t),
              sizeof(uint32_t));
  std::memcpy(&out_len, storage.data() + lengths_base + static_cast<size_t>(idx) * sizeof(uint32_t),
              sizeof(uint32_t));
  return {};
}

} // namespace leanstore::column_store
