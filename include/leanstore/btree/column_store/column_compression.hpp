#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/btree/column_store/column_store.hpp"

#include <cstdint>
#include <vector>

namespace leanstore::column_store {

struct ColumnCompressionInput {
  ColumnType type_;
  uint32_t row_count_;
  const std::vector<uint8_t>& nulls_;
  const std::vector<int64_t>& ints_;
  const std::vector<uint64_t>& uints_;
  const std::vector<double>& doubles_;
  const std::vector<uint32_t>& string_offsets_;
  const std::vector<uint32_t>& string_lengths_;
  const std::vector<uint8_t>& string_data_;
};

class ColumnCompressionAlgorithm {
public:
  virtual ~ColumnCompressionAlgorithm() = default;

  virtual CompressionType Type() const = 0;
  virtual bool SupportsType(ColumnType type) const = 0;

  // Returns true when the algorithm encoded the input and updated meta/storage.
  virtual Result<bool> TryEncode(const ColumnCompressionInput& input, ColumnMeta& meta,
                                 std::vector<uint8_t>& storage) const = 0;

  virtual Result<void> Validate(const ColumnBlockHeader& header, const ColumnMeta& meta,
                                const std::vector<uint8_t>& storage, uint32_t col_idx) const = 0;
  virtual Result<int> Compare(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                              uint32_t row_idx, const Datum& target, ColumnType type) const = 0;
  virtual Result<void> Decode(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                              uint32_t row_idx, Datum& out, ColumnType type) const = 0;
};

const ColumnCompressionAlgorithm* FindCompressionAlgorithm(CompressionType type);
const std::vector<const ColumnCompressionAlgorithm*>& CompressionCandidates(ColumnType type);

Result<void> EncodeColumnWithCompression(const ColumnCompressionInput& input, ColumnMeta& meta,
                                         std::vector<uint8_t>& storage);

} // namespace leanstore::column_store
