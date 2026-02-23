#pragma once

#include "leanstore/btree/column_store/column_store.hpp"

#include <string>
#include <vector>

namespace leanstore::column_store {

struct ColumnBlockPayload {
  std::vector<uint8_t> bytes_;
  std::string max_key_;
  uint32_t row_count_;
};

class ColumnBlockBuilder {
public:
  ColumnBlockBuilder(const TableDefinition& def, const ColumnStoreOptions& options);

  void AddRow(const lean_row& row);
  bool Empty() const;
  bool ShouldFlush() const;
  Result<ColumnBlockPayload> Finalize(Slice max_key);

private:
  struct ColumnValues {
    ColumnType type_;
    bool nullable_;
    uint32_t fixed_length_;
    std::vector<uint8_t> nulls_;
    std::vector<int64_t> ints_;
    std::vector<uint64_t> uints_;
    std::vector<double> doubles_;
    std::vector<uint32_t> string_offsets_;
    std::vector<uint32_t> string_lengths_;
    std::vector<uint8_t> string_data_;
  };

  void Reset();
  void AddDefaultValue(ColumnValues& col);
  Result<void> EncodeColumn(size_t index, ColumnMeta& meta, std::vector<uint8_t>& storage);

  const TableDefinition& def_;
  const ColumnStoreOptions& options_;
  std::vector<ColumnValues> columns_;
  uint32_t row_count_ = 0;
  uint64_t approx_bytes_ = 0;
};

} // namespace leanstore::column_store
