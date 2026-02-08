#include "leanstore/btree/column_store/column_leaf_ops.hpp"

#include "leanstore/base/error.hpp"

#include <memory>

namespace leanstore::column_store {

namespace {

Result<int32_t> FindFirstRowGE(ColumnBlockReader& reader, const std::vector<Datum>& key_datums) {
  // Binary search inside a column block by key columns.
  uint32_t low = 0;
  uint32_t high = reader.RowCount();
  while (low < high) {
    uint32_t mid = low + (high - low) / 2;
    auto cmp = reader.CompareRowKey(mid, key_datums);
    if (!cmp) {
      return std::move(cmp.error());
    }
    if (cmp.value() < 0) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  if (low >= reader.RowCount()) {
    return -1;
  }
  return static_cast<int32_t>(low);
}

Result<int32_t> FindLastRowLE(ColumnBlockReader& reader, const std::vector<Datum>& key_datums) {
  // Binary search inside a column block by key columns.
  uint32_t low = 0;
  uint32_t high = reader.RowCount();
  while (low < high) {
    uint32_t mid = low + (high - low) / 2;
    auto cmp = reader.CompareRowKey(mid, key_datums);
    if (!cmp) {
      return std::move(cmp.error());
    }
    if (cmp.value() <= 0) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  if (low == 0) {
    return -1;
  }
  return static_cast<int32_t>(low - 1);
}

Result<int32_t> ResolveStartRowAsc(ColumnBlockReader& reader, Slice start_key,
                                   std::vector<Datum>& key_datums, bool* key_decoded) {
  if (start_key.size() == 0) {
    return 0;
  }
  if (!*key_decoded) {
    if (auto res = reader.DecodeKey(start_key, key_datums); !res) {
      return std::move(res.error());
    }
    *key_decoded = true;
  }
  return FindFirstRowGE(reader, key_datums);
}

Result<int32_t> ResolveStartRowDesc(ColumnBlockReader& reader, Slice start_key,
                                    std::vector<Datum>& key_datums, bool* key_decoded) {
  if (start_key.size() == 0) {
    return static_cast<int32_t>(reader.RowCount()) - 1;
  }
  if (!*key_decoded) {
    if (auto res = reader.DecodeKey(start_key, key_datums); !res) {
      return std::move(res.error());
    }
    *key_decoded = true;
  }
  return FindLastRowLE(reader, key_datums);
}

} // namespace

Result<bool> LookupColumnBlock(LeanStore* store, const ColumnBlockRef& ref, Slice key,
                               std::string* out_value) {
  if (out_value == nullptr) {
    return Error::General("null column block output value");
  }
  auto reader_res = ReadColumnBlock(store, ref);
  if (!reader_res) {
    return std::move(reader_res.error());
  }
  auto reader = std::move(reader_res.value());
  std::vector<Datum> key_datums;
  if (auto res = reader.DecodeKey(key, key_datums); !res) {
    return std::move(res.error());
  }

  auto row_lb = FindFirstRowGE(reader, key_datums);
  if (!row_lb) {
    return std::move(row_lb.error());
  }
  if (row_lb.value() < 0) {
    return false;
  }
  auto cmp = reader.CompareRowKey(static_cast<uint32_t>(row_lb.value()), key_datums);
  if (!cmp) {
    return std::move(cmp.error());
  }
  if (cmp.value() != 0) {
    return false;
  }

  std::vector<Datum> datums(reader.ColumnCount());
  auto nulls = std::make_unique<bool[]>(reader.ColumnCount());
  if (auto res = reader.EncodeValue(static_cast<uint32_t>(row_lb.value()), datums.data(),
                                    nulls.get(), reader.ColumnCount(), out_value);
      !res) {
    return std::move(res.error());
  }
  return true;
}

Result<ColumnBlockScanResult> ScanColumnBlockAsc(LeanStore* store, const ColumnBlockRef& ref,
                                                 Slice start_key, ColumnBlockScanState* state,
                                                 const ColumnBlockScanCallback& callback) {
  if (state == nullptr) {
    return Error::General("null column block scan state");
  }
  ColumnBlockScanResult result;
  auto reader_res = ReadColumnBlock(store, ref);
  if (!reader_res) {
    return std::move(reader_res.error());
  }
  auto reader = std::move(reader_res.value());
  int32_t row_start = 0;
  if (state->need_start_row_) {
    auto row_lb = ResolveStartRowAsc(reader, start_key, state->key_datums_, &state->key_decoded_);
    if (!row_lb) {
      return std::move(row_lb.error());
    }
    if (row_lb.value() < 0) {
      state->need_start_row_ = false;
      return result;
    }
    row_start = row_lb.value();
    state->need_start_row_ = false;
  }
  std::vector<Datum> datums(reader.ColumnCount());
  auto nulls = std::make_unique<bool[]>(reader.ColumnCount());
  EncodedRow out;
  for (uint32_t row = static_cast<uint32_t>(row_start); row < reader.RowCount(); ++row) {
    if (auto res = reader.EncodeRow(row, datums.data(), nulls.get(), reader.ColumnCount(), &out);
        !res) {
      return std::move(res.error());
    }
    result.emitted_ = true;
    if (!callback(Slice(out.key_), Slice(out.value_))) {
      result.stop_ = true;
      return result;
    }
  }
  return result;
}

Result<ColumnBlockScanResult> ScanColumnBlockDesc(LeanStore* store, const ColumnBlockRef& ref,
                                                  Slice start_key, ColumnBlockScanState* state,
                                                  const ColumnBlockScanCallback& callback) {
  if (state == nullptr) {
    return Error::General("null column block scan state");
  }
  ColumnBlockScanResult result;
  auto reader_res = ReadColumnBlock(store, ref);
  if (!reader_res) {
    return std::move(reader_res.error());
  }
  auto reader = std::move(reader_res.value());
  int32_t row_start = static_cast<int32_t>(reader.RowCount()) - 1;
  if (state->need_start_row_) {
    auto row_le = ResolveStartRowDesc(reader, start_key, state->key_datums_, &state->key_decoded_);
    if (!row_le) {
      return std::move(row_le.error());
    }
    if (row_le.value() < 0) {
      state->need_start_row_ = false;
      return result;
    }
    row_start = row_le.value();
    state->need_start_row_ = false;
  }
  std::vector<Datum> datums(reader.ColumnCount());
  auto nulls = std::make_unique<bool[]>(reader.ColumnCount());
  EncodedRow out;
  for (int32_t row = row_start; row >= 0; --row) {
    if (auto res = reader.EncodeRow(static_cast<uint32_t>(row), datums.data(), nulls.get(),
                                    reader.ColumnCount(), &out);
        !res) {
      return std::move(res.error());
    }
    result.emitted_ = true;
    if (!callback(Slice(out.key_), Slice(out.value_))) {
      result.stop_ = true;
      return result;
    }
  }
  return result;
}

} // namespace leanstore::column_store
