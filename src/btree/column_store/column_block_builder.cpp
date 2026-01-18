#include "leanstore/btree/column_store/column_block_builder.hpp"

#include "leanstore/base/error.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <utility>

namespace leanstore::column_store {

namespace {

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

template <typename T>
void AppendPod(std::vector<uint8_t>& buffer, const T& value) {
  const auto* ptr = reinterpret_cast<const uint8_t*>(&value);
  buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
}

void AppendBytes(std::vector<uint8_t>& buffer, const void* data, size_t size) {
  const auto* ptr = reinterpret_cast<const uint8_t*>(data);
  buffer.insert(buffer.end(), ptr, ptr + size);
}

} // namespace

ColumnBlockBuilder::ColumnBlockBuilder(const TableDefinition& def,
                                       const ColumnStoreOptions& options)
    : def_(def),
      options_(options) {
  // Initialize per-column buffers based on the schema.
  const auto& cols = def.schema_.Columns();
  columns_.reserve(cols.size());
  for (const auto& col : cols) {
    ColumnValues values;
    values.type_ = col.type_;
    values.nullable_ = col.nullable_;
    values.fixed_length_ = col.fixed_length_;
    columns_.push_back(std::move(values));
  }
}

void ColumnBlockBuilder::AddRow(const std::string& key_bytes, const lean_row& row) {
  // Rows are assumed to be appended in key order.
  max_key_ = key_bytes;
  const auto& cols = def_.schema_.Columns();
  for (size_t i = 0; i < cols.size(); ++i) {
    auto& col = columns_[i];
    bool is_null = row.nulls != nullptr ? row.nulls[i] : false;
    col.nulls_.push_back(is_null ? 1 : 0);
    if (is_null) {
      AddDefaultValue(col);
      continue;
    }
    const auto& datum = row.columns[i];
    switch (col.type_) {
    case ColumnType::kBool:
      col.ints_.push_back(datum.b ? 1 : 0);
      approx_bytes_ += sizeof(uint8_t);
      break;
    case ColumnType::kInt32:
      col.ints_.push_back(datum.i32);
      approx_bytes_ += sizeof(int32_t);
      break;
    case ColumnType::kInt64:
      col.ints_.push_back(datum.i64);
      approx_bytes_ += sizeof(int64_t);
      break;
    case ColumnType::kUInt64:
      col.uints_.push_back(datum.u64);
      approx_bytes_ += sizeof(uint64_t);
      break;
    case ColumnType::kFloat32:
      col.doubles_.push_back(static_cast<double>(datum.f32));
      approx_bytes_ += sizeof(float);
      break;
    case ColumnType::kFloat64:
      col.doubles_.push_back(datum.f64);
      approx_bytes_ += sizeof(double);
      break;
    case ColumnType::kBinary:
    case ColumnType::kString: {
      std::string value;
      if (datum.str.size > 0 && datum.str.data != nullptr) {
        value.assign(datum.str.data, datum.str.data + datum.str.size);
      }
      approx_bytes_ += value.size();
      col.strings_.push_back(std::move(value));
      break;
    }
    default:
      AddDefaultValue(col);
      break;
    }
  }
  row_count_++;
}

bool ColumnBlockBuilder::Empty() const {
  return row_count_ == 0;
}

bool ColumnBlockBuilder::ShouldFlush() const {
  // Flush when row count or approximate byte budget is exceeded.
  if (options_.max_rows_per_block_ > 0 && row_count_ >= options_.max_rows_per_block_) {
    return true;
  }
  if (options_.max_block_bytes_ > 0 && approx_bytes_ >= options_.max_block_bytes_) {
    return true;
  }
  return false;
}

Result<ColumnBlockPayload> ColumnBlockBuilder::Finalize() {
  if (row_count_ == 0) {
    return Error::General("empty column block");
  }

  // Encode per-column data and assemble the column block payload.
  std::vector<ColumnMeta> metas(columns_.size());
  std::vector<uint8_t> storage;

  for (size_t i = 0; i < columns_.size(); ++i) {
    if (auto res = EncodeColumn(i, metas[i], storage); !res) {
      return std::move(res.error());
    }
  }

  ColumnBlockHeader header{};
  header.magic_ = kColumnBlockMagic;
  header.version_ = kColumnBlockVersion;
  header.column_count_ = static_cast<uint16_t>(columns_.size());
  header.row_count_ = row_count_;
  header.key_column_count_ = static_cast<uint16_t>(def_.schema_.PrimaryKeyColumns().size());

  // Layout: header | column metas | key column indices | storage bytes.
  std::vector<uint8_t> block_bytes;
  AppendPod(block_bytes, header);
  AppendBytes(block_bytes, metas.data(), metas.size() * sizeof(ColumnMeta));
  for (auto idx : def_.schema_.PrimaryKeyColumns()) {
    const uint16_t key_idx = static_cast<uint16_t>(idx);
    AppendPod(block_bytes, key_idx);
  }
  AppendBytes(block_bytes, storage.data(), storage.size());

  ColumnBlockPayload payload;
  payload.bytes_ = std::move(block_bytes);
  payload.max_key_ = std::move(max_key_);
  payload.row_count_ = row_count_;
  Reset();
  return payload;
}

void ColumnBlockBuilder::Reset() {
  row_count_ = 0;
  approx_bytes_ = 0;
  max_key_.clear();
  for (auto& col : columns_) {
    col.nulls_.clear();
    col.ints_.clear();
    col.uints_.clear();
    col.doubles_.clear();
    col.strings_.clear();
  }
}

void ColumnBlockBuilder::AddDefaultValue(ColumnValues& col) {
  switch (col.type_) {
  case ColumnType::kBool:
  case ColumnType::kInt32:
  case ColumnType::kInt64:
    col.ints_.push_back(0);
    break;
  case ColumnType::kUInt64:
    col.uints_.push_back(0);
    break;
  case ColumnType::kFloat32:
  case ColumnType::kFloat64:
    col.doubles_.push_back(0.0);
    break;
  case ColumnType::kBinary:
  case ColumnType::kString:
    col.strings_.emplace_back();
    break;
  default:
    break;
  }
}

Result<void> ColumnBlockBuilder::EncodeColumn(size_t index, ColumnMeta& meta,
                                              std::vector<uint8_t>& storage) {
  auto& col = columns_[index];
  meta.type_ = static_cast<uint8_t>(col.type_);
  meta.nullable_ = col.nullable_ ? 1 : 0;
  meta.fixed_length_ = col.fixed_length_;
  meta.base_.u64_ = 0;

  const bool has_nulls =
      std::any_of(col.nulls_.begin(), col.nulls_.end(), [](uint8_t v) { return v != 0; });
  if (has_nulls) {
    // Pack nulls into a bitmap.
    meta.nulls_offset_ = static_cast<uint32_t>(storage.size());
    meta.nulls_bytes_ = static_cast<uint32_t>((row_count_ + 7) / 8);
    std::vector<uint8_t> null_bytes(meta.nulls_bytes_, 0);
    for (uint32_t row = 0; row < row_count_; ++row) {
      if (col.nulls_[row] != 0) {
        null_bytes[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
      }
    }
    AppendBytes(storage, null_bytes.data(), null_bytes.size());
  } else {
    meta.nulls_offset_ = 0;
    meta.nulls_bytes_ = 0;
  }

  switch (col.type_) {
  case ColumnType::kBool:
  case ColumnType::kInt32:
  case ColumnType::kInt64: {
    // Prefer SingleValue or Truncation unless truncation width matches raw.
    EncodeIntegerColumn(col, meta, storage);
    return {};
  }
  case ColumnType::kUInt64: {
    // Prefer SingleValue or Truncation unless truncation width matches raw.
    EncodeUnsignedColumn(col, meta, storage);
    return {};
  }
  case ColumnType::kFloat32:
  case ColumnType::kFloat64: {
    // Float columns use SingleValue or Raw encoding.
    EncodeFloatColumn(col, meta, storage);
    return {};
  }
  case ColumnType::kBinary:
  case ColumnType::kString: {
    // Strings use SingleValue or OrderedDictionary.
    EncodeStringColumn(col, meta, storage);
    return {};
  }
  default:
    return Error::General("unsupported column type");
  }
}

void ColumnBlockBuilder::EncodeIntegerColumn(ColumnValues& col, ColumnMeta& meta,
                                             std::vector<uint8_t>& storage) {
  const uint32_t type_width =
      (col.type_ == ColumnType::kBool) ? 1 : (col.type_ == ColumnType::kInt32 ? 4 : 8);
  int64_t min_value = 0;
  int64_t max_value = 0;
  bool has_value = false;
  bool all_equal = true;
  int64_t first_value = 0;

  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] != 0) {
      continue;
    }
    int64_t v = col.ints_[i];
    if (!has_value) {
      min_value = max_value = first_value = v;
      has_value = true;
    } else {
      min_value = std::min(min_value, v);
      max_value = std::max(max_value, v);
      if (v != first_value) {
        all_equal = false;
      }
    }
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  if (!has_value || all_equal) {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kSingleValue);
    meta.value_width_ = static_cast<uint8_t>(type_width);
    const int64_t value = has_value ? first_value : 0;
    AppendBytes(storage, &value, type_width);
    meta.data_bytes_ = type_width;
    return;
  }

  const auto write_raw = [&]() {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kRaw);
    meta.value_width_ = static_cast<uint8_t>(type_width);
    meta.data_bytes_ = row_count_ * type_width;
    storage.resize(storage.size() + meta.data_bytes_);
    for (uint32_t i = 0; i < row_count_; ++i) {
      const int64_t v = col.nulls_[i] == 0 ? col.ints_[i] : 0;
      std::memcpy(storage.data() + meta.data_offset_ + i * type_width, &v, type_width);
    }
  };

  const __int128 range = static_cast<__int128>(max_value) - static_cast<__int128>(min_value);
  if (range < 0 || range > static_cast<__int128>(std::numeric_limits<uint64_t>::max())) {
    write_raw();
    return;
  }
  const uint64_t range_u = static_cast<uint64_t>(range);
  const uint8_t width = WidthForRange(range_u);
  if (width >= type_width) {
    write_raw();
    return;
  }

  meta.compression_ = static_cast<uint8_t>(CompressionType::kTruncation);
  meta.value_width_ = width;
  meta.base_.i64_ = min_value;
  meta.data_bytes_ = row_count_ * width;
  storage.resize(storage.size() + meta.data_bytes_);
  for (uint32_t i = 0; i < row_count_; ++i) {
    uint64_t delta = 0;
    if (col.nulls_[i] == 0) {
      const __int128 delta_i =
          static_cast<__int128>(col.ints_[i]) - static_cast<__int128>(min_value);
      delta = static_cast<uint64_t>(delta_i);
    }
    std::memcpy(storage.data() + meta.data_offset_ + i * width, &delta, width);
  }
}

void ColumnBlockBuilder::EncodeUnsignedColumn(ColumnValues& col, ColumnMeta& meta,
                                              std::vector<uint8_t>& storage) {
  uint64_t min_value = 0;
  uint64_t max_value = 0;
  bool has_value = false;
  bool all_equal = true;
  uint64_t first_value = 0;

  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] != 0) {
      continue;
    }
    uint64_t v = col.uints_[i];
    if (!has_value) {
      min_value = max_value = first_value = v;
      has_value = true;
    } else {
      min_value = std::min(min_value, v);
      max_value = std::max(max_value, v);
      if (v != first_value) {
        all_equal = false;
      }
    }
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  if (!has_value || all_equal) {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kSingleValue);
    meta.value_width_ = 8;
    const uint64_t value = has_value ? first_value : 0;
    AppendBytes(storage, &value, sizeof(uint64_t));
    meta.data_bytes_ = sizeof(uint64_t);
    return;
  }

  const uint64_t range = max_value - min_value;
  const uint8_t width = WidthForRange(range);
  if (width >= sizeof(uint64_t)) {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kRaw);
    meta.value_width_ = sizeof(uint64_t);
    meta.data_bytes_ = row_count_ * sizeof(uint64_t);
    storage.resize(storage.size() + meta.data_bytes_);
    for (uint32_t i = 0; i < row_count_; ++i) {
      const uint64_t v = col.nulls_[i] == 0 ? col.uints_[i] : 0;
      std::memcpy(storage.data() + meta.data_offset_ + i * sizeof(uint64_t), &v, sizeof(uint64_t));
    }
    return;
  }
  meta.compression_ = static_cast<uint8_t>(CompressionType::kTruncation);
  meta.value_width_ = width;
  meta.base_.u64_ = min_value;
  meta.data_bytes_ = row_count_ * width;
  storage.resize(storage.size() + meta.data_bytes_);
  for (uint32_t i = 0; i < row_count_; ++i) {
    uint64_t delta = 0;
    if (col.nulls_[i] == 0) {
      delta = col.uints_[i] - min_value;
    }
    std::memcpy(storage.data() + meta.data_offset_ + i * width, &delta, width);
  }
}

void ColumnBlockBuilder::EncodeFloatColumn(ColumnValues& col, ColumnMeta& meta,
                                           std::vector<uint8_t>& storage) {
  bool has_value = false;
  bool all_equal = true;
  double first_value = 0.0;
  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] != 0) {
      continue;
    }
    double v = col.doubles_[i];
    if (!has_value) {
      first_value = v;
      has_value = true;
    } else if (v != first_value) {
      all_equal = false;
    }
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  const uint32_t width = (col.type_ == ColumnType::kFloat32) ? sizeof(float) : sizeof(double);
  meta.value_width_ = static_cast<uint8_t>(width);
  if (!has_value || all_equal) {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kSingleValue);
    if (col.type_ == ColumnType::kFloat32) {
      const float v = static_cast<float>(has_value ? first_value : 0.0);
      AppendBytes(storage, &v, sizeof(float));
      meta.data_bytes_ = sizeof(float);
    } else {
      const double v = has_value ? first_value : 0.0;
      AppendBytes(storage, &v, sizeof(double));
      meta.data_bytes_ = sizeof(double);
    }
    return;
  }

  meta.compression_ = static_cast<uint8_t>(CompressionType::kRaw);
  meta.data_bytes_ = row_count_ * width;
  storage.resize(storage.size() + meta.data_bytes_);
  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.type_ == ColumnType::kFloat32) {
      const float v = col.nulls_[i] == 0 ? static_cast<float>(col.doubles_[i]) : 0.0f;
      std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
    } else {
      const double v = col.nulls_[i] == 0 ? col.doubles_[i] : 0.0;
      std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
    }
  }
}

void ColumnBlockBuilder::EncodeStringColumn(ColumnValues& col, ColumnMeta& meta,
                                            std::vector<uint8_t>& storage) {
  bool has_value = false;
  bool all_equal = true;
  std::string first_value;
  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] != 0) {
      continue;
    }
    const auto& v = col.strings_[i];
    if (!has_value) {
      first_value = v;
      has_value = true;
    } else if (v != first_value) {
      all_equal = false;
    }
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  if (!has_value || all_equal) {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kSingleValue);
    const std::string* value = has_value ? &first_value : nullptr;
    const uint32_t len = value ? static_cast<uint32_t>(value->size()) : 0;
    AppendBytes(storage, &len, sizeof(uint32_t));
    if (len > 0) {
      AppendBytes(storage, value->data(), value->size());
    }
    meta.data_bytes_ = sizeof(uint32_t) + len;
    meta.value_width_ = 0;
    return;
  }

  meta.compression_ = static_cast<uint8_t>(CompressionType::kOrderedDictionary);
  std::vector<std::string> dict;
  dict.reserve(col.strings_.size());
  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] == 0) {
      dict.push_back(col.strings_[i]);
    }
  }
  std::sort(dict.begin(), dict.end());
  dict.erase(std::unique(dict.begin(), dict.end()), dict.end());

  const uint32_t dict_size = static_cast<uint32_t>(dict.size());
  const uint8_t width = dict_size <= 0x100 ? 1 : (dict_size <= 0x10000 ? 2 : 4);
  meta.value_width_ = width;

  std::map<std::string, uint32_t> value_to_index;
  for (uint32_t i = 0; i < dict_size; ++i) {
    value_to_index[dict[i]] = i;
  }

  meta.dict_offset_ = static_cast<uint32_t>(storage.size());
  meta.dict_entries_ = static_cast<uint32_t>(dict_size);
  std::vector<uint32_t> offsets(dict_size);
  std::vector<uint32_t> lengths(dict_size);
  std::vector<uint8_t> string_bytes;
  uint32_t string_offset = 0;
  for (uint32_t i = 0; i < dict_size; ++i) {
    offsets[i] = string_offset;
    lengths[i] = static_cast<uint32_t>(dict[i].size());
    AppendBytes(string_bytes, dict[i].data(), dict[i].size());
    string_offset += lengths[i];
  }

  AppendBytes(storage, offsets.data(), offsets.size() * sizeof(uint32_t));
  AppendBytes(storage, lengths.data(), lengths.size() * sizeof(uint32_t));
  meta.string_offset_ = static_cast<uint32_t>(storage.size());
  if (!string_bytes.empty()) {
    AppendBytes(storage, string_bytes.data(), string_bytes.size());
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  meta.data_bytes_ = row_count_ * width;
  storage.resize(storage.size() + meta.data_bytes_);
  for (uint32_t i = 0; i < row_count_; ++i) {
    uint32_t idx = 0;
    if (col.nulls_[i] == 0) {
      idx = value_to_index[col.strings_[i]];
    }
    std::memcpy(storage.data() + meta.data_offset_ + i * width, &idx, width);
  }
}

} // namespace leanstore::column_store
