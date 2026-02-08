#include "leanstore/btree/column_store/column_block_builder.hpp"

#include "leanstore/base/error.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <numeric>
#include <string_view>
#include <unordered_map>
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
  const uint32_t reserve_rows = options_.max_rows_per_block_;
  const auto& cols = def.schema_.Columns();
  columns_.reserve(cols.size());
  for (const auto& col : cols) {
    ColumnValues values;
    values.type_ = col.type_;
    values.nullable_ = col.nullable_;
    values.fixed_length_ = col.fixed_length_;
    if (reserve_rows > 0) {
      values.nulls_.reserve(reserve_rows);
      switch (col.type_) {
      case ColumnType::kBool:
      case ColumnType::kInt32:
      case ColumnType::kInt64:
        values.ints_.reserve(reserve_rows);
        break;
      case ColumnType::kUInt64:
        values.uints_.reserve(reserve_rows);
        break;
      case ColumnType::kFloat32:
      case ColumnType::kFloat64:
        values.doubles_.reserve(reserve_rows);
        break;
      case ColumnType::kBinary:
      case ColumnType::kString:
        values.string_offsets_.reserve(reserve_rows);
        values.string_lengths_.reserve(reserve_rows);
        values.string_data_.reserve(static_cast<size_t>(reserve_rows) *
                                    (col.fixed_length_ > 0 ? col.fixed_length_ : 32));
        break;
      default:
        break;
      }
    }
    columns_.push_back(std::move(values));
  }
}

void ColumnBlockBuilder::AddRow(const lean_row& row) {
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
      const uint32_t offset = static_cast<uint32_t>(col.string_data_.size());
      uint32_t len = 0;
      if (datum.str.size > 0 && datum.str.data != nullptr) {
        len = static_cast<uint32_t>(datum.str.size);
        const auto* src = reinterpret_cast<const uint8_t*>(datum.str.data);
        col.string_data_.insert(col.string_data_.end(), src, src + len);
      }
      approx_bytes_ += len;
      col.string_offsets_.push_back(offset);
      col.string_lengths_.push_back(len);
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

Result<ColumnBlockPayload> ColumnBlockBuilder::Finalize(Slice max_key) {
  if (row_count_ == 0) {
    return Error::General("empty column block");
  }

  // Encode per-column data and assemble the column block payload.
  std::vector<ColumnMeta> metas(columns_.size());
  std::vector<uint8_t> storage;
  storage.reserve(static_cast<size_t>(approx_bytes_) + columns_.size() * sizeof(ColumnMeta));

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
  block_bytes.reserve(sizeof(ColumnBlockHeader) + metas.size() * sizeof(ColumnMeta) +
                      def_.schema_.PrimaryKeyColumns().size() * sizeof(uint16_t) + storage.size());
  AppendPod(block_bytes, header);
  AppendBytes(block_bytes, metas.data(), metas.size() * sizeof(ColumnMeta));
  for (auto idx : def_.schema_.PrimaryKeyColumns()) {
    const uint16_t key_idx = static_cast<uint16_t>(idx);
    AppendPod(block_bytes, key_idx);
  }
  AppendBytes(block_bytes, storage.data(), storage.size());

  ColumnBlockPayload payload;
  payload.bytes_ = std::move(block_bytes);
  payload.max_key_.assign(reinterpret_cast<const char*>(max_key.data()), max_key.size());
  payload.row_count_ = row_count_;
  Reset();
  return payload;
}

void ColumnBlockBuilder::Reset() {
  row_count_ = 0;
  approx_bytes_ = 0;
  for (auto& col : columns_) {
    col.nulls_.clear();
    col.ints_.clear();
    col.uints_.clear();
    col.doubles_.clear();
    col.string_offsets_.clear();
    col.string_lengths_.clear();
    col.string_data_.clear();
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
    col.string_offsets_.push_back(static_cast<uint32_t>(col.string_data_.size()));
    col.string_lengths_.push_back(0);
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
    storage.resize(storage.size() + meta.nulls_bytes_, 0);
    auto* null_bytes = storage.data() + meta.nulls_offset_;
    for (uint32_t row = 0; row < row_count_; ++row) {
      if (col.nulls_[row] != 0) {
        null_bytes[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
      }
    }
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
  const auto get_string = [&](uint32_t row_idx) -> std::string_view {
    const uint32_t len = col.string_lengths_[row_idx];
    if (len == 0) {
      return {};
    }
    const uint32_t offset = col.string_offsets_[row_idx];
    return {reinterpret_cast<const char*>(col.string_data_.data() + offset), len};
  };

  bool has_value = false;
  bool all_equal = true;
  uint32_t first_length = 0;
  std::string_view first_value;
  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] != 0) {
      continue;
    }
    const std::string_view v = get_string(i);
    if (!has_value) {
      first_value = v;
      first_length = static_cast<uint32_t>(v.size());
      has_value = true;
    } else {
      if (v != first_value) {
        all_equal = false;
      }
    }
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  if (!has_value || all_equal) {
    meta.compression_ = static_cast<uint8_t>(CompressionType::kSingleValue);
    const uint32_t len = has_value ? first_length : 0;
    AppendBytes(storage, &len, sizeof(uint32_t));
    if (len > 0) {
      AppendBytes(storage, first_value.data(), first_value.size());
    }
    meta.data_bytes_ = sizeof(uint32_t) + len;
    meta.value_width_ = 0;
    return;
  }

  meta.compression_ = static_cast<uint8_t>(CompressionType::kOrderedDictionary);
  std::vector<uint32_t> row_dict_idx(row_count_, 0);
  std::vector<std::string_view> unique_values;
  unique_values.reserve(row_count_);
  std::unordered_map<std::string_view, uint32_t> value_to_unsorted_idx;
  value_to_unsorted_idx.reserve(static_cast<size_t>(row_count_) * 2);
  for (uint32_t i = 0; i < row_count_; ++i) {
    if (col.nulls_[i] != 0) {
      continue;
    }
    const std::string_view value = get_string(i);
    auto [it, inserted] =
        value_to_unsorted_idx.try_emplace(value, static_cast<uint32_t>(unique_values.size()));
    if (inserted) {
      unique_values.push_back(value);
    }
    row_dict_idx[i] = it->second;
  }

  const uint32_t dict_size = static_cast<uint32_t>(unique_values.size());
  std::vector<uint32_t> sorted_unique(dict_size);
  std::iota(sorted_unique.begin(), sorted_unique.end(), 0);
  std::sort(sorted_unique.begin(), sorted_unique.end(),
            [&](uint32_t lhs, uint32_t rhs) { return unique_values[lhs] < unique_values[rhs]; });

  std::vector<uint32_t> unsorted_to_sorted(dict_size);
  for (uint32_t sorted_idx = 0; sorted_idx < dict_size; ++sorted_idx) {
    unsorted_to_sorted[sorted_unique[sorted_idx]] = sorted_idx;
  }

  const uint8_t width = dict_size <= 0x100 ? 1 : (dict_size <= 0x10000 ? 2 : 4);
  meta.value_width_ = width;

  meta.dict_offset_ = static_cast<uint32_t>(storage.size());
  meta.dict_entries_ = static_cast<uint32_t>(dict_size);
  std::vector<uint32_t> offsets(dict_size);
  std::vector<uint32_t> lengths(dict_size);
  uint32_t string_offset = 0;
  size_t dict_string_bytes = 0;
  for (uint32_t i = 0; i < dict_size; ++i) {
    const auto entry = unique_values[sorted_unique[i]];
    offsets[i] = string_offset;
    lengths[i] = static_cast<uint32_t>(entry.size());
    string_offset += lengths[i];
    dict_string_bytes += entry.size();
  }

  AppendBytes(storage, offsets.data(), offsets.size() * sizeof(uint32_t));
  AppendBytes(storage, lengths.data(), lengths.size() * sizeof(uint32_t));
  meta.string_offset_ = static_cast<uint32_t>(storage.size());
  if (dict_string_bytes > 0) {
    const size_t string_base = storage.size();
    storage.resize(storage.size() + dict_string_bytes);
    size_t write_offset = 0;
    for (uint32_t i = 0; i < dict_size; ++i) {
      const auto entry = unique_values[sorted_unique[i]];
      if (!entry.empty()) {
        std::memcpy(storage.data() + string_base + write_offset, entry.data(), entry.size());
        write_offset += entry.size();
      }
    }
  }

  meta.data_offset_ = static_cast<uint32_t>(storage.size());
  meta.data_bytes_ = row_count_ * width;
  storage.resize(storage.size() + meta.data_bytes_);
  for (uint32_t i = 0; i < row_count_; ++i) {
    uint32_t idx = 0;
    if (col.nulls_[i] == 0) {
      idx = unsorted_to_sorted[row_dict_idx[i]];
    }
    std::memcpy(storage.data() + meta.data_offset_ + i * width, &idx, width);
  }
}

} // namespace leanstore::column_store
