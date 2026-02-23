#include "leanstore/btree/column_store/column_block_builder.hpp"

#include "leanstore/base/error.hpp"
#include "leanstore/btree/column_store/column_compression.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

namespace leanstore::column_store {

namespace {

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
  meta.value_width_ = 0;
  meta.data_offset_ = 0;
  meta.data_bytes_ = 0;
  meta.dict_offset_ = 0;
  meta.dict_entries_ = 0;
  meta.string_offset_ = 0;

  const bool has_nulls =
      std::any_of(col.nulls_.begin(), col.nulls_.end(), [](uint8_t v) { return v != 0; });
  if (has_nulls) {
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

  ColumnCompressionInput input{
      .type_ = col.type_,
      .row_count_ = row_count_,
      .nulls_ = col.nulls_,
      .ints_ = col.ints_,
      .uints_ = col.uints_,
      .doubles_ = col.doubles_,
      .string_offsets_ = col.string_offsets_,
      .string_lengths_ = col.string_lengths_,
      .string_data_ = col.string_data_,
  };
  return EncodeColumnWithCompression(input, meta, storage);
}

} // namespace leanstore::column_store
