#include "leanstore/base/error.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/base/small_vector.hpp"
#include "leanstore/btree/column_store/column_store.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/table/table_schema.hpp"

#include <algorithm>
#include <cstring>
#include <format>
#include <limits>
#include <map>
#include <memory>

namespace leanstore::column_store {

namespace {

Result<TableDefinition> BuildDefinitionFromMeta(const std::vector<ColumnMeta>& metas,
                                                const std::vector<uint16_t>& key_columns) {
  // Reconstruct a minimal schema so TableCodec can decode and re-encode rows.
  std::vector<ColumnDefinition> columns;
  columns.reserve(metas.size());
  for (size_t i = 0; i < metas.size(); ++i) {
    ColumnDefinition col;
    col.name_ = std::format("c{}", i);
    col.type_ = static_cast<ColumnType>(metas[i].type_);
    col.nullable_ = metas[i].nullable_ != 0;
    col.fixed_length_ = metas[i].fixed_length_;
    columns.emplace_back(std::move(col));
  }

  std::vector<uint32_t> key_cols;
  key_cols.reserve(key_columns.size());
  for (auto idx : key_columns) {
    key_cols.emplace_back(static_cast<uint32_t>(idx));
  }

  auto schema_res = TableSchema::Create(std::move(columns), std::move(key_cols));
  if (!schema_res) {
    return std::move(schema_res.error());
  }

  auto def_res = TableDefinition::Create(
      "column_block", std::move(schema_res.value()), lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
      lean_btree_config{.enable_wal_ = false, .use_bulk_insert_ = false});
  if (!def_res) {
    return std::move(def_res.error());
  }
  return def_res;
}

bool RangeWithinStorage(uint32_t offset, uint32_t bytes, size_t storage_size) {
  if (offset > storage_size) {
    return false;
  }
  if (bytes > storage_size - offset) {
    return false;
  }
  return true;
}

bool AddOverflow(size_t lhs, size_t rhs, size_t* out) {
  if (lhs > std::numeric_limits<size_t>::max() - rhs) {
    return true;
  }
  *out = lhs + rhs;
  return false;
}

bool MulOverflow(size_t lhs, size_t rhs, size_t* out) {
  if (lhs == 0 || rhs == 0) {
    *out = 0;
    return false;
  }
  if (lhs > std::numeric_limits<size_t>::max() / rhs) {
    return true;
  }
  *out = lhs * rhs;
  return false;
}

Result<void> ValidateColumnMeta(const ColumnBlockHeader& header, const ColumnMeta& meta,
                                const std::vector<uint8_t>& storage, uint32_t col_idx) {
  const size_t storage_size = storage.size();
  if (meta.nulls_bytes_ > 0 &&
      !RangeWithinStorage(meta.nulls_offset_, meta.nulls_bytes_, storage_size)) {
    return Error::General(std::format("null bitmap out of range for column {}", col_idx));
  }
  if (meta.data_bytes_ > 0 &&
      !RangeWithinStorage(meta.data_offset_, meta.data_bytes_, storage_size)) {
    return Error::General(std::format("data bytes out of range for column {}", col_idx));
  }

  const auto compression = static_cast<CompressionType>(meta.compression_);
  if (compression == CompressionType::kRaw || compression == CompressionType::kTruncation ||
      compression == CompressionType::kOrderedDictionary) {
    if (meta.value_width_ == 0) {
      return Error::General(std::format("value width is zero for column {}", col_idx));
    }
  }

  if ((compression == CompressionType::kRaw || compression == CompressionType::kTruncation) &&
      header.row_count_ > 0) {
    const uint64_t expected = static_cast<uint64_t>(header.row_count_) * meta.value_width_;
    if (meta.data_bytes_ != expected) {
      return Error::General(std::format("data bytes mismatch for column {}", col_idx));
    }
  }

  if (compression == CompressionType::kOrderedDictionary) {
    if (meta.dict_entries_ == 0) {
      return Error::General(std::format("empty dictionary for column {}", col_idx));
    }
    const uint64_t dict_bytes = static_cast<uint64_t>(meta.dict_entries_) * sizeof(uint32_t) * 2;
    if (meta.dict_offset_ > storage_size || dict_bytes > storage_size - meta.dict_offset_) {
      return Error::General(std::format("dictionary out of range for column {}", col_idx));
    }
    if (meta.string_offset_ > storage_size) {
      return Error::General(std::format("string offset out of range for column {}", col_idx));
    }

    const auto* dict_base = storage.data() + meta.dict_offset_;
    const auto* offsets = reinterpret_cast<const uint32_t*>(dict_base);
    const auto* lengths =
        reinterpret_cast<const uint32_t*>(dict_base + meta.dict_entries_ * sizeof(uint32_t));
    const uint64_t string_bytes = storage_size - meta.string_offset_;
    for (uint32_t i = 0; i < meta.dict_entries_; ++i) {
      const uint64_t off = offsets[i];
      const uint64_t len = lengths[i];
      if (off + len > string_bytes) {
        return Error::General(std::format("dictionary string out of range for column {}", col_idx));
      }
    }

    if (header.row_count_ > 0) {
      const uint64_t expected = static_cast<uint64_t>(header.row_count_) * meta.value_width_;
      if (meta.data_bytes_ != expected) {
        return Error::General(std::format("dictionary data bytes mismatch for column {}", col_idx));
      }
    }
  }

  return {};
}

int CompareBytesByLength(const char* lhs, size_t lhs_len, const char* rhs, size_t rhs_len) {
  // Match TableCodec key ordering: length prefix first, then byte comparison.
  if (lhs_len != rhs_len) {
    return lhs_len < rhs_len ? -1 : 1;
  }
  if (lhs_len == 0) {
    return 0;
  }
  return std::memcmp(lhs, rhs, lhs_len);
}

bool IsNullAt(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx) {
  // Nulls are stored as a packed bitmap.
  if (meta.nulls_bytes_ == 0) {
    return false;
  }
  const uint32_t byte_idx = row_idx / 8;
  const uint32_t bit_idx = row_idx % 8;
  if (byte_idx >= meta.nulls_bytes_) {
    return false;
  }
  const uint8_t* nulls = storage.data() + meta.nulls_offset_;
  return ((nulls[byte_idx] >> bit_idx) & 0x1u) != 0;
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

Result<int> CompareSingleValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                               const Datum& target, ColumnType type) {
  const uint8_t* data_base = storage.data() + meta.data_offset_;
  switch (type) {
  case ColumnType::kBool: {
    const uint8_t v = *data_base;
    const uint8_t t = target.b ? 1 : 0;
    return v < t ? -1 : (v > t ? 1 : 0);
  }
  case ColumnType::kInt32: {
    const int32_t v = static_cast<int32_t>(LoadSigned(data_base, meta.value_width_));
    if (v < target.i32) {
      return -1;
    }
    if (v > target.i32) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kInt64: {
    const int64_t v = LoadSigned(data_base, meta.value_width_);
    if (v < target.i64) {
      return -1;
    }
    if (v > target.i64) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kUInt64: {
    const uint64_t v = LoadUnsigned(data_base, meta.value_width_);
    if (v < target.u64) {
      return -1;
    }
    if (v > target.u64) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kFloat32: {
    float v = 0;
    std::memcpy(&v, data_base, sizeof(float));
    if (v < target.f32) {
      return -1;
    }
    if (v > target.f32) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kFloat64: {
    double v = 0;
    std::memcpy(&v, data_base, sizeof(double));
    if (v < target.f64) {
      return -1;
    }
    if (v > target.f64) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kBinary:
  case ColumnType::kString: {
    uint32_t len = 0;
    std::memcpy(&len, data_base, sizeof(uint32_t));
    const char* ptr = reinterpret_cast<const char*>(data_base + sizeof(uint32_t));
    return CompareBytesByLength(ptr, len, target.str.data, target.str.size);
  }
  default:
    return Error::General("unsupported type compare");
  }
}

Result<int> CompareTruncationValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                                   uint32_t row_idx, const Datum& target, ColumnType type) {
  const uint8_t* data_base = storage.data() + meta.data_offset_;
  const auto* delta_ptr = data_base + row_idx * meta.value_width_;
  switch (type) {
  case ColumnType::kBool:
  case ColumnType::kInt32:
  case ColumnType::kInt64: {
    const uint64_t delta_u = LoadUnsigned(delta_ptr, meta.value_width_);
    const int64_t value = meta.base_.i64_ + static_cast<int64_t>(delta_u);
    const int64_t target_val = (type == ColumnType::kInt32)
                                   ? target.i32
                                   : (type == ColumnType::kBool ? target.b : target.i64);
    if (value < target_val) {
      return -1;
    }
    if (value > target_val) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kUInt64: {
    const uint64_t delta = LoadUnsigned(delta_ptr, meta.value_width_);
    const uint64_t value = meta.base_.u64_ + delta;
    if (value < target.u64) {
      return -1;
    }
    if (value > target.u64) {
      return 1;
    }
    return 0;
  }
  default:
    return Error::General("unsupported truncation type compare");
  }
}

Result<int> CompareRawValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                            uint32_t row_idx, const Datum& target, ColumnType type) {
  const uint8_t* data_base = storage.data() + meta.data_offset_;
  switch (type) {
  case ColumnType::kBool: {
    const uint8_t v = *(data_base + row_idx * meta.value_width_);
    const uint8_t t = target.b ? 1 : 0;
    return v < t ? -1 : (v > t ? 1 : 0);
  }
  case ColumnType::kInt32: {
    const int32_t v = static_cast<int32_t>(
        LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_));
    if (v < target.i32) {
      return -1;
    }
    if (v > target.i32) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kInt64: {
    const int64_t v = LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_);
    if (v < target.i64) {
      return -1;
    }
    if (v > target.i64) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kUInt64: {
    const uint64_t v = LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_);
    if (v < target.u64) {
      return -1;
    }
    if (v > target.u64) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kFloat32: {
    float v = 0;
    std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(float));
    if (v < target.f32) {
      return -1;
    }
    if (v > target.f32) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kFloat64: {
    double v = 0;
    std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(double));
    if (v < target.f64) {
      return -1;
    }
    if (v > target.f64) {
      return 1;
    }
    return 0;
  }
  case ColumnType::kBinary:
  case ColumnType::kString: {
    if (meta.compression_ == static_cast<uint8_t>(CompressionType::kSingleValue)) {
      uint32_t len = 0;
      std::memcpy(&len, data_base, sizeof(uint32_t));
      const char* ptr = reinterpret_cast<const char*>(data_base + sizeof(uint32_t));
      return CompareBytesByLength(ptr, len, target.str.data, target.str.size);
    }
    const uint32_t idx = static_cast<uint32_t>(
        LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_));
    const uint8_t* dict_base = storage.data() + meta.dict_offset_;
    const uint32_t* offsets = reinterpret_cast<const uint32_t*>(dict_base);
    const uint32_t* lengths =
        reinterpret_cast<const uint32_t*>(dict_base + meta.dict_entries_ * sizeof(uint32_t));
    if (idx >= meta.dict_entries_) {
      return Error::General("dictionary index out of range");
    }
    const uint32_t off = offsets[idx];
    const uint32_t len = lengths[idx];
    const char* ptr = reinterpret_cast<const char*>(storage.data() + meta.string_offset_ + off);
    return CompareBytesByLength(ptr, len, target.str.data, target.str.size);
  }
  default:
    break;
  }
  return Error::General("unsupported type compare");
}

Result<void> DecodeSingleValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                               Datum& out, ColumnType type) {
  const uint8_t* data_base = storage.data() + meta.data_offset_;
  switch (type) {
  case ColumnType::kBool:
    out.b = (*data_base) != 0;
    return {};
  case ColumnType::kInt32:
    out.i32 = static_cast<int32_t>(LoadSigned(data_base, meta.value_width_));
    return {};
  case ColumnType::kInt64:
    out.i64 = LoadSigned(data_base, meta.value_width_);
    return {};
  case ColumnType::kUInt64:
    out.u64 = LoadUnsigned(data_base, meta.value_width_);
    return {};
  case ColumnType::kFloat32: {
    float v = 0;
    std::memcpy(&v, data_base, sizeof(float));
    out.f32 = v;
    return {};
  }
  case ColumnType::kFloat64: {
    double v = 0;
    std::memcpy(&v, data_base, sizeof(double));
    out.f64 = v;
    return {};
  }
  case ColumnType::kBinary:
  case ColumnType::kString: {
    uint32_t len = 0;
    std::memcpy(&len, data_base, sizeof(uint32_t));
    out.str.data = reinterpret_cast<const char*>(data_base + sizeof(uint32_t));
    out.str.size = len;
    return {};
  }
  default:
    return Error::General("unsupported single value decode");
  }
}

Result<void> DecodeTruncationValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                                   uint32_t row_idx, Datum& out, ColumnType type) {
  const uint8_t* data_base = storage.data() + meta.data_offset_;
  const auto* delta_ptr = data_base + row_idx * meta.value_width_;
  const uint64_t delta_u = LoadUnsigned(delta_ptr, meta.value_width_);
  const int64_t value = meta.base_.i64_ + static_cast<int64_t>(delta_u);
  switch (type) {
  case ColumnType::kBool:
    out.b = value != 0;
    return {};
  case ColumnType::kInt32:
    out.i32 = static_cast<int32_t>(value);
    return {};
  case ColumnType::kInt64:
    out.i64 = value;
    return {};
  case ColumnType::kUInt64: {
    out.u64 = meta.base_.u64_ + delta_u;
    return {};
  }
  default:
    return Error::General("invalid truncation type");
  }
}

Result<void> DecodeRawValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                            uint32_t row_idx, Datum& out, ColumnType type) {
  const uint8_t* data_base = storage.data() + meta.data_offset_;
  switch (type) {
  case ColumnType::kBool: {
    const uint8_t v = *(data_base + row_idx * meta.value_width_);
    out.b = v != 0;
    return {};
  }
  case ColumnType::kInt32: {
    const int32_t v = static_cast<int32_t>(
        LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_));
    out.i32 = v;
    return {};
  }
  case ColumnType::kInt64:
    out.i64 = LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_);
    return {};
  case ColumnType::kUInt64:
    out.u64 = LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_);
    return {};
  case ColumnType::kFloat32: {
    float v = 0;
    std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(float));
    out.f32 = v;
    return {};
  }
  case ColumnType::kFloat64: {
    double v = 0;
    std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(double));
    out.f64 = v;
    return {};
  }
  case ColumnType::kBinary:
  case ColumnType::kString: {
    if (meta.compression_ == static_cast<uint8_t>(CompressionType::kSingleValue)) {
      uint32_t len = 0;
      std::memcpy(&len, data_base, sizeof(uint32_t));
      out.str.data = reinterpret_cast<const char*>(data_base + sizeof(uint32_t));
      out.str.size = len;
      return {};
    }
    const uint32_t idx = static_cast<uint32_t>(
        LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_));
    const uint8_t* dict_base = storage.data() + meta.dict_offset_;
    const uint32_t* offsets = reinterpret_cast<const uint32_t*>(dict_base);
    const uint32_t* lengths =
        reinterpret_cast<const uint32_t*>(dict_base + meta.dict_entries_ * sizeof(uint32_t));
    if (idx >= meta.dict_entries_) {
      return Error::General("dictionary index out of range");
    }
    const uint32_t off = offsets[idx];
    const uint32_t len = lengths[idx];
    out.str.data = reinterpret_cast<const char*>(storage.data() + meta.string_offset_ + off);
    out.str.size = len;
    return {};
  }
  default:
    return Error::General("unsupported type decode");
  }
}

Result<int> CompareValue(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                         uint32_t row_idx, const Datum& target, ColumnType type) {
  // Nulls sort after non-null values for now.
  if (IsNullAt(meta, storage, row_idx)) {
    return 1;
  }

  const auto compression = static_cast<CompressionType>(meta.compression_);
  if (compression == CompressionType::kSingleValue) {
    return CompareSingleValue(meta, storage, target, type);
  }
  if (compression == CompressionType::kTruncation) {
    return CompareTruncationValue(meta, storage, row_idx, target, type);
  }
  return CompareRawValue(meta, storage, row_idx, target, type);
}

Result<void> DecodeValueAt(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                           uint32_t row_idx, Datum& out, ColumnType type) {
  // Decode a single column value for row_idx.
  if (IsNullAt(meta, storage, row_idx)) {
    return {};
  }

  const auto compression = static_cast<CompressionType>(meta.compression_);
  if (compression == CompressionType::kSingleValue) {
    return DecodeSingleValue(meta, storage, out, type);
  }
  if (compression == CompressionType::kTruncation) {
    return DecodeTruncationValue(meta, storage, row_idx, out, type);
  }
  return DecodeRawValue(meta, storage, row_idx, out, type);
}

} // namespace

ColumnBlockReader::ColumnBlockReader(ColumnBlockHeader header, std::vector<ColumnMeta> metas,
                                     std::vector<uint16_t> key_columns,
                                     std::vector<uint8_t> storage, TableDefinition definition)
    : header_(header),
      metas_(std::move(metas)),
      key_columns_(std::move(key_columns)),
      storage_(std::move(storage)),
      definition_(std::move(definition)) {
  codec_ = std::make_unique<TableCodec>(definition_);
}

ColumnBlockReader::ColumnBlockReader(ColumnBlockReader&& other) noexcept
    : header_(other.header_),
      metas_(std::move(other.metas_)),
      key_columns_(std::move(other.key_columns_)),
      storage_(std::move(other.storage_)),
      definition_(std::move(other.definition_)) {
  codec_ = std::make_unique<TableCodec>(definition_);
}

ColumnBlockReader& ColumnBlockReader::operator=(ColumnBlockReader&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  header_ = other.header_;
  metas_ = std::move(other.metas_);
  key_columns_ = std::move(other.key_columns_);
  storage_ = std::move(other.storage_);
  definition_ = std::move(other.definition_);
  codec_ = std::make_unique<TableCodec>(definition_);
  return *this;
}

Result<ColumnBlockReader> ColumnBlockReader::FromBlockBytes(std::vector<uint8_t> bytes) {
  // Parse header, column metas, key column list, then keep the remaining bytes as storage.
  if (bytes.size() < sizeof(ColumnBlockHeader)) {
    return Error::General("column block too small");
  }
  ColumnBlockHeader header{};
  std::memcpy(&header, bytes.data(), sizeof(ColumnBlockHeader));
  if (header.magic_ != kColumnBlockMagic || header.version_ != kColumnBlockVersion) {
    return Error::General("column block header mismatch");
  }
  if (header.column_count_ == 0) {
    return Error::General("column block has no columns");
  }
  if (header.key_column_count_ > header.column_count_) {
    return Error::General("column block key column count invalid");
  }
  size_t meta_bytes = 0;
  if (MulOverflow(sizeof(ColumnMeta), header.column_count_, &meta_bytes)) {
    return Error::General("column block meta bytes overflow");
  }
  size_t key_bytes = 0;
  if (MulOverflow(sizeof(uint16_t), header.key_column_count_, &key_bytes)) {
    return Error::General("column block key bytes overflow");
  }
  const size_t meta_offset = sizeof(ColumnBlockHeader);
  size_t key_offset = 0;
  if (AddOverflow(meta_offset, meta_bytes, &key_offset)) {
    return Error::General("column block key offset overflow");
  }
  size_t storage_offset = 0;
  if (AddOverflow(key_offset, key_bytes, &storage_offset)) {
    return Error::General("column block storage offset overflow");
  }
  if (bytes.size() < storage_offset) {
    return Error::General("column block truncated");
  }

  std::vector<ColumnMeta> metas(header.column_count_);
  std::memcpy(metas.data(), bytes.data() + meta_offset, meta_bytes);

  std::vector<uint16_t> key_columns(header.key_column_count_);
  if (key_bytes > 0) {
    std::memcpy(key_columns.data(), bytes.data() + key_offset, key_bytes);
  }
  for (size_t i = 0; i < key_columns.size(); ++i) {
    if (key_columns[i] >= header.column_count_) {
      return Error::General("column block key column index out of range");
    }
  }

  std::vector<uint8_t> storage(bytes.begin() + static_cast<size_t>(storage_offset), bytes.end());
  for (uint32_t i = 0; i < header.column_count_; ++i) {
    if (auto res = ValidateColumnMeta(header, metas[i], storage, i); !res) {
      return std::move(res.error());
    }
  }

  auto def_res = BuildDefinitionFromMeta(metas, key_columns);
  if (!def_res) {
    return std::move(def_res.error());
  }
  TableDefinition definition = std::move(def_res.value());
  return ColumnBlockReader(header, std::move(metas), std::move(key_columns), std::move(storage),
                           std::move(definition));
}

Result<void> ColumnBlockReader::DecodeKey(Slice key_bytes, std::vector<Datum>& key_datums) const {
  key_datums.clear();
  key_datums.resize(key_columns_.size());
  const auto col_count = definition_.schema_.Columns().size();
  std::vector<Datum> cols(col_count);
  auto nulls = std::make_unique<bool[]>(col_count);
  std::fill_n(nulls.get(), col_count, true);
  lean_row key_row{.columns = cols.data(),
                   .nulls = nulls.get(),
                   .num_columns = static_cast<uint32_t>(cols.size())};
  if (auto res = codec_->DecodeKey(key_bytes, &key_row); !res) {
    return std::move(res.error());
  }
  for (size_t i = 0; i < key_columns_.size(); ++i) {
    key_datums[i] = cols[key_columns_[i]];
  }
  return {};
}

Result<int> ColumnBlockReader::CompareRowKey(uint32_t row_idx,
                                             const std::vector<Datum>& key_datums) const {
  if (key_datums.size() != key_columns_.size()) {
    return Error::General("key datum size mismatch");
  }
  for (size_t i = 0; i < key_columns_.size(); ++i) {
    const uint16_t col_idx = key_columns_[i];
    const auto& meta = metas_[col_idx];
    const auto type = static_cast<ColumnType>(meta.type_);
    auto cmp = CompareValue(meta, storage_, row_idx, key_datums[i], type);
    if (!cmp) {
      return std::move(cmp.error());
    }
    if (cmp.value() != 0) {
      return cmp.value();
    }
  }
  return 0;
}

Result<EncodedRow> ColumnBlockReader::EncodeRow(uint32_t row_idx) const {
  const auto& cols = definition_.schema_.Columns();
  std::vector<Datum> datums(cols.size());
  auto nulls = std::make_unique<bool[]>(cols.size());
  std::fill_n(nulls.get(), cols.size(), false);
  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& meta = metas_[i];
    const auto type = static_cast<ColumnType>(meta.type_);
    if (IsNullAt(meta, storage_, row_idx)) {
      nulls[i] = true;
      continue;
    }
    if (auto res = DecodeValueAt(meta, storage_, row_idx, datums[i], type); !res) {
      return std::move(res.error());
    }
  }

  lean_row row{.columns = datums.data(),
               .nulls = nulls.get(),
               .num_columns = static_cast<uint32_t>(datums.size())};
  auto encoded = codec_->Encode(&row);
  if (!encoded) {
    return std::move(encoded.error());
  }
  return encoded.value();
}

Result<ColumnBlockReader> ReadColumnBlock(LeanStore* store, const ColumnBlockRef& ref) {
  if (ref.magic_ != kColumnBlockRefMagic || ref.version_ != kColumnBlockVersion) {
    return Error::General("column block ref mismatch");
  }
  std::vector<uint8_t> block_bytes(ref.block_bytes_, 0);
  // Column blocks span multiple fixed-size pages linked by next_page_id.
  const auto page_size = store->store_option_->page_size_;
  if (page_size <= sizeof(Page) + sizeof(ColumnPageHeader)) {
    return Error::General("column page size too small");
  }
  const uint32_t payload_capacity =
      static_cast<uint32_t>(page_size - sizeof(Page) - sizeof(ColumnPageHeader));
  SmallBuffer512Aligned<4096> page_buf(page_size);
  lean_pid_t page_id = ref.first_page_id_;
  uint32_t pages_read = 0;
  while (page_id != 0 && pages_read < ref.page_count_) {
    store->buffer_manager_->ReadPageSync(page_id, page_buf.Data());
    auto* page = reinterpret_cast<const Page*>(page_buf.Data());
    if (!IsColumnPage(*page)) {
      return Error::General("invalid column page magic");
    }
    auto* header = reinterpret_cast<const ColumnPageHeader*>(page->payload_);
    if (header->magic_ != kColumnPageHeaderMagic) {
      return Error::General("column page header mismatch");
    }
    if (header->payload_bytes_ > payload_capacity) {
      return Error::General("column page payload too large");
    }
    // Scatter-gather the payload into the logical block buffer.
    if (header->block_offset_ + header->payload_bytes_ > block_bytes.size()) {
      return Error::General("column page payload out of range");
    }
    const uint8_t* payload = page->payload_ + sizeof(ColumnPageHeader);
    std::memcpy(block_bytes.data() + header->block_offset_, payload, header->payload_bytes_);
    page_id = header->next_page_id_;
    pages_read++;
  }
  if (pages_read != ref.page_count_) {
    return Error::General("column page chain incomplete");
  }
  return ColumnBlockReader::FromBlockBytes(std::move(block_bytes));
}

} // namespace leanstore::column_store
