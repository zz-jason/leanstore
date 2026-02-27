#include "leanstore/base/error.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/base/small_vector.hpp"
#include "leanstore/btree/column_store/column_compression.hpp"
#include "leanstore/btree/column_store/column_store.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/lean_store.hpp"

#include <algorithm>
#include <cstring>
#include <format>
#include <limits>

namespace leanstore::column_store {

namespace {

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
                                const std::vector<uint8_t>& storage, uint32_t col_idx,
                                const ColumnCompressionAlgorithm** out_algorithm) {
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
  const auto* algorithm = FindCompressionAlgorithm(compression);
  if (algorithm == nullptr) {
    return Error::General(
        std::format("unknown compression {} for column {}", meta.compression_, col_idx));
  }

  const auto type = static_cast<ColumnType>(meta.type_);
  if (!algorithm->SupportsType(type)) {
    return Error::General(std::format("compression/type mismatch for column {}", col_idx));
  }

  if (out_algorithm != nullptr) {
    // Resolve once at load time so row-wise compare/decode avoids repeated registry lookups.
    *out_algorithm = algorithm;
  }
  return algorithm->Validate(header, meta, storage, col_idx);
}

RowEncodingLayout BuildLayoutFromMeta(const std::vector<ColumnMeta>& metas,
                                      const std::vector<uint16_t>& key_columns) {
  std::vector<CodecColumnDesc> columns;
  columns.reserve(metas.size());
  for (const auto& meta : metas) {
    columns.push_back(
        {.type_ = static_cast<ColumnType>(meta.type_), .nullable_ = meta.nullable_ != 0});
  }
  std::vector<uint32_t> layout_key_columns;
  layout_key_columns.reserve(key_columns.size());
  for (const auto key_col : key_columns) {
    layout_key_columns.push_back(static_cast<uint32_t>(key_col));
  }
  return RowEncodingLayout(std::move(columns), std::move(layout_key_columns));
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

Result<int> CompareValue(const ColumnMeta& meta, const ColumnCompressionAlgorithm* algorithm,
                         const std::vector<uint8_t>& storage, uint32_t row_idx, const Datum& target,
                         ColumnType type) {
  // Nulls sort after non-null values for now.
  if (IsNullAt(meta, storage, row_idx)) {
    return 1;
  }
  if (algorithm == nullptr) {
    return Error::General("unknown compression for compare");
  }
  return algorithm->Compare(meta, storage, row_idx, target, type);
}

Result<void> DecodeNonNullValueAt(const ColumnMeta& meta,
                                  const ColumnCompressionAlgorithm* algorithm,
                                  const std::vector<uint8_t>& storage, uint32_t row_idx, Datum& out,
                                  ColumnType type) {
  if (algorithm == nullptr) {
    return Error::General("unknown compression for decode");
  }
  return algorithm->Decode(meta, storage, row_idx, out, type);
}

} // namespace

ColumnBlockReader::ColumnBlockReader(
    ColumnBlockHeader header, std::vector<ColumnMeta> metas, std::vector<uint16_t> key_columns,
    std::vector<uint8_t> storage,
    std::vector<const ColumnCompressionAlgorithm*> compression_algorithms, RowEncodingLayout layout)
    : header_(header),
      metas_(std::move(metas)),
      key_columns_(std::move(key_columns)),
      storage_(std::move(storage)),
      compression_algorithms_(std::move(compression_algorithms)),
      layout_(std::move(layout)) {
}

ColumnBlockReader::ColumnBlockReader(ColumnBlockReader&& other) noexcept
    : header_(other.header_),
      metas_(std::move(other.metas_)),
      key_columns_(std::move(other.key_columns_)),
      storage_(std::move(other.storage_)),
      compression_algorithms_(std::move(other.compression_algorithms_)),
      layout_(std::move(other.layout_)) {
}

ColumnBlockReader& ColumnBlockReader::operator=(ColumnBlockReader&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  header_ = other.header_;
  metas_ = std::move(other.metas_);
  key_columns_ = std::move(other.key_columns_);
  storage_ = std::move(other.storage_);
  compression_algorithms_ = std::move(other.compression_algorithms_);
  layout_ = std::move(other.layout_);
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
  // Cache the resolved algorithm per column for hot read paths (binary search + scan loops).
  std::vector<const ColumnCompressionAlgorithm*> compression_algorithms(header.column_count_,
                                                                        nullptr);
  for (uint32_t i = 0; i < header.column_count_; ++i) {
    if (auto res = ValidateColumnMeta(header, metas[i], storage, i, &compression_algorithms[i]);
        !res) {
      return std::move(res.error());
    }
  }

  auto layout = BuildLayoutFromMeta(metas, key_columns);
  return ColumnBlockReader(header, std::move(metas), std::move(key_columns), std::move(storage),
                           std::move(compression_algorithms), std::move(layout));
}

Result<void> ColumnBlockReader::DecodeKey(Slice key_bytes, std::vector<Datum>& key_datums) const {
  return DecodeKeyDatumsWithLayout(layout_, key_bytes, key_datums);
}

Result<int> ColumnBlockReader::CompareRowKey(uint32_t row_idx,
                                             const std::vector<Datum>& key_datums) const {
  if (row_idx >= RowCount()) {
    return Error::General("row index out of range");
  }
  if (key_datums.size() != key_columns_.size()) {
    return Error::General("key datum size mismatch");
  }
  for (size_t i = 0; i < key_columns_.size(); ++i) {
    const uint16_t col_idx = key_columns_[i];
    const auto& meta = metas_[col_idx];
    const auto type = static_cast<ColumnType>(meta.type_);
    auto cmp = CompareValue(meta, compression_algorithms_[col_idx], storage_, row_idx,
                            key_datums[i], type);
    if (!cmp) {
      return std::move(cmp.error());
    }
    if (cmp.value() != 0) {
      return cmp.value();
    }
  }
  return 0;
}

Result<void> ColumnBlockReader::DecodeRow(uint32_t row_idx, Datum* datums, bool* nulls,
                                          uint32_t column_capacity) const {
  if (datums == nullptr || nulls == nullptr) {
    return Error::General("null row scratch buffers");
  }
  if (row_idx >= RowCount()) {
    return Error::General("row index out of range");
  }
  if (column_capacity < metas_.size()) {
    return Error::General("row scratch capacity too small");
  }
  for (size_t i = 0; i < metas_.size(); ++i) {
    const auto& meta = metas_[i];
    const auto type = static_cast<ColumnType>(meta.type_);
    if (IsNullAt(meta, storage_, row_idx)) {
      nulls[i] = true;
      continue;
    }
    nulls[i] = false;
    if (auto res = DecodeNonNullValueAt(meta, compression_algorithms_[i], storage_, row_idx,
                                        datums[i], type);
        !res) {
      return std::move(res.error());
    }
  }
  return {};
}

Result<std::string> ColumnBlockReader::EncodeValue(uint32_t row_idx) const {
  const uint32_t column_count = static_cast<uint32_t>(layout_.Columns().size());
  std::vector<Datum> datums(column_count);
  auto nulls = std::make_unique<bool[]>(column_count);
  std::string encoded;
  if (auto res = EncodeValue(row_idx, datums.data(), nulls.get(), column_count, &encoded); !res) {
    return std::move(res.error());
  }
  return encoded;
}

Result<void> ColumnBlockReader::EncodeValue(uint32_t row_idx, Datum* datums, bool* nulls,
                                            uint32_t column_capacity,
                                            std::string* out_value) const {
  if (out_value == nullptr) {
    return Error::General("null encoded value output");
  }
  if (auto res = DecodeRow(row_idx, datums, nulls, column_capacity); !res) {
    return std::move(res.error());
  }
  lean_row row{
      .columns = datums, .nulls = nulls, .num_columns = static_cast<uint32_t>(metas_.size())};
  return EncodeValueWithLayout(layout_, &row, *out_value);
}

Result<EncodedRow> ColumnBlockReader::EncodeRow(uint32_t row_idx) const {
  const uint32_t column_count = static_cast<uint32_t>(layout_.Columns().size());
  std::vector<Datum> datums(column_count);
  auto nulls = std::make_unique<bool[]>(column_count);
  EncodedRow encoded;
  if (auto res = EncodeRow(row_idx, datums.data(), nulls.get(), column_count, &encoded); !res) {
    return std::move(res.error());
  }
  return encoded;
}

Result<void> ColumnBlockReader::EncodeRow(uint32_t row_idx, Datum* datums, bool* nulls,
                                          uint32_t column_capacity, EncodedRow* out_row) const {
  if (out_row == nullptr) {
    return Error::General("null encoded row output");
  }
  if (auto res = DecodeRow(row_idx, datums, nulls, column_capacity); !res) {
    return std::move(res.error());
  }
  lean_row row{
      .columns = datums, .nulls = nulls, .num_columns = static_cast<uint32_t>(metas_.size())};
  if (auto res = EncodeKeyWithLayout(layout_, &row, out_row->key_); !res) {
    return std::move(res.error());
  }
  return EncodeValueWithLayout(layout_, &row, out_row->value_);
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
