#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/c/portable.h"
#include "leanstore/c/types.h"
#include "leanstore/table/encoding.hpp"

#include <cstdint>
#include <vector>

namespace leanstore {

class LeanStore;

namespace column_store {

class ColumnCompressionAlgorithm;

constexpr uint64_t kColumnLeafMagic = 0x434F4C4C45414631ULL; // "COLLEAF1"
constexpr uint64_t kColumnPageMagic = 0x434F4C5041474531ULL; // "COLPAGE1"
constexpr uint32_t kColumnPageHeaderMagic = 0x434F4C50;      // "COLP"
constexpr uint32_t kColumnBlockMagic = 0x434F4C42;           // "COLB"
constexpr uint32_t kColumnBlockRefMagic = 0x434F4C52;        // "COLR"
constexpr uint16_t kColumnBlockVersion = 1;

enum class CompressionType : uint8_t {
  kSingleValue = 0,
  kTruncation = 1,
  kOrderedDictionary = 2,
  kRaw = 3,
};

struct PACKED ColumnBlockHeader {
  // Serialized header for a column block payload stored across pages.
  uint32_t magic_;
  uint16_t version_;
  uint16_t column_count_;
  uint32_t row_count_;
  // Number of leading key columns used for block ordering and lookup.
  uint16_t key_column_count_;
  uint16_t reserved_;
};

struct PACKED ColumnMeta {
  // Per-column metadata used to decode compressed column values.
  uint8_t type_;
  uint8_t compression_;
  uint8_t nullable_;
  uint8_t value_width_;
  uint32_t fixed_length_;
  // Base value for truncation compression.
  union {
    int64_t i64_;
    uint64_t u64_;
  } base_;
  // Null bitmap location inside the block payload.
  uint32_t nulls_offset_;
  uint32_t nulls_bytes_;
  // Encoded column data location inside the block payload.
  uint32_t data_offset_;
  uint32_t data_bytes_;
  // Dictionary section for ordered dictionary compression.
  uint32_t dict_offset_;
  uint32_t dict_entries_;
  // Concatenated dictionary strings.
  uint32_t string_offset_;
};

struct PACKED ColumnBlockRef {
  uint32_t magic_;
  uint16_t version_;
  uint16_t reserved_;
  // The block payload is stored as a linked list of column pages.
  lean_pid_t first_page_id_;
  uint32_t page_count_;
  uint32_t block_bytes_;
  uint32_t row_count_;
};

struct PACKED ColumnPageHeader {
  uint32_t magic_;
  // Number of payload bytes stored in this page.
  uint32_t payload_bytes_;
  // Next page in the column block chain.
  lean_pid_t next_page_id_;
  // Offset into the logical column block payload.
  uint32_t block_offset_;
};

struct ColumnStoreOptions {
  // Split blocks based on row count/byte budget. Zero disables the limit.
  uint32_t max_rows_per_block_ = 0;
  uint64_t max_block_bytes_ = 0;
  // Target subtree height to convert. Zero keeps the current tree height.
  uint64_t target_height_ = 0;
};

struct ColumnStoreStats {
  // Counts only the generated column blocks/pages, not row-store pages.
  uint64_t row_count_ = 0;
  uint64_t block_count_ = 0;
  uint64_t column_pages_ = 0;
  uint64_t column_bytes_ = 0;
};

inline bool IsColumnLeaf(const BufferFrame& bf) {
  return bf.page_.magic_debugging_ == kColumnLeafMagic;
}

inline bool IsColumnPage(const Page& page) {
  return page.magic_debugging_ == kColumnPageMagic;
}

class ColumnBlockReader {
public:
  // Builds a self-contained reader from serialized column block bytes.
  static Result<ColumnBlockReader> FromBlockBytes(std::vector<uint8_t> bytes);

  ColumnBlockReader(ColumnBlockReader&& other) noexcept;
  ColumnBlockReader& operator=(ColumnBlockReader&& other) noexcept;
  ColumnBlockReader(const ColumnBlockReader&) = delete;
  ColumnBlockReader& operator=(const ColumnBlockReader&) = delete;

  Result<void> DecodeKey(Slice key_bytes, std::vector<Datum>& key_datums) const;
  // Compares a row's key columns against the given key datums.
  Result<int> CompareRowKey(uint32_t row_idx, const std::vector<Datum>& key_datums) const;
  // Encodes only value bytes in row-store layout.
  Result<std::string> EncodeValue(uint32_t row_idx) const;
  Result<void> EncodeValue(uint32_t row_idx, Datum* datums, bool* nulls, uint32_t column_capacity,
                           std::string* out_value) const;
  // Re-encodes a row back to the row-store key/value layout.
  Result<EncodedRow> EncodeRow(uint32_t row_idx) const;
  Result<void> EncodeRow(uint32_t row_idx, Datum* datums, bool* nulls, uint32_t column_capacity,
                         EncodedRow* out_row) const;

  uint32_t RowCount() const {
    return header_.row_count_;
  }

  uint32_t ColumnCount() const {
    return static_cast<uint32_t>(metas_.size());
  }

  const std::vector<uint16_t>& KeyColumns() const {
    return key_columns_;
  }

private:
  ColumnBlockReader(ColumnBlockHeader header, std::vector<ColumnMeta> metas,
                    std::vector<uint16_t> key_columns, std::vector<uint8_t> storage,
                    std::vector<const ColumnCompressionAlgorithm*> compression_algorithms,
                    RowEncodingLayout layout);
  Result<void> DecodeRow(uint32_t row_idx, Datum* datums, bool* nulls,
                         uint32_t column_capacity) const;

  ColumnBlockHeader header_{};
  std::vector<ColumnMeta> metas_;
  std::vector<uint16_t> key_columns_;
  std::vector<uint8_t> storage_;
  std::vector<const ColumnCompressionAlgorithm*> compression_algorithms_;
  RowEncodingLayout layout_;
};

// Reads a linked column page chain into a single in-memory block.
Result<ColumnBlockReader> ReadColumnBlock(LeanStore* store, const ColumnBlockRef& ref);
// Writes a block payload into a linked list of fixed-size pages.
Result<ColumnBlockRef> WriteColumnBlock(LeanStore* store, lean_treeid_t tree_id,
                                        const std::vector<uint8_t>& block_bytes,
                                        uint32_t row_count);

} // namespace column_store
} // namespace leanstore
