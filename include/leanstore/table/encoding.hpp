#pragma once

#include "leanstore/base/slice.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/table/table_schema.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace leanstore {

/// Datum representation reused from the C API.
using Datum = lean_datum;

struct EncodedRow {
  std::string key_;
  std::string value_;
};

struct CodecColumnDesc {
  ColumnType type_;
  bool nullable_;
};

class RowEncodingLayout {
public:
  RowEncodingLayout() = default;
  RowEncodingLayout(std::vector<CodecColumnDesc> columns, std::vector<uint32_t> key_columns);

  const std::vector<CodecColumnDesc>& Columns() const {
    return columns_;
  }

  const std::vector<uint32_t>& KeyColumns() const {
    return key_columns_;
  }

  const std::vector<uint8_t>& ValueFieldWidths() const {
    return value_field_widths_;
  }

  const std::vector<uint8_t>& IsVarlen() const {
    return is_varlen_;
  }

private:
  std::vector<CodecColumnDesc> columns_;
  std::vector<uint32_t> key_columns_;
  std::vector<uint8_t> value_field_widths_;
  std::vector<uint8_t> is_varlen_;
};

Result<void> EncodeValueWithLayout(const RowEncodingLayout& layout, const lean_row* row,
                                   std::string& out_value);
Result<void> EncodeKeyWithLayout(const RowEncodingLayout& layout, const lean_row* row,
                                 std::string& out_key);
Result<void> DecodeValueWithLayout(const RowEncodingLayout& layout, Slice value, lean_row* out_row);
Result<void> DecodeKeyWithLayout(const RowEncodingLayout& layout, Slice key, lean_row* out_row);
Result<void> DecodeKeyDatumsWithLayout(const RowEncodingLayout& layout, Slice key,
                                       std::vector<Datum>& out_key_datums);

/// Codec to transform typed Datums into persisted key/value bytes and back.
class TableCodec {
public:
  explicit TableCodec(const TableDefinition& def);

  /// Encode a row into primary-key bytes and value bytes.
  /// The row must contain at least schema.columns().size() elements.
  /// Returns std::unexpected on validation errors.
  Result<EncodedRow> Encode(const lean_row* row) const;

  /// Encode only the value portion of a row.
  Result<std::string> EncodeValue(const lean_row* row) const;
  Result<void> EncodeValue(const lean_row* row, std::string& out_value) const;

  /// Encode only the primary key portion of a row (used for lookup/delete).
  Result<std::string> EncodeKey(const lean_row* row) const;
  Result<void> EncodeKey(const lean_row* row, std::string& out_key) const;

  /// Decode a value slice into datum vector.
  Result<void> DecodeValue(Slice value, lean_row* out_row) const;

  /// Decode a key slice into the key columns of a row.
  Result<void> DecodeKey(Slice key, lean_row* out_row) const;

private:
  RowEncodingLayout layout_;
};

} // namespace leanstore
