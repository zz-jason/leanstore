#pragma once

#include "leanstore/c/leanstore.h"
#include "leanstore/cpp/base/slice.hpp"
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

/// Codec to transform typed Datums into persisted key/value bytes and back.
class TableCodec {
public:
  explicit TableCodec(const TableDefinition& def) : def_(def) {
  }

  /// Encode a row into primary-key bytes and value bytes.
  /// The row must contain at least schema.columns().size() elements.
  /// Returns std::unexpected on validation errors.
  Result<EncodedRow> Encode(const lean_row* row) const;

  /// Encode only the primary key portion of a row (used for lookup/delete).
  Result<std::string> EncodeKey(const lean_row* row) const;

  /// Decode a value slice into datum vector.
  Result<void> DecodeValue(Slice value, lean_row* out_row) const;

private:
  Result<void> EncodeFixedDatum(const Datum& datum, ColumnType type, std::string& dest,
                                bool key_encoding) const;
  Result<void> EncodeVarlenDatum(const Datum& datum, ColumnType type, std::string& payload,
                                 uint64_t& encoded) const;

  const TableDefinition& def_;
};

} // namespace leanstore
