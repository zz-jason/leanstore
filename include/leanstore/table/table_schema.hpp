#pragma once

#include "leanstore/base/error.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/c/types.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace leanstore {

/// Column types supported by the logical table schema definition.
enum class ColumnType : uint8_t {
  kBool = 0,
  kInt32,
  kInt64,
  kUInt64,
  kFloat32,
  kFloat64,
  kBinary,
  kString,
};

inline std::string_view ToString(ColumnType type) {
  switch (type) {
  case ColumnType::kBool:
    return "BOOL";
  case ColumnType::kInt32:
    return "INT32";
  case ColumnType::kInt64:
    return "INT64";
  case ColumnType::kUInt64:
    return "UINT64";
  case ColumnType::kFloat32:
    return "FLOAT32";
  case ColumnType::kFloat64:
    return "FLOAT64";
  case ColumnType::kBinary:
    return "BINARY";
  case ColumnType::kString:
    return "STRING";
  }
  return "UNKNOWN";
}

/// Definition of a single column in a table.
struct ColumnDefinition {
  std::string name_;
  ColumnType type_ = ColumnType::kBinary;
  bool nullable_ = true;
  /// For fixed length types (CHAR, BINARY, etc). Zero implies variable sized.
  uint32_t fixed_length_ = 0;
};

class TableSchema {
public:
  TableSchema(const TableSchema&) = default;
  TableSchema(TableSchema&&) noexcept = default;
  TableSchema& operator=(const TableSchema&) = default;
  TableSchema& operator=(TableSchema&&) noexcept = default;
  ~TableSchema() = default;

  static Result<TableSchema> Create(std::vector<ColumnDefinition> columns,
                                    std::vector<uint32_t> primary_key_columns);

  const std::vector<ColumnDefinition>& Columns() const {
    return columns_;
  }

  const std::vector<uint32_t>& PrimaryKeyColumns() const {
    return primary_key_columns_;
  }

  Result<void> Validate() const;

private:
  friend struct TableDefinition;

  TableSchema();
  TableSchema(std::vector<ColumnDefinition> columns, std::vector<uint32_t> primary_key_columns);

  std::vector<ColumnDefinition> columns_;
  std::vector<uint32_t> primary_key_columns_;
};

struct TableDefinition {
  std::string name_;
  TableSchema schema_;
  lean_btree_type primary_index_type_ = lean_btree_type::LEAN_BTREE_TYPE_MVCC;
  lean_btree_config primary_index_config_{
      .enable_wal_ = true,
      .use_bulk_insert_ = false,
  };

  static Result<TableDefinition> Create(std::string name, TableSchema schema,
                                        lean_btree_type primary_index_type,
                                        lean_btree_config primary_index_config);

  Result<void> Validate() const {
    if (name_.empty()) {
      return Error::General("table name cannot be empty");
    }
    return schema_.Validate();
  }

  TableDefinition(const TableDefinition&) = default;
  TableDefinition(TableDefinition&&) noexcept = default;
  TableDefinition& operator=(const TableDefinition&) = default;
  TableDefinition& operator=(TableDefinition&&) noexcept = default;

private:
  TableDefinition();
  TableDefinition(std::string name, TableSchema schema, lean_btree_type primary_index_type,
                  lean_btree_config primary_index_config);
};

} // namespace leanstore
