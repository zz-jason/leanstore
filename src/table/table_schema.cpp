#include "leanstore/table/table_schema.hpp"

#include <unordered_set>

namespace leanstore {

Result<void> TableSchema::Validate() const {
  if (columns_.empty()) {
    return Error::General("table must contain at least one column");
  }

  if (primary_key_columns_.empty()) {
    return Error::General("table must define at least one primary key column");
  }

  std::unordered_set<std::string> seen_names;
  for (const auto& col : columns_) {
    if (col.name_.empty()) {
      return Error::General("column name cannot be empty");
    }
    auto insert_res = seen_names.emplace(col.name_);
    if (!insert_res.second) {
      return Error::General(std::string("duplicated column name: ") + col.name_);
    }
  }

  std::unordered_set<uint32_t> seen_pk_indexes;
  for (auto pk_index : primary_key_columns_) {
    if (pk_index >= columns_.size()) {
      return Error::General("primary key column index out of range");
    }
    if (!seen_pk_indexes.emplace(pk_index).second) {
      return Error::General("duplicate primary key column index");
    }
    if (columns_[pk_index].nullable_) {
      return Error::General(std::string("primary key column must be NOT NULL: ") +
                            columns_[pk_index].name_);
    }
  }

  return {};
}

Result<TableSchema> TableSchema::Create(std::vector<ColumnDefinition> columns,
                                        std::vector<uint32_t> primary_key_columns) {
  TableSchema schema(std::move(columns), std::move(primary_key_columns));
  if (auto res = schema.Validate(); !res) {
    return std::move(res.error());
  }
  return schema;
}

TableSchema::TableSchema() = default;

TableSchema::TableSchema(std::vector<ColumnDefinition> columns,
                         std::vector<uint32_t> primary_key_columns)
    : columns_(std::move(columns)),
      primary_key_columns_(std::move(primary_key_columns)) {
}

TableDefinition::TableDefinition() = default;

TableDefinition::TableDefinition(std::string name, TableSchema schema,
                                 lean_btree_type primary_index_type,
                                 lean_btree_config primary_index_config)
    : name_(std::move(name)),
      schema_(std::move(schema)),
      primary_index_type_(primary_index_type),
      primary_index_config_(primary_index_config) {
}

Result<TableDefinition> TableDefinition::Create(std::string name, TableSchema schema,
                                                lean_btree_type primary_index_type,
                                                lean_btree_config primary_index_config) {
  TableDefinition def(std::move(name), std::move(schema), primary_index_type, primary_index_config);
  if (auto res = def.Validate(); !res) {
    return std::move(res.error());
  }
  return def;
}

} // namespace leanstore
