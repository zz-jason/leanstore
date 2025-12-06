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
    if (col.name.empty()) {
      return Error::General("column name cannot be empty");
    }
    auto insert_res = seen_names.emplace(col.name);
    if (!insert_res.second) {
      return Error::General(std::string("duplicated column name: ") + col.name);
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
    if (columns_[pk_index].nullable) {
      return Error::General(std::string("primary key column must be NOT NULL: ") +
                            columns_[pk_index].name);
    }
  }

  return {};
}

} // namespace leanstore
