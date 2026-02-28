#include "c/session_impl.hpp"

#include "c/btree_impl.hpp"
#include "c/btree_mvcc_impl.hpp"
#include "c/table_impl.hpp"
#include "leanstore/base/error.hpp"
#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/buffer/tree_registry.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/tx/transaction_kv.hpp"
#include "leanstore/tx/tx_manager.hpp"

#include <iostream>
#include <utility>
#include <vector>

namespace leanstore {

namespace {

ColumnType ConvertColumnType(lean_column_type type) {
  switch (type) {
  case LEAN_COLUMN_TYPE_BOOL:
    return ColumnType::kBool;
  case LEAN_COLUMN_TYPE_INT32:
    return ColumnType::kInt32;
  case LEAN_COLUMN_TYPE_INT64:
    return ColumnType::kInt64;
  case LEAN_COLUMN_TYPE_UINT64:
    return ColumnType::kUInt64;
  case LEAN_COLUMN_TYPE_FLOAT32:
    return ColumnType::kFloat32;
  case LEAN_COLUMN_TYPE_FLOAT64:
    return ColumnType::kFloat64;
  case LEAN_COLUMN_TYPE_STRING:
    return ColumnType::kString;
  case LEAN_COLUMN_TYPE_BINARY:
  default:
    return ColumnType::kBinary;
  }
}

Result<TableDefinition> BuildTableDefinition(const lean_table_def* table_def) {
  if (table_def == nullptr) {
    return Error::General("table definition cannot be null");
  }
  if (table_def->name.data == nullptr || table_def->name.size == 0) {
    return Error::General("table name cannot be empty");
  }
  if (table_def->columns == nullptr || table_def->num_columns == 0) {
    return Error::General("table must define at least one column");
  }
  if (table_def->pk_cols_count == 0) {
    return Error::General("table must define at least one primary key column");
  }
  if (table_def->pk_cols == nullptr) {
    return Error::General("primary key column indexes cannot be null");
  }

  std::vector<ColumnDefinition> columns;
  columns.reserve(table_def->num_columns);
  for (uint32_t i = 0; i < table_def->num_columns; ++i) {
    const auto& c_def = table_def->columns[i];
    if (c_def.name.data == nullptr || c_def.name.size == 0) {
      return Error::General("column name cannot be empty");
    }
    ColumnDefinition column;
    column.name_.assign(c_def.name.data, c_def.name.size);
    column.type_ = ConvertColumnType(c_def.type);
    column.nullable_ = c_def.nullable;
    column.fixed_length_ = c_def.fixed_length;
    columns.emplace_back(std::move(column));
  }

  std::vector<uint32_t> pk_columns;
  pk_columns.reserve(table_def->pk_cols_count);
  for (uint32_t i = 0; i < table_def->pk_cols_count; ++i) {
    pk_columns.push_back(table_def->pk_cols[i]);
  }

  auto schema_res = TableSchema::Create(std::move(columns), std::move(pk_columns));
  if (!schema_res) {
    return std::move(schema_res.error());
  }

  std::string name(table_def->name.data, table_def->name.size);
  return TableDefinition::Create(std::move(name), std::move(schema_res.value()),
                                 table_def->primary_index_type, table_def->primary_index_config);
}

} // namespace

lean_status SessionImpl::CreateBTree(const char* btree_name, lean_btree_type btree_type) {
  auto res = session_.CreateBTree(btree_name, btree_type);
  if (!res) {
    std::cerr << "CreateBTree failed: " << res.error().ToString() << std::endl;
    return lean_status::LEAN_ERR_CTEATE_BTREE;
  }
  return lean_status::LEAN_STATUS_OK;
}

void SessionImpl::DropBTree(const char* btree_name) {
  session_.DropBTree(btree_name);
}

struct lean_btree* SessionImpl::GetBTree(const char* btree_name) {
  auto exists_res = session_.GetBTree(btree_name);
  if (!exists_res) {
    return nullptr;
  }

  auto* tree = store_->tree_registry_->GetTree(btree_name);
  auto* generic_btree = dynamic_cast<BTreeGeneric*>(tree);
  if (generic_btree == nullptr) {
    return nullptr;
  }
  switch (generic_btree->tree_type_) {
  case BTreeType::kBasicKV:
    return BTreeImpl::Create(dynamic_cast<BasicKV*>(generic_btree), this);
  case BTreeType::kTransactionKV:
    return BTreeMvccImpl::Create(dynamic_cast<TransactionKV*>(generic_btree), this);
  default:
    return nullptr;
  }
}

lean_status SessionImpl::CreateTable(const struct lean_table_def* table_def) {
  auto def_res = BuildTableDefinition(table_def);
  if (!def_res) {
    std::cerr << "CreateTable failed: " << def_res.error().ToString() << std::endl;
    return lean_status::LEAN_ERR_CREATE_TABLE;
  }

  lean_status status = lean_status::LEAN_STATUS_OK;
  auto definition = std::move(def_res.value());
  session_.ExecSync([&]() {
    auto res = store_->CreateTable(definition);
    if (!res) {
      std::cerr << "CreateTable failed: " << res.error().ToString() << std::endl;
      status = lean_status::LEAN_ERR_CREATE_TABLE;
    }
  });
  return status;
}

lean_status SessionImpl::DropTable(const char* table_name) {
  if (table_name == nullptr) {
    return lean_status::LEAN_ERR_TABLE_NOT_FOUND;
  }
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_.ExecSync([&]() {
    auto res = store_->DropTable(table_name);
    if (!res) {
      std::cerr << "DropTable failed: " << res.error().ToString() << std::endl;
      status = lean_status::LEAN_ERR_TABLE_NOT_FOUND;
    }
  });
  return status;
}

struct lean_table* SessionImpl::GetTable(const char* table_name) {
  struct lean_table* table_handle = nullptr;
  session_.ExecSync([&]() {
    auto* table = store_->GetTable(table_name);
    if (table == nullptr) {
      table_handle = nullptr;
      return;
    }
    table_handle = TableImpl::Create(table, this);
  });
  return table_handle;
}

} // namespace leanstore
