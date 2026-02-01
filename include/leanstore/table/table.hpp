#pragma once

#include "leanstore/btree/column_store/column_store.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/table/encoding.hpp"
#include "leanstore/table/table_schema.hpp"

#include <memory>
#include <string>
#include <variant>

namespace leanstore {

class LeanStore;
class BTreeGeneric;
class BasicKV;
class TransactionKV;

using KVVariant = std::variant<BasicKV*, TransactionKV*>;

class TableCursor {
public:
  TableCursor(KVVariant kv, const TableDefinition& def);
  ~TableCursor();

  bool SeekToFirst();
  bool SeekToFirstGreaterEqual(const lean_row* key_row);
  bool SeekToLast();
  bool SeekToLastLessEqual(const lean_row* key_row);
  bool Next();
  bool Prev();
  bool IsValid() const;
  Result<void> CurrentRow(lean_row* row);
  OpCode RemoveCurrent();
  OpCode UpdateCurrent(const lean_row* row);

private:
  bool Assign(Slice key, Slice val);

  KVVariant kv_;
  std::string current_key_;
  std::string current_value_;
  bool is_valid_ = false;
  TableCodec codec_;
};

class Table {
public:
  static Result<std::unique_ptr<Table>> Create(LeanStore* store, TableDefinition definition);
  static Result<std::unique_ptr<Table>> WrapExisting(LeanStore* store, TableDefinition definition);
  ~Table() = default;

  const TableDefinition& Definition() const {
    return definition_;
  }

  OpCode Insert(const lean_row* row);
  OpCode Remove(const lean_row* key_row);
  OpCode Lookup(const lean_row* key_row, lean_row* out_row, std::string& value_buf);
  // Convert row-store leaf groups into column leaves in place.
  Result<column_store::ColumnStoreStats> BuildColumnStore(
      const column_store::ColumnStoreOptions& options);

  std::unique_ptr<TableCursor> NewCursor();

  Table(TableDefinition definition, KVVariant kv)
      : definition_(std::move(definition)),
        kv_(kv),
        codec_(definition_) {
  }

private:
  TableDefinition definition_;
  KVVariant kv_;
  TableCodec codec_;
};

} // namespace leanstore
