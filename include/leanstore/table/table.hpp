#pragma once

#include "leanstore/kv_interface.hpp"
#include "leanstore/table/encoding.hpp"
#include "leanstore/table/table_schema.hpp"

#include <memory>
#include <string>

namespace leanstore {

class LeanStore;
class BTreeGeneric;

class TableCursor {
public:
  TableCursor(KVInterface* kv_interface, const TableDefinition& def);
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

  KVInterface* kv_interface_;
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

  const TableDefinition& definition() const {
    return definition_;
  }

  OpCode Insert(const lean_row* row);
  OpCode Remove(const lean_row* key_row);
  OpCode Lookup(const lean_row* key_row, lean_row* out_row);

  std::unique_ptr<TableCursor> NewCursor();

private:
  Table(LeanStore* store, TableDefinition definition, KVInterface* kv_interface, BTreeGeneric* tree)
      : store_(store),
        definition_(std::move(definition)),
        kv_interface_(kv_interface),
        tree_(tree),
        codec_(definition_) {
  }

private:
  LeanStore* store_;
  TableDefinition definition_;
  KVInterface* kv_interface_;
  BTreeGeneric* tree_;
  TableCodec codec_;
};

} // namespace leanstore
