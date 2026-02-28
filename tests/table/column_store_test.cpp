#include "common/lean_test_suite.hpp"
#include "leanstore/btree/b_tree_node.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/table/table.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <format>
#include <string>
#include <vector>

namespace leanstore::test {

namespace {

Result<TableDefinition> BuildBinaryTableDefinition(const std::string& name) {
  std::vector<ColumnDefinition> columns;
  columns.push_back(
      ColumnDefinition{.name_ = "k", .type_ = ColumnType::kBinary, .nullable_ = false});
  columns.push_back(
      ColumnDefinition{.name_ = "v", .type_ = ColumnType::kBinary, .nullable_ = false});
  std::vector<uint32_t> pk_columns{0};
  auto schema_res = TableSchema::Create(std::move(columns), std::move(pk_columns));
  if (!schema_res) {
    return std::move(schema_res.error());
  }
  return TableDefinition::Create(
      name, std::move(schema_res.value()), lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
      lean_btree_config{.enable_wal_ = false, .use_bulk_insert_ = false});
}

std::string MakeKey(size_t key_size, uint64_t value) {
  std::string suffix = std::format("{:016d}", value);
  if (suffix.size() >= key_size) {
    return suffix.substr(suffix.size() - key_size);
  }
  std::string key(key_size - suffix.size(), 'k');
  key += suffix;
  return key;
}

std::string MakeValue(size_t value_size, uint64_t value) {
  std::string suffix = std::format("{:016d}", value);
  if (value_size == 0) {
    return {};
  }
  if (suffix.size() >= value_size) {
    return suffix.substr(suffix.size() - value_size);
  }
  std::string padded(value_size, 'v');
  padded.replace(value_size - suffix.size(), suffix.size(), suffix);
  return padded;
}

void InsertBinaryRow(Table* table, const std::string& key, const std::string& value) {
  lean_datum cols[2];
  bool nulls[2] = {false, false};
  lean_str_view key_view{.data = key.data(), .size = key.size()};
  lean_str_view value_view{.data = value.data(), .size = value.size()};
  cols[0].str = key_view;
  cols[1].str = value_view;
  lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
  EXPECT_EQ(table->Insert(&row), OpCode::kOK);
}

bool LookupBinaryRow(Table* table, const std::string& key, std::string& out_value) {
  lean_datum key_cols[2];
  bool key_nulls[2] = {false, true};
  lean_str_view key_view{.data = key.data(), .size = key.size()};
  key_cols[0].str = key_view;
  key_cols[1].str = lean_str_view{.data = nullptr, .size = 0};
  lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = 2};

  lean_datum out_cols[2] = {};
  bool out_nulls[2] = {false, false};
  lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = 2};

  if (table->Lookup(&key_row, &out_row, out_value) != OpCode::kOK) {
    return false;
  }
  out_value.assign(out_cols[1].str.data, out_cols[1].str.size);
  return true;
}

} // namespace

class ColumnStoreApiTest : public LeanTestSuite {
protected:
  lean_store* store_ = nullptr;

  void SetUp() override {
    lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    ASSERT_EQ(lean_open_store(option, &store_), lean_status::LEAN_STATUS_OK);
    ASSERT_NE(store_, nullptr);
  }

  void TearDown() override {
    if (store_ != nullptr) {
      store_->close(store_);
      store_ = nullptr;
    }
  }
};

class ColumnStoreBtreeTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;

  void SetUp() override {
    lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);
    store_ = std::move(res.value());
    ASSERT_NE(store_, nullptr);
  }

  void TearDown() override {
    store_.reset();
  }
};

TEST_F(ColumnStoreBtreeTest, BuildColumnStoreMultiLeafGroup) {
  const std::string table_name = "column_store_multi_leaf_group";
  auto session = store_->Connect(0);
  auto def_res = BuildBinaryTableDefinition(table_name);
  ASSERT_TRUE(def_res);
  auto definition = std::move(def_res.value());

  auto btree_res = session.CreateBTree(table_name, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                                       definition.primary_index_config_);
  ASSERT_TRUE(btree_res);

  session.ExecSync([&]() {
    auto* btree = dynamic_cast<BasicKV*>(store_->tree_registry_->GetTree(table_name));
    ASSERT_NE(btree, nullptr);

    auto table_res = store_->RegisterTableWithExisting(definition);
    ASSERT_TRUE(table_res);
    auto* table = table_res.value();

    const size_t key_size = 64;
    const size_t value_size = 256;
    const size_t row_count = 96;
    for (size_t i = 0; i < row_count; ++i) {
      auto key = MakeKey(key_size, i);
      auto value = MakeValue(value_size, i);
      InsertBinaryRow(table, key, value);
    }

    auto stats_res = table->BuildColumnStore(
        column_store::ColumnStoreOptions{.max_rows_per_block_ = 1, .max_block_bytes_ = 0});
    ASSERT_TRUE(stats_res) << stats_res.error().ToString();

    const uint64_t leaf_count = btree->CountAllPages() - btree->CountInnerPages();
    EXPECT_GT(leaf_count, 1);

    std::string out_value;
    auto probe_key = MakeKey(key_size, row_count / 2);
    ASSERT_TRUE(LookupBinaryRow(table, probe_key, out_value));

    auto drop_res = store_->DropTable(table_name);
    ASSERT_TRUE(drop_res);
  });
}

TEST_F(ColumnStoreBtreeTest, BuildColumnStoreHeightGreaterThanTwo) {
  const std::string table_name = "column_store_height_gt2";
  auto session = store_->Connect(0);
  auto def_res = BuildBinaryTableDefinition(table_name);
  ASSERT_TRUE(def_res);
  auto definition = std::move(def_res.value());

  auto btree_res = session.CreateBTree(table_name, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                                       definition.primary_index_config_);
  ASSERT_TRUE(btree_res);

  session.ExecSync([&]() {
    auto* btree = dynamic_cast<BasicKV*>(store_->tree_registry_->GetTree(table_name));
    ASSERT_NE(btree, nullptr);

    auto table_res = store_->RegisterTableWithExisting(definition);
    ASSERT_TRUE(table_res);
    auto* table = table_res.value();

    const size_t key_size = 512;
    const size_t value_size = 512;
    const size_t max_rows = 400;
    size_t rows_inserted = 0;
    for (size_t i = 0; i < max_rows; ++i) {
      auto key = MakeKey(key_size, i);
      auto value = MakeValue(value_size, i);
      InsertBinaryRow(table, key, value);
      rows_inserted++;
      if (btree->GetHeight() > 2) {
        break;
      }
    }
    const uint64_t height_before = btree->GetHeight();
    ASSERT_GT(height_before, 2u);

    auto stats_res = table->BuildColumnStore(
        column_store::ColumnStoreOptions{.max_rows_per_block_ = 32, .max_block_bytes_ = 0});
    ASSERT_TRUE(stats_res) << stats_res.error().ToString();
    const uint64_t height_after = btree->GetHeight();
    EXPECT_EQ(height_after, height_before);

    std::string out_value;
    auto probe_key = MakeKey(key_size, rows_inserted / 2);
    ASSERT_TRUE(LookupBinaryRow(table, probe_key, out_value));

    auto drop_res = store_->DropTable(table_name);
    ASSERT_TRUE(drop_res);
  });
}

TEST_F(ColumnStoreBtreeTest, BuildColumnStoreParentUpperFenceSplit) { // NOLINT
  const std::string table_name = "column_store_parent_upper_fence_split";
  auto session = store_->Connect(0);
  auto def_res = BuildBinaryTableDefinition(table_name);
  ASSERT_TRUE(def_res);
  auto definition = std::move(def_res.value());

  auto btree_res = session.CreateBTree(table_name, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                                       definition.primary_index_config_);
  ASSERT_TRUE(btree_res);

  session.ExecSync([&]() {
    auto* btree = dynamic_cast<BasicKV*>(store_->tree_registry_->GetTree(table_name));
    ASSERT_NE(btree, nullptr);

    auto table_res = store_->RegisterTableWithExisting(definition);
    ASSERT_TRUE(table_res);
    auto* table = table_res.value();

    const size_t key_size = 512;
    const size_t value_size = 512;
    const size_t target_rows = 64;
    const size_t max_rows = 200;
    size_t rows_inserted = 0;
    while (
        (btree->GetHeight() <= 2 || btree->CountInnerPages() <= 1 || rows_inserted < target_rows) &&
        rows_inserted < max_rows) {
      auto key = MakeKey(key_size, rows_inserted);
      auto value = MakeValue(value_size, rows_inserted);
      InsertBinaryRow(table, key, value);
      rows_inserted++;
    }
    ASSERT_GE(rows_inserted, target_rows);
    ASSERT_GT(btree->GetHeight(), 2u);
    ASSERT_GT(btree->CountInnerPages(), 1u);

    const column_store::ColumnStoreOptions options{.max_rows_per_block_ = 4, .max_block_bytes_ = 0};
    auto stats_res = table->BuildColumnStore(options);
    ASSERT_TRUE(stats_res) << stats_res.error().ToString();
    EXPECT_EQ(stats_res.value().row_count_, rows_inserted);
    const uint64_t expected_blocks =
        (rows_inserted + options.max_rows_per_block_ - 1) / options.max_rows_per_block_;
    EXPECT_EQ(stats_res.value().block_count_, expected_blocks);

    std::string out_value;
    auto probe_key = MakeKey(key_size, rows_inserted / 2);
    ASSERT_TRUE(LookupBinaryRow(table, probe_key, out_value));

    auto drop_res = store_->DropTable(table_name);
    ASSERT_TRUE(drop_res);
  });
}

} // namespace leanstore::test
