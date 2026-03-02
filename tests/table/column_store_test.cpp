#include "common/lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"

#include <gtest/gtest.h>

#include <cstring>
#include <format>
#include <string>

namespace leanstore::test {

namespace {

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

void InsertBinaryRow(lean_table* table, const std::string& key, const std::string& value) {
  lean_datum cols[2];
  bool nulls[2] = {false, false};
  cols[0].str = lean_str_view{.data = key.data(), .size = key.size()};
  cols[1].str = lean_str_view{.data = value.data(), .size = value.size()};
  lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
  ASSERT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
}

bool LookupBinaryRow(lean_table* table, const std::string& key, std::string& out_value) {
  lean_datum key_cols[2];
  bool key_nulls[2] = {false, true};
  key_cols[0].str = lean_str_view{.data = key.data(), .size = key.size()};
  key_cols[1].str = lean_str_view{.data = nullptr, .size = 0};
  lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = 2};

  lean_datum out_cols[2] = {};
  bool out_nulls[2] = {false, false};
  lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = 2};

  if (table->lookup(table, &key_row, &out_row) != lean_status::LEAN_STATUS_OK) {
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

TEST_F(ColumnStoreApiTest, BuildColumnStoreMultiLeafGroup) {
  const char* table_name = "column_store_multi_leaf_group";
  lean_table_column_def columns[2] = {
      {.name = {.data = "k", .size = 1},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "v", .size = 1},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};
  lean_table_def table_def = {.name = {.data = table_name, .size = strlen(table_name)},
                              .columns = columns,
                              .num_columns = 2,
                              .pk_cols = pk_columns,
                              .pk_cols_count = 1,
                              .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                              .primary_index_config = {
                                  .enable_wal_ = false,
                                  .use_bulk_insert_ = false,
                              }};

  auto* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);
  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);
  auto* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  const size_t key_size = 64;
  const size_t value_size = 256;
  const size_t row_count = 96;
  for (size_t i = 0; i < row_count; ++i) {
    InsertBinaryRow(table, MakeKey(key_size, i), MakeValue(value_size, i));
  }

  lean_column_store_options options{
      .max_rows_per_block = 1, .max_block_bytes = 0, .target_height = 0};
  lean_column_store_stats stats{};
  ASSERT_EQ(table->build_column_store(table, &options, &stats), lean_status::LEAN_STATUS_OK);
  EXPECT_EQ(stats.row_count, row_count);
  EXPECT_EQ(stats.block_count, row_count);

  std::string out_value;
  ASSERT_TRUE(LookupBinaryRow(table, MakeKey(key_size, row_count / 2), out_value));

  table->close(table);
  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

TEST_F(ColumnStoreApiTest, BuildColumnStoreHeightGreaterThanTwo) {
  const char* table_name = "column_store_height_gt2";
  lean_table_column_def columns[2] = {
      {.name = {.data = "k", .size = 1},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "v", .size = 1},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};
  lean_table_def table_def = {.name = {.data = table_name, .size = strlen(table_name)},
                              .columns = columns,
                              .num_columns = 2,
                              .pk_cols = pk_columns,
                              .pk_cols_count = 1,
                              .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                              .primary_index_config = {
                                  .enable_wal_ = false,
                                  .use_bulk_insert_ = false,
                              }};

  auto* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);
  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);
  auto* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  const size_t key_size = 512;
  const size_t value_size = 512;
  const size_t row_count = 200;
  for (size_t i = 0; i < row_count; ++i) {
    InsertBinaryRow(table, MakeKey(key_size, i), MakeValue(value_size, i));
  }

  lean_column_store_options options{
      .max_rows_per_block = 32, .max_block_bytes = 0, .target_height = 0};
  lean_column_store_stats stats{};
  ASSERT_EQ(table->build_column_store(table, &options, &stats), lean_status::LEAN_STATUS_OK);
  EXPECT_EQ(stats.row_count, row_count);
  EXPECT_EQ(stats.block_count,
            (row_count + options.max_rows_per_block - 1) / options.max_rows_per_block);

  std::string out_value;
  ASSERT_TRUE(LookupBinaryRow(table, MakeKey(key_size, row_count / 2), out_value));

  table->close(table);
  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

TEST_F(ColumnStoreApiTest, BuildColumnStoreParentUpperFenceSplit) {
  const char* table_name = "column_store_parent_upper_fence_split";
  lean_table_column_def columns[2] = {
      {.name = {.data = "k", .size = 1},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "v", .size = 1},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};
  lean_table_def table_def = {.name = {.data = table_name, .size = strlen(table_name)},
                              .columns = columns,
                              .num_columns = 2,
                              .pk_cols = pk_columns,
                              .pk_cols_count = 1,
                              .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                              .primary_index_config = {
                                  .enable_wal_ = false,
                                  .use_bulk_insert_ = false,
                              }};

  auto* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);
  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);
  auto* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  const size_t key_size = 512;
  const size_t value_size = 512;
  const size_t row_count = 128;
  for (size_t i = 0; i < row_count; ++i) {
    InsertBinaryRow(table, MakeKey(key_size, i), MakeValue(value_size, i));
  }

  lean_column_store_options options{
      .max_rows_per_block = 4, .max_block_bytes = 0, .target_height = 0};
  lean_column_store_stats stats{};
  ASSERT_EQ(table->build_column_store(table, &options, &stats), lean_status::LEAN_STATUS_OK);
  EXPECT_EQ(stats.row_count, row_count);
  EXPECT_EQ(stats.block_count,
            (row_count + options.max_rows_per_block - 1) / options.max_rows_per_block);

  std::string out_value;
  ASSERT_TRUE(LookupBinaryRow(table, MakeKey(key_size, row_count / 2), out_value));

  table->close(table);
  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

} // namespace leanstore::test
