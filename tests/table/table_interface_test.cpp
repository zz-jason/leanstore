#include "common/lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"

#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <numbers>
#include <string>
#include <vector>

namespace leanstore::test {

class TableInterfaceTest : public LeanTestSuite {
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

TEST_F(TableInterfaceTest, CreateAndUseTable) {
  const char* table_name = "test_table";
  lean_table_column_def columns[2] = {
      {.name = {.data = "id", .size = 2},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "value", .size = 5},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = true,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};

  lean_table_def table_def = {
      .name = {.data = table_name, .size = strlen(table_name)},
      .columns = columns,
      .num_columns = 2,
      .pk_cols = pk_columns,
      .pk_cols_count = 1,
      .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
      .primary_index_config =
          {
              .enable_wal_ = true,
              .use_bulk_insert_ = false,
          },
  };

  lean_session* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);

  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);

  lean_table* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  std::vector<std::pair<std::string, std::string>> rows = {
      {"key1", "value1"},
      {"key2", "value2"},
      {"key3", "value3"},
  };

  for (const auto& [key, value] : rows) {
    lean_row row;
    lean_datum cols[2];
    bool nulls[2] = {false, false};
    lean_str_view key_str{.data = key.data(), .size = key.size()};
    lean_str_view val_str{.data = value.data(), .size = value.size()};
    cols[0].str = key_str;
    cols[1].str = val_str;
    row.columns = cols;
    row.nulls = nulls;
    row.num_columns = 2;
    EXPECT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  for (const auto& [key, value] : rows) {
    lean_row key_row;
    lean_datum key_cols[2];
    bool key_nulls[2] = {false, true};
    lean_str_view key_str{.data = key.data(), .size = key.size()};
    key_cols[0].str = key_str;
    key_row.columns = key_cols;
    key_row.nulls = key_nulls;
    key_row.num_columns = 2;

    lean_row out_row;
    lean_datum out_cols[2] = {};
    bool out_nulls[2] = {false, false};
    out_row.columns = out_cols;
    out_row.nulls = out_nulls;
    out_row.num_columns = 2;

    auto status = table->lookup(table, &key_row, &out_row);
    EXPECT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_EQ(std::string(out_cols[1].str.data, out_cols[1].str.size), value);
  }

  auto* cursor = table->open_cursor(table);
  ASSERT_NE(cursor, nullptr);

  ASSERT_TRUE(cursor->seek_to_first(cursor));
  size_t seen = 0;
  do {
    lean_row row;
    lean_datum cols[2] = {};
    bool nulls[2] = {false, false};
    row.columns = cols;
    row.nulls = nulls;
    row.num_columns = 2;
    cursor->current_row(cursor, &row);
    EXPECT_EQ(std::string(cols[1].str.data, cols[1].str.size), rows[seen].second);
    seen++;
  } while (cursor->next(cursor));
  EXPECT_EQ(seen, rows.size());
  cursor->close(cursor);

  table->close(table);

  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

TEST_F(TableInterfaceTest, RejectNullsForNotNullColumns) {
  const char* table_name = "null_checks";
  lean_table_column_def columns[2] = {
      {.name = {.data = "id", .size = 2},
       .type = LEAN_COLUMN_TYPE_INT64,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "val", .size = 3},
       .type = LEAN_COLUMN_TYPE_STRING,
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
                                  .enable_wal_ = true,
                                  .use_bulk_insert_ = false,
                              }};

  lean_session* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);
  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);
  lean_table* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  // Null primary key should be rejected.
  {
    lean_datum cols[2];
    bool nulls[2] = {true, false};
    cols[0].i64 = 0;
    const char* v = "value";
    lean_str_view val{.data = v, .size = strlen(v)};
    cols[1].str = val;
    lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
    EXPECT_NE(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  // Null non-nullable value column should also be rejected.
  {
    lean_datum cols[2];
    bool nulls[2] = {false, true};
    cols[0].i64 = 1;
    lean_str_view empty{.data = nullptr, .size = 0};
    cols[1].str = empty;
    lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
    EXPECT_NE(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  table->close(table);
  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

TEST_F(TableInterfaceTest, InsertAndReadVariousTypes) {
  const char* table_name = "typed_table";
  lean_table_column_def columns[6] = {
      {.name = {.data = "k", .size = 1},
       .type = LEAN_COLUMN_TYPE_INT64,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "b", .size = 1},
       .type = LEAN_COLUMN_TYPE_BOOL,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "i32", .size = 3},
       .type = LEAN_COLUMN_TYPE_INT32,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "u64", .size = 3},
       .type = LEAN_COLUMN_TYPE_UINT64,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "f64", .size = 3},
       .type = LEAN_COLUMN_TYPE_FLOAT64,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "s", .size = 1},
       .type = LEAN_COLUMN_TYPE_STRING,
       .nullable = false,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};
  lean_table_def table_def = {.name = {.data = table_name, .size = strlen(table_name)},
                              .columns = columns,
                              .num_columns = 6,
                              .pk_cols = pk_columns,
                              .pk_cols_count = 1,
                              .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                              .primary_index_config = {
                                  .enable_wal_ = true,
                                  .use_bulk_insert_ = false,
                              }};

  lean_session* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);
  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);
  lean_table* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  // Insert one row
  {
    lean_datum cols[6];
    bool nulls[6] = {false, false, false, false, false, false};
    cols[0].i64 = 42;
    cols[1].b = true;
    cols[2].i32 = -123;
    cols[3].u64 = 9999;
    cols[4].f64 = std::numbers::pi;
    const char* str = "hello";
    lean_str_view str_view{.data = str, .size = strlen(str)};
    cols[5].str = str_view;
    lean_row row{.columns = cols, .nulls = nulls, .num_columns = 6};
    EXPECT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  // Lookup and verify decoded values
  {
    lean_datum key_cols[6] = {};
    bool key_nulls[6] = {false, true, true, true, true, true};
    key_cols[0].i64 = 42;
    lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = 6};

    lean_datum out_cols[6] = {};
    bool out_nulls[6] = {false, false, false, false, false, false};
    lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = 6};

    auto status = table->lookup(table, &key_row, &out_row);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_EQ(out_cols[0].i64, 42);
    EXPECT_TRUE(out_cols[1].b);
    EXPECT_EQ(out_cols[2].i32, -123);
    EXPECT_EQ(out_cols[3].u64, 9999u);
    EXPECT_NEAR(out_cols[4].f64, 3.14159, 1e-5);
    EXPECT_EQ(std::string(out_cols[5].str.data, out_cols[5].str.size), "hello");
  }

  table->close(table);
  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

TEST_F(TableInterfaceTest, TransactionCommitAndAbort) {
  const char* table_name = "txn_table";
  lean_table_column_def columns[2] = {
      {.name = {.data = "id", .size = 2},
       .type = LEAN_COLUMN_TYPE_INT64,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "val", .size = 3},
       .type = LEAN_COLUMN_TYPE_STRING,
       .nullable = true,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};
  lean_table_def table_def = {.name = {.data = table_name, .size = strlen(table_name)},
                              .columns = columns,
                              .num_columns = 2,
                              .pk_cols = pk_columns,
                              .pk_cols_count = 1,
                              .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_MVCC,
                              .primary_index_config = {
                                  .enable_wal_ = true,
                                  .use_bulk_insert_ = false,
                              }};

  lean_session* writer = store_->connect(store_);
  lean_session* reader = store_->connect(store_);
  ASSERT_NE(writer, nullptr);
  ASSERT_NE(reader, nullptr);
  ASSERT_EQ(writer->create_table(writer, &table_def), lean_status::LEAN_STATUS_OK);
  lean_table* table_w = writer->get_table(writer, table_name);
  lean_table* table_r = reader->get_table(reader, table_name);
  ASSERT_NE(table_w, nullptr);
  ASSERT_NE(table_r, nullptr);

  // Insert and commit
  writer->start_tx(writer);
  {
    lean_datum cols[2];
    bool nulls[2] = {false, false};
    cols[0].i64 = 1;
    const char* v = "committed";
    lean_str_view val{.data = v, .size = strlen(v)};
    cols[1].str = val;
    lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
    EXPECT_EQ(table_w->insert(table_w, &row), lean_status::LEAN_STATUS_OK);
  }
  writer->commit_tx(writer);

  // Reader should see committed row
  {
    lean_datum key_cols[2] = {};
    bool key_nulls[2] = {false, true};
    key_cols[0].i64 = 1;
    lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = 2};
    lean_datum out_cols[2] = {};
    bool out_nulls[2] = {false, false};
    lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = 2};
    auto status = table_r->lookup(table_r, &key_row, &out_row);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_EQ(std::string(out_cols[1].str.data, out_cols[1].str.size), "committed");
  }

  // Insert and abort
  writer->start_tx(writer);
  {
    lean_datum cols[2];
    bool nulls[2] = {false, false};
    cols[0].i64 = 2;
    const char* v = "rolledback";
    lean_str_view val{.data = v, .size = strlen(v)};
    cols[1].str = val;
    lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
    EXPECT_EQ(table_w->insert(table_w, &row), lean_status::LEAN_STATUS_OK);
  }
  writer->abort_tx(writer);

  // Reader should not see aborted row
  {
    lean_datum key_cols[2] = {};
    bool key_nulls[2] = {false, true};
    key_cols[0].i64 = 2;
    lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = 2};
    lean_datum out_cols[2] = {};
    bool out_nulls[2] = {false, false};
    lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = 2};
    auto status = table_r->lookup(table_r, &key_row, &out_row);
    EXPECT_EQ(status, lean_status::LEAN_ERR_NOT_FOUND);
  }

  table_w->close(table_w);
  table_r->close(table_r);
  ASSERT_EQ(writer->drop_table(writer, table_name), lean_status::LEAN_STATUS_OK);
  writer->close(writer);
  reader->close(reader);
}

TEST_F(TableInterfaceTest, Float32PrimaryKeyOrdering) {
  const char* table_name = "float_pk_table";
  lean_table_column_def columns[2] = {
      {.name = {.data = "k", .size = 1},
       .type = LEAN_COLUMN_TYPE_FLOAT32,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "v", .size = 1},
       .type = LEAN_COLUMN_TYPE_STRING,
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
                                  .enable_wal_ = true,
                                  .use_bulk_insert_ = false,
                              }};

  lean_session* session = store_->connect(store_);
  ASSERT_NE(session, nullptr);
  ASSERT_EQ(session->create_table(session, &table_def), lean_status::LEAN_STATUS_OK);
  lean_table* table = session->get_table(session, table_name);
  ASSERT_NE(table, nullptr);

  std::map<float, std::string> rows = {
      {-1.25f, "neg"},
      {0.0f, "zero"},
      {3.5f, "pos"},
  };

  for (const auto& [key, value] : rows) {
    lean_datum cols[2];
    bool nulls[2] = {false, false};
    cols[0].f32 = key;
    lean_str_view val{.data = value.data(), .size = value.size()};
    cols[1].str = val;
    lean_row row{.columns = cols, .nulls = nulls, .num_columns = 2};
    EXPECT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  // Verify lookup works.
  for (const auto& [key, value] : rows) {
    lean_datum key_cols[2] = {};
    bool key_nulls[2] = {false, true};
    key_cols[0].f32 = key;
    lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = 2};

    lean_datum out_cols[2] = {};
    bool out_nulls[2] = {false, false};
    lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = 2};

    auto status = table->lookup(table, &key_row, &out_row);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_NEAR(out_cols[0].f32, key, 1e-6);
    EXPECT_EQ(std::string(out_cols[1].str.data, out_cols[1].str.size), value);
  }

  // Verify cursor order matches ascending primary keys.
  auto* cursor = table->open_cursor(table);
  ASSERT_NE(cursor, nullptr);
  ASSERT_TRUE(cursor->seek_to_first(cursor));
  size_t seen = 0;
  for (auto it = rows.begin(); cursor->is_valid(cursor) && it != rows.end(); ++it) {
    lean_row row;
    lean_datum cols[2] = {};
    bool nulls[2] = {false, false};
    row.columns = cols;
    row.nulls = nulls;
    row.num_columns = 2;
    cursor->current_row(cursor, &row);
    EXPECT_NEAR(cols[0].f32, it->first, 1e-6);
    EXPECT_EQ(std::string(cols[1].str.data, cols[1].str.size), it->second);
    seen++;
    if (!cursor->next(cursor)) {
      break;
    }
  }
  EXPECT_EQ(seen, rows.size());
  cursor->close(cursor);

  table->close(table);
  ASSERT_EQ(session->drop_table(session, table_name), lean_status::LEAN_STATUS_OK);
  session->close(session);
}

} // namespace leanstore::test
