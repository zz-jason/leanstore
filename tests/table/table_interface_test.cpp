#include "leanstore/c/leanstore.h"

#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

namespace leanstore::test {

class TableInterfaceTest : public ::testing::Test {
protected:
  lean_store* store_ = nullptr;

  void SetUp() override {
    lean_store_option* option = lean_store_option_create(GetTestDataDir().c_str());
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

  std::string GetTestDataDir() const {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string("/tmp/leanstore/") + cur_test->name();
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
      .primary_key_column_indexes = pk_columns,
      .num_primary_key_columns = 1,
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
    lean_str key_str{.data = key.data(), .size = key.size(), .capacity = 0};
    lean_str val_str{.data = value.data(), .size = value.size(), .capacity = 0};
    cols[0] = {.type = LEAN_COLUMN_TYPE_BINARY, .is_null = false, .value = {.str = key_str}};
    cols[1] = {.type = LEAN_COLUMN_TYPE_BINARY, .is_null = false, .value = {.str = val_str}};
    row.columns = cols;
    row.num_columns = 2;
    EXPECT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  for (const auto& [key, value] : rows) {
    lean_row key_row;
    lean_datum key_cols[2];
    lean_str key_str{.data = key.data(), .size = key.size(), .capacity = 0};
    key_cols[0] = {.type = LEAN_COLUMN_TYPE_BINARY, .is_null = false, .value = {.str = key_str}};
    key_cols[1] = {.type = LEAN_COLUMN_TYPE_BINARY, .is_null = true, .value = {.str = {}}};
    key_row.columns = key_cols;
    key_row.num_columns = 2;

    lean_row out_row;
    lean_datum out_cols[2] = {};
    out_cols[0].type = LEAN_COLUMN_TYPE_BINARY;
    out_cols[1].type = LEAN_COLUMN_TYPE_BINARY;
    out_cols[0].is_null = false;
    out_cols[1].is_null = false;
    out_row.columns = out_cols;
    out_row.num_columns = 2;

    auto status = table->lookup(table, &key_row, &out_row);
    EXPECT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_EQ(std::string(out_cols[1].value.str.data, out_cols[1].value.str.size), value);
    lean_row_deinit(&out_row);
  }

  auto* cursor = table->open_cursor(table);
  ASSERT_NE(cursor, nullptr);

  ASSERT_TRUE(cursor->seek_to_first(cursor));
  size_t seen = 0;
  do {
    lean_row row;
    lean_datum cols[2] = {};
    cols[0].type = LEAN_COLUMN_TYPE_BINARY;
    cols[1].type = LEAN_COLUMN_TYPE_BINARY;
    row.columns = cols;
    row.num_columns = 2;
    cursor->current_row(cursor, &row);
    EXPECT_EQ(std::string(cols[1].value.str.data, cols[1].value.str.size), rows[seen].second);
    lean_row_deinit(&row);
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
                              .primary_key_column_indexes = pk_columns,
                              .num_primary_key_columns = 1,
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
    cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = true, .value = {.i64 = 0}};
    const char* v = "value";
    lean_str val{.data = v, .size = strlen(v), .capacity = 0};
    cols[1] = {.type = LEAN_COLUMN_TYPE_STRING, .is_null = false, .value = {.str = val}};
    lean_row row{.columns = cols, .num_columns = 2};
    EXPECT_NE(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  // Null non-nullable value column should also be rejected.
  {
    lean_datum cols[2];
    cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 1}};
    lean_str empty{.data = nullptr, .size = 0, .capacity = 0};
    cols[1] = {.type = LEAN_COLUMN_TYPE_STRING, .is_null = true, .value = {.str = empty}};
    lean_row row{.columns = cols, .num_columns = 2};
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
                              .primary_key_column_indexes = pk_columns,
                              .num_primary_key_columns = 1,
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
    cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 42}};
    cols[1] = {.type = LEAN_COLUMN_TYPE_BOOL, .is_null = false, .value = {.b = true}};
    cols[2] = {.type = LEAN_COLUMN_TYPE_INT32, .is_null = false, .value = {.i32 = -123}};
    cols[3] = {.type = LEAN_COLUMN_TYPE_UINT64, .is_null = false, .value = {.u64 = 9999}};
    cols[4] = {.type = LEAN_COLUMN_TYPE_FLOAT64, .is_null = false, .value = {.f64 = 3.14159}};
    const char* str = "hello";
    lean_str str_view{.data = str, .size = strlen(str), .capacity = 0};
    cols[5] = {.type = LEAN_COLUMN_TYPE_STRING, .is_null = false, .value = {.str = str_view}};
    lean_row row{.columns = cols, .num_columns = 6};
    EXPECT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  // Lookup and verify decoded values
  {
    lean_datum key_cols[6] = {};
    key_cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 42}};
    lean_row key_row{.columns = key_cols, .num_columns = 6};

    lean_datum out_cols[6] = {};
    out_cols[0].type = LEAN_COLUMN_TYPE_INT64;
    out_cols[1].type = LEAN_COLUMN_TYPE_BOOL;
    out_cols[2].type = LEAN_COLUMN_TYPE_INT32;
    out_cols[3].type = LEAN_COLUMN_TYPE_UINT64;
    out_cols[4].type = LEAN_COLUMN_TYPE_FLOAT64;
    out_cols[5].type = LEAN_COLUMN_TYPE_STRING;
    lean_row out_row{.columns = out_cols, .num_columns = 6};

    auto status = table->lookup(table, &key_row, &out_row);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_EQ(out_cols[0].value.i64, 42);
    EXPECT_TRUE(out_cols[1].value.b);
    EXPECT_EQ(out_cols[2].value.i32, -123);
    EXPECT_EQ(out_cols[3].value.u64, 9999u);
    EXPECT_NEAR(out_cols[4].value.f64, 3.14159, 1e-5);
    EXPECT_EQ(std::string(out_cols[5].value.str.data, out_cols[5].value.str.size), "hello");
    lean_row_deinit(&out_row);
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
                              .primary_key_column_indexes = pk_columns,
                              .num_primary_key_columns = 1,
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
    cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 1}};
    const char* v = "committed";
    lean_str val{.data = v, .size = strlen(v), .capacity = 0};
    cols[1] = {.type = LEAN_COLUMN_TYPE_STRING, .is_null = false, .value = {.str = val}};
    lean_row row{.columns = cols, .num_columns = 2};
    EXPECT_EQ(table_w->insert(table_w, &row), lean_status::LEAN_STATUS_OK);
  }
  writer->commit_tx(writer);

  // Reader should see committed row
  {
    lean_datum key_cols[2] = {};
    key_cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 1}};
    lean_row key_row{.columns = key_cols, .num_columns = 2};
    lean_datum out_cols[2] = {};
    out_cols[0].type = LEAN_COLUMN_TYPE_INT64;
    out_cols[1].type = LEAN_COLUMN_TYPE_STRING;
    lean_row out_row{.columns = out_cols, .num_columns = 2};
    auto status = table_r->lookup(table_r, &key_row, &out_row);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_EQ(std::string(out_cols[1].value.str.data, out_cols[1].value.str.size), "committed");
    lean_row_deinit(&out_row);
  }

  // Insert and abort
  writer->start_tx(writer);
  {
    lean_datum cols[2];
    cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 2}};
    const char* v = "rolledback";
    lean_str val{.data = v, .size = strlen(v), .capacity = 0};
    cols[1] = {.type = LEAN_COLUMN_TYPE_STRING, .is_null = false, .value = {.str = val}};
    lean_row row{.columns = cols, .num_columns = 2};
    EXPECT_EQ(table_w->insert(table_w, &row), lean_status::LEAN_STATUS_OK);
  }
  writer->abort_tx(writer);

  // Reader should not see aborted row
  {
    lean_datum key_cols[2] = {};
    key_cols[0] = {.type = LEAN_COLUMN_TYPE_INT64, .is_null = false, .value = {.i64 = 2}};
    lean_row key_row{.columns = key_cols, .num_columns = 2};
    lean_datum out_cols[2] = {};
    out_cols[0].type = LEAN_COLUMN_TYPE_INT64;
    out_cols[1].type = LEAN_COLUMN_TYPE_STRING;
    lean_row out_row{.columns = out_cols, .num_columns = 2};
    auto status = table_r->lookup(table_r, &key_row, &out_row);
    EXPECT_EQ(status, lean_status::LEAN_ERR_NOT_FOUND);
    lean_row_deinit(&out_row);
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
                              .primary_key_column_indexes = pk_columns,
                              .num_primary_key_columns = 1,
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
    cols[0] = {.type = LEAN_COLUMN_TYPE_FLOAT32, .is_null = false, .value = {.f32 = key}};
    lean_str val{.data = value.data(), .size = value.size(), .capacity = 0};
    cols[1] = {.type = LEAN_COLUMN_TYPE_STRING, .is_null = false, .value = {.str = val}};
    lean_row row{.columns = cols, .num_columns = 2};
    EXPECT_EQ(table->insert(table, &row), lean_status::LEAN_STATUS_OK);
  }

  // Verify lookup works.
  for (const auto& [key, value] : rows) {
    lean_datum key_cols[2] = {};
    key_cols[0] = {.type = LEAN_COLUMN_TYPE_FLOAT32, .is_null = false, .value = {.f32 = key}};
    lean_row key_row{.columns = key_cols, .num_columns = 2};

    lean_datum out_cols[2] = {};
    out_cols[0].type = LEAN_COLUMN_TYPE_FLOAT32;
    out_cols[1].type = LEAN_COLUMN_TYPE_STRING;
    lean_row out_row{.columns = out_cols, .num_columns = 2};

    auto status = table->lookup(table, &key_row, &out_row);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    EXPECT_NEAR(out_cols[0].value.f32, key, 1e-6);
    EXPECT_EQ(std::string(out_cols[1].value.str.data, out_cols[1].value.str.size), value);
    lean_row_deinit(&out_row);
  }

  // Verify cursor order matches ascending primary keys.
  auto* cursor = table->open_cursor(table);
  ASSERT_NE(cursor, nullptr);
  ASSERT_TRUE(cursor->seek_to_first(cursor));
  size_t seen = 0;
  for (auto it = rows.begin(); cursor->is_valid(cursor) && it != rows.end(); ++it) {
    lean_row row;
    lean_datum cols[2] = {};
    cols[0].type = LEAN_COLUMN_TYPE_FLOAT32;
    cols[1].type = LEAN_COLUMN_TYPE_STRING;
    row.columns = cols;
    row.num_columns = 2;
    cursor->current_row(cursor, &row);
    EXPECT_NEAR(cols[0].value.f32, it->first, 1e-6);
    EXPECT_EQ(std::string(cols[1].value.str.data, cols[1].value.str.size), it->second);
    lean_row_deinit(&row);
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
