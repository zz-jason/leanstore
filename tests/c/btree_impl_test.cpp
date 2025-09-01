#include "lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/common/types.h"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace leanstore::test {

class BTreeImplTest : public LeanTestSuite {
protected:
  void SetUp() override {
    lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->enable_bulk_insert_ = false;
    option->enable_eager_gc_ = true;

    auto status = lean_open_store(option, &store_);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    ASSERT_NE(store_, nullptr);

    auto* btree_name = "test_tree_0";
    session_ = store_->connect(store_);
    ASSERT_NE(session_, nullptr);

    status = session_->create_btree(session_, btree_name, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);

    btree_ = session_->get_btree(session_, btree_name);
    ASSERT_NE(btree_, nullptr);
  }

  void TearDown() override {
    btree_->close(btree_);
    session_->close(session_);
    store_->close(store_);
  }

  lean_store* store_;
  lean_session* session_;
  lean_btree* btree_;
};

TEST_F(BTreeImplTest, BTreeOperations) {
  // prepare 100 key-value pairs for insert
  const int num_entries = 100;
  std::vector<std::string> keys(num_entries);
  std::vector<std::string> vals(num_entries);
  for (int i = 0; i < num_entries; i++) {
    keys[i] = std::to_string(i);
    vals[i] = std::to_string(i * 2);
  }

  // insert
  for (auto i = 0; i < num_entries; i++) {
    lean_str_view key = {keys[i].data(), keys[i].size()};
    lean_str_view val = {vals[i].data(), vals[i].size()};
    ASSERT_EQ(btree_->insert(btree_, key, val), lean_status::LEAN_STATUS_OK);
  }

  // lookup
  lean_str val_lookup;
  lean_str_init(&val_lookup, 8);
  for (auto i = 0; i < num_entries; i++) {
    auto status = btree_->lookup(btree_, {keys[i].data(), keys[i].size()}, &val_lookup);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(val_lookup.size, vals[i].size());
    ASSERT_EQ(memcmp(val_lookup.data, vals[i].data(), val_lookup.size), 0);
  }

  // remove 50 key-value pairs
  uint64_t num_entries_to_remove = num_entries / 2;
  for (auto i = 0u; i < num_entries_to_remove; i++) {
    auto status = btree_->remove(btree_, {keys[i].data(), keys[i].size()});
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
  }
}

TEST_F(BTreeImplTest, CursorOnEmptyBTree) {
  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  // next without seek
  {
    ASSERT_FALSE(cursor->next(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));
  }

  // iterate ascending
  {
    ASSERT_FALSE(cursor->seek_to_first(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));

    ASSERT_FALSE(cursor->next(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));
  }

  // seek to first greater equal
  {
    ASSERT_FALSE(cursor->seek_to_first_ge(cursor, {"hello", 5}));
    ASSERT_FALSE(cursor->is_valid(cursor));

    ASSERT_FALSE(cursor->next(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));
  }

  // destroy the cursor
  cursor->close(cursor);
}

TEST_F(BTreeImplTest, CursorAscendingIteration) {
  // prepare 130 key-value pairs for insert
  const int num_entries = 130;
  std::vector<std::string> keys(num_entries);
  std::vector<std::string> vals(num_entries);
  std::string smallest_key{""};
  std::string biggest_key{""};
  for (int i = 0; i < num_entries; i++) {
    keys[i] = std::to_string(i);
    vals[i] = std::to_string(i * 2);
    for (int i = 0; i < num_entries; i++) {
      keys[i] = std::to_string(i);
      vals[i] = std::to_string(i * 2);
      if (i == 0 || keys[i] < smallest_key) {
        smallest_key = keys[i];
      }
      if (i == 0 || keys[i] > biggest_key) {
        biggest_key = keys[i];
      }
    }
  }
  for (auto i = 0; i < num_entries; i++) {
    lean_str_view key = {keys[i].data(), keys[i].size()};
    lean_str_view val = {vals[i].data(), vals[i].size()};
    ASSERT_EQ(btree_->insert(btree_, key, val), lean_status::LEAN_STATUS_OK);
  }

  // create iterator handle
  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  // next without seek
  {
    ASSERT_FALSE(cursor->next(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));
  }

  // iterate ascending
  {
    lean_str key, value;
    lean_str_init(&key, 8);
    lean_str_init(&value, 8);

    for (cursor->seek_to_first(cursor); cursor->is_valid(cursor); cursor->next(cursor)) {
      cursor->current_key(cursor, &key);
      cursor->current_value(cursor, &value);
      int key_int = std::stoi(std::string(key.data, key.size));
      int val_int = std::stoi(std::string(value.data, value.size));
      ASSERT_EQ(key_int * 2, val_int);
    }

    // iterate one more time to check if the iterator is still valid
    ASSERT_FALSE(cursor->next(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));

    lean_str_deinit(&key);
    lean_str_deinit(&value);
  }

  // iterate ascending with seek to first greater equal
  {
    lean_str key, value;
    lean_str_init(&key, 8);
    lean_str_init(&value, 8);

    for (auto i = 0; i < num_entries; i++) {
      ASSERT_TRUE(cursor->seek_to_first_ge(cursor, {keys[i].data(), keys[i].size()}));
      ASSERT_TRUE(cursor->is_valid(cursor));

      cursor->current_key(cursor, &key);
      cursor->current_value(cursor, &value);
      int key_int = std::stoi(std::string(key.data, key.size));
      int val_int = std::stoi(std::string(value.data, value.size));
      ASSERT_EQ(key_int * 2, val_int);
    }

    lean_str_deinit(&key);
    lean_str_deinit(&value);
  }

  // destroy the cursor
  cursor->close(cursor);
}

TEST_F(BTreeImplTest, CursorDescendingIteration) {
  // prepare 130 key-value pairs for insert
  const int num_entries = 130;
  std::vector<std::string> keys(num_entries);
  std::vector<std::string> vals(num_entries);
  std::string smallest_key{""};
  std::string biggest_key{""};
  for (int i = 0; i < num_entries; i++) {
    keys[i] = std::to_string(i);
    vals[i] = std::to_string(i * 2);
    if (i == 0 || keys[i] < smallest_key) {
      smallest_key = keys[i];
    }
    if (i == 0 || keys[i] > biggest_key) {
      biggest_key = keys[i];
    }
  }
  for (auto i = 0; i < num_entries; i++) {
    lean_str_view key = {keys[i].data(), keys[i].size()};
    lean_str_view val = {vals[i].data(), vals[i].size()};
    ASSERT_EQ(btree_->insert(btree_, key, val), lean_status::LEAN_STATUS_OK);
  }

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 8);
  lean_str_init(&value, 8);

  // iterate descending
  {

    for (cursor->seek_to_last(cursor); cursor->is_valid(cursor); cursor->prev(cursor)) {
      cursor->current_key(cursor, &key);
      cursor->current_value(cursor, &value);
      int key_int = std::stoi(std::string(key.data, key.size));
      int val_int = std::stoi(std::string(value.data, value.size));
      ASSERT_EQ(key_int * 2, val_int);
    }

    // iterate one more time to check if the iterator is still valid
    ASSERT_FALSE(cursor->prev(cursor));
    ASSERT_FALSE(cursor->is_valid(cursor));
  }

  // iterate descending with seek to last less equal
  {
    for (auto i = 0; i < num_entries; i++) {
      ASSERT_TRUE(cursor->seek_to_last_le(cursor, {keys[i].data(), keys[i].size()}));
      ASSERT_TRUE(cursor->is_valid(cursor));
      cursor->current_key(cursor, &key);
      cursor->current_value(cursor, &value);
      int key_int = std::stoi(std::string(key.data, key.size));
      int val_int = std::stoi(std::string(value.data, value.size));
      ASSERT_EQ(key_int * 2, val_int);
    }
  }

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

} // namespace leanstore::test