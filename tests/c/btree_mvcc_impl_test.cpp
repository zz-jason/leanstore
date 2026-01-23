#include "common/lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/c/status.h"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <cstdlib>
#include <string>
#include <unordered_map>

namespace leanstore::test {

#define NEW_STR_VIEW(var_name, c_str) lean_str_view var_name(c_str, strlen(c_str))

#define TMP_STR_VIEW(c_str) lean_str_view(c_str, strlen(c_str))

class BTreeMvccImplTest : public LeanTestSuite {
protected:
  void MustEqual(const lean_str& lhs, const lean_str_view& rhs) {
    ASSERT_EQ(lhs.size, rhs.size);
    ASSERT_EQ(strncmp(lhs.data, rhs.data, rhs.size), 0);
  }
};

TEST_F(BTreeMvccImplTest, CursorMvccAsc) {
  lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = true;
  option->worker_threads_ = 2;
  option->max_concurrent_transaction_per_worker_ = 4;

  lean_store* store = nullptr;
  ASSERT_EQ(lean_open_store(option, &store), lean_status::LEAN_STATUS_OK);
  ASSERT_NE(store, nullptr);

  lean_session* s0 = store->connect(store);
  lean_session* s1 = store->connect(store);
  ASSERT_NE(s0, nullptr);
  ASSERT_NE(s1, nullptr);

  lean_str key, value;
  lean_str_init(&key, 8);
  lean_str_init(&value, 8);

  lean_str_view key_view_1{"key1", 4};
  lean_str_view key_view_2{"key2", 4};
  lean_str_view key_view_11{"key11", 5};
  lean_str_view val_view_1{"val1", 4};
  lean_str_view val_view_2{"value2", 6};
  lean_str_view val_view_11{"value11", 7};

  // create btree mvcc in session 0
  const char* btree_name = "test_btree_mvcc";
  ASSERT_EQ(s0->create_btree(s0, btree_name, lean_btree_type::LEAN_BTREE_TYPE_MVCC),
            lean_status::LEAN_STATUS_OK);

  // insert 2 values in session 0
  {
    lean_btree* btree_s0 = s0->get_btree(s0, btree_name);
    ASSERT_NE(btree_s0, nullptr);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_1, val_view_1), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_2, val_view_2), lean_status::LEAN_STATUS_OK);
    btree_s0->close(btree_s0);
  }

  // read the values in session 1
  {
    lean_btree* btree_s1 = s1->get_btree(s1, btree_name);
    ASSERT_NE(btree_s1, nullptr);

    ASSERT_EQ(btree_s1->lookup(btree_s1, key_view_1, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_1);

    ASSERT_EQ(btree_s1->lookup(btree_s1, key_view_2, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_2);

    btree_s1->close(btree_s1);
  }

  // read with cursor in session 0
  s0->start_tx(s0);
  lean_btree* btree_s0 = s0->get_btree(s0, btree_name);
  lean_cursor* cursor_s0 = btree_s0->open_cursor(btree_s0);
  ASSERT_FALSE(cursor_s0->is_valid(cursor_s0));

  ASSERT_TRUE(cursor_s0->seek_to_first(cursor_s0));
  ASSERT_TRUE(cursor_s0->is_valid(cursor_s0));
  {
    cursor_s0->current_key(cursor_s0, &key);
    cursor_s0->current_value(cursor_s0, &value);
    MustEqual(key, key_view_1);
    MustEqual(value, val_view_1);
  }

  // insert in session 2
  {
    lean_btree* btree_s1 = s1->get_btree(s1, btree_name);
    ASSERT_NE(btree_s1, nullptr);
    ASSERT_EQ(btree_s1->insert(btree_s1, key_view_11, val_view_11), lean_status::LEAN_STATUS_OK);

    // can be read in another transaction
    ASSERT_EQ(btree_s1->lookup(btree_s1, key_view_11, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_11);
    btree_s1->close(btree_s1);
  }

  // continue read in session 0
  ASSERT_TRUE(cursor_s0->next(cursor_s0));
  ASSERT_TRUE(cursor_s0->is_valid(cursor_s0));
  {
    cursor_s0->current_key(cursor_s0, &key);
    cursor_s0->current_value(cursor_s0, &value);
    MustEqual(key, key_view_2);
    MustEqual(value, val_view_2);
  }

  ASSERT_FALSE(cursor_s0->next(cursor_s0));
  ASSERT_FALSE(cursor_s0->is_valid(cursor_s0));
  s0->commit_tx(s0);

  cursor_s0->close(cursor_s0);
  btree_s0->close(btree_s0);

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  s0->close(s0);
  s1->close(s1);
  store->close(store);
}

TEST_F(BTreeMvccImplTest, InsertAfterRemove) {
  lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = true;
  option->worker_threads_ = 2;
  option->max_concurrent_transaction_per_worker_ = 4;
  auto num_keys = 100u;
  auto max_value_size = 600u;
  auto min_value_size = 100u;

  lean_store* store = nullptr;
  ASSERT_EQ(lean_open_store(option, &store), lean_status::LEAN_STATUS_OK);
  ASSERT_NE(store, nullptr);

  lean_session* s0 = store->connect(store);
  ASSERT_NE(s0, nullptr);

  // create btree mvcc in session 0
  const char* btree_name = "test_btree_mvcc";
  ASSERT_EQ(s0->create_btree(s0, btree_name, lean_btree_type::LEAN_BTREE_TYPE_MVCC),
            lean_status::LEAN_STATUS_OK);
  lean_btree* btree_s0 = s0->get_btree(s0, btree_name);
  ASSERT_NE(btree_s0, nullptr);

  // insert initial keys
  std::unordered_map<std::string, std::string> all_keys;
  for (auto i = 0U; i < num_keys; i++) {
    auto key_str = "key_" + std::to_string(i);
    auto val_str = utils::RandomGenerator::RandAlphString(
        utils::RandomGenerator::RandU64(min_value_size, max_value_size));
    auto key_view = TMP_STR_VIEW(key_str.c_str());
    auto val_view = TMP_STR_VIEW(val_str.c_str());
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view, val_view), lean_status::LEAN_STATUS_OK);
    all_keys.emplace(key_str, val_str);
  }

  // check all key-values
  auto check_all = [&]() {
    lean_str value;
    lean_str_init(&value, max_value_size + 1);
    for (auto i = 0U; i < num_keys; i++) {
      auto key_str = "key_" + std::to_string(i);
      auto key_view = TMP_STR_VIEW(key_str.c_str());
      ASSERT_EQ(btree_s0->lookup(btree_s0, key_view, &value), lean_status::LEAN_STATUS_OK);
      MustEqual(value, TMP_STR_VIEW(all_keys[key_str].c_str()));
    }
    lean_str_deinit(&value);
  };

  // remove and insert a random key 1000000 times
  for (auto i = 0U; i < 100; i++) {
    // key to be removed and inserted
    auto key_idx = utils::RandomGenerator::RandU64(0, num_keys);
    auto key_str = "key_" + std::to_string(key_idx);
    auto key_view = TMP_STR_VIEW(key_str.c_str());

    // value buffer for lookup
    lean_str value;
    lean_str_init(&value, max_value_size + 1);

    // insert and remove in a transaction
    s0->start_tx(s0);
    auto val_new = utils::RandomGenerator::RandAlphString(
        utils::RandomGenerator::RandU64(min_value_size, max_value_size));
    auto val_view_new = TMP_STR_VIEW(val_new.c_str());
    ASSERT_EQ(btree_s0->remove(btree_s0, key_view), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view, val_view_new), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->lookup(btree_s0, key_view, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_new);
    s0->commit_tx(s0);
    all_keys[key_str] = val_new;

    // check all key-values
    check_all();

    lean_str_deinit(&value);
  }

  btree_s0->close(btree_s0);
  s0->close(s0);
  store->close(store);
}

} // namespace leanstore::test
