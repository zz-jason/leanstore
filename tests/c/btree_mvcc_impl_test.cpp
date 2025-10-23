#include "lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"

#include <gtest/gtest.h>

#include <cassert>

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

  lean_store* store = nullptr;
  ASSERT_EQ(lean_open_store(option, &store), lean_status::LEAN_STATUS_OK);
  ASSERT_NE(store, nullptr);

  lean_session* s0 = store->connect(store);
  ASSERT_NE(s0, nullptr);

  NEW_STR_VIEW(key_view_1, "11111111");
  NEW_STR_VIEW(key_view_2, "22222222");
  NEW_STR_VIEW(key_view_3, "33333333");
  NEW_STR_VIEW(key_view_4, "44444444");

  NEW_STR_VIEW(val_view_1, "val11111");
  NEW_STR_VIEW(val_view_2, "val22222");
  NEW_STR_VIEW(val_view_3, "val33333");
  NEW_STR_VIEW(val_view_4, "val44444");

  NEW_STR_VIEW(new_val_view_2, "new_val2");

  // create btree mvcc in session 0
  const char* btree_name = "test_btree_mvcc";
  ASSERT_EQ(s0->create_btree(s0, btree_name, lean_btree_type::LEAN_BTREE_TYPE_MVCC),
            lean_status::LEAN_STATUS_OK);

  // insert 4 values in session 0
  {
    lean_btree* btree_s0 = s0->get_btree(s0, btree_name);
    ASSERT_NE(btree_s0, nullptr);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_1, val_view_1), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_2, val_view_2), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_3, val_view_3), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_4, val_view_4), lean_status::LEAN_STATUS_OK);
    btree_s0->close(btree_s0);
  }

  // remove key_view_2 and reinsert it with a new value
  {
    lean_btree* btree_s0 = s0->get_btree(s0, btree_name);
    ASSERT_NE(btree_s0, nullptr);
    ASSERT_EQ(btree_s0->remove(btree_s0, key_view_2), lean_status::LEAN_STATUS_OK);
    ASSERT_EQ(btree_s0->insert(btree_s0, key_view_2, new_val_view_2), lean_status::LEAN_STATUS_OK);
    btree_s0->close(btree_s0);
  }

  // read all values and check key_view_2 has the new value
  {
    lean_str value;
    lean_str_init(&value, 16);

    lean_btree* btree_s0 = s0->get_btree(s0, btree_name);
    ASSERT_NE(btree_s0, nullptr);

    ASSERT_EQ(btree_s0->lookup(btree_s0, key_view_1, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_1);

    ASSERT_EQ(btree_s0->lookup(btree_s0, key_view_2, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, new_val_view_2);

    ASSERT_EQ(btree_s0->lookup(btree_s0, key_view_3, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_3);

    ASSERT_EQ(btree_s0->lookup(btree_s0, key_view_4, &value), lean_status::LEAN_STATUS_OK);
    MustEqual(value, val_view_4);

    btree_s0->close(btree_s0);
    lean_str_deinit(&value);
  }

  s0->close(s0);
  store->close(store);
}

} // namespace leanstore::test