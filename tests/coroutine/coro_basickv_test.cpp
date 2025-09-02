#include "lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/common/types.h"
#include "leanstore/utils/defer.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <cstring>
#include <string>
#include <vector>

namespace leanstore::test {

class CoroBasicKvTest : public LeanTestSuite {
public:
  void SetUp() override {
    lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->max_concurrent_transaction_per_worker_ = 4;
    option->enable_bulk_insert_ = false;
    option->enable_eager_gc_ = true;
    ASSERT_EQ(lean_open_store(option, &store_), lean_status::LEAN_STATUS_OK);

    // connect to leanstore
    lean_session* sess = store_->connect(store_);
    ASSERT_NE(sess, nullptr);
    SCOPED_DEFER(sess->close(sess));

    auto status = sess->create_btree(sess, btree_name_, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
  }

  void TearDown() override {
    store_->close(store_);
  }

protected:
  lean_store* store_;
  const char* btree_name_ = "test_tree_0";
};

TEST_F(CoroBasicKvTest, BasicKvIterMut) {
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

  // connect to leanstore
  auto* s0 = store_->connect(store_);
  ASSERT_NE(s0, nullptr);
  SCOPED_DEFER(s0->close(s0));

  auto* btree_s0 = s0->get_btree(s0, btree_name_);
  ASSERT_NE(btree_s0, nullptr);

  // insert
  for (auto i = 0; i < num_entries; i++) {
    auto key = lean_str_view{keys[i].data(), keys[i].size()};
    auto val = lean_str_view{vals[i].data(), vals[i].size()};
    ASSERT_EQ(btree_s0->insert(btree_s0, key, val), lean_status::LEAN_STATUS_OK);
  }

  // create cursor
  auto* cursor_s0 = btree_s0->open_cursor(btree_s0);
  ASSERT_NE(cursor_s0, nullptr);
  SCOPED_DEFER(cursor_s0->close(cursor_s0));

  lean_str key, value;
  lean_str_init(&key, 8);
  lean_str_init(&value, 8);
  SCOPED_DEFER({
    lean_str_deinit(&key);
    lean_str_deinit(&value);
  });

  cursor_s0->seek_to_first(cursor_s0);
  ASSERT_TRUE(cursor_s0->is_valid(cursor_s0));
  cursor_s0->current_key(cursor_s0, &key);
  cursor_s0->current_value(cursor_s0, &value);

  auto* cursor_s0_another = btree_s0->open_cursor(btree_s0);
  ASSERT_NE(cursor_s0_another, nullptr);
  SCOPED_DEFER(cursor_s0_another->close(cursor_s0_another));

  lean_str key_another, value_another;
  lean_str_init(&key_another, 8);
  lean_str_init(&value_another, 8);
  SCOPED_DEFER({
    lean_str_deinit(&key_another);
    lean_str_deinit(&value_another);
  });

  cursor_s0_another->seek_to_first(cursor_s0_another);
  ASSERT_TRUE(cursor_s0_another->is_valid(cursor_s0_another));
  cursor_s0_another->current_key(cursor_s0_another, &key_another);
  cursor_s0_another->current_value(cursor_s0_another, &value_another);

  EXPECT_EQ(key.size, key_another.size);
  EXPECT_EQ(value.size, value_another.size);
  EXPECT_EQ(memcmp(key.data, key_another.data, key.size), 0);
  EXPECT_EQ(memcmp(value.data, value_another.data, value.size), 0);

  // remove current key
  cursor_s0_another->remove_current(cursor_s0_another);
  EXPECT_TRUE(cursor_s0_another->is_valid(cursor_s0_another));

  lean_str key_after_remove, value_after_remove;
  lean_str_init(&key_after_remove, 8);
  lean_str_init(&value_after_remove, 8);
  SCOPED_DEFER({
    lean_str_deinit(&key_after_remove);
    lean_str_deinit(&value_after_remove);
  });

  cursor_s0_another->current_key(cursor_s0_another, &key_after_remove);
  cursor_s0_another->current_value(cursor_s0_another, &value_after_remove);
  EXPECT_EQ(key_after_remove.size, key.size);
  EXPECT_EQ(memcmp(key_after_remove.data, key.data, key.size), 0);
  EXPECT_EQ(value_after_remove.size, value.size);
  EXPECT_EQ(memcmp(value_after_remove.data, value.data, value.size), 0);

  // insert a new key-value pair
  auto* new_key = "new_key";
  auto* new_val = "new_val";
  auto status = btree_s0->insert(btree_s0, {new_key, strlen(new_key)}, {new_val, strlen(new_val)});
  ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
}

} // namespace leanstore::test