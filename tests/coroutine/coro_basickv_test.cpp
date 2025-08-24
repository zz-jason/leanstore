#include "lean_test_suite.hpp"
#include "leanstore-c/kv_basic.h"
#include "leanstore-c/leanstore.h"
#include "leanstore-c/store_option.h"
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
    StoreOption* option = CreateStoreOption(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->max_concurrent_transaction_per_worker_ = 4;
    option->enable_bulk_insert_ = false;
    option->enable_eager_gc_ = true;
    store_handle_ = CreateLeanStore(option);
    ASSERT_NE(store_handle_, nullptr);

    // connect to leanstore
    auto* session_handle = LeanStoreConnect(store_handle_);
    assert(session_handle != nullptr);
    SCOPED_DEFER(LeanStoreDisconnect(session_handle));

    kv_handle_ = CoroCreateBasicKv(session_handle, "test_tree_0");
    ASSERT_NE(kv_handle_, nullptr);
  }

  void TearDown() override {
    DestroyBasicKv(kv_handle_);
    DestroyLeanStore(store_handle_);
  }

protected:
  LeanStoreHandle* store_handle_;
  BasicKvHandle* kv_handle_;
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
  auto* session_handle = LeanStoreConnect(store_handle_);
  assert(session_handle != nullptr);
  SCOPED_DEFER(LeanStoreDisconnect(session_handle));

  // insert
  for (auto i = 0; i < num_entries; i++) {
    auto succeed = CoroBasicKvInsert(kv_handle_, session_handle, {keys[i].data(), keys[i].size()},
                                     {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // create iterator handle
  BasicKvIterHandle* iter_handle = CoroCreateBasicKvIter(kv_handle_, session_handle);
  ASSERT_NE(iter_handle, nullptr);
  SCOPED_DEFER(DestroyBasicKvIter(iter_handle));

  BasicKvIterSeekToFirst(iter_handle);
  auto key1 = BasicKvIterKey(iter_handle);
  auto val1 = BasicKvIterVal(iter_handle);
  EXPECT_TRUE(BasicKvIterValid(iter_handle));
  EXPECT_TRUE(BasicKvIterHasNext(iter_handle));

  BasicKvIterMutHandle* iter_mut_handle = IntoBasicKvIterMut(iter_handle);
  ASSERT_NE(iter_mut_handle, nullptr);
  ASSERT_FALSE(BasicKvIterValid(iter_handle));
  ASSERT_TRUE(BasicKvIterMutValid(iter_mut_handle));
  SCOPED_DEFER(DestroyBasicKvIterMut(iter_mut_handle));

  auto key2 = BasicKvIterMutKey(iter_mut_handle);
  auto val2 = BasicKvIterMutVal(iter_mut_handle);
  EXPECT_EQ(key1.size_, key2.size_);
  EXPECT_EQ(val1.size_, val2.size_);
  EXPECT_EQ(memcmp(key1.data_, key2.data_, key1.size_), 0);
  EXPECT_EQ(memcmp(val1.data_, val2.data_, val1.size_), 0);

  // remove current key
  BasicKvIterMutRemove(iter_mut_handle);
  EXPECT_TRUE(BasicKvIterMutValid(iter_mut_handle));
  auto key3 = BasicKvIterMutKey(iter_mut_handle);
  EXPECT_EQ(key1.size_, key3.size_);
  EXPECT_NE(memcmp(key1.data_, key3.data_, key1.size_), 0);

  // insert a new key-value pair
  auto* new_key = "new_key";
  auto* new_val = "new_val";
  EXPECT_TRUE(BasicKvIterMutInsert(iter_mut_handle, {new_key, strlen(new_key)},
                                   {new_val, strlen(new_val)}));
  EXPECT_TRUE(BasicKvIterMutValid(iter_mut_handle));
}

} // namespace leanstore::test