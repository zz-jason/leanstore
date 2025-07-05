#include "leanstore-c/kv_basic.h"
#include "leanstore-c/leanstore.h"
#include "leanstore-c/store_option.h"
#include "leanstore/utils/defer.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace leanstore::test {

class BasicKvIteratorTest : public ::testing::Test {
protected:
  LeanStoreHandle* store_handle_;
  BasicKvHandle* kv_handle_;

  void SetUp() override {
    StoreOption* option = CreateStoreOption("/tmp/leanstore/examples/BasicKvExample");
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->enable_bulk_insert_ = false;
    option->enable_eager_gc_ = true;
    store_handle_ = CreateLeanStore(option);
    ASSERT_NE(store_handle_, nullptr);

    kv_handle_ = CreateBasicKv(store_handle_, 0, "testTree1");
    ASSERT_NE(kv_handle_, nullptr);
  }

  void TearDown() override {
    DestroyBasicKv(kv_handle_);
    DestroyLeanStore(store_handle_);
  }
};

TEST_F(BasicKvIteratorTest, BasicKvHandle) {
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
    auto succeed = BasicKvInsert(kv_handle_, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // lookup
  for (auto i = 0; i < num_entries; i++) {
    OwnedString* val = CreateOwnedString(nullptr, 0);
    bool found = BasicKvLookup(kv_handle_, 1, {keys[i].data(), keys[i].size()}, &val);
    ASSERT_TRUE(found);
    EXPECT_EQ(val->size_, vals[i].size());
    EXPECT_EQ(memcmp(val->data_, vals[i].data(), val->size_), 0);
  }

  // remove 50 key-value pairs
  uint64_t num_entries_to_remove = num_entries / 2;
  for (auto i = 0u; i < num_entries_to_remove; i++) {
    auto succeed = BasicKvRemove(kv_handle_, 0, {keys[i].data(), keys[i].size()});
    ASSERT_TRUE(succeed);
  }

  // numEntries
  uint64_t num_entries_remained = BasicKvNumEntries(kv_handle_, 0);
  EXPECT_EQ(num_entries_remained, num_entries - num_entries_to_remove);
}

TEST_F(BasicKvIteratorTest, BasicKvAssendingIterationEmpty) {
  // create iterator handle
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_, 0);
  ASSERT_NE(iter_handle, nullptr);

  // next without seek
  {
    BasicKvIterNext(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
  }

  // iterate ascending
  {
    BasicKvIterSeekToFirst(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle));

    BasicKvIterNext(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
  }

  // seek to first greater equal
  {
    BasicKvIterSeekToFirstGreaterEqual(iter_handle, {"hello", 5});
    EXPECT_FALSE(BasicKvIterValid(iter_handle));

    BasicKvIterNext(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
  }

  // destroy the iterator
  DestroyBasicKvIter(iter_handle);
}

TEST_F(BasicKvIteratorTest, BasicKvAssendingIteration) {
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
    auto succeed = BasicKvInsert(kv_handle_, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // create iterator handle
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_, 0);
  ASSERT_NE(iter_handle, nullptr);

  // next without seek
  {
    BasicKvIterNext(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
  }

  // iterate ascending
  {
    uint64_t num_entries_iterated = 0;
    for (BasicKvIterSeekToFirst(iter_handle); BasicKvIterValid(iter_handle);
         BasicKvIterNext(iter_handle)) {
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);
      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);

      num_entries_iterated++;
      if (num_entries_iterated < num_entries) {
        EXPECT_TRUE(BasicKvIterHasNext(iter_handle));
      } else {
        EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
      }
    }

    // iterate one more time to check if the iterator is still valid
    BasicKvIterNext(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
  }

  // iterate ascending with seek to first greater equal
  {
    for (auto i = 0; i < num_entries; i++) {
      BasicKvIterSeekToFirstGreaterEqual(iter_handle, {keys[i].data(), keys[i].size()});
      ASSERT_TRUE(BasicKvIterValid(iter_handle));
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);

      if (std::string{key.data_, key.size_} == biggest_key) {
        EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
      } else {
        EXPECT_TRUE(BasicKvIterHasNext(iter_handle));
      }

      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);
    }
  }

  // destroy the iterator
  DestroyBasicKvIter(iter_handle);
}

TEST_F(BasicKvIteratorTest, BasicKvDescendingIteration) {
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
    auto succeed = BasicKvInsert(kv_handle_, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // create iterator handle
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_, 0);
  ASSERT_NE(iter_handle, nullptr);

  // iterate descending
  {
    uint64_t num_entries_iterated = 0;
    for (BasicKvIterSeekToLast(iter_handle); BasicKvIterValid(iter_handle);
         BasicKvIterPrev(iter_handle)) {
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);
      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);

      num_entries_iterated++;
      if (num_entries_iterated < num_entries) {
        EXPECT_TRUE(BasicKvIterHasPrev(iter_handle));
      } else {
        EXPECT_FALSE(BasicKvIterHasPrev(iter_handle));
      }
    }

    // iterate one more time to check if the iterator is still valid
    BasicKvIterPrev(iter_handle);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasPrev(iter_handle));
  }

  // iterate descending with seek to last less equal
  {
    for (auto i = 0; i < num_entries; i++) {
      BasicKvIterSeekToLastLessEqual(iter_handle, {keys[i].data(), keys[i].size()});
      ASSERT_TRUE(BasicKvIterValid(iter_handle));
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);

      if (std::string{key.data_, key.size_} == biggest_key) {
        EXPECT_FALSE(BasicKvIterHasNext(iter_handle));
      } else {
        EXPECT_TRUE(BasicKvIterHasNext(iter_handle));
      }

      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);
    }
  }

  DestroyBasicKvIter(iter_handle);
}

TEST_F(BasicKvIteratorTest, BasicKvIterMut) {
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
    auto succeed = BasicKvInsert(kv_handle_, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // create iterator handle
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_, 0);
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