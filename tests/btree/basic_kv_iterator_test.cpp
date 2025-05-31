#include "leanstore-c/kv_basic.h"
#include "leanstore-c/leanstore.h"
#include "leanstore-c/store_option.h"

#include <gtest/gtest.h>

#include <cstdint>
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
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_);
  ASSERT_NE(iter_handle, nullptr);

  // next without seek
  {
    BasicKvIterNext(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
  }

  // iterate ascending
  {
    BasicKvIterSeekToFirst(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));

    BasicKvIterNext(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
  }

  // seek to first greater equal
  {
    BasicKvIterSeekToFirstGreaterEqual(iter_handle, 0, {"hello", 5});
    EXPECT_FALSE(BasicKvIterValid(iter_handle));

    BasicKvIterNext(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
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
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_);
  ASSERT_NE(iter_handle, nullptr);

  // next without seek
  {
    BasicKvIterNext(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
  }

  // iterate ascending
  {
    uint64_t num_entries_iterated = 0;
    for (BasicKvIterSeekToFirst(iter_handle, 0); BasicKvIterValid(iter_handle);
         BasicKvIterNext(iter_handle, 0)) {
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);
      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);

      num_entries_iterated++;
      if (num_entries_iterated < num_entries) {
        EXPECT_TRUE(BasicKvIterHasNext(iter_handle, 0));
      } else {
        EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
      }
    }

    // iterate one more time to check if the iterator is still valid
    BasicKvIterNext(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
  }

  // iterate ascending with seek to first greater equal
  {
    for (auto i = 0; i < num_entries; i++) {
      BasicKvIterSeekToFirstGreaterEqual(iter_handle, 0, {keys[i].data(), keys[i].size()});
      ASSERT_TRUE(BasicKvIterValid(iter_handle));
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);

      if (std::string{key.data_, key.size_} == biggest_key) {
        EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
      } else {
        EXPECT_TRUE(BasicKvIterHasNext(iter_handle, 0));
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
  BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle_);
  ASSERT_NE(iter_handle, nullptr);

  // iterate descending
  {
    uint64_t num_entries_iterated = 0;
    for (BasicKvIterSeekToLast(iter_handle, 0); BasicKvIterValid(iter_handle);
         BasicKvIterPrev(iter_handle, 0)) {
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);
      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);

      num_entries_iterated++;
      if (num_entries_iterated < num_entries) {
        EXPECT_TRUE(BasicKvIterHasPrev(iter_handle, 0));
      } else {
        EXPECT_FALSE(BasicKvIterHasPrev(iter_handle, 0));
      }
    }

    // iterate one more time to check if the iterator is still valid
    BasicKvIterPrev(iter_handle, 0);
    EXPECT_FALSE(BasicKvIterValid(iter_handle));
    EXPECT_FALSE(BasicKvIterHasPrev(iter_handle, 0));
  }

  // iterate descending with seek to last less equal
  {
    for (auto i = 0; i < num_entries; i++) {
      BasicKvIterSeekToLastLessEqual(iter_handle, 0, {keys[i].data(), keys[i].size()});
      ASSERT_TRUE(BasicKvIterValid(iter_handle));
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);

      if (std::string{key.data_, key.size_} == biggest_key) {
        EXPECT_FALSE(BasicKvIterHasNext(iter_handle, 0));
      } else {
        EXPECT_TRUE(BasicKvIterHasNext(iter_handle, 0));
      }

      int key_int = std::stoi(std::string(key.data_, key.size_));
      int val_int = std::stoi(std::string(val.data_, val.size_));
      EXPECT_EQ(key_int * 2, val_int);
    }
  }
}

} // namespace leanstore::test