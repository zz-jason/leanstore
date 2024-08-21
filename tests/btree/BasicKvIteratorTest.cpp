#include "leanstore-c/StoreOption.h"
#include "leanstore-c/leanstore-c.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

namespace leanstore::test {

class BasicKvIteratorTest : public ::testing::Test {
protected:
  LeanStoreHandle* mStoreHandle;
  BasicKvHandle* mKvHandle;

  void SetUp() override {
    StoreOption* option = CreateStoreOption("/tmp/leanstore/examples/BasicKvExample");
    option->mCreateFromScratch = true;
    option->mWorkerThreads = 2;
    option->mEnableBulkInsert = false;
    option->mEnableEagerGc = true;
    mStoreHandle = CreateLeanStore(option);
    ASSERT_NE(mStoreHandle, nullptr);

    mKvHandle = CreateBasicKV(mStoreHandle, 0, "testTree1");
    ASSERT_NE(mKvHandle, nullptr);
  }

  void TearDown() override {
    DestroyBasicKV(mKvHandle);
    DestroyLeanStore(mStoreHandle);
  }
};

TEST_F(BasicKvIteratorTest, BasicKvHandle) {
  // prepare 100 key-value pairs for insert
  const int numEntries = 100;
  std::vector<std::string> keys(numEntries);
  std::vector<std::string> vals(numEntries);
  for (int i = 0; i < numEntries; i++) {
    keys[i] = std::to_string(i);
    vals[i] = std::to_string(i * 2);
  }

  // insert
  for (auto i = 0; i < numEntries; i++) {
    auto succeed = BasicKvInsert(mKvHandle, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // lookup
  for (auto i = 0; i < numEntries; i++) {
    String* val = CreateString(nullptr, 0);
    bool found = BasicKvLookup(mKvHandle, 1, {keys[i].data(), keys[i].size()}, &val);
    ASSERT_TRUE(found);
    EXPECT_EQ(val->mSize, vals[i].size());
    EXPECT_EQ(memcmp(val->mData, vals[i].data(), val->mSize), 0);
  }

  // remove 50 key-value pairs
  uint64_t numEntriesToRemove = numEntries / 2;
  for (auto i = 0u; i < numEntriesToRemove; i++) {
    auto succeed = BasicKvRemove(mKvHandle, 0, {keys[i].data(), keys[i].size()});
    ASSERT_TRUE(succeed);
  }

  // numEntries
  uint64_t numEntriesRemained = BasicKvNumEntries(mKvHandle, 0);
  EXPECT_EQ(numEntriesRemained, numEntries - numEntriesToRemove);
}

TEST_F(BasicKvIteratorTest, BasicKvAssendingIterationEmpty) {
  // create iterator handle
  BasicKvIterHandle* iterHandle = CreateBasicKvIter(mKvHandle);
  ASSERT_NE(iterHandle, nullptr);

  // next without seek
  {
    BasicKvIterNext(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
  }

  // iterate ascending
  {
    BasicKvIterSeekToFirst(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));

    BasicKvIterNext(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
  }

  // seek to first greater equal
  {
    BasicKvIterSeekToFirstGreaterEqual(iterHandle, 0, {"hello", 5});
    EXPECT_FALSE(BasicKvIterValid(iterHandle));

    BasicKvIterNext(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
  }

  // destroy the iterator
  DestroyBasicKvIter(iterHandle);
}

TEST_F(BasicKvIteratorTest, BasicKvAssendingIteration) {
  // prepare 130 key-value pairs for insert
  const int numEntries = 130;
  std::vector<std::string> keys(numEntries);
  std::vector<std::string> vals(numEntries);
  std::string smallestKey{""};
  std::string biggestKey{""};
  for (int i = 0; i < numEntries; i++) {
    keys[i] = std::to_string(i);
    vals[i] = std::to_string(i * 2);
    for (int i = 0; i < numEntries; i++) {
      keys[i] = std::to_string(i);
      vals[i] = std::to_string(i * 2);
      if (i == 0 || keys[i] < smallestKey) {
        smallestKey = keys[i];
      }
      if (i == 0 || keys[i] > biggestKey) {
        biggestKey = keys[i];
      }
    }
  }
  for (auto i = 0; i < numEntries; i++) {
    auto succeed = BasicKvInsert(mKvHandle, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // create iterator handle
  BasicKvIterHandle* iterHandle = CreateBasicKvIter(mKvHandle);
  ASSERT_NE(iterHandle, nullptr);

  // next without seek
  {
    BasicKvIterNext(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
  }

  // iterate ascending
  {
    uint64_t numEntriesIterated = 0;
    for (BasicKvIterSeekToFirst(iterHandle, 0); BasicKvIterValid(iterHandle);
         BasicKvIterNext(iterHandle, 0)) {
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);
      int keyInt = std::stoi(std::string(key.mData, key.mSize));
      int valInt = std::stoi(std::string(val.mData, val.mSize));
      EXPECT_EQ(keyInt * 2, valInt);

      numEntriesIterated++;
      if (numEntriesIterated < numEntries) {
        EXPECT_TRUE(BasicKvIterHasNext(iterHandle, 0));
      } else {
        EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
      }
    }

    // iterate one more time to check if the iterator is still valid
    BasicKvIterNext(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
  }

  // iterate ascending with seek to first greater equal
  {
    for (auto i = 0; i < numEntries; i++) {
      BasicKvIterSeekToFirstGreaterEqual(iterHandle, 0, {keys[i].data(), keys[i].size()});
      ASSERT_TRUE(BasicKvIterValid(iterHandle));
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);

      if (std::string{key.mData, key.mSize} == biggestKey) {
        EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
      } else {
        EXPECT_TRUE(BasicKvIterHasNext(iterHandle, 0));
      }

      int keyInt = std::stoi(std::string(key.mData, key.mSize));
      int valInt = std::stoi(std::string(val.mData, val.mSize));
      EXPECT_EQ(keyInt * 2, valInt);
    }
  }

  // destroy the iterator
  DestroyBasicKvIter(iterHandle);
}

TEST_F(BasicKvIteratorTest, BasicKvDescendingIteration) {
  // prepare 130 key-value pairs for insert
  const int numEntries = 130;
  std::vector<std::string> keys(numEntries);
  std::vector<std::string> vals(numEntries);
  std::string smallestKey{""};
  std::string biggestKey{""};
  for (int i = 0; i < numEntries; i++) {
    keys[i] = std::to_string(i);
    vals[i] = std::to_string(i * 2);
    if (i == 0 || keys[i] < smallestKey) {
      smallestKey = keys[i];
    }
    if (i == 0 || keys[i] > biggestKey) {
      biggestKey = keys[i];
    }
  }
  for (auto i = 0; i < numEntries; i++) {
    auto succeed = BasicKvInsert(mKvHandle, 0, {keys[i].data(), keys[i].size()},
                                 {vals[i].data(), vals[i].size()});
    ASSERT_TRUE(succeed);
  }

  // create iterator handle
  BasicKvIterHandle* iterHandle = CreateBasicKvIter(mKvHandle);
  ASSERT_NE(iterHandle, nullptr);

  // iterate descending
  {
    uint64_t numEntriesIterated = 0;
    for (BasicKvIterSeekToLast(iterHandle, 0); BasicKvIterValid(iterHandle);
         BasicKvIterPrev(iterHandle, 0)) {
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);
      int keyInt = std::stoi(std::string(key.mData, key.mSize));
      int valInt = std::stoi(std::string(val.mData, val.mSize));
      EXPECT_EQ(keyInt * 2, valInt);

      numEntriesIterated++;
      if (numEntriesIterated < numEntries) {
        EXPECT_TRUE(BasicKvIterHasPrev(iterHandle, 0));
      } else {
        EXPECT_FALSE(BasicKvIterHasPrev(iterHandle, 0));
      }
    }

    // iterate one more time to check if the iterator is still valid
    BasicKvIterPrev(iterHandle, 0);
    EXPECT_FALSE(BasicKvIterValid(iterHandle));
    EXPECT_FALSE(BasicKvIterHasPrev(iterHandle, 0));
  }

  // iterate descending with seek to last less equal
  {
    for (auto i = 0; i < numEntries; i++) {
      BasicKvIterSeekToLastLessEqual(iterHandle, 0, {keys[i].data(), keys[i].size()});
      ASSERT_TRUE(BasicKvIterValid(iterHandle));
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);

      if (std::string{key.mData, key.mSize} == biggestKey) {
        EXPECT_FALSE(BasicKvIterHasNext(iterHandle, 0));
      } else {
        EXPECT_TRUE(BasicKvIterHasNext(iterHandle, 0));
      }

      int keyInt = std::stoi(std::string(key.mData, key.mSize));
      int valInt = std::stoi(std::string(val.mData, val.mSize));
      EXPECT_EQ(keyInt * 2, valInt);
    }
  }
}

} // namespace leanstore::test