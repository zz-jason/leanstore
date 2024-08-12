#include "leanstore/leanstore-c.h"

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
    mStoreHandle = CreateLeanStore(1, "/tmp/leanstore/examples/BasicKvExample", 2, 0, 1);
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
    String* valStr = BasicKvLookup(mKvHandle, 1, {keys[i].data(), keys[i].size()});
    ASSERT_NE(valStr, nullptr);
    EXPECT_EQ(valStr->mSize, vals[i].size());
    EXPECT_EQ(memcmp(valStr->mData, vals[i].data(), valStr->mSize), 0);
    DestroyString(valStr);
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

TEST_F(BasicKvIteratorTest, BasicKvAssendingIteration) {
  // prepare 130 key-value pairs for insert
  const int numEntries = 130;
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

  // create iterator handle
  BasicKvIterHandle* iterHandle = CreateBasicKvIter(mKvHandle);
  ASSERT_NE(iterHandle, nullptr);

  // iterate ascending
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

  // iterate ascending with seek to first greater equal
  for (auto i = 0; i < numEntries; i++) {
    BasicKvIterSeekToFirstGreaterEqual(iterHandle, 0, {keys[i].data(), keys[i].size()});
    ASSERT_TRUE(BasicKvIterValid(iterHandle));

    StringSlice key = BasicKvIterKey(iterHandle);
    StringSlice val = BasicKvIterVal(iterHandle);
    int keyInt = std::stoi(std::string(key.mData, key.mSize));
    int valInt = std::stoi(std::string(val.mData, val.mSize));
    EXPECT_EQ(keyInt * 2, valInt);
  }

  // destroy the iterator
  DestroyBasicKvIter(iterHandle);
}

} // namespace leanstore::test