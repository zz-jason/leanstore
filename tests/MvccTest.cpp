#include "leanstore-c/StoreOption.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <memory>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class MvccTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;
  std::string mTreeName;
  TransactionKV* mBTree;

protected:
  // create a leanstore instance for current test case
  MvccTest() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" + std::string(curTest->name());
    auto storeDirStr = "/tmp/" + curTestName;
    StoreOption* option = CreateStoreOption(storeDirStr.c_str());
    option->mCreateFromScratch = true;
    option->mWorkerThreads = 3;
    auto res = LeanStore::Open(option);
    mStore = std::move(res.value());
  }

  ~MvccTest() = default;

  void SetUp() override {
    // create a btree name for test
    mTreeName = RandomGenerator::RandAlphString(10);
    mStore->ExecSync(0, [&]() {
      auto res = mStore->CreateTransactionKV(mTreeName);
      ASSERT_TRUE(res);
      mBTree = res.value();
      ASSERT_NE(mBTree, nullptr);
    });
  }

  void TearDown() override {
    mStore->ExecSync(1, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      mStore->DropTransactionKV(mTreeName);
    });
  }
};

TEST_F(MvccTest, LookupWhileInsert) {
  // insert a base record
  auto key0 = RandomGenerator::RandAlphString(42);
  auto val0 = RandomGenerator::RandAlphString(151);
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const uint8_t*)key0.data(), key0.size()),
                              Slice((const uint8_t*)val0.data(), val0.size()));
    cr::Worker::My().CommitTx();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert another record, don't commit
  auto key1 = RandomGenerator::RandAlphString(17);
  auto val1 = RandomGenerator::RandAlphString(131);
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to lookup the base record
  // the lookup should not be blocked
  mStore->ExecSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::My().StartTx();
    EXPECT_EQ(mBTree->Lookup(Slice((const uint8_t*)key0.data(), key0.size()), copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val0);
    cr::Worker::My().CommitTx();
  });

  // commit the transaction
  mStore->ExecSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(mBTree->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });

  // now we can see the latest record
  mStore->ExecSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::My().StartTx();
    EXPECT_EQ(mBTree->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(MvccTest, InsertConflict) {
  // insert a base record
  auto key0 = RandomGenerator::RandAlphString(42);
  auto val0 = RandomGenerator::RandAlphString(151);
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const uint8_t*)key0.data(), key0.size()),
                              Slice((const uint8_t*)val0.data(), val0.size()));
    cr::Worker::My().CommitTx();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert a bigger key, don't commit
  auto key1 = key0 + "a";
  auto val1 = val0;
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start another transaction to insert the same key
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    cr::Worker::My().AbortTx();
  });

  // start another transaction to insert a smaller key
  auto key2 = std::string(key0.data(), key0.size() - 1);
  auto val2 = val0;
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx();
    auto res = mBTree->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    cr::Worker::My().AbortTx();
  });

  // commit the transaction
  mStore->ExecSync(1, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(mBTree->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });

  // now we can see the latest record
  mStore->ExecSync(2, [&]() {
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    cr::Worker::My().StartTx();
    EXPECT_EQ(mBTree->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copyValueOut),
              OpCode::kOK);
    EXPECT_EQ(copiedValue, val1);
    cr::Worker::My().CommitTx();
  });
}

} // namespace leanstore::test
