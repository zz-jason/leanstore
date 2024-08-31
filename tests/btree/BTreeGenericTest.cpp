#include "leanstore-c/StoreOption.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class BTreeGenericTest : public ::testing::Test {

protected:
  std::unique_ptr<LeanStore> mStore;
  std::string mTreeName;
  TransactionKV* mBTree;

  BTreeGenericTest() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" + std::string(curTest->name());
    auto storeDirStr = "/tmp/leanstore/" + curTestName;
    StoreOption* option = CreateStoreOption(storeDirStr.c_str());
    option->mCreateFromScratch = true;
    option->mWorkerThreads = 2;
    auto res = LeanStore::Open(option);
    mStore = std::move(res.value());
  }

  ~BTreeGenericTest() = default;

  void SetUp() override {
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
      cr::WorkerContext::My().StartTx();
      SCOPED_DEFER(cr::WorkerContext::My().CommitTx());
      mStore->DropTransactionKV(mTreeName);
    });
  }
};

TEST_F(BTreeGenericTest, GetSummary) {
  // insert 200 key-value pairs
  for (int i = 0; i < 200; i++) {
    mStore->ExecSync(0, [&]() {
      auto key = RandomGenerator::RandAlphString(24) + std::to_string(i);
      auto val = RandomGenerator::RandAlphString(176);

      cr::WorkerContext::My().StartTx();
      SCOPED_DEFER(cr::WorkerContext::My().CommitTx());
      mBTree->Insert(key, val);
    });
  }

  auto* btree = dynamic_cast<BTreeGeneric*>(mBTree);
  ASSERT_NE(btree, nullptr);
  EXPECT_TRUE(btree->Summary().contains("entries=200"));
}

} // namespace leanstore::test