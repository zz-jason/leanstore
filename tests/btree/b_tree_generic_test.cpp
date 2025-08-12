#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class BTreeGenericTest : public ::testing::Test {

protected:
  std::unique_ptr<LeanStore> store_;
  std::string tree_name_;
  TransactionKV* btree_;

  BTreeGenericTest() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    auto store_dir_str = "/tmp/leanstore/" + cur_test_name;
    StoreOption* option = CreateStoreOption(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto res = LeanStore::Open(option);
    store_ = std::move(res.value());
  }

  ~BTreeGenericTest() = default;

  void SetUp() override {
    tree_name_ = RandomGenerator::RandAlphString(10);
    store_->ExecSync(0, [&]() {
      auto res = store_->CreateTransactionKV(tree_name_);
      ASSERT_TRUE(res);
      btree_ = res.value();
      ASSERT_NE(btree_, nullptr);
    });
  }

  void TearDown() override {
    store_->ExecSync(1, [&]() {
      cr::TxManager::My().StartTx();
      SCOPED_DEFER(cr::TxManager::My().CommitTx());
      store_->DropTransactionKV(tree_name_);
    });
  }
};

TEST_F(BTreeGenericTest, GetSummary) {
  // insert 200 key-value pairs
  for (int i = 0; i < 200; i++) {
    store_->ExecSync(0, [&]() {
      auto key = RandomGenerator::RandAlphString(24) + std::to_string(i);
      auto val = RandomGenerator::RandAlphString(176);

      cr::TxManager::My().StartTx();
      SCOPED_DEFER(cr::TxManager::My().CommitTx());
      btree_->Insert(key, val);
    });
  }

  auto* btree = dynamic_cast<BTreeGeneric*>(btree_);
  ASSERT_NE(btree, nullptr);
  EXPECT_TRUE(btree->Summary().contains("entries=200"));
}

} // namespace leanstore::test