#include "leanstore/base/defer.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_store.hpp"
#include "leanstore/tx/cr_manager.hpp"
#include "leanstore/tx/transaction_kv.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <string>

namespace leanstore::test {

class BTreeGenericTest : public ::testing::Test {

protected:
  std::unique_ptr<LeanStore> store_;
  std::string tree_name_;
  TransactionKV* btree_;

  BTreeGenericTest() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_suite_name()) + "_" + std::string(cur_test->name());
    auto store_dir_str = "/tmp/leanstore/" + cur_test_name;
    lean_store_option* option = lean_store_option_create(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto res = LeanStore::Open(option);
    store_ = std::move(res.value());
  }

  ~BTreeGenericTest() override = default;

  void SetUp() override {
    tree_name_ = utils::RandomGenerator::RandAlphString(10);
    store_->ExecSync(0, [&]() {
      auto res = store_->CreateTransactionKV(tree_name_);
      ASSERT_TRUE(res);
      btree_ = res.value();
      ASSERT_NE(btree_, nullptr);
    });
  }

  void TearDown() override {
    store_->ExecSync(1, [&]() {
      CoroEnv::CurTxMgr().StartTx();
      LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());
      store_->DropTransactionKV(tree_name_);
    });
  }
};

TEST_F(BTreeGenericTest, GetSummary) {
  // insert 200 key-value pairs
  for (int i = 0; i < 200; i++) {
    store_->ExecSync(0, [&]() {
      auto key = utils::RandomGenerator::RandAlphString(24) + std::to_string(i);
      auto val = utils::RandomGenerator::RandAlphString(176);

      CoroEnv::CurTxMgr().StartTx();
      LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());
      btree_->Insert(key, val);
    });
  }

  auto* btree = dynamic_cast<BTreeGeneric*>(btree_);
  ASSERT_NE(btree, nullptr);
  EXPECT_TRUE(btree->Summary().contains("entries=200"));
}

} // namespace leanstore::test