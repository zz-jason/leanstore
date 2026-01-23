#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/optimistic_guarded.hpp"
#include "leanstore/tx/cr_manager.hpp"

#include <gtest/gtest.h>

#include <memory>

namespace leanstore::test {

class OptimisticGuardedTest : public ::testing::Test {
protected:
  struct TestPayload {
    int64_t a_;
    int64_t b_;
  };

  std::unique_ptr<LeanStore> store_;

  OptimisticGuardedTest() = default;

  void SetUp() override {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_suite_name()) + "_" + std::string(cur_test->name());
    auto store_dir_str = "/tmp/leanstore/" + cur_test_name;
    auto* option = lean_store_option_create(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);
    store_ = std::move(res.value());
  }
};

TEST_F(OptimisticGuardedTest, Set) {
  OptimisticGuarded<TestPayload> guarded_val({.a_ = 0, .b_ = 100});

  // TxManager 0, set the guardedVal 100 times
  store_->ExecSync(0, [&]() {
    for (int64_t i = 0; i < 100; i++) {
      guarded_val.Set(TestPayload{.a_ = i, .b_ = 100 - i});
    }
  });

  // TxManager 1, read the guardedVal 200 times
  store_->ExecSync(1, [&]() {
    TestPayload copied_val;
    auto version = guarded_val.Get(copied_val);
    for (int64_t i = 0; i < 200; i++) {
      auto curr_version = guarded_val.Get(copied_val);
      if (curr_version != version) {
        EXPECT_EQ(copied_val.a_ + copied_val.b_, 100);
        EXPECT_EQ((curr_version - version) % 2, 0U);
        version = curr_version;
      }
    }
  });
}

TEST_F(OptimisticGuardedTest, UpdateAttribute) {
  OptimisticGuarded<TestPayload> guarded_val({.a_ = 0, .b_ = 100});

  // TxManager 0, update the guardedVal 100 times
  store_->ExecSync(0, [&]() {
    for (int64_t i = 0; i < 100; i++) {
      guarded_val.UpdateAttribute(&TestPayload::a_, i);
    }
  });

  // TxManager 1, read the guardedVal 200 times
  store_->ExecSync(1, [&]() {
    TestPayload copied_val;
    auto version = guarded_val.Get(copied_val);
    for (int64_t i = 0; i < 200; i++) {
      auto curr_version = guarded_val.Get(copied_val);
      if (curr_version != version) {
        EXPECT_EQ(copied_val.b_, 100);
        EXPECT_EQ((curr_version - version) % 2, 0U);
        version = curr_version;
      }
    }
  });
}

} // namespace leanstore::test