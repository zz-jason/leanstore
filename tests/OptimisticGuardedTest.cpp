#include "leanstore/sync/OptimisticGuarded.hpp"

#include "leanstore-c/StoreOption.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"

#include <gtest/gtest.h>

#include <memory>

namespace leanstore::test {

class OptimisticGuardedTest : public ::testing::Test {
protected:
  struct TestPayload {
    int64_t mA;
    int64_t mB;
  };

  std::unique_ptr<LeanStore> mStore;

  OptimisticGuardedTest() {
  }

  void SetUp() override {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" + std::string(curTest->name());
    auto storeDirStr = "/tmp/" + curTestName;
    auto* option = CreateStoreOption(storeDirStr.c_str());
    option->mCreateFromScratch = true;
    option->mWorkerThreads = 2;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);
    mStore = std::move(res.value());
  }
};

TEST_F(OptimisticGuardedTest, Set) {
  storage::OptimisticGuarded<TestPayload> guardedVal({0, 100});

  // Worker 0, set the guardedVal 100 times
  mStore->ExecSync(0, [&]() {
    for (int64_t i = 0; i < 100; i++) {
      guardedVal.Set(TestPayload{i, 100 - i});
    }
  });

  // Worker 1, read the guardedVal 200 times
  mStore->ExecSync(1, [&]() {
    TestPayload copiedVal;
    auto version = guardedVal.Get(copiedVal);
    for (int64_t i = 0; i < 200; i++) {
      auto currVersion = guardedVal.Get(copiedVal);
      if (currVersion != version) {
        EXPECT_EQ(copiedVal.mA + copiedVal.mB, 100);
        EXPECT_EQ((currVersion - version) % 2, 0u);
        version = currVersion;
      }
    }
  });

  // Wait for all jobs to finish
  mStore->WaitAll();
}

TEST_F(OptimisticGuardedTest, UpdateAttribute) {
  storage::OptimisticGuarded<TestPayload> guardedVal({0, 100});

  // Worker 0, update the guardedVal 100 times
  mStore->ExecSync(0, [&]() {
    for (int64_t i = 0; i < 100; i++) {
      guardedVal.UpdateAttribute(&TestPayload::mA, i);
    }
  });

  // Worker 1, read the guardedVal 200 times
  mStore->ExecSync(1, [&]() {
    TestPayload copiedVal;
    auto version = guardedVal.Get(copiedVal);
    for (int64_t i = 0; i < 200; i++) {
      auto currVersion = guardedVal.Get(copiedVal);
      if (currVersion != version) {
        EXPECT_EQ(copiedVal.mB, 100);
        EXPECT_EQ((currVersion - version) % 2, 0u);
        version = currVersion;
      }
    }
  });

  // Wait for all jobs to finish
  mStore->WaitAll();
}

} // namespace leanstore::test