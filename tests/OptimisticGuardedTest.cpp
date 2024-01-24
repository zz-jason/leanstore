#include "sync-primitives/OptimisticGuarded.hpp"

#include "Config.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/buffer-manager/BufferManager.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

namespace leanstore::test {

class OptimisticGuardedTest : public ::testing::Test {
protected:
  struct TestPayload {
    s64 mA;
    s64 mB;
  };

  OptimisticGuardedTest() {
    FLAGS_worker_threads = 2;
  }

  void SetUp() override {
    ASSERT_NE(GetLeanStore(), nullptr);
  }

public:
  inline static auto CreateLeanStore() {
    FLAGS_worker_threads = 2;
    FLAGS_init = true;
    FLAGS_data_dir = "/tmp/OptimisticGuardedTest";

    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    return std::make_unique<leanstore::LeanStore>();
  }

  inline static leanstore::LeanStore* GetLeanStore() {
    static std::unique_ptr<LeanStore> sTore = CreateLeanStore();
    return sTore.get();
  }
};

TEST_F(OptimisticGuardedTest, Set) {
  storage::OptimisticGuarded<TestPayload> guardedVal({0, 100});

  // Worker 0, set the guardedVal 100 times
  GetLeanStore()->mCRManager->ScheduleJobAsync(0, [&]() {
    for (s64 i = 0; i < 100; i++) {
      guardedVal.Set(TestPayload{i, 100 - i});
    }
  });

  // Worker 1, read the guardedVal 200 times
  GetLeanStore()->mCRManager->ScheduleJobAsync(1, [&]() {
    TestPayload copiedVal;
    u64 version;
    for (s64 i = 0; i < 200; i++) {
      auto currVersion = guardedVal.Get(copiedVal);
      if (currVersion != version) {
        EXPECT_EQ(copiedVal.mA + copiedVal.mB, 100);
        EXPECT_EQ((currVersion - version) % 2, 0u);
        version = currVersion;
      }
    }
  });

  // Wait for all jobs to finish
  GetLeanStore()->mCRManager->JoinAll();
}

TEST_F(OptimisticGuardedTest, UpdateAttribute) {
  storage::OptimisticGuarded<TestPayload> guardedVal({0, 100});

  // Worker 0, update the guardedVal 100 times
  GetLeanStore()->mCRManager->ScheduleJobAsync(0, [&]() {
    for (s64 i = 0; i < 100; i++) {
      guardedVal.UpdateAttribute(&TestPayload::mA, i);
    }
  });

  // Worker 1, read the guardedVal 200 times
  GetLeanStore()->mCRManager->ScheduleJobAsync(1, [&]() {
    TestPayload copiedVal;
    u64 version;
    for (s64 i = 0; i < 200; i++) {
      auto currVersion = guardedVal.Get(copiedVal);
      if (currVersion != version) {
        EXPECT_EQ(copiedVal.mB, 100);
        EXPECT_EQ((currVersion - version) % 2, 0u);
        version = currVersion;
      }
    }
  });

  // Wait for all jobs to finish
  GetLeanStore()->mCRManager->JoinAll();
}

} // namespace leanstore::test