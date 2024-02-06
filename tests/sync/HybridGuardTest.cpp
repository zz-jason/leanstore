#include "sync/HybridGuard.hpp"

#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "sync/HybridLatch.hpp"
#include "utils/JumpMU.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

namespace leanstore::test {

class HybridGuardTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  /// Create a leanstore instance for each test case
  HybridGuardTest() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" +
                       std::string(curTest->name());
    FLAGS_init = true;
    FLAGS_logtostdout = true;
    FLAGS_data_dir = "/tmp/" + curTestName;
    FLAGS_worker_threads = 2;
    FLAGS_enable_eager_garbage_collection = true;
    auto res = LeanStore::Open();
    mStore = std::move(res.value());
  }

  ~HybridGuardTest() = default;
};

TEST_F(HybridGuardTest, OptimisticShared) {
  uint64_t value = 0;
  storage::HybridLatch latch;

  std::atomic<bool> startToWrite(false);
  std::atomic<bool> startToRead(false);
  std::atomic<bool> startToUnlock(false);

  mStore->ExecAsync(0, [&]() {
    // allow the other thread to write
    startToWrite.store(true);

    // read the value
    auto valueRead(0);
    auto jumped(false);
    while (true) {
      JUMPMU_TRY() {
        // wait for the other thread to init the pessimistic latch guard
        while (!startToRead.load()) {
        }

        // init the optimistic latch guard
        storage::HybridGuard guard(&latch);
        guard.ToOptimisticOrJump();
        EXPECT_EQ(guard.mVersion, 1);
        EXPECT_EQ(guard.mState, storage::GuardState::kOptimisticShared);

        // read the value
        valueRead = value;

        // allow the other thread to unlock the latch
        startToUnlock.store(true);

        // jump if the latch is modified by others
        guard.JumpIfModifiedByOthers();
        guard.Unlock();
        EXPECT_EQ(guard.mState, storage::GuardState::kOptimisticShared);
        EXPECT_EQ(guard.mVersion, 2);

        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }

    EXPECT_EQ(valueRead, 42);
    EXPECT_TRUE(jumped);
  });

  mStore->ExecAsync(1, [&]() {
    // wait for the other thread to init the optimistic latch guard
    while (!startToWrite.load()) {
    }

    // init the pessimistic latch guard
    storage::HybridGuard guard(&latch);
    EXPECT_EQ(guard.mLatch->GetOptimisticVersion(), 0);

    // exclusive lock the latch
    guard.ToExclusiveMayJump();
    EXPECT_EQ(guard.mLatch->GetOptimisticVersion(), 1);
    EXPECT_EQ(guard.mState, storage::GuardState::kPessimisticExclusive);

    // allow the other thread to read
    startToRead.store(true);

    // write the value
    value = 42;

    // wait for the other thread to read before unlocking the latch
    while (!startToUnlock.load()) {
    }

    guard.Unlock();
    EXPECT_FALSE(guard.mLatch->IsLockedExclusively());
    EXPECT_EQ(guard.mState, storage::GuardState::kOptimisticShared);
    EXPECT_EQ(guard.mVersion, 2);
  });

  mStore->Wait(0);
  mStore->Wait(1);
}

} // namespace leanstore::test