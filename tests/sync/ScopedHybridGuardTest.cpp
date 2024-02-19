#include "sync/ScopedHybridGuard.hpp"

#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "sync/HybridLatch.hpp"
#include "utils/JumpMU.hpp"
#include "utils/RandomGenerator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <atomic>
#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <utility>

namespace leanstore::test {

class ScopedHybridGuardTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  /// Create a leanstore instance for each test case
  ScopedHybridGuardTest() {
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

  ~ScopedHybridGuardTest() = default;

  static storage::LatchMode getLatchMode(
      const storage::ScopedHybridGuard& guard) {
    return guard.mLatchMode;
  }

  static uint64_t getVersionOnLock(const storage::ScopedHybridGuard& guard) {
    return guard.mVersionOnLock;
  }

  static bool isEncounteredContention(const storage::ScopedHybridGuard& guard) {
    return guard.mEncounteredContention;
  }

  static bool isLocked(const storage::ScopedHybridGuard& guard) {
    return guard.mLocked;
  }
};

// | timeline | thread 0         | thread 1                 |
// | -------- | ---------------- | ------------------------ |
// |          | lock exclusively |                          |
// |          | write the value  | lock optimistically spin |
// |          | unlock           | lock optimistically spin |
// |          |                  | read the value           |
// |          |                  | unlock                   |
TEST_F(ScopedHybridGuardTest, OptimisticSpinAfterExclusive) {
  uint64_t value = 0;
  storage::HybridLatch latch;

  std::atomic<bool> startToRead(false);
  std::atomic<bool> readStarted(false);

  mStore->ExecAsync(0, [&]() {
    // lock exclusively
    storage::ScopedHybridGuard guard(latch,
                                     storage::LatchMode::kPessimisticExclusive);
    EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kPessimisticExclusive);
    EXPECT_EQ(getVersionOnLock(guard), 0);
    EXPECT_FALSE(isEncounteredContention(guard));
    EXPECT_TRUE(isLocked(guard));

    // allow the other thread to read
    startToRead.store(true);
    while (!readStarted) {
    }

    // write the value
    value = 42;

    // unlock
    guard.Unlock();
    EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kPessimisticExclusive);
    EXPECT_EQ(getVersionOnLock(guard), 0);
    EXPECT_FALSE(isEncounteredContention(guard));
    EXPECT_FALSE(isLocked(guard));
  });

  mStore->ExecAsync(1, [&]() {
    auto valueRead(0);
    auto jumped(false);
    while (true) {
      JUMPMU_TRY() {
        // wait for the other thread to init the pessimistic latch guard
        while (!startToRead.load()) {
        }
        readStarted.store(true);

        // lock optimistically, spin until the latch is not locked
        storage::ScopedHybridGuard guard(latch,
                                         storage::LatchMode::kOptimisticSpin);
        EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kOptimisticSpin);
        EXPECT_EQ(getVersionOnLock(guard), 2);
        EXPECT_TRUE(isLocked(guard));

        // read the value
        valueRead = value;

        // jump if the latch is modified by others
        guard.Unlock();
        EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kOptimisticSpin);
        EXPECT_EQ(getVersionOnLock(guard), 2);
        EXPECT_FALSE(isLocked(guard));

        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }

    EXPECT_EQ(valueRead, 42);
    EXPECT_FALSE(jumped);
  });

  mStore->Wait(0);
  mStore->Wait(1);
}

// | timeline | thread 0         | thread 1                 |
// | -------- | ---------------- | ------------------------ |
// |          |                  | lock optimistically spin |
// |          | lock exclusively |                          |
// |          | write the value  |                          |
// |          | unlock           |                          |
// |          |                  | read the value           |
// |          |                  | unlock (jump)            |
// |          |                  | lock optimistically spin |
// |          |                  | read the value           |
// |          |                  | unlock                   |
TEST_F(ScopedHybridGuardTest, OptimisticSpinBeforeExclusive) {
  uint64_t value = 0;
  storage::HybridLatch latch;

  std::atomic<bool> startToWrite(false);
  std::atomic<bool> startToRead(false);

  mStore->ExecAsync(0, [&]() {
    // wait to write
    while (!startToWrite) {
    }

    // lock exclusively
    storage::ScopedHybridGuard guard(latch,
                                     storage::LatchMode::kPessimisticExclusive);
    EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kPessimisticExclusive);
    EXPECT_EQ(getVersionOnLock(guard), 0);
    EXPECT_FALSE(isEncounteredContention(guard));
    EXPECT_TRUE(isLocked(guard));

    // write the value
    value = 42;

    // unlock
    guard.Unlock();
    EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kPessimisticExclusive);
    EXPECT_EQ(getVersionOnLock(guard), 0);
    EXPECT_FALSE(isEncounteredContention(guard));
    EXPECT_FALSE(isLocked(guard));

    // allow the other thread to read
    startToRead.store(true);
  });

  mStore->ExecAsync(1, [&]() {
    auto valueRead(0);
    auto jumped(false);
    while (true) {
      JUMPMU_TRY() {
        // lock optimistically, spin until the latch is not locked
        storage::ScopedHybridGuard guard(latch,
                                         storage::LatchMode::kOptimisticSpin);
        EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kOptimisticSpin);
        if (jumped) {
          EXPECT_EQ(getVersionOnLock(guard), 2);
        } else {
          EXPECT_EQ(getVersionOnLock(guard), 0);
        }
        EXPECT_FALSE(isEncounteredContention(guard));
        EXPECT_TRUE(isLocked(guard));

        // allow the other thread to write
        startToWrite.store(true);

        // wait for the other thread to finish writing
        while (!startToRead.load()) {
        }

        // read the value
        valueRead = value;

        // jump if the latch is modified by others
        guard.Unlock();
        EXPECT_EQ(getLatchMode(guard), storage::LatchMode::kOptimisticSpin);
        EXPECT_EQ(getVersionOnLock(guard), 2);
        EXPECT_FALSE(isLocked(guard));

        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }

    EXPECT_EQ(valueRead, 42);
    EXPECT_TRUE(jumped);
  });

  mStore->Wait(0);
  mStore->Wait(1);
}

TEST_F(ScopedHybridGuardTest, MixedSharedMode) {
  uint64_t a = 0;
  uint64_t b = 100;
  storage::HybridLatch latch;

  // thread 0: pessimistic shared lock
  mStore->ExecAsync(0, [&]() {
    for (int i = 0; i < 1000; i++) {
      auto guard = storage::ScopedHybridGuard(
          latch, storage::LatchMode::kPessimisticShared);
      auto aCopy = a;
      auto bCopy = b;
      guard.Unlock();
      EXPECT_EQ(aCopy + bCopy, 100);
    }
  });

  // thread 1: optimistic spin/jump lock
  mStore->ExecAsync(1, [&]() {
    for (int i = 0; i < 1000; i++) {
      auto jumped(false);
      while (true) {
        JUMPMU_TRY() {
          auto guard = storage::ScopedHybridGuard(
              latch, storage::LatchMode::kOptimisticSpin);
          auto aCopy = a;
          auto bCopy = b;
          guard.Unlock();
          EXPECT_EQ(aCopy + bCopy, 100);
          JUMPMU_BREAK;
        }
        JUMPMU_CATCH() {
          jumped = true;
        }
      }
      // pessimistic shared lock should not content with optimistic spin/jump
      EXPECT_FALSE(jumped);

      jumped = false;
      while (true) {
        JUMPMU_TRY() {
          auto guard = storage::ScopedHybridGuard(
              latch, storage::LatchMode::kOptimisticOrJump);
          auto aCopy = a;
          auto bCopy = b;
          guard.Unlock();
          EXPECT_EQ(aCopy + bCopy, 100);
          JUMPMU_BREAK;
        }
        JUMPMU_CATCH() {
          jumped = true;
        }
      }
      // pessimistic shared lock should not content with optimistic spin/jump
      EXPECT_FALSE(jumped);
    }
  });

  mStore->Wait(0);
  mStore->Wait(1);
}

TEST_F(ScopedHybridGuardTest, OptimisticBankTransfer) {
  uint64_t a = 0;
  uint64_t b = 100;
  storage::HybridLatch latch;

  // thread 0: transfer random amount between a and b 1000 times
  mStore->ExecAsync(0, [&]() {
    for (int i = 0; i < 1000; i++) {
      {
        auto guard = storage::ScopedHybridGuard(
            latch, storage::LatchMode::kPessimisticExclusive);
        EXPECT_TRUE(isLocked(guard));

        // transfer random amount from a to b
        auto amount = utils::RandomGenerator::RandU64(0, a + 1);
        a -= amount;
        b += amount;
      }

      {
        auto guard = storage::ScopedHybridGuard(
            latch, storage::LatchMode::kPessimisticExclusive);
        EXPECT_TRUE(isLocked(guard));

        // transfer random amount from b to a
        auto amount = utils::RandomGenerator::RandU64(0, b + 1);
        b -= amount;
        a += amount;
      }
    }
  });

  // thread 1: check if a + b is always 100, 1000 times
  mStore->ExecAsync(1, [&]() {
    uint64_t aCopy;
    uint64_t bCopy;

    for (int i = 0; i < 1000; i++) {
      // lock optimistically, spin until the latch is not exclusively locked
      storage::ScopedHybridGuard::GetOptimistic(
          latch, storage::LatchMode::kOptimisticSpin, [&]() {
            aCopy = a;
            bCopy = b;
          });
      EXPECT_EQ(aCopy + bCopy, 100);

      // lock optimistically, jump if the latch is exclusively locked
      storage::ScopedHybridGuard::GetOptimistic(
          latch, storage::LatchMode::kOptimisticOrJump, [&]() {
            aCopy = a;
            bCopy = b;
          });
      EXPECT_EQ(aCopy + bCopy, 100);
    }
  });

  mStore->Wait(0);
  mStore->Wait(1);
}

TEST_F(ScopedHybridGuardTest, PessimisticBankTransfer) {
  uint64_t a = 0;
  uint64_t b = 100;
  storage::HybridLatch latch;

  // thread 0: transfer random amount between a and b 1000 times
  mStore->ExecAsync(0, [&]() {
    for (int i = 0; i < 1000; i++) {
      {
        auto guard = storage::ScopedHybridGuard(
            latch, storage::LatchMode::kPessimisticExclusive);
        EXPECT_TRUE(isLocked(guard));

        // transfer random amount from a to b
        auto amount = utils::RandomGenerator::RandU64(0, a + 1);
        a -= amount;
        b += amount;
      }

      {
        auto guard = storage::ScopedHybridGuard(
            latch, storage::LatchMode::kPessimisticExclusive);
        EXPECT_TRUE(isLocked(guard));

        // transfer random amount from b to a
        auto amount = utils::RandomGenerator::RandU64(0, b + 1);
        b -= amount;
        a += amount;
      }
    }
  });

  // thread 1: check if a + b is always 100, 1000 times
  mStore->ExecAsync(1, [&]() {
    for (int i = 0; i < 1000; i++) {
      auto guard = storage::ScopedHybridGuard(
          latch, storage::LatchMode::kPessimisticShared);
      auto aCopy = a;
      auto bCopy = b;
      guard.Unlock();

      EXPECT_EQ(aCopy + bCopy, 100);
    }
  });

  mStore->Wait(0);
  mStore->Wait(1);
}

} // namespace leanstore::test