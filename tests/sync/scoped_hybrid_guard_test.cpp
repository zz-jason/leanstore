#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/cpp/base/jump_mu.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/sync/scoped_hybrid_guard.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace leanstore::test {

class ScopedHybridGuardTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> store_;

  /// Create a leanstore instance for each test case
  ScopedHybridGuardTest() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_suite_name()) + "_" + std::string(cur_test->name());
    auto* option = lean_store_option_create(("/tmp/leanstore/" + cur_test_name).c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->enable_eager_gc_ = true;
    auto res = LeanStore::Open(option);
    store_ = std::move(res.value());
  }

  ~ScopedHybridGuardTest() override = default;

  static LatchMode GetLatchMode(const ScopedHybridGuard& guard) {
    return guard.latch_mode_;
  }

  static uint64_t GetVersionOnLock(const ScopedHybridGuard& guard) {
    return guard.version_on_lock_;
  }

  static bool IsEncounteredContention(const ScopedHybridGuard& guard) {
    return guard.contented_;
  }

  static bool IsLocked(const ScopedHybridGuard& guard) {
    return guard.locked_;
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
  HybridMutex latch;

  std::atomic<bool> start_to_read(false);
  std::atomic<bool> read_started(false);

  store_->ExecAsync(0, [&]() {
    // lock exclusively
    ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetLatchMode(guard), LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetVersionOnLock(guard), 0);
    EXPECT_FALSE(IsEncounteredContention(guard));
    EXPECT_TRUE(IsLocked(guard));

    // allow the other thread to read
    start_to_read.store(true);
    while (!read_started) {
    }

    // write the value
    value = 42;

    // unlock
    guard.Unlock();
    EXPECT_EQ(GetLatchMode(guard), LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetVersionOnLock(guard), 0);
    EXPECT_FALSE(IsEncounteredContention(guard));
    EXPECT_FALSE(IsLocked(guard));
  });

  store_->ExecAsync(1, [&]() {
    auto value_read(0);
    auto jumped(false);
    while (true) {
      JUMPMU_TRY() {
        // wait for the other thread to init the pessimistic latch guard
        while (!start_to_read.load()) {
        }
        read_started.store(true);

        // lock optimistically, spin until the latch is not locked
        ScopedHybridGuard guard(latch, LatchMode::kOptimisticSpin);
        EXPECT_EQ(GetLatchMode(guard), LatchMode::kOptimisticSpin);
        EXPECT_EQ(GetVersionOnLock(guard), 2);
        EXPECT_TRUE(IsLocked(guard));

        // read the value
        value_read = value;

        // jump if the latch is modified by others
        guard.Unlock();
        EXPECT_EQ(GetLatchMode(guard), LatchMode::kOptimisticSpin);
        EXPECT_EQ(GetVersionOnLock(guard), 2);
        EXPECT_FALSE(IsLocked(guard));

        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }

    EXPECT_EQ(value_read, 42);
    EXPECT_FALSE(jumped);
  });

  store_->Wait(0);
  store_->Wait(1);
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
  HybridMutex latch;

  std::atomic<bool> start_to_write(false);
  std::atomic<bool> start_to_read(false);

  store_->ExecAsync(0, [&]() {
    // wait to write
    while (!start_to_write) {
    }

    // lock exclusively
    ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetLatchMode(guard), LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetVersionOnLock(guard), 0);
    EXPECT_FALSE(IsEncounteredContention(guard));
    EXPECT_TRUE(IsLocked(guard));

    // write the value
    value = 42;

    // unlock
    guard.Unlock();
    EXPECT_EQ(GetLatchMode(guard), LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetVersionOnLock(guard), 0);
    EXPECT_FALSE(IsEncounteredContention(guard));
    EXPECT_FALSE(IsLocked(guard));

    // allow the other thread to read
    start_to_read.store(true);
  });

  store_->ExecAsync(1, [&]() {
    auto value_read(0);
    auto jumped(false);
    while (true) {
      JUMPMU_TRY() {
        // lock optimistically, spin until the latch is not locked
        ScopedHybridGuard guard(latch, LatchMode::kOptimisticSpin);
        EXPECT_EQ(GetLatchMode(guard), LatchMode::kOptimisticSpin);
        if (jumped) {
          EXPECT_EQ(GetVersionOnLock(guard), 2);
        } else {
          EXPECT_EQ(GetVersionOnLock(guard), 0);
        }
        EXPECT_FALSE(IsEncounteredContention(guard));
        EXPECT_TRUE(IsLocked(guard));

        // allow the other thread to write
        start_to_write.store(true);

        // wait for the other thread to finish writing
        while (!start_to_read.load()) {
        }

        // read the value
        value_read = value;

        // jump if the latch is modified by others
        guard.Unlock();
        EXPECT_EQ(GetLatchMode(guard), LatchMode::kOptimisticSpin);
        EXPECT_EQ(GetVersionOnLock(guard), 2);
        EXPECT_FALSE(IsLocked(guard));

        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }

    EXPECT_EQ(value_read, 42);
    EXPECT_TRUE(jumped);
  });

  store_->Wait(0);
  store_->Wait(1);
}

TEST_F(ScopedHybridGuardTest, MixedSharedMode) {
  uint64_t a = 0;
  uint64_t b = 100;
  HybridMutex latch;

  // thread 0: pessimistic shared lock
  store_->ExecAsync(0, [&]() {
    for (int i = 0; i < 1000; i++) {
      auto guard = ScopedHybridGuard(latch, LatchMode::kSharedPessimistic);
      auto a_copy = a;
      auto b_copy = b;
      guard.Unlock();
      EXPECT_EQ(a_copy + b_copy, 100);
    }
  });

  // thread 1: optimistic spin/jump lock
  store_->ExecAsync(1, [&]() {
    for (int i = 0; i < 1000; i++) {
      auto jumped(false);
      while (true) {
        JUMPMU_TRY() {
          auto guard = ScopedHybridGuard(latch, LatchMode::kOptimisticSpin);
          auto a_copy = a;
          auto b_copy = b;
          guard.Unlock();
          EXPECT_EQ(a_copy + b_copy, 100);
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
          auto guard = ScopedHybridGuard(latch, LatchMode::kOptimisticOrJump);
          auto a_copy = a;
          auto b_copy = b;
          guard.Unlock();
          EXPECT_EQ(a_copy + b_copy, 100);
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

  store_->Wait(0);
  store_->Wait(1);
}

TEST_F(ScopedHybridGuardTest, OptimisticBankTransfer) {
  uint64_t a = 0;
  uint64_t b = 100;
  HybridMutex latch;

  // thread 0: transfer random amount between a and b 1000 times
  store_->ExecAsync(0, [&]() {
    for (int i = 0; i < 1000; i++) {
      {
        auto guard = ScopedHybridGuard(latch, LatchMode::kExclusivePessimistic);
        EXPECT_TRUE(IsLocked(guard));

        // transfer random amount from a to b
        auto amount = utils::RandomGenerator::RandU64(0, a + 1);
        a -= amount;
        b += amount;
      }

      {
        auto guard = ScopedHybridGuard(latch, LatchMode::kExclusivePessimistic);
        EXPECT_TRUE(IsLocked(guard));

        // transfer random amount from b to a
        auto amount = utils::RandomGenerator::RandU64(0, b + 1);
        b -= amount;
        a += amount;
      }
    }
  });

  // thread 1: check if a + b is always 100, 1000 times
  store_->ExecAsync(1, [&]() {
    uint64_t a_copy;
    uint64_t b_copy;

    for (int i = 0; i < 1000; i++) {
      // lock optimistically, spin until the latch is not exclusively locked
      ScopedHybridGuard::GetOptimistic(latch, LatchMode::kOptimisticSpin, [&]() {
        a_copy = a;
        b_copy = b;
      });
      EXPECT_EQ(a_copy + b_copy, 100);

      // lock optimistically, jump if the latch is exclusively locked
      ScopedHybridGuard::GetOptimistic(latch, LatchMode::kOptimisticOrJump, [&]() {
        a_copy = a;
        b_copy = b;
      });
      EXPECT_EQ(a_copy + b_copy, 100);
    }
  });

  store_->Wait(0);
  store_->Wait(1);
}

TEST_F(ScopedHybridGuardTest, PessimisticBankTransfer) {
  uint64_t a = 0;
  uint64_t b = 100;
  HybridMutex latch;

  // thread 0: transfer random amount between a and b 1000 times
  store_->ExecAsync(0, [&]() {
    for (int i = 0; i < 1000; i++) {
      {
        auto guard = ScopedHybridGuard(latch, LatchMode::kExclusivePessimistic);
        EXPECT_TRUE(IsLocked(guard));

        // transfer random amount from a to b
        auto amount = utils::RandomGenerator::RandU64(0, a + 1);
        a -= amount;
        b += amount;
      }

      {
        auto guard = ScopedHybridGuard(latch, LatchMode::kExclusivePessimistic);
        EXPECT_TRUE(IsLocked(guard));

        // transfer random amount from b to a
        auto amount = utils::RandomGenerator::RandU64(0, b + 1);
        b -= amount;
        a += amount;
      }
    }
  });

  // thread 1: check if a + b is always 100, 1000 times
  store_->ExecAsync(1, [&]() {
    for (int i = 0; i < 1000; i++) {
      auto guard = ScopedHybridGuard(latch, LatchMode::kSharedPessimistic);
      auto a_copy = a;
      auto b_copy = b;
      guard.Unlock();

      EXPECT_EQ(a_copy + b_copy, 100);
    }
  });

  store_->Wait(0);
  store_->Wait(1);
}

} // namespace leanstore::test