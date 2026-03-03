#include "leanstore/base/jump_mu.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/sync/scoped_hybrid_guard.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>

namespace leanstore::test {

class ScopedHybridGuardTest : public ::testing::Test {
protected:
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

TEST_F(ScopedHybridGuardTest, OptimisticSpinAfterExclusive) {
  uint64_t value = 0;
  HybridMutex latch;

  std::atomic<bool> start_to_read(false);
  std::atomic<bool> read_started(false);

  std::thread t0([&]() {
    ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetLatchMode(guard), LatchMode::kExclusivePessimistic);
    EXPECT_EQ(GetVersionOnLock(guard), 0);
    EXPECT_FALSE(IsEncounteredContention(guard));
    EXPECT_TRUE(IsLocked(guard));

    start_to_read.store(true);
    while (!read_started.load()) {
    }
    value = 42;

    guard.Unlock();
  });

  std::thread t1([&]() {
    auto value_read = uint64_t{0};
    auto jumped = false;
    while (true) {
      JUMPMU_TRY() {
        while (!start_to_read.load()) {
        }
        read_started.store(true);

        ScopedHybridGuard guard(latch, LatchMode::kOptimisticSpin);
        value_read = value;
        guard.Unlock();
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }
    EXPECT_EQ(value_read, 42u);
    EXPECT_FALSE(jumped);
  });

  t0.join();
  t1.join();
}

TEST_F(ScopedHybridGuardTest, OptimisticSpinBeforeExclusive) {
  uint64_t value = 0;
  HybridMutex latch;
  std::atomic<bool> start_to_write(false);
  std::atomic<bool> done_write(false);

  std::thread t0([&]() {
    while (!start_to_write.load()) {
    }
    ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
    value = 42;
    done_write.store(true);
  });

  std::thread t1([&]() {
    auto jumped = false;
    auto value_read = uint64_t{0};
    while (true) {
      JUMPMU_TRY() {
        ScopedHybridGuard guard(latch, LatchMode::kOptimisticSpin);
        start_to_write.store(true);
        while (!done_write.load()) {
        }
        value_read = value;
        guard.Unlock();
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
        jumped = true;
      }
    }
    EXPECT_EQ(value_read, 42u);
    EXPECT_TRUE(jumped);
  });

  t0.join();
  t1.join();
}

TEST_F(ScopedHybridGuardTest, MixedSharedMode) {
  uint64_t a = 0;
  uint64_t b = 100;
  HybridMutex latch;

  std::thread t0([&]() {
    for (int i = 0; i < 1000; i++) {
      ScopedHybridGuard guard(latch, LatchMode::kSharedPessimistic);
      auto a_copy = a;
      auto b_copy = b;
      guard.Unlock();
      EXPECT_EQ(a_copy + b_copy, 100);
    }
  });

  std::thread t1([&]() {
    for (int i = 0; i < 1000; i++) {
      ScopedHybridGuard::GetOptimistic(latch, LatchMode::kOptimisticSpin,
                                       [&]() { EXPECT_EQ(a + b, 100); });
      ScopedHybridGuard::GetOptimistic(latch, LatchMode::kOptimisticOrJump,
                                       [&]() { EXPECT_EQ(a + b, 100); });
    }
  });

  t0.join();
  t1.join();
}

TEST_F(ScopedHybridGuardTest, OptimisticBankTransfer) {
  uint64_t a = 0;
  uint64_t b = 100;
  HybridMutex latch;

  std::thread t0([&]() {
    for (int i = 0; i < 1000; i++) {
      {
        ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
        auto amount = utils::RandomGenerator::RandU64(0, a + 1);
        a -= amount;
        b += amount;
      }
      {
        ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
        auto amount = utils::RandomGenerator::RandU64(0, b + 1);
        b -= amount;
        a += amount;
      }
    }
  });

  std::thread t1([&]() {
    for (int i = 0; i < 1000; i++) {
      ScopedHybridGuard::GetOptimistic(latch, LatchMode::kOptimisticSpin,
                                       [&]() { EXPECT_EQ(a + b, 100); });
      ScopedHybridGuard::GetOptimistic(latch, LatchMode::kOptimisticOrJump,
                                       [&]() { EXPECT_EQ(a + b, 100); });
    }
  });

  t0.join();
  t1.join();
}

TEST_F(ScopedHybridGuardTest, PessimisticBankTransfer) {
  uint64_t a = 0;
  uint64_t b = 100;
  HybridMutex latch;

  for (int i = 0; i < 1000; i++) {
    {
      ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
      auto amount = utils::RandomGenerator::RandU64(0, a + 1);
      a -= amount;
      b += amount;
    }
    {
      ScopedHybridGuard guard(latch, LatchMode::kExclusivePessimistic);
      auto amount = utils::RandomGenerator::RandU64(0, b + 1);
      b -= amount;
      a += amount;
    }
    {
      ScopedHybridGuard guard(latch, LatchMode::kSharedPessimistic);
      EXPECT_EQ(a + b, 100);
    }
  }
}

} // namespace leanstore::test
