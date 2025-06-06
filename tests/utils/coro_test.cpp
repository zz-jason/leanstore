#include "utils/coroutine/coro_scheduler.hpp"
#include "utils/coroutine/coroutine.hpp"
#include "utils/coroutine/thread.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>

namespace leanstore::test {

class CoroTest : public ::testing::Test {};

TEST_F(CoroTest, Submit) {
  CoroScheduler coro_scheduler(2);
  coro_scheduler.Init();

  // case 1
  {
    std::atomic<bool> executed = false;
    auto future = coro_scheduler.Submit([&executed]() { executed.store(true); });
    future->Wait();
    EXPECT_TRUE(executed.load(std::memory_order_acquire));
  }

  // case 2
  {
    auto future = coro_scheduler.Submit([]() -> int64_t { return 42; });
    future->Wait();
    EXPECT_EQ(future->GetResult(), 42);
  }

  coro_scheduler.Deinit();
}

TEST_F(CoroTest, Synchronization) {
  CoroScheduler coro_scheduler(2);
  coro_scheduler.Init();

  CoroMutex mutex;
  int64_t value_x = 100;
  int64_t value_y = 0;
  for (int i = 0; i < 10; ++i) {
    auto future1 = coro_scheduler.Submit(
        [&]() {
          mutex.Lock();
          EXPECT_EQ(Thread::CurrentCoroutine()->GetState(), CoroState::kRunning);
          value_x -= 50;
          value_y += 50;
          EXPECT_EQ(value_x + value_y, 100);
          mutex.Unlock();
          EXPECT_EQ(Thread::CurrentCoroutine()->GetState(), CoroState::kRunning);
        },
        0);

    auto future2 = coro_scheduler.Submit(
        [&]() {
          mutex.Lock();
          EXPECT_EQ(Thread::CurrentCoroutine()->GetState(), CoroState::kRunning);
          value_x -= 30;
          value_y += 30;
          EXPECT_EQ(value_x + value_y, 100);
          mutex.Unlock();
          EXPECT_EQ(Thread::CurrentCoroutine()->GetState(), CoroState::kRunning);
        },
        1);

    future1->Wait();
    future2->Wait();
    EXPECT_EQ(value_x + value_y, 100);
  }

  coro_scheduler.Deinit();
}

} // namespace leanstore::test