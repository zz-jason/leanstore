#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "utils/coroutine/coro_mutex.hpp"
#include "utils/coroutine/coro_scheduler.hpp"
#include "utils/coroutine/coroutine.hpp"
#include "utils/coroutine/thread.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace leanstore::test {

class CoroTest : public ::testing::Test {};

TEST_F(CoroTest, Submit) {
  auto start_ts = std::chrono::steady_clock::now();
  {

    CoroScheduler coro_scheduler(1);
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

  auto elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start_ts)
                        .count() /
                    1000.0;
  Log::Info("CoroTest::Submit finished, elapsed={}ms", elapsed_ms);
}

TEST_F(CoroTest, Mutex) {
  CoroScheduler coro_scheduler(2);
  coro_scheduler.Init();

  CoroMutex mutex;
  int64_t value_x = 100;
  int64_t value_y = 0;
  for (int i = 0; i < 10; ++i) {
    auto future1 = coro_scheduler.Submit(
        [&]() {
          mutex.Lock();
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
          value_x -= 50;
          value_y += 50;
          EXPECT_EQ(value_x + value_y, 100);
          mutex.Unlock();
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
        },
        0);

    auto future2 = coro_scheduler.Submit(
        [&]() {
          mutex.Lock();
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
          value_x -= 30;
          value_y += 30;
          EXPECT_EQ(value_x + value_y, 100);
          mutex.Unlock();
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
        },
        1);

    future1->Wait();
    future2->Wait();
    EXPECT_EQ(value_x + value_y, 100);
  }

  coro_scheduler.Deinit();
}

TEST_F(CoroTest, SharedMutex) {
  CoroScheduler coro_scheduler(2);
  coro_scheduler.Init();

  CoroSharedMutex shared_mutex;
  int64_t value_x = 100;
  int64_t value_y = 0;

  std::vector<std::shared_ptr<CoroFuture<void>>> futures;
  for (int i = 0; i < 20; ++i) {
    futures.push_back(coro_scheduler.Submit(
        [&]() {
          shared_mutex.Lock();
          value_x -= 10 + i;
          value_y += 10 + i;
          EXPECT_EQ(value_x + value_y, 100);

          shared_mutex.Unlock();
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
        },
        i % 2));

    futures.push_back(coro_scheduler.Submit(
        [&]() {
          shared_mutex.LockShared();
          value_x -= 30;
          value_y += 30;
          EXPECT_EQ(value_x + value_y, 100);

          shared_mutex.UnlockShared();
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
        },
        i % 2));
  }

  for (auto& future : futures) {
    future->Wait();
  }

  EXPECT_EQ(value_x + value_y, 100);
  coro_scheduler.Deinit();
}

TEST_F(CoroTest, Io) {
  CoroScheduler coro_scheduler(2);
  coro_scheduler.Init();

  std::string filedir = "/tmp/leanstore/test/coro_test";
  auto ret = system(std::format("mkdir -p {}", filedir).c_str());
  EXPECT_EQ(ret, 0) << std::format(
      "Failed to create test directory, testDir={}, errno={}, error={}", filedir, errno,
      strerror(errno));

  auto filepath = std::format("{}/{}", filedir, utils::RandomGenerator::RandAlphString(8));
  auto flag = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
  int fd = open(filepath.c_str(), flag, 0666);
  EXPECT_NE(fd, -1);

  auto buf_size = 512;
  auto rand_str = utils::RandomGenerator::RandAlphString(buf_size);

  // prepare for write
  utils::AlignedBuffer<> write_buf_aligned(buf_size);
  auto* write_buf = write_buf_aligned.Get();
  memcpy(write_buf, rand_str.data(), buf_size);

  // coro 0: write to file
  auto future_write = coro_scheduler.Submit(
      [&]() {
        CoroWrite(fd, write_buf, buf_size, 0);
        CoroFsync(fd);
        EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
      },
      0);
  future_write->Wait();

  constexpr int64_t kReadCoros = 10;
  std::vector<std::shared_ptr<CoroFuture<void>>> read_futures;
  for (int i = 0; i < kReadCoros; ++i) {
    // coro 1: read from file
    auto future_read = coro_scheduler.Submit(
        [&]() {
          // prepare for read
          utils::AlignedBuffer<> read_buf_aligned(rand_str.size());
          auto* read_buf = read_buf_aligned.Get();
          memset(read_buf, 0, buf_size);

          CoroRead(fd, read_buf, buf_size, 0);
          EXPECT_EQ(Thread::CurrentCoro()->GetState(), CoroState::kRunning);
          EXPECT_EQ(std::string((char*)read_buf, 10), std::string((char*)write_buf, 10));
        },
        i % 2);
    read_futures.push_back(future_read);
  }
  for (auto& read_future : read_futures) {
    read_future->Wait();
  }

  coro_scheduler.Deinit();
}

} // namespace leanstore::test