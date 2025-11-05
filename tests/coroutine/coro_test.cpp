#include "coroutine/coro_executor.hpp"
#include "coroutine/coro_mutex.hpp"
#include "coroutine/coro_scheduler.hpp"
#include "coroutine/coro_session.hpp"
#include "coroutine/coroutine.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace leanstore::test {

class CoroTest : public ::testing::Test {};

TEST_F(CoroTest, Submit) {
  CoroScheduler coro_scheduler(nullptr, 1);
  coro_scheduler.Init();

  // case 1
  {
    auto* coro_session = coro_scheduler.ReserveCoroSession(0);
    std::atomic<bool> executed = false;
    coro_scheduler.Submit(coro_session, [&executed]() { executed.store(true); })->Wait();
    EXPECT_TRUE(executed.load(std::memory_order_acquire));
    coro_scheduler.ReleaseCoroSession(coro_session);
  }

  // case 2
  {
    auto* coro_session = coro_scheduler.TryReserveCoroSession(0);
    auto future = coro_scheduler.Submit(coro_session, []() -> int64_t { return 42; });
    future->Wait();
    EXPECT_EQ(future->GetResult(), 42);
    coro_scheduler.ReleaseCoroSession(coro_session);
  }

  coro_scheduler.Deinit();
}

TEST_F(CoroTest, Mutex) {
  CoroScheduler coro_scheduler(nullptr, 2);
  coro_scheduler.Init();
  auto* coro_session_0 = coro_scheduler.ReserveCoroSession(0);
  auto* coro_session_1 = coro_scheduler.ReserveCoroSession(1);

  CoroMutex mutex;
  int64_t value_x = 100;
  int64_t value_y = 0;
  for (int i = 0; i < 10; ++i) {
    auto future1 = coro_scheduler.Submit(coro_session_0, [&]() {
      mutex.lock();
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
      value_x -= 50;
      value_y += 50;
      EXPECT_EQ(value_x + value_y, 100);
      mutex.unlock();
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
    });

    auto future2 = coro_scheduler.Submit(coro_session_1, [&]() {
      mutex.lock();
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
      value_x -= 30;
      value_y += 30;
      EXPECT_EQ(value_x + value_y, 100);
      mutex.unlock();
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
    });

    future1->Wait();
    future2->Wait();
    EXPECT_EQ(value_x + value_y, 100);
  }

  coro_scheduler.ReleaseCoroSession(coro_session_0);
  coro_scheduler.ReleaseCoroSession(coro_session_1);
  coro_scheduler.Deinit();
}

TEST_F(CoroTest, SharedMutex) {
  CoroScheduler coro_scheduler(nullptr, 2);
  coro_scheduler.Init();
  auto* coro_session_0 = coro_scheduler.ReserveCoroSession(0);
  auto* coro_session_1 = coro_scheduler.ReserveCoroSession(1);

  CoroSharedMutex shared_mutex;
  int64_t value_x = 100;
  int64_t value_y = 0;

  for (auto i = 0u; i < 20; ++i) {
    auto future0 = coro_scheduler.Submit(coro_session_0, [&]() {
      auto rand_num = utils::RandomGenerator::RandU64(1, 100);
      shared_mutex.lock();
      value_x -= 10 + rand_num;
      value_y += 10 + rand_num;
      EXPECT_EQ(value_x + value_y, 100);

      shared_mutex.unlock();
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
    });

    auto future1 = coro_scheduler.Submit(coro_session_1, [&]() {
      shared_mutex.lock_shared();
      EXPECT_EQ(value_x + value_y, 100);

      shared_mutex.unlock_shared();
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
    });

    future0->Wait();
    future1->Wait();
  }

  EXPECT_EQ(value_x + value_y, 100);

  coro_scheduler.ReleaseCoroSession(coro_session_0);
  coro_scheduler.ReleaseCoroSession(coro_session_1);
  coro_scheduler.Deinit();
}

TEST_F(CoroTest, Io) {
  CoroScheduler coro_scheduler(nullptr, 2);
  coro_scheduler.Init();
  auto* coro_session_0 = coro_scheduler.ReserveCoroSession(0);

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
  auto future_write = coro_scheduler.Submit(coro_session_0, [&]() {
    CoroWrite(fd, write_buf, buf_size, 0);
    CoroFsync(fd);
    EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
  });
  future_write->Wait();

  constexpr auto kReadCoros = 10u;
  std::vector<std::shared_ptr<CoroFuture<void>>> read_futures;
  std::vector<CoroSession*> reserved_sessions;
  for (auto i = 0u; i < kReadCoros; ++i) {
    // coro 1: read from file
    reserved_sessions.push_back(coro_scheduler.TryReserveCoroSession(i % 2));
    assert(reserved_sessions.back() != nullptr &&
           "Failed to reserve a CoroSession for coroutine execution");
    auto future_read = coro_scheduler.Submit(reserved_sessions.back(), [&]() {
      // prepare for read
      utils::AlignedBuffer<> read_buf_aligned(rand_str.size());
      auto* read_buf = read_buf_aligned.Get();
      memset(read_buf, 0, buf_size);

      CoroRead(fd, read_buf, buf_size, 0);
      EXPECT_EQ(CoroExecutor::CurrentCoro()->GetState(), CoroState::kRunning);
      EXPECT_EQ(std::string((char*)read_buf, 10), std::string((char*)write_buf, 10));
    });
    read_futures.push_back(future_read);
  }
  for (auto& read_future : read_futures) {
    read_future->Wait();
  }
  for (auto* session : reserved_sessions) {
    coro_scheduler.ReleaseCoroSession(session);
  }
  coro_scheduler.Deinit();
}

TEST_F(CoroTest, ChildCoro) {
  CoroScheduler coro_scheduler(nullptr, 1);
  coro_scheduler.Init();

  std::vector<std::string> messages;
  std::vector<CoroSession*> reserved_sessions;
  int64_t value = 0;
  reserved_sessions.push_back(coro_scheduler.TryReserveCoroSession(0));
  assert(reserved_sessions.back() != nullptr &&
         "Failed to reserve a CoroSession for coroutine execution");
  auto future = coro_scheduler.Submit(reserved_sessions[0], [&]() {
    messages.push_back("Parent coroutine started");

    Coroutine child_coro([&]() {
      messages.push_back("Child coroutine started");

      value = 42;

      CoroExecutor::CurrentCoro()->Yield(CoroState::kDone);

      // this will not be executed
      value = 43;
      messages.push_back("Child coroutine finished");
    });

    CoroExecutor::CurrentThread()->RunCoroutine(&child_coro);

    // after child coroutine finishes
    EXPECT_EQ(value, 42);
    EXPECT_NE(CoroExecutor::CurrentCoro(), &child_coro);
    messages.push_back("Parent coroutine finished");
  });

  future->Wait();
  // concat messages to a single string for easier comparison
  std::string messages_str;
  for (const auto& msg : messages) {
    messages_str += msg + "\n";
  }
  EXPECT_EQ(messages_str, "Parent coroutine started\n"
                          "Child coroutine started\n"
                          "Parent coroutine finished\n");

  for (auto* session : reserved_sessions) {
    coro_scheduler.ReleaseCoroSession(session);
  }
  coro_scheduler.Deinit();
}

TEST_F(CoroTest, JumpContext) {
  CoroScheduler coro_scheduler(nullptr, 1);
  coro_scheduler.Init();

  std::atomic<int64_t> version = 1; // inited to exclusively locked
  std::atomic<bool> conflicted = false;
  int64_t value = 0;

  Coroutine coro1([&]() {
    EXPECT_EQ(JumpContext::Current(), CoroExecutor::CurrentCoro()->GetJumpContext());

    while (true) {
      JUMPMU_TRY() {
        // 1. lock
        auto version_lock = version.load(std::memory_order_acquire);
        if ((version_lock & 1) == 1) {
          conflicted.store(true, std::memory_order_release);
          JUMPMU_CONTINUE; // continue to lock if conflicted
        }

        // 2. read value
        auto value_read = value;

        // 3. unlock
        auto version_unlock = version.load(std::memory_order_acquire);
        if (version_unlock != version_lock) {
          JumpContext::Jump(JumpContext::JumpReason::kWaitingLock); // jump if conflict
        }

        // 4. read value success
        EXPECT_EQ(value_read, 42);

        // 5. finished, break the loop
        JUMPMU_BREAK;
      }
      JUMPMU_CATCH() {
      }
    }
  });

  auto* coro_session_0 = coro_scheduler.ReserveCoroSession(0);
  auto future1 = coro_scheduler.Submit(
      coro_session_0, [&]() { CoroExecutor::CurrentThread()->RunCoroutine(&coro1); });

  while (!conflicted.load(std::memory_order_acquire)) {
  }

  value = 42;
  version.store(2, std::memory_order_release); // release the lock
  future1->Wait();

  EXPECT_EQ(value, 42);

  coro_scheduler.ReleaseCoroSession(coro_session_0);
  coro_scheduler.Deinit();
}

} // namespace leanstore::test