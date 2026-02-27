#include "leanstore/coro/coro_executor.hpp"
#include "leanstore/coro/coro_scheduler.hpp"
#include "leanstore/coro/coro_session.hpp"
#include "leanstore/coro/coroutine.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace leanstore::test {

class StreamingTest : public ::testing::Test {};

template <typename T>
class StreamBuffer {
private:
  std::queue<T> buffer_;
  mutable std::mutex mutex_;
  std::atomic<bool> finished_{false};
  static constexpr size_t kMaxBufferSize = 1000;

public:
  bool TryPush(const T& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (buffer_.size() >= kMaxBufferSize) {
      return false;
    }
    buffer_.push(item);
    return true;
  }

  bool TryPop(T& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (buffer_.empty()) {
      return false;
    }
    item = buffer_.front();
    buffer_.pop();
    return true;
  }

  void SetFinished() {
    finished_.store(true, std::memory_order_release);
  }

  bool IsFinished() const {
    if (!finished_.load(std::memory_order_acquire)) {
      return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return buffer_.empty();
  }

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buffer_.size();
  }
};

TEST_F(StreamingTest, BasicProducerConsumer) {
  CoroScheduler coro_scheduler(nullptr, 2);
  coro_scheduler.Init();
  auto* producer_session = coro_scheduler.ReserveCoroSession(0);
  auto* consumer_session = coro_scheduler.ReserveCoroSession(1);

  StreamBuffer<int> stream;
  std::atomic<int> consumed_sum{0};
  std::atomic<int> produced_count{0};

  constexpr int kNumItems = 100;

  auto producer_future = coro_scheduler.Submit(producer_session, [&]() {
    for (int i = 1; i <= kNumItems; ++i) {
      while (!stream.TryPush(i)) {
        CoroExecutor::CurrentCoro()->Yield(CoroState::kReady);
      }
      produced_count.fetch_add(1, std::memory_order_relaxed);
    }
    stream.SetFinished();
  });

  auto consumer_future = coro_scheduler.Submit(consumer_session, [&]() {
    int item;
    while (!stream.IsFinished()) {
      if (stream.TryPop(item)) {
        consumed_sum.fetch_add(item, std::memory_order_relaxed);
      } else {
        CoroExecutor::CurrentCoro()->Yield(CoroState::kReady);
      }
    }
  });

  producer_future->Wait();
  consumer_future->Wait();

  EXPECT_EQ(produced_count.load(), kNumItems);
  EXPECT_EQ(consumed_sum.load(), (kNumItems * (kNumItems + 1)) / 2);

  coro_scheduler.ReleaseCoroSession(producer_session);
  coro_scheduler.ReleaseCoroSession(consumer_session);
  coro_scheduler.Deinit();
}

TEST_F(StreamingTest, StreamDataIntegrity) {
  CoroScheduler coro_scheduler(nullptr, 2);
  coro_scheduler.Init();
  auto* producer_session = coro_scheduler.ReserveCoroSession(0);
  auto* consumer_session = coro_scheduler.ReserveCoroSession(1);

  StreamBuffer<std::string> stream;
  std::vector<std::string> expected_data;
  std::vector<std::string> received_data;
  std::mutex received_mutex;

  constexpr int kNumItems = 50;

  auto producer_future = coro_scheduler.Submit(producer_session, [&]() {
    for (int i = 0; i < kNumItems; ++i) {
      std::string data = "item_" + std::to_string(i);
      expected_data.push_back(data);
      while (!stream.TryPush(data)) {
        CoroExecutor::CurrentCoro()->Yield(CoroState::kReady);
      }
    }
    stream.SetFinished();
  });

  auto consumer_future = coro_scheduler.Submit(consumer_session, [&]() {
    std::string item;
    while (!stream.IsFinished()) {
      if (stream.TryPop(item)) {
        std::lock_guard<std::mutex> lock(received_mutex);
        received_data.push_back(item);
      } else {
        CoroExecutor::CurrentCoro()->Yield(CoroState::kReady);
      }
    }
  });

  producer_future->Wait();
  consumer_future->Wait();

  EXPECT_EQ(received_data.size(), expected_data.size());
  for (size_t i = 0; i < received_data.size(); ++i) {
    EXPECT_EQ(received_data[i], expected_data[i]);
  }

  coro_scheduler.ReleaseCoroSession(producer_session);
  coro_scheduler.ReleaseCoroSession(consumer_session);
  coro_scheduler.Deinit();
}

TEST_F(StreamingTest, ConcurrentStreamAccess) {
  constexpr int kNumProducers = 2;
  constexpr int kNumConsumers = 2;
  constexpr int kItemsPerProducer = 25;

  CoroScheduler coro_scheduler(nullptr, kNumProducers + kNumConsumers);
  coro_scheduler.Init();

  std::vector<CoroSession*> sessions;
  for (int i = 0; i < kNumProducers + kNumConsumers; ++i) {
    sessions.push_back(coro_scheduler.ReserveCoroSession(i));
  }

  StreamBuffer<std::pair<int, int>> stream; // (producer_id, item_id)
  std::atomic<int> total_consumed{0};

  std::vector<std::shared_ptr<CoroFuture<void>>> producer_futures;
  for (int p = 0; p < kNumProducers; ++p) {
    producer_futures.push_back(coro_scheduler.Submit(sessions[p], [p, &stream]() {
      for (int i = 1; i <= kItemsPerProducer; ++i) {
        while (!stream.TryPush({p, i})) {
          CoroExecutor::CurrentCoro()->Yield(CoroState::kReady);
        }
      }
    }));
  }

  std::atomic<bool> producers_finished{false};
  std::thread finisher_thread([&]() {
    for (auto& future : producer_futures) {
      future->Wait();
    }
    producers_finished.store(true, std::memory_order_release);
  });

  std::vector<std::shared_ptr<CoroFuture<void>>> consumer_futures;
  for (int c = 0; c < kNumConsumers; ++c) {
    consumer_futures.push_back(coro_scheduler.Submit(
        sessions[kNumProducers + c], [&stream, &total_consumed, &producers_finished]() {
          std::pair<int, int> item;
          while (!stream.IsFinished() && !producers_finished.load(std::memory_order_acquire)) {
            if (stream.TryPop(item)) {
              total_consumed.fetch_add(1, std::memory_order_relaxed);
            } else {
              CoroExecutor::CurrentCoro()->Yield(CoroState::kReady);
            }
          }
          // Final drain of the buffer after producers are done
          while (stream.TryPop(item)) {
            total_consumed.fetch_add(1, std::memory_order_relaxed);
          }
        }));
  }

  finisher_thread.join();

  // Set stream finished after all producers are done
  stream.SetFinished();

  for (auto& future : consumer_futures) {
    future->Wait();
  }

  int expected_total = kNumProducers * kItemsPerProducer;
  EXPECT_EQ(total_consumed.load(), expected_total);

  for (auto* session : sessions) {
    coro_scheduler.ReleaseCoroSession(session);
  }
  coro_scheduler.Deinit();
}

} // namespace leanstore::test
