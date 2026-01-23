#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>

namespace leanstore {

template <typename T>
class BlockingQueueMpsc {
public:
  explicit BlockingQueueMpsc(size_t capacity) : queue_(capacity), capacity_(capacity) {
  }
  ~BlockingQueueMpsc() = default;

  BlockingQueueMpsc(const BlockingQueueMpsc&) = delete;
  BlockingQueueMpsc& operator=(const BlockingQueueMpsc&) = delete;

  void PushBack(T&& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this]() { return size_ < capacity_ || stopped_; });

    if (stopped_) {
      return;
    }

    queue_[tail_] = std::move(value);
    tail_ = (tail_ + 1) % capacity_;
    size_++;

    lock.unlock();
    not_empty_.notify_one();
  }

  bool PopFront(T& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this]() { return size_ > 0 || stopped_; });

    if (size_ == 0 && stopped_) {
      return false;
    }

    value = std::move(queue_[head_]);
    head_ = (head_ + 1) % capacity_;
    size_--;

    lock.unlock();
    not_full_.notify_one();
    return true;
  }

  bool TryPopFront(T& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (size_ == 0) {
      return false;
    }
    value = std::move(queue_[head_]);
    head_ = (head_ + 1) % capacity_;
    size_--;

    lock.unlock();
    not_full_.notify_one();
    return true;
  }

  void Shutdown() {
    std::unique_lock<std::mutex> lock(mutex_);
    stopped_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();
  }

private:
  std::vector<T> queue_;
  const size_t capacity_;
  bool stopped_ = false;

  size_t head_ = 0;
  size_t tail_ = 0;
  size_t size_ = 0;

  std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
};

} // namespace leanstore