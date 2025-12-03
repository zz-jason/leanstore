#pragma once

#include "coroutine/lean_mutex.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/cpp/base/log.hpp"

#include <cstdint>

namespace leanstore {

class FreeList {
public:
  void PushFront(BufferFrame& bf);

  void PushFront(BufferFrame* head, BufferFrame* tail, uint64_t size);

  uint64_t Size() const {
    return size_.load(std::memory_order_relaxed);
  }

  BufferFrame* TryPopFront();

private:
  LeanMutex mutex_;
  BufferFrame* head_ = nullptr;
  std::atomic<uint64_t> size_ = 0;
};

inline void FreeList::PushFront(BufferFrame& bf) {
  LEAN_DCHECK(bf.header_.state_ == State::kFree);
  LEAN_DCHECK(!bf.header_.latch_.IsLockedExclusively());

  LEAN_UNIQUE_LOCK(mutex_);

  bf.header_.next_free_bf_ = head_;
  head_ = &bf;
  size_++;
}

inline void FreeList::PushFront(BufferFrame* head, BufferFrame* tail, uint64_t size) {
  LEAN_UNIQUE_LOCK(mutex_);

  tail->header_.next_free_bf_ = head_;
  head_ = head;
  size_ += size;
}

inline BufferFrame* FreeList::TryPopFront() {
  LEAN_UNIQUE_LOCK(mutex_);

  BufferFrame* free_bf = head_;
  if (head_ == nullptr) {
    return nullptr;
  }

  head_ = head_->header_.next_free_bf_;
  size_--;
  LEAN_DCHECK(free_bf->header_.state_ == State::kFree);
  return free_bf;
}

} // namespace leanstore
