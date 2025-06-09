#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/utils/log.hpp"

#include <cstdint>
#include <mutex>

namespace leanstore::storage {

class FreeList {
public:
  std::mutex mutex_;
  BufferFrame* head_ = nullptr;
  std::atomic<uint64_t> size_ = 0;

public:
  void PushFront(BufferFrame& bf);

  void PushFront(BufferFrame* head, BufferFrame* tail, uint64_t size);

  uint64_t Size() const {
    return size_.load(std::memory_order_relaxed);
  }

  BufferFrame& PopFrontMayJump();
};

inline void FreeList::PushFront(BufferFrame& bf) {
  LS_DCHECK(bf.header_.state_ == State::kFree);
  LS_DCHECK(!bf.header_.latch_.IsLockedExclusively());

  JumpScoped<std::unique_lock<std::mutex>> guard(mutex_);
  bf.header_.next_free_bf_ = head_;
  head_ = &bf;
  size_++;
}

inline void FreeList::PushFront(BufferFrame* head, BufferFrame* tail, uint64_t size) {
  JumpScoped<std::unique_lock<std::mutex>> guard(mutex_);
  tail->header_.next_free_bf_ = head_;
  head_ = head;
  size_ += size;
}

inline BufferFrame& FreeList::PopFrontMayJump() {
  JumpScoped<std::unique_lock<std::mutex>> guard(mutex_);
  BufferFrame* free_bf = head_;
  if (head_ == nullptr) {
    jumpmu::Jump();
  } else {
    head_ = head_->header_.next_free_bf_;
    size_--;
    LS_DCHECK(free_bf->header_.state_ == State::kFree);
  }
  return *free_bf;
}

} // namespace leanstore::storage
