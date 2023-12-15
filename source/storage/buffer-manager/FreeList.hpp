#pragma once

#include "BufferFrame.hpp"
#include "Units.hpp"

#include <mutex>

namespace leanstore {
namespace storage {

class FreeList {
public:
  std::mutex mutex;
  BufferFrame* mHead = nullptr;
  std::atomic<u64> mSize = 0;

public:
  void PushFront(BufferFrame& bf);

  void PushFront(BufferFrame* head, BufferFrame* tail, u64 size);

  BufferFrame& PopFrontMayJump();
};

inline void FreeList::PushFront(BufferFrame& bf) {
  PARANOID(bf.header.state == STATE::FREE);
  assert(!bf.header.mLatch.IsLockedExclusively());

  JumpScoped<std::unique_lock<std::mutex>> guard(mutex);
  bf.header.mNextFreeBf = mHead;
  mHead = &bf;
  mSize++;
}

inline void FreeList::PushFront(BufferFrame* head, BufferFrame* tail,
                                u64 size) {
  JumpScoped<std::unique_lock<std::mutex>> guard(mutex);
  tail->header.mNextFreeBf = mHead;
  mHead = head;
  mSize += size;
}

inline BufferFrame& FreeList::PopFrontMayJump() {
  JumpScoped<std::unique_lock<std::mutex>> guard(mutex);
  BufferFrame* free_bf = mHead;
  if (mHead == nullptr) {
    jumpmu::jump();
  } else {
    mHead = mHead->header.mNextFreeBf;
    mSize--;
    PARANOID(free_bf->header.state == STATE::FREE);
  }
  return *free_bf;
}

} // namespace storage
} // namespace leanstore
