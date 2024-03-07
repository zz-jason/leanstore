#pragma once

#include "BufferFrame.hpp"
#include "leanstore/Exceptions.hpp"

#include <mutex>

namespace leanstore {
namespace storage {

class FreeList {
public:
  std::mutex mMutex;
  BufferFrame* mHead = nullptr;
  std::atomic<uint64_t> mSize = 0;

public:
  void PushFront(BufferFrame& bf);

  void PushFront(BufferFrame* head, BufferFrame* tail, uint64_t size);

  BufferFrame& PopFrontMayJump();
};

inline void FreeList::PushFront(BufferFrame& bf) {
  PARANOID(bf.header.state == STATE::FREE);
  assert(!bf.header.mLatch.IsLockedExclusively());

  JumpScoped<std::unique_lock<std::mutex>> guard(mMutex);
  bf.header.mNextFreeBf = mHead;
  mHead = &bf;
  mSize++;
}

inline void FreeList::PushFront(BufferFrame* head, BufferFrame* tail,
                                uint64_t size) {
  JumpScoped<std::unique_lock<std::mutex>> guard(mMutex);
  tail->header.mNextFreeBf = mHead;
  mHead = head;
  mSize += size;
}

inline BufferFrame& FreeList::PopFrontMayJump() {
  JumpScoped<std::unique_lock<std::mutex>> guard(mMutex);
  BufferFrame* freeBf = mHead;
  if (mHead == nullptr) {
    jumpmu::Jump();
  } else {
    mHead = mHead->header.mNextFreeBf;
    mSize--;
    PARANOID(freeBf->header.state == STATE::FREE);
  }
  return *freeBf;
}

} // namespace storage
} // namespace leanstore