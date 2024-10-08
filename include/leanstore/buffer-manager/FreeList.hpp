#pragma once

#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/utils/Log.hpp"

#include <mutex>

namespace leanstore::storage {

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
  LS_DCHECK(bf.mHeader.mState == State::kFree);
  LS_DCHECK(!bf.mHeader.mLatch.IsLockedExclusively());

  JumpScoped<std::unique_lock<std::mutex>> guard(mMutex);
  bf.mHeader.mNextFreeBf = mHead;
  mHead = &bf;
  mSize++;
}

inline void FreeList::PushFront(BufferFrame* head, BufferFrame* tail, uint64_t size) {
  JumpScoped<std::unique_lock<std::mutex>> guard(mMutex);
  tail->mHeader.mNextFreeBf = mHead;
  mHead = head;
  mSize += size;
}

inline BufferFrame& FreeList::PopFrontMayJump() {
  JumpScoped<std::unique_lock<std::mutex>> guard(mMutex);
  BufferFrame* freeBf = mHead;
  if (mHead == nullptr) {
    jumpmu::Jump();
  } else {
    mHead = mHead->mHeader.mNextFreeBf;
    mSize--;
    LS_DCHECK(freeBf->mHeader.mState == State::kFree);
  }
  return *freeBf;
}

} // namespace leanstore::storage
