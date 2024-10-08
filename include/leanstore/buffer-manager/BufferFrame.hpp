#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/buffer-manager/Swip.hpp"
#include "leanstore/sync/HybridLatch.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <atomic>
#include <cstdint>
#include <limits>

namespace leanstore::storage {

//! Used for contention based split. See more details in: "Contention and Space Management in
//! B-Trees"
class ContentionStats {
public:
  //! Represents the number of lock contentions encountered on the page.
  uint32_t mNumContentions = 0;

  //! Represents the number of updates on the page.
  uint32_t mNumUpdates = 0;

  //! Represents the last updated slot id on the page.
  int32_t mLastUpdatedSlot = -1;

public:
  void Update(bool encounteredContention, int32_t lastUpdatedSlot) {
    mNumContentions += encounteredContention;
    mNumUpdates++;
    mLastUpdatedSlot = lastUpdatedSlot;
  }

  uint32_t ContentionPercentage() {
    return 100.0 * mNumContentions / mNumUpdates;
  }

  void Reset() {
    mNumContentions = 0;
    mNumUpdates = 0;
    mLastUpdatedSlot = -1;
  }
};

class BufferFrame;

enum class State : uint8_t { kFree = 0, kHot = 1, kCool = 2, kLoaded = 3 };

class BufferFrameHeader {
public:
  //! The state of the buffer frame.
  State mState = State::kFree;

  //! Latch of the buffer frame. The optimistic version in the latch is never
  //! decreased.
  HybridLatch mLatch = 0;

  //! Used to make the buffer frame remain in memory.
  bool mKeepInMemory = false;

  //! The free buffer frame in the free list of each buffer partition.
  BufferFrame* mNextFreeBf = nullptr;

  //! ID of page resides in this buffer frame.
  PID mPageId = std::numeric_limits<PID>::max();

  //! ID of the last worker who has modified the containing page. For remote flush avoidance (RFA),
  //! see "Rethinking Logging, Checkpoints, and Recovery for High-Performance Storage Engines,
  //! SIGMOD 2020" for details.
  WORKERID mLastWriterWorker = std::numeric_limits<uint8_t>::max();

  //! The flushed page sequence number of the containing page. Initialized when the containing page
  //! is loaded from disk.
  uint64_t mFlushedPsn = 0;

  //! Whether the containing page is being written back to disk.
  std::atomic<bool> mIsBeingWrittenBack = false;

  //! Contention statistics about the BTreeNode in the containing page. Used for contention-based
  //! node split for BTrees.
  ContentionStats mContentionStats;

  //! CRC checksum of the containing page.
  uint64_t mCrc = 0;

public:
  // Prerequisite: the buffer frame is exclusively locked
  void Reset() {
    LS_DCHECK(!mIsBeingWrittenBack);
    LS_DCHECK(mLatch.IsLockedExclusively());

    mState = State::kFree;
    mKeepInMemory = false;
    mNextFreeBf = nullptr;

    mPageId = std::numeric_limits<PID>::max();
    mLastWriterWorker = std::numeric_limits<uint8_t>::max();
    mFlushedPsn = 0;
    mIsBeingWrittenBack.store(false, std::memory_order_release);
    mContentionStats.Reset();
    mCrc = 0;
  }

  std::string StateString() {
    switch (mState) {
    case State::kFree: {
      return "kFree";
    }
    case State::kHot: {
      return "kHot";
    }
    case State::kCool: {
      return "kCool";
    }
    case State::kLoaded: {
      return "kLoaded";
    }
    }
    return "unknown state";
  }
};

//! Page is the content stored in the disk file. Page id is not here because it
//! is determined by the offset in the disk file, no need to store it
//! explicitly.
class Page {
public:
  //! Short for "global sequence number", increased when a page is modified.
  //! It's used to check whether the page has been read or written by
  //! transactions in other workers.
  uint64_t mGSN = 0;

  //! Short for "system transaction id", increased when a system transaction modifies the page.
  uint64_t mSysTxId = 0;

  //! Short for "page sequence number", increased when a page is modified by any user or system
  //! transaction. A page is "dirty" when mPage.mPsn > mHeader.mFlushedPsn.
  uint64_t mPsn = 0;

  //! The btree ID it belongs to.
  TREEID mBTreeId = std::numeric_limits<TREEID>::max();

  //! Used for debug, page id is stored in it when evicted to disk.
  uint64_t mMagicDebugging;

  //! The data stored in this page. The btree node content is stored here.
  uint8_t mPayload[];

public:
  uint64_t CRC() {
    return utils::CRC(mPayload, utils::tlsStore->mStoreOption->mPageSize - sizeof(Page));
  }
};

//! The unit of buffer pool. Buffer pool is partitioned into several partitions,
//! and each partition is composed of BufferFrames. A BufferFrame is used to
//! store the content of a disk page. The BufferFrame contains all the needed
//! data structures to control concurrent page access.
//!
//! NOTE: BufferFrame usually used together with GuardedBufferFrame which shared
//! or exclusively lock the latch on the BufferFrame for data access. For
//! convenient, lots of BufferFrame related operations are implementated in
//! GuardedBufferFrame.
class BufferFrame {
public:
  //! The control part. Information used by buffer manager, concurrent
  //! transaction control, etc. are stored here.
  alignas(512) BufferFrameHeader mHeader;

  // The persisted data part. Each page maps to a underlying disk page. It's
  // persisted to disk when the checkpoint happens, or when the storage is
  // shutdown. It should be recovered based on the old page content and the
  // write-ahead log of the page.
  alignas(512) Page mPage;

  BufferFrame() = default;

  bool operator==(const BufferFrame& other) {
    return this == &other;
  }

  bool IsDirty() const {
    return mPage.mPsn != mHeader.mFlushedPsn;
  }

  bool IsFree() const {
    return mHeader.mState == State::kFree;
  }

  bool ShouldRemainInMem() {
    return mHeader.mKeepInMemory || mHeader.mIsBeingWrittenBack ||
           mHeader.mLatch.IsLockedExclusively();
  }

  void Init(PID pageId) {
    LS_DCHECK(mHeader.mState == State::kFree);
    mHeader.mPageId = pageId;
    mHeader.mState = State::kHot;
    mHeader.mFlushedPsn = 0;

    mPage.mGSN = 0;
    mPage.mSysTxId = 0;
    mPage.mPsn = 0;
  }

  // Pre: bf is exclusively locked
  void Reset() {
    mHeader.Reset();
  }
};

} // namespace leanstore::storage
