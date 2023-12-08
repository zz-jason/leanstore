#pragma once

#include "Swip.hpp"
#include "Units.hpp"
#include "sync-primitives/Latch.hpp"

#include <atomic>
#include <cstring>
#include <limits>
#include <vector>

#include <alloca.h>

namespace leanstore {
namespace storage {

const u64 PAGE_SIZE = 4096; // 4KB

/// @brief ContentionStats is used for contention based split. See more
/// details in: "Contention and Space Management in B-Trees"
class ContentionStats {
public:
  /// @brief mNumContentions represents the number of lock contentions
  /// encountered on the page.
  u32 mNumContentions = 0;

  /// @brief mNumUpdates represents the number of updates on the page.
  u32 mNumUpdates = 0;

  /// @brief mLastUpdatedSlot represents the last updated slot id on the page.
  s32 mLastUpdatedSlot = -1;

public:
  void Update(bool encounteredContention, s32 lastUpdatedSlot) {
    mNumContentions += encounteredContention;
    mNumUpdates++;
    mLastUpdatedSlot = lastUpdatedSlot;
  }

  u32 ContentionPercentage() {
    return 100.0 * mNumContentions / mNumUpdates;
  }

  void Reset() {
    mNumContentions = 0;
    mNumUpdates = 0;
    mLastUpdatedSlot = -1;
  }
};

class BufferFrame;

enum class STATE : u8 { FREE = 0, HOT = 1, COOL = 2, LOADED = 3 };

class Header {
public:
  // For remote flush avoidance (RFA), see "Rethinking Logging, Checkpoints,
  // and Recovery for High-Performance Storage Engines, SIGMOD 2020" for
  // details.
  WORKERID mLastWriterWorker = std::numeric_limits<u8>::max();

  LID mFlushedPSN = 0;
  STATE state = STATE::FREE;
  std::atomic<bool> mIsBeingWritternBack = false;
  bool mKeepInMemory = false;
  PID pid = std::numeric_limits<PID>::max();
  HybridLatch mLatch = 0; // ATTENTION: NEVER DECREMENT
  BufferFrame* mNextFreeBf = nullptr;

  /// @brief mContentionStats is used for contention split.
  ContentionStats mContentionStats;

  u64 crc = 0;

public:
  // Prerequisite: the buffer frame is exclusively locked
  void Reset() {
    assert(!mIsBeingWritternBack);
    mLatch.assertExclusivelyLatched();

    mLastWriterWorker = std::numeric_limits<u8>::max();
    mFlushedPSN = 0;
    state = STATE::FREE; // INIT:
    mIsBeingWritternBack.store(false, std::memory_order_release);
    mKeepInMemory = false;
    pid = std::numeric_limits<PID>::max();
    mNextFreeBf = nullptr;
    mContentionStats.Reset();
    crc = 0;
  }
};

/// @brief Page is the content stored in the disk file. Page id is not here
/// because it is determined by the offset in the disk file, no need to store it
/// explicitly.
class alignas(512) Page {
public:
  /// mPSN is short for "page sequence number", increased when a page is
  /// modified. A page is "dirty" when mPSN > mFlushedPSN in the header.
  LID mPSN = 0;

  /// mGSN is short for "global sequence number", increased when a page is
  /// accessed. It's used to check whether the page has been read or written by
  /// transactions in other workers.
  LID mGSN = 0;

  /// mBTreeId is the btree ID it belongs to.
  TREEID mBTreeId = std::numeric_limits<TREEID>::max();

  /// used for debug.
  u64 mMagicDebuging;

  /// The data stored in this page. The btree node content is stored here.
  u8 mPayload[PAGE_SIZE - sizeof(mPSN) - sizeof(mGSN) - sizeof(mBTreeId) -
              sizeof(mMagicDebuging)];
};

class BufferFrame {
public:
  Header header;

  // The persisted part
  Page page;

public:
  BufferFrame() {
    header.mLatch->store(0ul);
  }

  bool operator==(const BufferFrame& other) {
    return this == &other;
  }

  inline bool isDirty() const {
    return page.mPSN != header.mFlushedPSN;
  }

  inline bool isFree() const {
    return header.state == STATE::FREE;
  }

  inline bool ShouldRemainInMem() {
    return header.mKeepInMemory || header.mIsBeingWritternBack ||
           header.mLatch.isExclusivelyLatched();
  }

  void Init(PID pageId) {
    assert(header.state == STATE::FREE);

    header.mLatch.assertNotExclusivelyLatched();
    header.mLatch.mutex.lock(); // Exclusive lock before changing to HOT
    header.mLatch->fetch_add(LATCH_EXCLUSIVE_BIT);
    header.pid = pageId;
    header.state = STATE::HOT;
    header.mFlushedPSN = 0;

    page.mPSN = 0;
    page.mGSN = 0;
    header.mLatch.assertExclusivelyLatched();
  }

  // Pre: bf is exclusively locked
  void reset() {
    header.Reset();
  }
};

static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(Page::mPayload);

static_assert(sizeof(Page) == PAGE_SIZE, "The total sizeof page");
// static_assert((sizeof(BufferFrame) - sizeof(Page)) == 512, "");

} // namespace storage
} // namespace leanstore
