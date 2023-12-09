#pragma once

#include "Swip.hpp"
#include "Units.hpp"
#include "sync-primitives/Latch.hpp"

#include <rapidjson/document.h>

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
  /// The state of the buffer frame.
  STATE state = STATE::FREE;

  /// Latch of the buffer frame. The optismitic version in the latch is nerer
  /// decreased.
  HybridLatch mLatch = 0;

  /// Used to make the buffer frame remain in memory.
  bool mKeepInMemory = false;

  /// The free buffer frame in the free list of each buffer partition.
  BufferFrame* mNextFreeBf = nullptr;

  /// ID of page resides in this buffer frame.
  PID pid = std::numeric_limits<PID>::max();

  /// ID of the last worker who has modified the containing page.  For remote
  /// flush avoidance (RFA), see "Rethinking Logging, Checkpoints, and Recovery
  /// for High-Performance Storage Engines, SIGMOD 2020" for details.
  WORKERID mLastWriterWorker = std::numeric_limits<u8>::max();

  /// The flushed page sequence number of the containing page.
  LID mFlushedPSN = 0;

  /// Whether the containing page is being written back to disk.
  std::atomic<bool> mIsBeingWritternBack = false;

  /// Contention statistics about the BTreeNode in the containing page. Used for
  /// contention-based node split for BTrees.
  ContentionStats mContentionStats;

  /// CRC checksum of the containing page.
  /// TODO(jian.z): should it be put to page?
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
  /// Short for "page sequence number", increased when a page is modified. A
  /// page is "dirty" when mPSN > mFlushedPSN in the header.
  LID mPSN = 0;

  /// Short for "global sequence number", increased when a page is accessed.
  /// It's used to check whether the page has been read or written by
  /// transactions in other workers.
  LID mGSN = 0;

  /// The btree ID it belongs to.
  TREEID mBTreeId = std::numeric_limits<TREEID>::max();

  /// Used for debug, page id is stored in it when evicted to disk.
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

  rapidjson::Document ToJSON();
};

static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(Page::mPayload);

static_assert(sizeof(Page) == PAGE_SIZE, "The total sizeof page");
static_assert((sizeof(BufferFrame) - sizeof(Page)) == 512, "");

inline rapidjson::Document BufferFrame::ToJSON() {
  rapidjson::Document doc;
  // auto& allocator = doc.GetAllocator();
  doc.SetObject();
  return doc;
}

} // namespace storage
} // namespace leanstore
