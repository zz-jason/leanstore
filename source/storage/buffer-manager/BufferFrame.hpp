#pragma once

#include "Swip.hpp"
#include "Units.hpp"
#include "sync-primitives/Latch.hpp"
#include "utils/JsonUtil.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <atomic>
#include <cstring>
#include <limits>
#include <vector>

#include <alloca.h>

namespace leanstore {
namespace storage {

const u64 PAGE_SIZE = 4096; // 4KB

/// Used for contention based split. See more details in: "Contention and Space
/// Management in B-Trees"
class ContentionStats {
public:
  /// Represents the number of lock contentions encountered on the page.
  u32 mNumContentions = 0;

  /// Represents the number of updates on the page.
  u32 mNumUpdates = 0;

  /// Represents the last updated slot id on the page.
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

class BufferFrameHeader {
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
  PID mPageId = std::numeric_limits<PID>::max();

  /// ID of the last worker who has modified the containing page.  For remote
  /// flush avoidance (RFA), see "Rethinking Logging, Checkpoints, and Recovery
  /// for High-Performance Storage Engines, SIGMOD 2020" for details.
  WORKERID mLastWriterWorker = std::numeric_limits<u8>::max();

  /// The flushed page sequence number of the containing page.
  LID mFlushedPSN = 0;

  /// Whether the containing page is being written back to disk.
  std::atomic<bool> mIsBeingWrittenBack = false;

  /// Contention statistics about the BTreeNode in the containing page. Used for
  /// contention-based node split for BTrees.
  ContentionStats mContentionStats;

  /// CRC checksum of the containing page.
  /// TODO(jian.z): should it be put to page?
  u64 crc = 0;

public:
  // Prerequisite: the buffer frame is exclusively locked
  void Reset() {
    DCHECK(!mIsBeingWrittenBack);
    DCHECK(mLatch.isExclusivelyLatched());

    state = STATE::FREE;
    mKeepInMemory = false;
    mNextFreeBf = nullptr;

    mPageId = std::numeric_limits<PID>::max();
    mLastWriterWorker = std::numeric_limits<u8>::max();
    mFlushedPSN = 0;
    mIsBeingWrittenBack.store(false, std::memory_order_release);
    mContentionStats.Reset();
    crc = 0;
  }

  std::string StateString() {
    switch (state) {
    case STATE::FREE: {
      return "FREE";
    }
    case STATE::HOT: {
      return "HOT";
    }
    case STATE::COOL: {
      return "COOL";
    }
    case STATE::LOADED: {
      return "LOADED";
    }
    }
    return "unknown state";
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

static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(Page::mPayload);

class BufferFrame {
public:
  /// The control part. Information used by buffer manager, concurrent
  /// transaction control, etc. are stored here.
  BufferFrameHeader header;

  // The persisted data part. Each page maps to a underlying disk page. It's
  // persisted to disk when the checkpoint happens, or when the storage is
  // shutdown. It should be recovered based on the old page content and the
  // write-ahead log of the page.
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
    return header.mKeepInMemory || header.mIsBeingWrittenBack ||
           header.mLatch.isExclusivelyLatched();
  }

  void Init(PID pageId) {
    DCHECK(header.state == STATE::FREE);
    DCHECK(!header.mLatch.isExclusivelyLatched());

    // Exclusive lock before changing to HOT
    header.mLatch.mutex.lock();
    header.mLatch->fetch_add(LATCH_EXCLUSIVE_BIT);
    DCHECK(header.mLatch.isExclusivelyLatched());
    header.mPageId = pageId;
    header.state = STATE::HOT;
    header.mFlushedPSN = 0;

    page.mPSN = 0;
    page.mGSN = 0;
  }

  // Pre: bf is exclusively locked
  void reset() {
    header.Reset();
  }

  rapidjson::Document ToJSON();
};

static_assert(sizeof(Page) == PAGE_SIZE, "The total sizeof page");
// static_assert((sizeof(BufferFrame) - sizeof(Page)) == 512, "");

// -----------------------------------------------------------------------------
// BufferFrame
// -----------------------------------------------------------------------------
inline rapidjson::Document BufferFrame::ToJSON() {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  {
    auto stateStr = header.StateString();
    rapidjson::Value member;
    member.SetString(stateStr.data(), stateStr.size(), doc.GetAllocator());
    doc.AddMember("header.mState", member, doc.GetAllocator());
  }
  leanstore::utils::AddMemberToJson(&doc, allocator, "header.mKeepInMemory",
                                    header.mKeepInMemory);
  leanstore::utils::AddMemberToJson(&doc, allocator, "header.mPageId",
                                    header.mPageId);
  leanstore::utils::AddMemberToJson(&doc, allocator, "header.mLastWriterWorker",
                                    header.mLastWriterWorker);
  leanstore::utils::AddMemberToJson(&doc, allocator, "header.mFlushedPSN",
                                    header.mFlushedPSN);
  leanstore::utils::AddMemberToJson(&doc, allocator,
                                    "header.mIsBeingWrittenBack",
                                    header.mIsBeingWrittenBack);
  leanstore::utils::AddMemberToJson(&doc, allocator, "page.mPSN", page.mPSN);
  leanstore::utils::AddMemberToJson(&doc, allocator, "page.mGSN", page.mGSN);
  leanstore::utils::AddMemberToJson(&doc, allocator, "page.mBTreeId",
                                    page.mBTreeId);
  leanstore::utils::AddMemberToJson(&doc, allocator, "page.mMagicDebuging",
                                    page.mMagicDebuging);

  return doc;
}

} // namespace storage
} // namespace leanstore
