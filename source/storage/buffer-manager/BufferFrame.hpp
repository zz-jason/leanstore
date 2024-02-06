#pragma once

#include "Swip.hpp"
#include "shared-headers/Units.hpp"
#include "sync/HybridLatch.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <atomic>
#include <cstring>
#include <limits>

namespace leanstore {
namespace storage {

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
    DCHECK(mLatch.IsLockedExclusively());

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

/// Page is the content stored in the disk file. Page id is not here because it
/// is determined by the offset in the disk file, no need to store it
/// explicitly.
class Page {
public:
  /// Short for "page sequence number", increased when a page is modified. A
  /// page is "dirty" when mPSN > mFlushedPSN in the header.
  LID mPSN = 0;

  /// Short for "global sequence number", increased when a page is accessed.
  /// It's used to check whether the page has been read or written by
  /// transactions in other workers.
  u64 mGSN = 0;

  /// The btree ID it belongs to.
  TREEID mBTreeId = std::numeric_limits<TREEID>::max();

  /// Used for debug, page id is stored in it when evicted to disk.
  u64 mMagicDebuging;

  /// The data stored in this page. The btree node content is stored here.
  u8 mPayload[];

public:
  u64 CRC() {
    return utils::CRC(mPayload, FLAGS_page_size - sizeof(Page));
  }
};

/// The unit of buffer pool. Buffer pool is partitioned into several partitions,
/// and each partition is composed of BufferFrames. A BufferFrame is used to
/// store the content of a disk page. The BufferFrame contains all the needed
/// data structures to control concurrent page access.
///
/// NOTE: BufferFrame usually used together with GuardedBufferFrame which shared
/// or exclusively lock the latch on the BufferFrame for data access. For
/// convenient, lots of BufferFrame related operations are implementated in
/// GuardedBufferFrame.
class BufferFrame {
public:
  /// The control part. Information used by buffer manager, concurrent
  /// transaction control, etc. are stored here.
  alignas(512) BufferFrameHeader header;

  // The persisted data part. Each page maps to a underlying disk page. It's
  // persisted to disk when the checkpoint happens, or when the storage is
  // shutdown. It should be recovered based on the old page content and the
  // write-ahead log of the page.
  alignas(512) Page page;

public:
  BufferFrame() {
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
           header.mLatch.IsLockedExclusively();
  }

  inline void Init(PID pageId) {
    DCHECK(header.state == STATE::FREE);
    header.mPageId = pageId;
    header.state = STATE::HOT;
    header.mFlushedPSN = 0;

    page.mPSN = 0;
    page.mGSN = 0;
  }

  // Pre: bf is exclusively locked
  void Reset() {
    header.Reset();
  }

  void ToJson(rapidjson::Value* resultObj,
              rapidjson::Value::AllocatorType& allocator);

public:
  static size_t Size() {
    return 512 + FLAGS_page_size;
  }
};

// -----------------------------------------------------------------------------
// BufferFrame
// -----------------------------------------------------------------------------
inline void BufferFrame::ToJson(rapidjson::Value* resultObj,
                                rapidjson::Value::AllocatorType& allocator) {
  DCHECK(resultObj->IsObject());

  // header
  rapidjson::Value headerObj(rapidjson::kObjectType);
  {
    auto stateStr = header.StateString();
    rapidjson::Value member;
    member.SetString(stateStr.data(), stateStr.size(), allocator);
    headerObj.AddMember("mState", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(header.mKeepInMemory);
    headerObj.AddMember("mKeepInMemory", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(header.mPageId);
    headerObj.AddMember("mPageId", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(header.mLastWriterWorker);
    headerObj.AddMember("mLastWriterWorker", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(header.mFlushedPSN);
    headerObj.AddMember("mFlushedPSN", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(header.mIsBeingWrittenBack);
    headerObj.AddMember("mIsBeingWrittenBack", member, allocator);
  }

  resultObj->AddMember("header", headerObj, allocator);

  // page without payload
  rapidjson::Value pageMetaObj(rapidjson::kObjectType);
  {
    rapidjson::Value member;
    member.SetUint64(page.mPSN);
    pageMetaObj.AddMember("mPSN", member, allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(page.mGSN);
    pageMetaObj.AddMember("mGSN", member, allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(page.mBTreeId);
    pageMetaObj.AddMember("mBTreeId", member, allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(page.mMagicDebuging);
    pageMetaObj.AddMember("mMagicDebuging", member, allocator);
  }
  resultObj->AddMember("pageWithoutPayload", pageMetaObj, allocator);
}

} // namespace storage
} // namespace leanstore
