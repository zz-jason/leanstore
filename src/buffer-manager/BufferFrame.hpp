#pragma once

#include "buffer-manager/Swip.hpp"
#include "leanstore/Units.hpp"
#include "sync/HybridLatch.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <atomic>
#include <cstring>
#include <limits>
#include <sstream>

namespace leanstore {
namespace storage {

/// Used for contention based split. See more details in: "Contention and Space
/// Management in B-Trees"
class ContentionStats {
public:
  /// Represents the number of lock contentions encountered on the page.
  uint32_t mNumContentions = 0;

  /// Represents the number of updates on the page.
  uint32_t mNumUpdates = 0;

  /// Represents the last updated slot id on the page.
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
  /// The state of the buffer frame.
  State mState = State::kFree;

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
  WORKERID mLastWriterWorker = std::numeric_limits<uint8_t>::max();

  /// The flushed page sequence number of the containing page.
  LID mFlushedPSN = 0;

  /// Whether the containing page is being written back to disk.
  std::atomic<bool> mIsBeingWrittenBack = false;

  /// Contention statistics about the BTreeNode in the containing page. Used for
  /// contention-based node split for BTrees.
  ContentionStats mContentionStats;

  /// CRC checksum of the containing page.
  uint64_t mCrc = 0;

public:
  // Prerequisite: the buffer frame is exclusively locked
  void Reset() {
    DCHECK(!mIsBeingWrittenBack);
    DCHECK(mLatch.IsLockedExclusively());

    mState = State::kFree;
    mKeepInMemory = false;
    mNextFreeBf = nullptr;

    mPageId = std::numeric_limits<PID>::max();
    mLastWriterWorker = std::numeric_limits<uint8_t>::max();
    mFlushedPSN = 0;
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

/// Page is the content stored in the disk file. Page id is not here because it
/// is determined by the offset in the disk file, no need to store it
/// explicitly.
class Page {
public:
  /// Short for "page sequence number", increased when a page is modified. A
  /// page is "dirty" when mPSN > mFlushedPSN in the mHeader.
  LID mPSN = 0;

  /// Short for "global sequence number", increased when a page is modified.
  /// It's used to check whether the page has been read or written by
  /// transactions in other workers.
  uint64_t mGSN = 0;

  /// The btree ID it belongs to.
  TREEID mBTreeId = std::numeric_limits<TREEID>::max();

  /// Used for debug, page id is stored in it when evicted to disk.
  uint64_t mMagicDebuging;

  /// The data stored in this page. The btree node content is stored here.
  uint8_t mPayload[];

public:
  uint64_t CRC() {
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
  alignas(512) BufferFrameHeader mHeader;

  // The persisted data part. Each page maps to a underlying disk page. It's
  // persisted to disk when the checkpoint happens, or when the storage is
  // shutdown. It should be recovered based on the old page content and the
  // write-ahead log of the page.
  alignas(512) Page mPage;

public:
  BufferFrame() {
  }

  bool operator==(const BufferFrame& other) {
    return this == &other;
  }

  inline bool IsDirty() const {
    return mPage.mPSN != mHeader.mFlushedPSN;
  }

  inline bool IsFree() const {
    return mHeader.mState == State::kFree;
  }

  inline bool ShouldRemainInMem() {
    return mHeader.mKeepInMemory || mHeader.mIsBeingWrittenBack ||
           mHeader.mLatch.IsLockedExclusively();
  }

  inline void Init(PID pageId) {
    DCHECK(mHeader.mState == State::kFree);
    mHeader.mPageId = pageId;
    mHeader.mState = State::kHot;
    mHeader.mFlushedPSN = 0;

    mPage.mPSN = 0;
    mPage.mGSN = 0;
  }

  // Pre: bf is exclusively locked
  void Reset() {
    mHeader.Reset();
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
    // write the memory address of the buffer frame
    rapidjson::Value member;
    std::stringstream ss;
    ss << (void*)this;
    auto hexStr = ss.str();
    member.SetString(hexStr.data(), hexStr.size(), allocator);
    headerObj.AddMember("mAddress", member, allocator);
  }

  {
    auto stateStr = mHeader.StateString();
    rapidjson::Value member;
    member.SetString(stateStr.data(), stateStr.size(), allocator);
    headerObj.AddMember("mState", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(mHeader.mKeepInMemory);
    headerObj.AddMember("mKeepInMemory", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(mHeader.mPageId);
    headerObj.AddMember("mPageId", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(mHeader.mLastWriterWorker);
    headerObj.AddMember("mLastWriterWorker", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(mHeader.mFlushedPSN);
    headerObj.AddMember("mFlushedPSN", member, allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(mHeader.mIsBeingWrittenBack);
    headerObj.AddMember("mIsBeingWrittenBack", member, allocator);
  }

  resultObj->AddMember("header", headerObj, allocator);

  // page without payload
  rapidjson::Value pageMetaObj(rapidjson::kObjectType);
  {
    rapidjson::Value member;
    member.SetUint64(mPage.mPSN);
    pageMetaObj.AddMember("mPSN", member, allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(mPage.mGSN);
    pageMetaObj.AddMember("mGSN", member, allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(mPage.mBTreeId);
    pageMetaObj.AddMember("mBTreeId", member, allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(mPage.mMagicDebuging);
    pageMetaObj.AddMember("mMagicDebuging", member, allocator);
  }
  resultObj->AddMember("pageWithoutPayload", pageMetaObj, allocator);
}

} // namespace storage
} // namespace leanstore
