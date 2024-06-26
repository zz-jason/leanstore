#pragma once

#include "leanstore/Units.hpp"
#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/buffer-manager/FreeList.hpp"
#include "leanstore/utils/Misc.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace leanstore::storage {

struct IOFrame {
  enum class State : uint8_t {
    kReading = 0,
    kReady = 1,
    kToDelete = 2,
    kUndefined = 3 // for debugging
  };

  std::mutex mMutex;

  State mState = State::kUndefined;

  BufferFrame* mBf = nullptr;

  //! Everything in CIOFrame is protected by partition lock except the following
  //! counter which is decremented outside to determine whether it is time to
  //! remove it
  std::atomic<int64_t> mNumReaders = 0;
};

struct HashTable {
  struct Entry {
    uint64_t mKey;

    Entry* mNext;

    IOFrame mValue;

    Entry(uint64_t key);
  };

  struct Handler {
    Entry** mHolder;

    operator bool() const {
      return mHolder != nullptr;
    }

    IOFrame& Frame() const {
      assert(mHolder != nullptr);
      return *reinterpret_cast<IOFrame*>(&((*mHolder)->mValue));
    }
  };

  uint64_t mMask;

  Entry** mEntries;

  uint64_t HashKey(uint64_t k);

  IOFrame& Insert(uint64_t key);

  Handler Lookup(uint64_t key);

  void Remove(Handler& handler);

  void Remove(uint64_t key);

  bool Has(uint64_t key); // for debugging

  HashTable(uint64_t sizeInBits);
};

//! The I/O partition for the underlying pages. Page read/write operations are
//! dispatched to partitions based on the page id.
class Partition {
public:
  //! Protects the concurrent access to mInflightIOs.
  std::mutex mInflightIOMutex;

  //! Stores all the inflight IOs in the partition.
  HashTable mInflightIOs;

  //! The maximum number of free buffer frames in the partition.
  const uint64_t mFreeBfsLimit;

  //! Stores all the free buffer frames in the partition.
  FreeList mFreeBfList;

  //! Protects the concurrent access to mReclaimedPageIds.
  std::mutex mReclaimedPageIdsMutex;

  //! Stores all the reclaimed page ids in the partition. Page id is reclaimed
  //! when the page is removed. The reclaimed page id can be reused when a new
  //! page is allocated.
  std::vector<PID> mReclaimedPageIds;

  //! The next page id to be allocated.
  uint64_t mNextPageId;

  //! The distance between two consecutive allocated page ids.
  const uint64_t mPageIdDistance;

public:
  Partition(uint64_t firstPageId, uint64_t pageIdDistance, uint64_t freeBfsLimit)
      : mInflightIOs(utils::GetBitsNeeded(freeBfsLimit)),
        mFreeBfsLimit(freeBfsLimit),
        mNextPageId(firstPageId),
        mPageIdDistance(pageIdDistance) {
  }

  //! Whether the partition needs more free buffer frames.
  bool NeedMoreFreeBfs() {
    return mFreeBfList.mSize < mFreeBfsLimit;
  }

  //! Allocates a new page id.
  PID NextPageId() {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    if (mReclaimedPageIds.size()) {
      const uint64_t pageId = mReclaimedPageIds.back();
      mReclaimedPageIds.pop_back();
      return pageId;
    }

    const uint64_t pageId = mNextPageId;
    mNextPageId += mPageIdDistance;
    return pageId;
  }

  //! Reclaims a freed page id.
  void ReclaimPageId(PID pageId) {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    mReclaimedPageIds.push_back(pageId);
  }

  //! How many pages have been allocated.
  uint64_t NumAllocatedPages() {
    return mNextPageId / mPageIdDistance;
  }

  //! How many pages have been reclaimed.
  uint64_t NumReclaimedPages() {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    return mReclaimedPageIds.size();
  }
};

} // namespace leanstore::storage
