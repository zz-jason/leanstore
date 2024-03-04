#pragma once

#include "BufferFrame.hpp"
#include "FreeList.hpp"
#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace leanstore {
namespace storage {

struct IOFrame {
  enum class STATE : uint8_t {
    READING = 0,
    READY = 1,
    TO_DELETE = 2,
    UNDEFINED = 3 // for debugging
  };
  std::mutex mutex;
  STATE state = STATE::UNDEFINED;
  BufferFrame* bf = nullptr;

  // Everything in CIOFrame is protected by partition lock
  // except the following counter which is decremented outside to determine
  // whether it is time to remove it
  std::atomic<int64_t> readers_counter = 0;
};

struct HashTable {
  struct Entry {
    uint64_t key;
    Entry* next;
    IOFrame value;
    Entry(uint64_t key);
  };

  struct Handler {
    Entry** holder;
    operator bool() const {
      return holder != nullptr;
    }
    IOFrame& frame() const {
      assert(holder != nullptr);
      return *reinterpret_cast<IOFrame*>(&((*holder)->value));
    }
  };

  uint64_t mask;

  Entry** entries;

  uint64_t hashKey(uint64_t k);

  IOFrame& Insert(uint64_t key);

  Handler Lookup(uint64_t key);

  void Remove(Handler& handler);

  void Remove(uint64_t key);

  bool has(uint64_t key); // for debugging

  HashTable(uint64_t size_in_bits);
};

class Partition {
public:
  /// protect mInflightIOs
  std::mutex mInflightIOMutex;

  HashTable mInflightIOs;

  const uint64_t mFreeBfsLimit;

  /// @brief mFreeBfList stores all the free buffer frames in the partition.
  FreeList mFreeBfList;

  /// @brief mReclaimedPageIdsMutex is used to protect mReclaimedPageIds.
  std::mutex mReclaimedPageIdsMutex;

  /// @brief mReclaimedPageIds stores the reclaimed page ids. Only when a page
  /// is removed and reclaimed, its page id can be reused for new pages.
  std::vector<PID> mReclaimedPageIds;

  uint64_t mNextPageId;

  const uint64_t mPageIdDistance;

public:
  //---------------------------------------------------------------------------
  // Constructor and Destructors
  //---------------------------------------------------------------------------
  Partition(uint64_t firstPageId, uint64_t pageIdDistance,
            uint64_t freeBfsLimit)
      : mInflightIOs(utils::GetBitsNeeded(freeBfsLimit)),
        mFreeBfsLimit(freeBfsLimit),
        mNextPageId(firstPageId),
        mPageIdDistance(pageIdDistance) {
  }

public:
  //---------------------------------------------------------------------------
  // Object Utils
  //---------------------------------------------------------------------------
  inline bool NeedMoreFreeBfs() {
    return mFreeBfList.mSize < mFreeBfsLimit;
  }

  inline PID NextPageId() {
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

  inline void ReclaimPageId(PID pageId) {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    mReclaimedPageIds.push_back(pageId);
  }

  inline uint64_t NumAllocatedPages() {
    return mNextPageId / mPageIdDistance;
  }

  inline uint64_t NumReclaimedPages() {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    return mReclaimedPageIds.size();
  }
};

} // namespace storage
} // namespace leanstore
