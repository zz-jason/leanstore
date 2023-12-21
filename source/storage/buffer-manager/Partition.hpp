#pragma once
#include "BufferFrame.hpp"
#include "Config.hpp"
#include "FreeList.hpp"
#include "Units.hpp"
#include "utils/Misc.hpp"

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace leanstore {
namespace storage {

struct IOFrame {
  enum class STATE : u8 {
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
  atomic<s64> readers_counter = 0;
};

struct HashTable {
  struct Entry {
    u64 key;
    Entry* next;
    IOFrame value;
    Entry(u64 key);
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

  u64 mask;
  Entry** entries;

  u64 hashKey(u64 k);
  IOFrame& insert(u64 key);
  Handler Lookup(u64 key);
  void remove(Handler& handler);
  void remove(u64 key);
  bool has(u64 key); // for debugging
  HashTable(u64 size_in_bits);
};

class Partition {
public:
  /// protect mInflightIOs
  std::mutex mInflightIOMutex;

  HashTable mInflightIOs;

  const u64 mFreeBfsLimit;

  /// @brief mFreeBfList stores all the free buffer frames in the partition.
  FreeList mFreeBfList;

  /// @brief mReclaimedPageIdsMutex is used to protect mReclaimedPageIds.
  std::mutex mReclaimedPageIdsMutex;

  /// @brief mReclaimedPageIds stores the reclaimed page ids. Only when a page
  /// is removed and reclaimed, its page id can be reused for new pages.
  std::vector<PID> mReclaimedPageIds;

  u64 mNextPageId;

  const u64 mPageIdDistance;

public:
  //---------------------------------------------------------------------------
  // Constructor and Destructors
  //---------------------------------------------------------------------------
  Partition(u64 firstPageId, u64 pageIdDistance, u64 freeBfsLimit)
      : mInflightIOs(utils::getBitsNeeded(freeBfsLimit)),
        mFreeBfsLimit(freeBfsLimit), mNextPageId(firstPageId),
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
      const u64 pageId = mReclaimedPageIds.back();
      mReclaimedPageIds.pop_back();
      return pageId;
    } else {
      const u64 pageId = mNextPageId;
      mNextPageId += mPageIdDistance;
      ENSURE(pageId * FLAGS_page_size <= FLAGS_db_file_capacity);
      return pageId;
    }
  }

  inline void ReclaimPageId(PID pageId) {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    mReclaimedPageIds.push_back(pageId);
  }

  inline u64 NumAllocatedPages() {
    return mNextPageId / mPageIdDistance;
  }

  inline u64 NumReclaimedPages() {
    std::unique_lock<std::mutex> guard(mReclaimedPageIdsMutex);
    return mReclaimedPageIds.size();
  }
};

} // namespace storage
} // namespace leanstore
