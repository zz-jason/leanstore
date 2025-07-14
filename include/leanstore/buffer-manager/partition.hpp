#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/free_list.hpp"
#include "leanstore/units.hpp"
#include "leanstore/utils/misc.hpp"
#include "utils/coroutine/coro_executor.hpp"
#include "utils/coroutine/lean_mutex.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <vector>

namespace leanstore::storage {

class BufferManager;

struct IOFrame {
  enum class State : uint8_t {
    kReading = 0,
    kReady = 1,
    kToDelete = 2,
    kUndefined = 3 // for debugging
  };

  LeanMutex mutex_;

  State state_ = State::kUndefined;

  BufferFrame* bf_ = nullptr;

  /// Everything in CIOFrame is protected by partition lock except the following
  /// counter which is decremented outside to determine whether it is time to
  /// remove it
  std::atomic<int64_t> num_readers_ = 0;
};

struct HashTable {
  struct Entry {
    uint64_t key_;

    Entry* next_;

    IOFrame value_;

    Entry(uint64_t key);
  };

  struct Handler {
    Entry** holder_;

    operator bool() const {
      return holder_ != nullptr;
    }

    IOFrame& Frame() const {
      assert(holder_ != nullptr);
      return *reinterpret_cast<IOFrame*>(&((*holder_)->value_));
    }
  };

  uint64_t mask_;

  Entry** entries_;

  uint64_t HashKey(uint64_t k);

  IOFrame& Insert(uint64_t key);

  Handler Lookup(uint64_t key);

  void Remove(Handler& handler);

  void Remove(uint64_t key);

  bool Has(uint64_t key); // for debugging

  HashTable(uint64_t size_in_bits);
};

/// The I/O partition for the underlying pages. Page read/write operations are
/// dispatched to partitions based on the page id.
class Partition {
private:
  friend class BufferManager;

  /// Protects the concurrent access to inflight_ios_.
  LeanSharedMutex inflight_ios_mutex_;

  /// Stores all the inflight IOs in the partition.
  HashTable inflight_ios_;

  /// The maximum number of free buffer frames in the partition.
  const uint64_t free_bfs_limit_;

  /// Stores all the free buffer frames in the partition.
  FreeList free_bf_list_;

  /// Protects the concurrent access to reclaimed_page_ids_.
  LeanSharedMutex reclaimed_page_ids_mutex_;

  /// Stores all the reclaimed page ids in the partition. Page id is reclaimed
  /// when the page is removed. The reclaimed page id can be reused when a new
  /// page is allocated.
  std::vector<PID> reclaimed_page_ids_;

  /// The next page id to be allocated.
  uint64_t next_page_id_;

  /// The distance between two consecutive allocated page ids.
  const uint64_t page_id_distance_;

  const uint64_t partition_id_;

public:
  Partition(uint64_t first_page_id, uint64_t page_id_distance, uint64_t free_bfs_limit)
      : inflight_ios_(utils::GetBitsNeeded(free_bfs_limit)),
        free_bfs_limit_(free_bfs_limit),
        next_page_id_(first_page_id),
        page_id_distance_(page_id_distance),
        partition_id_(first_page_id) {
  }

  /// Tries to insert a new inflight IO for the given cooled page id. Used to
  /// prevent multiple threads from trying to write the same cooled page at the
  /// same time.
  ///
  /// Returns true if the insertion was successful, false if the page id is
  /// already in the inflight IOs.
  bool IsBeingReadBack(PID cooled_page_id) {
    LEAN_SHARED_LOCK(inflight_ios_mutex_);
    return inflight_ios_.Lookup(cooled_page_id);
  }

  uint64_t PartitionId() const {
    return partition_id_;
  }

  /// Whether the partition needs more free buffer frames.
  bool NeedMoreFreeBfs() {
    return NumFreeBfs() < free_bfs_limit_;
  }

  /// Returns the number of free buffer frames in the partition.
  uint64_t NumFreeBfs() {
    return free_bf_list_.Size();
  }

  /// Allocates a new page id.
  PID NextPageId() {
    LEAN_UNIQUE_LOCK(reclaimed_page_ids_mutex_);
    if (reclaimed_page_ids_.size()) {
      const uint64_t page_id = reclaimed_page_ids_.back();
      reclaimed_page_ids_.pop_back();
      return page_id;
    }

    const uint64_t page_id = next_page_id_;
    next_page_id_ += page_id_distance_;
    return page_id;
  }

  /// Reclaims a freed page id.
  void ReclaimPageId(PID page_id) {
    LEAN_UNIQUE_LOCK(reclaimed_page_ids_mutex_);
    reclaimed_page_ids_.push_back(page_id);
  }

  /// How many pages have been allocated.
  uint64_t NumAllocatedPages() {
    return next_page_id_ / page_id_distance_;
  }

  uint64_t GetNextPageId() const {
    return next_page_id_;
  }

  void SetNextPageId(uint64_t next_page_id) {
    next_page_id_ = next_page_id;
  }

  /// How many pages have been reclaimed.
  uint64_t NumReclaimedPages() {
    LEAN_SHARED_LOCK(reclaimed_page_ids_mutex_);
    return reclaimed_page_ids_.size();
  }

  void AddFreeBf(BufferFrame& bf) {
    free_bf_list_.PushFront(bf);
  }

  void AddFreeBfs(BufferFrame* first, BufferFrame* last, uint64_t size) {
    free_bf_list_.PushFront(first, last, size);
  }

  /// Returns a reference to the next free buffer frame in the partition.
  BufferFrame* GetFreeBfMayJump() {
    auto* free_bf = free_bf_list_.TryPopFront();
    if (free_bf == nullptr) {
#ifdef ENABLE_COROUTINE
      CoroExecutor::CurrentThread()->AddEvictionPendingPartition(partition_id_);
#endif
      JumpContext::Jump(JumpContext::JumpReason::kWaitingBufferframe);
    }
    return free_bf;
  }
};

} // namespace leanstore::storage
