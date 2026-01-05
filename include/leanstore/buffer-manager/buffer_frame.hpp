#pragma once

#include "leanstore/common/portable.h"
#include "leanstore/common/types.h"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/misc.hpp"

#include <atomic>
#include <cstdint>
#include <limits>

namespace leanstore {

/// Used for contention based split. See more details in: "Contention and Space Management in
/// B-Trees"
class ContentionStats {
public:
  /// Represents the number of lock contentions encountered on the page.
  uint32_t num_contentions_ = 0;

  /// Represents the number of updates on the page.
  uint32_t num_updates_ = 0;

  /// Represents the last updated slot id on the page.
  int32_t last_updated_slot_ = -1;

  void Update(bool encountered_contention, int32_t last_updated_slot) {
    num_contentions_ += encountered_contention;
    num_updates_++;
    last_updated_slot_ = last_updated_slot;
  }

  uint32_t ContentionPercentage() {
    return 100.0 * num_contentions_ / num_updates_;
  }

  void Reset() {
    num_contentions_ = 0;
    num_updates_ = 0;
    last_updated_slot_ = -1;
  }
};

class BufferFrame;

enum class State : uint8_t { kFree = 0, kHot = 1, kCool = 2, kLoaded = 3 };

class BufferFrameHeader {
public:
  static constexpr lean_wid_t kInvalidWorkerId = std::numeric_limits<lean_wid_t>::max();
  static constexpr lean_pid_t kInvalidPageId = std::numeric_limits<lean_pid_t>::max();

  BufferFrameHeader() = default;

  /// Latch of the buffer frame. The optimistic version in the latch is never
  /// decreased.
  HybridMutex latch_;

  /// The free buffer frame in the free list of each buffer partition.
  BufferFrame* next_free_bf_ = nullptr;

  /// ID of page resides in this buffer frame.
  lean_pid_t page_id_ = kInvalidPageId;

  /// The flushed page version of the containing page. Initialized when the
  /// containing page is loaded from disk.
  lean_lid_t flushed_page_version_ = 0;

  /// ID of the last worker who has modified the containing page. For remote flush avoidance (RFA),
  /// see "Rethinking Logging, Checkpoints, and Recovery for High-Performance Storage Engines,
  /// SIGMOD 2020" for details.
  lean_wid_t last_writer_worker_ = kInvalidWorkerId;

  /// Contention statistics about the BTreeNode in the containing page. Used for contention-based
  /// node split for BTrees.
  ContentionStats contention_stats_;

  /// Crc32 checksum of the containing page.
  uint32_t crc32_ = 0;

  /// Whether the containing page is being written back to disk.
  std::atomic<bool> is_being_written_back_ = false;

  /// The state of the buffer frame.
  State state_ = State::kFree;

  /// Used to make the buffer frame remain in memory.
  bool keep_in_memory_ = false;

  // Prerequisite: the buffer frame is exclusively locked
  void Reset() {
    LEAN_DCHECK(latch_.IsLockedExclusively());
    LEAN_DCHECK(!is_being_written_back_);

    next_free_bf_ = nullptr;
    page_id_ = kInvalidPageId;
    flushed_page_version_ = 0;
    last_writer_worker_ = kInvalidWorkerId;
    contention_stats_.Reset();
    crc32_ = 0;
    is_being_written_back_.store(false, std::memory_order_release);
    state_ = State::kFree;
    keep_in_memory_ = false;
  }

  std::string StateString() {
    switch (state_) {
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
  /// Short for "system transaction id", increased when a system transaction modifies the page.
  uint64_t sys_tx_id_ = 0;

  /// Short for "page version", increased when a page is modified by any user or system
  /// transaction. A page is "dirty" when page_.page_version_ > header_.flushed_page_version_.
  lean_lid_t page_version_ = 0;

  /// The btree ID it belongs to.
  lean_treeid_t btree_id_ = std::numeric_limits<lean_treeid_t>::max();

  /// Used for debug, page id is stored in it when evicted to disk.
  uint64_t magic_debugging_;

  /// The data stored in this page. The btree node content is stored here.
  uint8_t payload_[];

  uint32_t Crc32() {
    return utils::Crc32(payload_, CoroEnv::CurStore().store_option_->page_size_ - sizeof(Page));
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
  ALIGNAS(512) BufferFrameHeader header_;

  // The persisted data part. Each page maps to a underlying disk page. It's
  // persisted to disk when the checkpoint happens, or when the storage is
  // shutdown. It should be recovered based on the old page content and the
  // write-ahead log of the page.
  ALIGNAS(512) Page page_;

  BufferFrame() = default;

  bool operator==(const BufferFrame& other) {
    return this == &other;
  }

  bool IsDirty() const {
    return page_.page_version_ > header_.flushed_page_version_;
  }

  bool IsFree() const {
    return header_.state_ == State::kFree;
  }

  bool ShouldRemainInMem() {
    return header_.keep_in_memory_ || header_.is_being_written_back_ ||
           header_.latch_.IsLockedExclusively();
  }

  void Init(lean_pid_t page_id) {
    LEAN_DCHECK(header_.state_ == State::kFree);
    header_.page_id_ = page_id;
    header_.state_ = State::kHot;
    header_.flushed_page_version_ = 0;

    page_.sys_tx_id_ = 0;
    page_.page_version_ = 0;
  }

  // Pre: bf is exclusively locked
  void Reset() {
    header_.Reset();
  }

  lean_pid_t GetPageId() {
    return header_.page_id_;
  }
};

} // namespace leanstore
