#pragma once

#include "AsyncWriteBuffer.hpp"
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "Config.hpp"
#include "Exceptions.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "PerfEvent.hpp"
#include "Swip.hpp"
#include "Tracing.hpp"
#include "TreeRegistry.hpp"
#include "Units.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/PPCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/AsyncWriteBuffer.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "utils/Defer.hpp"
#include "utils/FVector.hpp"
#include "utils/Misc.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstring>
#include <fcntl.h>
#include <list>
#include <mutex>
#include <queue>
#include <sys/resource.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

namespace leanstore {

namespace storage {

struct FreedBfsBatch {
  BufferFrame* freed_bfs_batch_head = nullptr;
  BufferFrame* freed_bfs_batch_tail = nullptr;
  u64 freed_bfs_counter = 0;

  void reset() {
    freed_bfs_batch_head = nullptr;
    freed_bfs_batch_tail = nullptr;
    freed_bfs_counter = 0;
  }

  void push(Partition& partition) {
    partition.mFreeBfList.PushFront(freed_bfs_batch_head, freed_bfs_batch_tail,
                                    freed_bfs_counter);
    reset();
  }

  u64 size() {
    return freed_bfs_counter;
  }

  void add(BufferFrame& bf) {
    bf.header.mNextFreeBf = freed_bfs_batch_head;
    if (freed_bfs_batch_head == nullptr) {
      freed_bfs_batch_tail = &bf;
    }
    freed_bfs_batch_head = &bf;
    freed_bfs_counter++;
  }
};

/// BufferFrameProvider provides free buffer frames for partitions.
class BufferFrameProvider {
public:
  const u64 mId;
  const std::string mThreadName;
  std::unique_ptr<std::thread> mThread;
  atomic<bool> mKeepRunning;

  const u64 mNumBfs;
  u8* mBufferPool;

  const u64 mNumPartitions;
  const u64 mPartitionsMask;
  std::vector<std::unique_ptr<Partition>>& mPartitions;

  const int mFD;

  AsyncWriteBuffer mAsyncWriteBuffer; // output of phase 2

  std::vector<BufferFrame*> mCoolCandidateBfs;  // input of phase 1
  std::vector<BufferFrame*> mEvictCandidateBfs; // output of phase 1
  // AsyncWriteBuffer mAsyncWriteBuffer;           // output of phase 2
  FreedBfsBatch mFreedBfsBatch; // output of phase 3

public:
  BufferFrameProvider(u64 id, const std::string& threadName, u64 numBfs,
                      u8* bfs, u64 numPartitions, u64 partitionMask,
                      std::vector<std::unique_ptr<Partition>>& partitions,
                      int fd)
      : mId(id), mThreadName(threadName), mThread(nullptr), mKeepRunning(false),
        mNumBfs(numBfs), mBufferPool(bfs), mNumPartitions(numPartitions),
        mPartitionsMask(partitionMask), mPartitions(partitions), mFD(fd),
        mAsyncWriteBuffer(fd, FLAGS_page_size, FLAGS_write_buffer_size) {
    mCoolCandidateBfs.reserve(FLAGS_buffer_frame_recycle_batch_size);
    mEvictCandidateBfs.reserve(FLAGS_buffer_frame_recycle_batch_size);
  }

  // no copy and assign
  BufferFrameProvider(const BufferFrameProvider&) = delete;
  BufferFrameProvider& operator=(const BufferFrameProvider&) = delete;

  // no move construct and assign
  BufferFrameProvider(BufferFrameProvider&& other) = delete;
  BufferFrameProvider& operator=(BufferFrameProvider&& other) = delete;

  ~BufferFrameProvider() {
    Stop();
  }

public:
  void Start() {
    if (mThread == nullptr) {
      mKeepRunning = true;
      mThread = std::make_unique<std::thread>(&BufferFrameProvider::Run, this);
      mThread->detach();
    }
  }

  void Stop() {
    mKeepRunning = false;
    if (mThread && mThread->joinable()) {
      mThread->join();
    }
    mThread = nullptr;
  }

  void Run();

  void InitThread();

  /**
   * @brief PickBufferFramesToCool randomly picks a batch of buffer frames from
   * the whole memory, gather the COOL buffer frames for the next round to
   * evict, cools the HOT buffer frames if all their children are evicted.
   *
   * @note:
   * 1. Only buffer frames that are COOL are added in the eviction batch and
   *    being evicted in the next phase.
   *
   * 2. Only buffer frames that are HOT and all the children are evicted
   *    can be cooled at this phase. Buffer frames cooled at this phase won't
   *    be evicted in the next phase directly, they will be added to the
   *    eviction batch in the future round of PickBufferFramesToCool() if they
   *    stay COOL at that time.
   *
   * @param targetPartition the target partition which needs more buffer frames
   * to load pages for worker threads.
   */
  void PickBufferFramesToCool(Partition& targetPartition);

  void PrepareAsyncWriteBuffer(Partition& targetPartition);

  void FlushAndRecycleBufferFrames(Partition& targetPartition);

private:
  inline void RandomBufferFramesToCoolOrEvict() {
    mCoolCandidateBfs.clear();
    for (u64 i = 0; i < FLAGS_buffer_frame_recycle_batch_size; i++) {
      auto randomBf = RandomBufferFrame();
      DO_NOT_OPTIMIZE(randomBf->header.state);
      mCoolCandidateBfs.push_back(randomBf);
    }
  }

  inline BufferFrame* RandomBufferFrame() {
    auto i = utils::RandomGenerator::getRand<u64>(0, mNumBfs);
    auto bfAddr = &mBufferPool[i * BufferFrame::Size()];
    return reinterpret_cast<BufferFrame*>(bfAddr);
  }

  inline Partition& randomPartition() {
    auto i = utils::RandomGenerator::getRand<u64>(0, mNumPartitions);
    return *mPartitions[i];
  }

  inline u64 GetPartitionId(PID pageId) {
    return pageId & mPartitionsMask;
  }

  void EvictFlushedBf(BufferFrame& cooledBf, BMOptimisticGuard& optimisticGuard,
                      Partition& targetPartition);
};

using Time = decltype(std::chrono::high_resolution_clock::now());

inline void BufferFrameProvider::Run() {
  LOG(INFO) << "BufferFrameProvider thread started"
            << ", thread id=" << mId << ", threadName=" << mThreadName
            << ", numBufferFrames=" << mNumBfs
            << ", numPartitions=" << mNumPartitions;
  SCOPED_DEFER(LOG(INFO) << "BufferFrameProvider thread stopped");

  InitThread();

  while (mKeepRunning) {
    auto& targetPartition = randomPartition();
    if (!targetPartition.NeedMoreFreeBfs()) {
      continue;
    }

    // Phase 1:
    PickBufferFramesToCool(targetPartition);

    // Phase 2:
    PrepareAsyncWriteBuffer(targetPartition);

    // Phase 3:
    FlushAndRecycleBufferFrames(targetPartition);

    COUNTERS_BLOCK() {
      PPCounters::myCounters().pp_thread_rounds++;
    }
  }
}

inline void BufferFrameProvider::InitThread() {
  if (FLAGS_enable_pin_worker_threads) {
    utils::pinThisThread(FLAGS_worker_threads + FLAGS_wal + mId);
  } else {
    utils::pinThisThread(FLAGS_wal + mId);
  }

  CPUCounters::registerThread(mThreadName);
  if (FLAGS_root) {
    // https://linux.die.net/man/2/setpriority
    POSIX_CHECK(setpriority(PRIO_PROCESS, 0, -20) == 0);
  }

  pthread_setname_np(pthread_self(), mThreadName.c_str());
}

inline void BufferFrameProvider::EvictFlushedBf(
    BufferFrame& cooledBf, BMOptimisticGuard& optimisticGuard,
    Partition& targetPartition) {
  TREEID btreeId = cooledBf.page.mBTreeId;
  optimisticGuard.JumpIfModifiedByOthers();
  ParentSwipHandler parentHandler =
      TreeRegistry::sInstance->findParent(btreeId, cooledBf);

  DCHECK(parentHandler.mParentGuard.mState == GUARD_STATE::OPTIMISTIC);
  BMExclusiveUpgradeIfNeeded parentWriteGuard(parentHandler.mParentGuard);
  optimisticGuard.mGuard.ToExclusiveMayJump();

  if (FLAGS_crc_check && cooledBf.header.crc) {
    DCHECK(cooledBf.page.CRC() == cooledBf.header.crc);
  }
  DCHECK(!cooledBf.isDirty());
  DCHECK(!cooledBf.header.mIsBeingWrittenBack);
  DCHECK(cooledBf.header.state == STATE::COOL);
  DCHECK(parentHandler.mChildSwip.isCOOL());

  parentHandler.mChildSwip.evict(cooledBf.header.mPageId);
  PID evictedPageId = cooledBf.header.mPageId;

  // Reclaim buffer frame
  cooledBf.reset();
  cooledBf.header.mLatch.UnlockExclusively();

  mFreedBfsBatch.add(cooledBf);
  if (mFreedBfsBatch.size() <= std::min<u64>(FLAGS_worker_threads, 128)) {
    mFreedBfsBatch.push(targetPartition);
  }

  if (FLAGS_pid_tracing) {
    Tracing::mutex.lock();
    if (Tracing::ht.contains(evictedPageId)) {
      std::get<1>(Tracing::ht[evictedPageId])++;
    } else {
      Tracing::ht[evictedPageId] = {btreeId, 1};
    }
    Tracing::mutex.unlock();
  }

  COUNTERS_BLOCK() {
    PPCounters::myCounters().evicted_pages++;
  }
};

// phase 1: find cool candidates and cool them
// HOT and all the children are evicted: COOL it
// HOT but one of the chidren is COOL: choose the child and restart
// COOL: EVICT it
inline void BufferFrameProvider::PickBufferFramesToCool(
    Partition& targetPartition) {
  DLOG(INFO) << "Phase1: PickBufferFramesToCool begins";
  SCOPED_DEFER(DLOG(INFO) << "Phase1: PickBufferFramesToCool ended"
                          << ", mEvictCandidateBfs.size="
                          << mEvictCandidateBfs.size());

  COUNTERS_BLOCK() {
    auto phase1Begin = std::chrono::high_resolution_clock::now();
    SCOPED_DEFER({
      auto phase1End = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().phase_1_ms +=
          (std::chrono::duration_cast<std::chrono::microseconds>(phase1End -
                                                                 phase1Begin)
               .count());
    });
  }

  // [corner cases]: prevent starving when free list is empty and cooling to
  // the required level can not be achieved
  volatile u64 failedAttempts = 0;
  if (targetPartition.NeedMoreFreeBfs() && failedAttempts < 10) {
    RandomBufferFramesToCoolOrEvict();
    while (mCoolCandidateBfs.size() > 0) {
      BufferFrame* coolCandidate;
      JUMPMU_TRY() {
        coolCandidate = mCoolCandidateBfs.back();
        mCoolCandidateBfs.pop_back();

        COUNTERS_BLOCK() {
          PPCounters::myCounters().phase_1_counter++;
        }

        BMOptimisticGuard readGuard(coolCandidate->header.mLatch);
        if (coolCandidate->ShouldRemainInMem()) {
          failedAttempts = failedAttempts + 1;
          DLOG(WARNING) << "Cool candidate discarded, should remain in memory"
                        << ", pageId=" << coolCandidate->header.mPageId;
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        if (coolCandidate->header.state == STATE::COOL) {
          mEvictCandidateBfs.push_back(coolCandidate);
          LOG(INFO) << "Find a COOL buffer frame, added to mEvictCandidateBfs"
                    << ", pageId=" << coolCandidate->header.mPageId;
          // TODO: maybe without failedAttempts?
          failedAttempts = failedAttempts + 1;
          DLOG(WARNING) << "Cool candidate discarded, it's already cool"
                        << ", pageId=" << coolCandidate->header.mPageId;
          JUMPMU_CONTINUE;
        }

        if (coolCandidate->header.state != STATE::HOT) {
          failedAttempts = failedAttempts + 1;
          DLOG(WARNING) << "Cool candidate discarded, it's not hot"
                        << ", pageId=" << coolCandidate->header.mPageId;
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        COUNTERS_BLOCK() {
          PPCounters::myCounters().touched_bfs_counter++;
        }

        // Iterate all the child pages to check wherher all the children are
        // evicted, otherwise pick the fist met unevicted child as the next
        // cool page candidate.
        bool allChildrenEvicted(true);
        bool pickedAChild(false);
        [[maybe_unused]] Time iterateChildrenBegin;
        [[maybe_unused]] Time iterateChildrenEnd;
        COUNTERS_BLOCK() {
          iterateChildrenBegin = std::chrono::high_resolution_clock::now();
        }

        TreeRegistry::sInstance->IterateChildSwips(
            coolCandidate->page.mBTreeId, *coolCandidate,
            [&](Swip<BufferFrame>& swip) {
              // Ignore when it has a child in the cooling stage
              allChildrenEvicted &= swip.isEVICTED();
              if (swip.isHOT()) {
                BufferFrame* childBf = &swip.AsBufferFrame();
                readGuard.JumpIfModifiedByOthers();
                pickedAChild = true;
                mCoolCandidateBfs.push_back(childBf);
                DLOG(WARNING)
                    << "Cool candidate discarded, one of its child is hot"
                    << ", pageId=" << coolCandidate->header.mPageId
                    << ", hotChildPageId=" << childBf->header.mPageId
                    << ", the hot child is picked as the next cool candidate";
                return false;
              }
              readGuard.JumpIfModifiedByOthers();
              return true;
            });

        COUNTERS_BLOCK() {
          iterateChildrenEnd = std::chrono::high_resolution_clock::now();
          PPCounters::myCounters().iterate_children_ms +=
              (std::chrono::duration_cast<std::chrono::microseconds>(
                   iterateChildrenEnd - iterateChildrenBegin)
                   .count());
        }
        if (!allChildrenEvicted || pickedAChild) {
          DLOG(WARNING)
              << "Cool candidate discarded, not all the children are evicted"
              << ", pageId=" << coolCandidate->header.mPageId
              << ", allChildrenEvicted=" << allChildrenEvicted
              << ", pickedAChild=" << pickedAChild;
          failedAttempts = failedAttempts + 1;
          JUMPMU_CONTINUE;
        }

        [[maybe_unused]] Time findParentBegin;
        [[maybe_unused]] Time findParentEnd;
        COUNTERS_BLOCK() {
          findParentBegin = std::chrono::high_resolution_clock::now();
        }
        TREEID btreeId = coolCandidate->page.mBTreeId;
        readGuard.JumpIfModifiedByOthers();
        auto parentHandler =
            TreeRegistry::sInstance->findParent(btreeId, *coolCandidate);

        DCHECK(parentHandler.mParentGuard.mState == GUARD_STATE::OPTIMISTIC);
        DCHECK(parentHandler.mParentGuard.mLatch !=
               reinterpret_cast<HybridLatch*>(0x99));
        COUNTERS_BLOCK() {
          findParentEnd = std::chrono::high_resolution_clock::now();
          PPCounters::myCounters().find_parent_ms +=
              (std::chrono::duration_cast<std::chrono::microseconds>(
                   findParentEnd - findParentBegin)
                   .count());
        }
        readGuard.JumpIfModifiedByOthers();
        const auto space_check_res =
            TreeRegistry::sInstance->checkSpaceUtilization(
                coolCandidate->page.mBTreeId, *coolCandidate);
        if (space_check_res == SpaceCheckResult::RESTART_SAME_BF ||
            space_check_res == SpaceCheckResult::PICK_ANOTHER_BF) {
          DLOG(WARNING)
              << "Cool candidate discarded, space check failed"
              << ", pageId=" << coolCandidate->header.mPageId
              << ", space_check_res is RESTART_SAME_BF || PICK_ANOTHER_BF";
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        // Suitable page founds, lets cool
        const PID pageId = coolCandidate->header.mPageId;
        {
          // writeGuard can only be acquired and released while the partition
          // mutex is locked
          BMExclusiveUpgradeIfNeeded parentWriteGuard(
              parentHandler.mParentGuard);
          BMExclusiveGuard writeGuard(readGuard);

          DCHECK(coolCandidate->header.mPageId == pageId);
          DCHECK(coolCandidate->header.state == STATE::HOT);
          DCHECK(coolCandidate->header.mIsBeingWrittenBack == false);
          DCHECK(parentHandler.mParentGuard.mVersion ==
                 parentHandler.mParentGuard.mLatch->GetOptimisticVersion());
          DCHECK(parentHandler.mChildSwip.bf == coolCandidate);

          // mark the buffer frame in cool state
          coolCandidate->header.state = STATE::COOL;
          // mark the swip to the buffer frame to cool state
          parentHandler.mChildSwip.cool();
          DLOG(WARNING) << "Cool candidate find, state changed to COOL"
                        << ", pageId=" << coolCandidate->header.mPageId;
        }

        COUNTERS_BLOCK() {
          PPCounters::myCounters().unswizzled_pages_counter++;
        }
        failedAttempts = 0;
      }
      JUMPMU_CATCH() {
        DLOG(WARNING)
            << "Cool candidate discarded, optimistic latch failed, someone has "
               "modified the buffer frame during cool validateion"
            << ", pageId=" << coolCandidate->header.mPageId;
      }
    }
  }
}

inline void BufferFrameProvider::PrepareAsyncWriteBuffer(
    Partition& targetPartition) {
  DLOG(INFO) << "Phase2: PrepareAsyncWriteBuffer begins";
  SCOPED_DEFER(DLOG(INFO) << "Phase2: PrepareAsyncWriteBuffer ended"
                          << ", mAsyncWriteBuffer.pending_requests="
                          << mAsyncWriteBuffer.pending_requests);

  mFreedBfsBatch.reset();
  for (volatile const auto& cooledBf : mEvictCandidateBfs) {
    JUMPMU_TRY() {
      BMOptimisticGuard optimisticGuard(cooledBf->header.mLatch);
      // Check if the BF got swizzled in or unswizzle another time in another
      // partition
      if (cooledBf->header.state != STATE::COOL ||
          cooledBf->header.mIsBeingWrittenBack) {
        DLOG(WARNING) << "COOLed buffer frame discarded"
                      << ", pageId=" << cooledBf->header.mPageId
                      << ", isCOOL=" << (cooledBf->header.state == STATE::COOL)
                      << ", isBeingWritternBack="
                      << cooledBf->header.mIsBeingWrittenBack;
        JUMPMU_CONTINUE;
      }
      const PID cooledPageId = cooledBf->header.mPageId;
      const u64 partitionId = GetPartitionId(cooledPageId);

      // Prevent evicting a page that already has an IO Frame with (possibly)
      // threads working on it.
      Partition& partition = *mPartitions[partitionId];
      JumpScoped<std::unique_lock<std::mutex>> io_guard(
          partition.mInflightIOMutex);
      if (partition.mInflightIOs.Lookup(cooledPageId)) {
        DLOG(WARNING) << "COOLed buffer frame discarded, already in IO stage"
                      << ", pageId=" << cooledBf->header.mPageId
                      << ", partitionId=" << partitionId;
        JUMPMU_CONTINUE;
      }

      // Evict clean pages. They can be safely cleared in memory without
      // writing any bytes back to the underlying disk.
      if (!cooledBf->isDirty()) {
        EvictFlushedBf(*cooledBf, optimisticGuard, targetPartition);
        DLOG(INFO) << "COOLed buffer frame is not dirty, reclaim directly"
                   << ", pageId=" << cooledBf->header.mPageId;
        JUMPMU_CONTINUE;
      }

      // Async write dirty pages back. They should keep in memory and stay in
      // cooling stage until all the contents are writtern back to the
      // underluing disk.
      if (mAsyncWriteBuffer.full()) {
        DLOG(INFO) << "Async write buffer is full"
                   << ", bufferSize=" << mAsyncWriteBuffer.pending_requests;
        JUMPMU_BREAK;
      }

      BMExclusiveGuard exclusiveGuard(optimisticGuard);
      DCHECK(!cooledBf->header.mIsBeingWrittenBack);
      cooledBf->header.mIsBeingWrittenBack.store(true,
                                                 std::memory_order_release);

      // performs crc check if necessary
      if (FLAGS_crc_check) {
        cooledBf->header.crc = cooledBf->page.CRC();
      }

      // TODO: preEviction callback according to TREEID
      mAsyncWriteBuffer.AddToIOBatch(*cooledBf, cooledPageId);
      DLOG(INFO) << "COOLed buffer frame is added to async write buffer"
                 << ", pageId=" << cooledBf->header.mPageId
                 << ", bufferSize=" << mAsyncWriteBuffer.pending_requests;
    }
    JUMPMU_CATCH() {
      DLOG(WARNING) << "COOLed buffer frame discarded, optimistic latch "
                       "failed, someone has modified the buffer frame during "
                       "cool validateion"
                    << ", pageId=" << cooledBf->header.mPageId;
    }
  }

  mEvictCandidateBfs.clear();
}

inline void BufferFrameProvider::FlushAndRecycleBufferFrames(
    Partition& targetPartition) {
  DLOG(INFO) << "Phase3: FlushAndRecycleBufferFrames begins";
  SCOPED_DEFER(DLOG(INFO) << "Phase3: FlushAndRecycleBufferFrames ended");

  if (mAsyncWriteBuffer.SubmitIORequest()) {
    const u32 numFlushedBfs = mAsyncWriteBuffer.WaitIORequestToComplete();
    mAsyncWriteBuffer.IterateFlushedBfs(
        [&](BufferFrame& writtenBf, u64 flushPSN) {
          JUMPMU_TRY() {
            // When the written back page is being exclusively locked, we should
            // rather waste the write and move on to another page Instead of
            // waiting on its latch because of the likelihood that a data
            // structure implementation keeps holding a parent latch while
            // trying to acquire a new page
            BMOptimisticGuard optimisticGuard(writtenBf.header.mLatch);
            BMExclusiveGuard exclusiveGuard(optimisticGuard);
            DCHECK(writtenBf.header.mIsBeingWrittenBack);
            DCHECK(writtenBf.header.mFlushedPSN < flushPSN);

            // For recovery, so much has to be done here...
            writtenBf.header.mFlushedPSN = flushPSN;
            writtenBf.header.mIsBeingWrittenBack = false;
            PPCounters::myCounters().flushed_pages_counter++;
          }
          JUMPMU_CATCH() {
            writtenBf.header.crc = 0;
            writtenBf.header.mIsBeingWrittenBack.store(
                false, std::memory_order_release);
          }

          JUMPMU_TRY() {
            BMOptimisticGuard optimisticGuard(writtenBf.header.mLatch);
            if (writtenBf.header.state == STATE::COOL &&
                !writtenBf.header.mIsBeingWrittenBack && !writtenBf.isDirty()) {
              EvictFlushedBf(writtenBf, optimisticGuard, targetPartition);
            }
          }
          JUMPMU_CATCH() {
          }
        },
        numFlushedBfs);
  }
  if (mFreedBfsBatch.size()) {
    mFreedBfsBatch.push(targetPartition);
  }
}

} // namespace storage
} // namespace leanstore