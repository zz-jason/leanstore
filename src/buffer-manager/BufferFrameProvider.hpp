#pragma once

#include "buffer-manager/AsyncWriteBuffer.hpp"
#include "buffer-manager/BMPlainGuard.hpp"
#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/FreeList.hpp"
#include "buffer-manager/Partition.hpp"
#include "buffer-manager/Swip.hpp"
#include "buffer-manager/TreeRegistry.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/Exceptions.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/PPCounters.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"
#include "utils/UserThread.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <mutex>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore {

namespace storage {

class FreeBfList {
private:
  BufferFrame* mFirst = nullptr;

  BufferFrame* mLast = nullptr;

  uint64_t mSize = 0;

public:
  void Reset() {
    mFirst = nullptr;
    mLast = nullptr;
    mSize = 0;
  }

  void PopTo(Partition& partition) {
    partition.mFreeBfList.PushFront(mFirst, mLast, mSize);
    Reset();
  }

  uint64_t Size() {
    return mSize;
  }

  void PushFront(BufferFrame& bf) {
    bf.mHeader.mNextFreeBf = mFirst;
    mFirst = &bf;
    mSize++;
    if (mLast == nullptr) {
      mLast = &bf;
    }
  }
};

/// BufferFrameProvider provides free buffer frames for partitions.
class BufferFrameProvider : public utils::UserThread {
public:
  leanstore::LeanStore* mStore;
  const uint64_t mNumBfs;
  uint8_t* mBufferPool;

  const uint64_t mNumPartitions;
  const uint64_t mPartitionsMask;
  std::vector<std::unique_ptr<Partition>>& mPartitions;

  const int mFD;

  std::vector<BufferFrame*> mCoolCandidateBfs;  // input of phase 1
  std::vector<BufferFrame*> mEvictCandidateBfs; // output of phase 1
  AsyncWriteBuffer mAsyncWriteBuffer;           // output of phase 2
  FreeBfList mFreeBfList;                       // output of phase 3

public:
  BufferFrameProvider(leanstore::LeanStore* store,
                      const std::string& threadName, uint64_t runningCPU,
                      uint64_t numBfs, uint8_t* bfs, uint64_t numPartitions,
                      uint64_t partitionMask,
                      std::vector<std::unique_ptr<Partition>>& partitions)
      : utils::UserThread(threadName, runningCPU),
        mStore(store),
        mNumBfs(numBfs),
        mBufferPool(bfs),
        mNumPartitions(numPartitions),
        mPartitionsMask(partitionMask),
        mPartitions(partitions),
        mFD(store->mPageFd),
        mCoolCandidateBfs(),
        mEvictCandidateBfs(),
        mAsyncWriteBuffer(store->mPageFd, store->mStoreOption.mPageSize,
                          FLAGS_write_buffer_size),
        mFreeBfList() {
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
  /// Randomly picks a batch of buffer frames from the whole memory, gather the
  /// cool buffer frames for the next round to evict, cools the hot buffer
  /// frames if all their children are evicted.
  ///
  /// NOTE:
  /// 1. Only buffer frames that are cool are added in the eviction batch and
  ///    being evicted in the next phase.
  ///
  /// 2. Only buffer frames that are hot and all the children are evicted
  ///    can be cooled at this phase. Buffer frames cooled at this phase won't
  ///    be evicted in the next phase directly, they will be added to the
  ///    eviction batch in the future round of PickBufferFramesToCool() if they
  ///    stay cool at that time.
  ///
  /// @param targetPartition the target partition which needs more buffer frames
  /// to load pages for worker threads.
  void PickBufferFramesToCool(Partition& targetPartition);

  void PrepareAsyncWriteBuffer(Partition& targetPartition);

  void FlushAndRecycleBufferFrames(Partition& targetPartition);

protected:
  void runImpl() override;

private:
  inline void randomBufferFramesToCoolOrEvict() {
    mCoolCandidateBfs.clear();
    for (uint64_t i = 0; i < FLAGS_buffer_frame_recycle_batch_size; i++) {
      auto* randomBf = randomBufferFrame();
      DO_NOT_OPTIMIZE(randomBf->mHeader.mState);
      mCoolCandidateBfs.push_back(randomBf);
    }
  }

  inline BufferFrame* randomBufferFrame() {
    auto i = utils::RandomGenerator::Rand<uint64_t>(0, mNumBfs);
    auto* bfAddr = &mBufferPool[i * BufferFrame::Size()];
    return reinterpret_cast<BufferFrame*>(bfAddr);
  }

  inline Partition& randomPartition() {
    auto i = utils::RandomGenerator::Rand<uint64_t>(0, mNumPartitions);
    return *mPartitions[i];
  }

  inline uint64_t getPartitionId(PID pageId) {
    return pageId & mPartitionsMask;
  }

  void evictFlushedBf(BufferFrame& cooledBf, BMOptimisticGuard& optimisticGuard,
                      Partition& targetPartition);
};

using Time = decltype(std::chrono::high_resolution_clock::now());

inline void BufferFrameProvider::runImpl() {
  CPUCounters::registerThread(mThreadName);
  if (FLAGS_root) {
    // https://linux.die.net/man/2/setpriority
    POSIX_CHECK(setpriority(PRIO_PROCESS, 0, -20) == 0);
  }

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
      PPCounters::MyCounters().pp_thread_rounds++;
    }
  }
}

inline void BufferFrameProvider::evictFlushedBf(
    BufferFrame& cooledBf, BMOptimisticGuard& optimisticGuard,
    Partition& targetPartition) {
  TREEID btreeId = cooledBf.mPage.mBTreeId;
  optimisticGuard.JumpIfModifiedByOthers();
  ParentSwipHandler parentHandler =
      mStore->mTreeRegistry->FindParent(btreeId, cooledBf);

  DCHECK(parentHandler.mParentGuard.mState == GuardState::kOptimisticShared);
  BMExclusiveUpgradeIfNeeded parentWriteGuard(parentHandler.mParentGuard);
  optimisticGuard.mGuard.ToExclusiveMayJump();

  if (FLAGS_crc_check && cooledBf.mHeader.mCrc) {
    DCHECK(cooledBf.mPage.CRC() == cooledBf.mHeader.mCrc);
  }
  DCHECK(!cooledBf.IsDirty());
  DCHECK(!cooledBf.mHeader.mIsBeingWrittenBack);
  DCHECK(cooledBf.mHeader.mState == State::kCool);
  DCHECK(parentHandler.mChildSwip.IsCool());

  parentHandler.mChildSwip.Evict(cooledBf.mHeader.mPageId);

  // Reclaim buffer frame
  cooledBf.Reset();
  cooledBf.mHeader.mLatch.UnlockExclusively();

  mFreeBfList.PushFront(cooledBf);
  if (mFreeBfList.Size() <=
      std::min<uint64_t>(mStore->mStoreOption.mNumTxWorkers, 128)) {
    mFreeBfList.PopTo(targetPartition);
  }

  COUNTERS_BLOCK() {
    PPCounters::MyCounters().evicted_pages++;
  }
};

// phase 1: find cool candidates and cool them
// hot and all the children are evicted: cool it
// hot but one of the chidren is cool: choose the child and restart
// cool: evict it
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
      PPCounters::MyCounters().mPhase1MS +=
          (std::chrono::duration_cast<std::chrono::microseconds>(phase1End -
                                                                 phase1Begin)
               .count());
    });
  }

  // [corner cases]: prevent starving when free list is empty and cooling to
  // the required level can not be achieved
  uint64_t failedAttempts = 0;
  if (targetPartition.NeedMoreFreeBfs() && failedAttempts < 10) {
    randomBufferFramesToCoolOrEvict();
    while (mCoolCandidateBfs.size() > 0) {
      auto* coolCandidate = mCoolCandidateBfs.back();
      mCoolCandidateBfs.pop_back();
      COUNTERS_BLOCK() {
        PPCounters::MyCounters().phase_1_counter++;
      }
      JUMPMU_TRY() {
        BMOptimisticGuard readGuard(coolCandidate->mHeader.mLatch);
        if (coolCandidate->ShouldRemainInMem()) {
          failedAttempts = failedAttempts + 1;
          DLOG(WARNING) << "Cool candidate discarded, should remain in memory"
                        << ", pageId=" << coolCandidate->mHeader.mPageId;
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        if (coolCandidate->mHeader.mState == State::kCool) {
          mEvictCandidateBfs.push_back(coolCandidate);
          LOG(INFO) << "Find a cool buffer frame, added to mEvictCandidateBfs"
                    << ", pageId=" << coolCandidate->mHeader.mPageId;
          // TODO: maybe without failedAttempts?
          failedAttempts = failedAttempts + 1;
          DLOG(WARNING) << "Cool candidate discarded, it's already cool"
                        << ", pageId=" << coolCandidate->mHeader.mPageId;
          JUMPMU_CONTINUE;
        }

        if (coolCandidate->mHeader.mState != State::kHot) {
          failedAttempts = failedAttempts + 1;
          DLOG(WARNING) << "Cool candidate discarded, it's not hot"
                        << ", pageId=" << coolCandidate->mHeader.mPageId;
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        COUNTERS_BLOCK() {
          PPCounters::MyCounters().touched_bfs_counter++;
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

        mStore->mTreeRegistry->IterateChildSwips(
            coolCandidate->mPage.mBTreeId, *coolCandidate, [&](Swip& swip) {
              // Ignore when it has a child in the cooling stage
              allChildrenEvicted &= swip.IsEvicted();
              if (swip.IsHot()) {
                BufferFrame* childBf = &swip.AsBufferFrame();
                readGuard.JumpIfModifiedByOthers();
                pickedAChild = true;
                mCoolCandidateBfs.push_back(childBf);
                DLOG(WARNING)
                    << "Cool candidate discarded, one of its child is hot"
                    << ", pageId=" << coolCandidate->mHeader.mPageId
                    << ", hotChildPageId=" << childBf->mHeader.mPageId
                    << ", the hot child is picked as the next cool candidate";
                return false;
              }
              readGuard.JumpIfModifiedByOthers();
              return true;
            });

        COUNTERS_BLOCK() {
          iterateChildrenEnd = std::chrono::high_resolution_clock::now();
          PPCounters::MyCounters().mIterateChildrenMS +=
              (std::chrono::duration_cast<std::chrono::microseconds>(
                   iterateChildrenEnd - iterateChildrenBegin)
                   .count());
        }
        if (!allChildrenEvicted || pickedAChild) {
          DLOG(WARNING)
              << "Cool candidate discarded, not all the children are evicted"
              << ", pageId=" << coolCandidate->mHeader.mPageId
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
        TREEID btreeId = coolCandidate->mPage.mBTreeId;
        readGuard.JumpIfModifiedByOthers();
        auto parentHandler =
            mStore->mTreeRegistry->FindParent(btreeId, *coolCandidate);

        DCHECK(parentHandler.mParentGuard.mState ==
               GuardState::kOptimisticShared);
        DCHECK(parentHandler.mParentGuard.mLatch !=
               reinterpret_cast<HybridLatch*>(0x99));
        COUNTERS_BLOCK() {
          findParentEnd = std::chrono::high_resolution_clock::now();
          PPCounters::MyCounters().mFindParentMS +=
              (std::chrono::duration_cast<std::chrono::microseconds>(
                   findParentEnd - findParentBegin)
                   .count());
        }
        readGuard.JumpIfModifiedByOthers();
        auto checkResult = mStore->mTreeRegistry->CheckSpaceUtilization(
            coolCandidate->mPage.mBTreeId, *coolCandidate);
        if (checkResult == SpaceCheckResult::kRestartSameBf ||
            checkResult == SpaceCheckResult::kPickAnotherBf) {
          DLOG(WARNING) << "Cool candidate discarded, space check failed"
                        << ", pageId=" << coolCandidate->mHeader.mPageId
                        << ", checkResult is kRestartSameBf || kPickAnotherBf";
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        // Suitable page founds, lets cool
        const PID pageId = coolCandidate->mHeader.mPageId;
        {
          // writeGuard can only be acquired and released while the partition
          // mutex is locked
          BMExclusiveUpgradeIfNeeded parentWriteGuard(
              parentHandler.mParentGuard);
          BMExclusiveGuard writeGuard(readGuard);

          DCHECK(coolCandidate->mHeader.mPageId == pageId);
          DCHECK(coolCandidate->mHeader.mState == State::kHot);
          DCHECK(coolCandidate->mHeader.mIsBeingWrittenBack == false);
          DCHECK(parentHandler.mParentGuard.mVersion ==
                 parentHandler.mParentGuard.mLatch->GetOptimisticVersion());
          DCHECK(parentHandler.mChildSwip.mBf == coolCandidate);

          // mark the buffer frame in cool state
          coolCandidate->mHeader.mState = State::kCool;
          // mark the swip to the buffer frame to cool state
          parentHandler.mChildSwip.Cool();
          DLOG(WARNING) << "Cool candidate find, state changed to cool"
                        << ", pageId=" << coolCandidate->mHeader.mPageId;
        }

        COUNTERS_BLOCK() {
          PPCounters::MyCounters().unswizzled_pages_counter++;
        }
        failedAttempts = 0;
      }
      JUMPMU_CATCH() {
        DLOG(WARNING)
            << "Cool candidate discarded, optimistic latch failed, someone has "
               "modified the buffer frame during cool validateion"
            << ", pageId=" << coolCandidate->mHeader.mPageId;
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

  mFreeBfList.Reset();
  for (const volatile auto& cooledBf : mEvictCandidateBfs) {
    JUMPMU_TRY() {
      BMOptimisticGuard optimisticGuard(cooledBf->mHeader.mLatch);
      // Check if the BF got swizzled in or unswizzle another time in another
      // partition
      if (cooledBf->mHeader.mState != State::kCool ||
          cooledBf->mHeader.mIsBeingWrittenBack) {
        DLOG(WARNING) << "COOLed buffer frame discarded"
                      << ", pageId=" << cooledBf->mHeader.mPageId << ", IsCool="
                      << (cooledBf->mHeader.mState == State::kCool)
                      << ", isBeingWritternBack="
                      << cooledBf->mHeader.mIsBeingWrittenBack;
        JUMPMU_CONTINUE;
      }
      const PID cooledPageId = cooledBf->mHeader.mPageId;
      const uint64_t partitionId = getPartitionId(cooledPageId);

      // Prevent evicting a page that already has an IO Frame with (possibly)
      // threads working on it.
      Partition& partition = *mPartitions[partitionId];
      JumpScoped<std::unique_lock<std::mutex>> ioGuard(
          partition.mInflightIOMutex);
      if (partition.mInflightIOs.Lookup(cooledPageId)) {
        DLOG(WARNING) << "COOLed buffer frame discarded, already in IO stage"
                      << ", pageId=" << cooledBf->mHeader.mPageId
                      << ", partitionId=" << partitionId;
        JUMPMU_CONTINUE;
      }

      // Evict clean pages. They can be safely cleared in memory without
      // writing any bytes back to the underlying disk.
      if (!cooledBf->IsDirty()) {
        evictFlushedBf(*cooledBf, optimisticGuard, targetPartition);
        DLOG(INFO) << "COOLed buffer frame is not dirty, reclaim directly"
                   << ", pageId=" << cooledBf->mHeader.mPageId;
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
      DCHECK(!cooledBf->mHeader.mIsBeingWrittenBack);
      cooledBf->mHeader.mIsBeingWrittenBack.store(true,
                                                  std::memory_order_release);

      // performs crc check if necessary
      if (FLAGS_crc_check) {
        cooledBf->mHeader.mCrc = cooledBf->mPage.CRC();
      }

      // TODO: preEviction callback according to TREEID
      mAsyncWriteBuffer.AddToIOBatch(*cooledBf, cooledPageId);
      DLOG(INFO) << "COOLed buffer frame is added to async write buffer"
                 << ", pageId=" << cooledBf->mHeader.mPageId
                 << ", bufferSize=" << mAsyncWriteBuffer.pending_requests;
    }
    JUMPMU_CATCH() {
      DLOG(WARNING) << "COOLed buffer frame discarded, optimistic latch "
                       "failed, someone has modified the buffer frame during "
                       "cool validateion"
                    << ", pageId=" << cooledBf->mHeader.mPageId;
    }
  }

  mEvictCandidateBfs.clear();
}

inline void BufferFrameProvider::FlushAndRecycleBufferFrames(
    Partition& targetPartition) {
  DLOG(INFO) << "Phase3: FlushAndRecycleBufferFrames begins";
  SCOPED_DEFER(DLOG(INFO) << "Phase3: FlushAndRecycleBufferFrames ended");

  if (mAsyncWriteBuffer.SubmitIORequest()) {
    const uint32_t numFlushedBfs = mAsyncWriteBuffer.WaitIORequestToComplete();
    mAsyncWriteBuffer.IterateFlushedBfs(
        [&](BufferFrame& writtenBf, uint64_t flushPSN) {
          JUMPMU_TRY() {
            // When the written back page is being exclusively locked, we should
            // rather waste the write and move on to another page Instead of
            // waiting on its latch because of the likelihood that a data
            // structure implementation keeps holding a parent latch while
            // trying to acquire a new page
            BMOptimisticGuard optimisticGuard(writtenBf.mHeader.mLatch);
            BMExclusiveGuard exclusiveGuard(optimisticGuard);
            DCHECK(writtenBf.mHeader.mIsBeingWrittenBack);
            DCHECK(writtenBf.mHeader.mFlushedPSN < flushPSN);

            // For recovery, so much has to be done here...
            writtenBf.mHeader.mFlushedPSN = flushPSN;
            writtenBf.mHeader.mIsBeingWrittenBack = false;
            PPCounters::MyCounters().flushed_pages_counter++;
          }
          JUMPMU_CATCH() {
            writtenBf.mHeader.mCrc = 0;
            writtenBf.mHeader.mIsBeingWrittenBack.store(
                false, std::memory_order_release);
          }

          JUMPMU_TRY() {
            BMOptimisticGuard optimisticGuard(writtenBf.mHeader.mLatch);
            if (writtenBf.mHeader.mState == State::kCool &&
                !writtenBf.mHeader.mIsBeingWrittenBack &&
                !writtenBf.IsDirty()) {
              evictFlushedBf(writtenBf, optimisticGuard, targetPartition);
            }
          }
          JUMPMU_CATCH() {
          }
        },
        numFlushedBfs);
  }
  if (mFreeBfList.Size()) {
    mFreeBfList.PopTo(targetPartition);
  }
}

} // namespace storage
} // namespace leanstore