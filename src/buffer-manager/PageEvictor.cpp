#include "leanstore/buffer-manager/PageEvictor.hpp"

#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/buffer-manager/TreeRegistry.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Log.hpp"

#include <mutex>

namespace leanstore::storage {

using Time = decltype(std::chrono::high_resolution_clock::now());

void PageEvictor::runImpl() {
  CPUCounters::registerThread(mThreadName);

  while (mKeepRunning) {
    auto& targetPartition = mStore->mBufferManager->RandomPartition();
    if (!targetPartition.NeedMoreFreeBfs()) {
      continue;
    }

    // Phase 1
    PickBufferFramesToCool(targetPartition);

    // Phase 2
    PrepareAsyncWriteBuffer(targetPartition);

    // Phase 3
    FlushAndRecycleBufferFrames(targetPartition);

    COUNTERS_BLOCK() {
      PPCounters::MyCounters().pp_thread_rounds++;
    }
  }
}

void PageEvictor::PickBufferFramesToCool(Partition& targetPartition) {
  LS_DLOG("Phase1: PickBufferFramesToCool begins");
  SCOPED_DEFER(LS_DLOG("Phase1: PickBufferFramesToCool ended, mEvictCandidateBfs.size={}",
                       mEvictCandidateBfs.size()));

  COUNTERS_BLOCK() {
    auto phase1Begin = std::chrono::high_resolution_clock::now();
    SCOPED_DEFER({
      auto phase1End = std::chrono::high_resolution_clock::now();
      PPCounters::MyCounters().mPhase1MS +=
          (std::chrono::duration_cast<std::chrono::microseconds>(phase1End - phase1Begin).count());
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
          LS_DLOG("Cool candidate discarded, should remain in memory, pageId={}",
                  coolCandidate->mHeader.mPageId);
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        if (coolCandidate->mHeader.mState == State::kCool) {
          mEvictCandidateBfs.push_back(coolCandidate);
          LS_DLOG("Find a cool buffer frame, added to mEvictCandidateBfs, "
                  "pageId={}",
                  coolCandidate->mHeader.mPageId);
          // TODO: maybe without failedAttempts?
          failedAttempts = failedAttempts + 1;
          LS_DLOG("Cool candidate discarded, it's already cool, pageId={}",
                  coolCandidate->mHeader.mPageId);
          JUMPMU_CONTINUE;
        }

        if (coolCandidate->mHeader.mState != State::kHot) {
          failedAttempts = failedAttempts + 1;
          LS_DLOG("Cool candidate discarded, it's not hot, pageId={}",
                  coolCandidate->mHeader.mPageId);
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        COUNTERS_BLOCK() {
          PPCounters::MyCounters().touched_bfs_counter++;
        }

        // Iterate all the child pages to check whether all the children are
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
            coolCandidate->mPage.mBTreeId, *coolCandidate, [&](Swip& childSwip) {
              // Ignore when it has a child in the cooling stage
              allChildrenEvicted = allChildrenEvicted && childSwip.IsEvicted();
              if (childSwip.IsHot()) {
                BufferFrame* childBf = &childSwip.AsBufferFrame();
                readGuard.JumpIfModifiedByOthers();
                pickedAChild = true;
                mCoolCandidateBfs.push_back(childBf);
                LS_DLOG("Cool candidate discarded, one of its child is hot, "
                        "pageId={}, hotChildPageId={}, the hot child is "
                        "picked as the next cool candidate",
                        coolCandidate->mHeader.mPageId, childBf->mHeader.mPageId);
                return false;
              }
              readGuard.JumpIfModifiedByOthers();
              return true;
            });

        COUNTERS_BLOCK() {
          iterateChildrenEnd = std::chrono::high_resolution_clock::now();
          PPCounters::MyCounters().mIterateChildrenMS +=
              (std::chrono::duration_cast<std::chrono::microseconds>(iterateChildrenEnd -
                                                                     iterateChildrenBegin)
                   .count());
        }
        if (!allChildrenEvicted || pickedAChild) {
          LS_DLOG("Cool candidate discarded, not all the children are "
                  "evicted, pageId={}, allChildrenEvicted={}, pickedAChild={}",
                  coolCandidate->mHeader.mPageId, allChildrenEvicted, pickedAChild);
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
        auto parentHandler = mStore->mTreeRegistry->FindParent(btreeId, *coolCandidate);

        LS_DCHECK(parentHandler.mParentGuard.mState == GuardState::kOptimisticShared);
        LS_DCHECK(parentHandler.mParentGuard.mLatch != reinterpret_cast<HybridLatch*>(0x99));
        COUNTERS_BLOCK() {
          findParentEnd = std::chrono::high_resolution_clock::now();
          PPCounters::MyCounters().mFindParentMS +=
              (std::chrono::duration_cast<std::chrono::microseconds>(findParentEnd -
                                                                     findParentBegin)
                   .count());
        }
        readGuard.JumpIfModifiedByOthers();
        auto checkResult = mStore->mTreeRegistry->CheckSpaceUtilization(
            coolCandidate->mPage.mBTreeId, *coolCandidate);
        if (checkResult == SpaceCheckResult::kRestartSameBf ||
            checkResult == SpaceCheckResult::kPickAnotherBf) {
          LS_DLOG("Cool candidate discarded, space check failed, "
                  "pageId={}, checkResult={}",
                  coolCandidate->mHeader.mPageId, (uint64_t)checkResult);
          JUMPMU_CONTINUE;
        }
        readGuard.JumpIfModifiedByOthers();

        // Suitable page founds, lets cool
        const PID pageId [[maybe_unused]] = coolCandidate->mHeader.mPageId;
        {
          // writeGuard can only be acquired and released while the partition
          // mutex is locked
          BMExclusiveUpgradeIfNeeded parentWriteGuard(parentHandler.mParentGuard);
          BMExclusiveGuard writeGuard(readGuard);

          LS_DCHECK(coolCandidate->mHeader.mPageId == pageId);
          LS_DCHECK(coolCandidate->mHeader.mState == State::kHot);
          LS_DCHECK(coolCandidate->mHeader.mIsBeingWrittenBack == false);
          LS_DCHECK(parentHandler.mParentGuard.mVersion ==
                    parentHandler.mParentGuard.mLatch->GetOptimisticVersion());
          LS_DCHECK(parentHandler.mChildSwip.mBf == coolCandidate);

          // mark the buffer frame in cool state
          coolCandidate->mHeader.mState = State::kCool;
          // mark the swip to the buffer frame to cool state
          parentHandler.mChildSwip.Cool();
          LS_DLOG("Cool candidate find, state changed to cool, pageId={}",
                  coolCandidate->mHeader.mPageId);
        }

        COUNTERS_BLOCK() {
          PPCounters::MyCounters().unswizzled_pages_counter++;
        }
        failedAttempts = 0;
      }
      JUMPMU_CATCH() {
        LS_DLOG("Cool candidate discarded, optimistic latch failed, "
                "someone has modified the buffer frame during cool "
                "validation, pageId={}",
                coolCandidate->mHeader.mPageId);
      }
    }
  }
}

void PageEvictor::randomBufferFramesToCoolOrEvict() {
  mCoolCandidateBfs.clear();
  for (auto i = 0u; i < mStore->mStoreOption->mBufferFrameRecycleBatchSize; i++) {
    auto* randomBf = &mStore->mBufferManager->RandomBufferFrame();
    DO_NOT_OPTIMIZE(randomBf->mHeader.mState);
    mCoolCandidateBfs.push_back(randomBf);
  }
}

void PageEvictor::PrepareAsyncWriteBuffer(Partition& targetPartition) {
  LS_DLOG("Phase2: PrepareAsyncWriteBuffer begins");
  SCOPED_DEFER(LS_DLOG("Phase2: PrepareAsyncWriteBuffer ended, "
                       "mAsyncWriteBuffer.PendingRequests={}",
                       mAsyncWriteBuffer.GetPendingRequests()));

  mFreeBfList.Reset();
  for (auto* cooledBf : mEvictCandidateBfs) {
    JUMPMU_TRY() {
      BMOptimisticGuard optimisticGuard(cooledBf->mHeader.mLatch);
      // Check if the BF got swizzled in or unswizzle another time in another
      // partition
      if (cooledBf->mHeader.mState != State::kCool || cooledBf->mHeader.mIsBeingWrittenBack) {
        LS_DLOG("COOLed buffer frame discarded, pageId={}, IsCool={}, "
                "isBeingWrittenBack={}",
                cooledBf->mHeader.mPageId, cooledBf->mHeader.mState == State::kCool,
                cooledBf->mHeader.mIsBeingWrittenBack.load());
        JUMPMU_CONTINUE;
      }
      PID cooledPageId = cooledBf->mHeader.mPageId;
      auto partitionId = mStore->mBufferManager->GetPartitionID(cooledPageId);

      // Prevent evicting a page that already has an IO Frame with (possibly)
      // threads working on it.
      Partition& partition = *mPartitions[partitionId];
      JumpScoped<std::unique_lock<std::mutex>> ioGuard(partition.mInflightIOMutex);
      if (partition.mInflightIOs.Lookup(cooledPageId)) {
        LS_DLOG("COOLed buffer frame discarded, already in IO stage, "
                "pageId={}, partitionId={}",
                cooledPageId, partitionId);
        JUMPMU_CONTINUE;
      }

      // Evict clean pages. They can be safely cleared in memory without
      // writing any bytes back to the underlying disk.
      if (!cooledBf->IsDirty()) {
        evictFlushedBf(*cooledBf, optimisticGuard, targetPartition);
        LS_DLOG("COOLed buffer frame is not dirty, reclaim directly, pageId={}",
                cooledBf->mHeader.mPageId);
        JUMPMU_CONTINUE;
      }

      // Async write dirty pages back. They should keep in memory and stay in
      // cooling stage until all the contents are written back to the
      // underlying disk.
      if (mAsyncWriteBuffer.IsFull()) {
        LS_DLOG("Async write buffer is full, bufferSize={}",
                mAsyncWriteBuffer.GetPendingRequests());
        JUMPMU_BREAK;
      }

      BMExclusiveGuard exclusiveGuard(optimisticGuard);
      LS_DCHECK(!cooledBf->mHeader.mIsBeingWrittenBack);
      cooledBf->mHeader.mIsBeingWrittenBack.store(true, std::memory_order_release);

      // performs crc check if necessary
      if (mStore->mStoreOption->mEnableBufferCrcCheck) {
        cooledBf->mHeader.mCrc = cooledBf->mPage.CRC();
      }

      // TODO: preEviction callback according to TREEID
      mAsyncWriteBuffer.Add(*cooledBf);
      LS_DLOG("COOLed buffer frame is added to async write buffer, "
              "pageId={}, bufferSize={}",
              cooledBf->mHeader.mPageId, mAsyncWriteBuffer.GetPendingRequests());
    }
    JUMPMU_CATCH() {
      LS_DLOG("COOLed buffer frame discarded, optimistic latch failed, "
              "someone has modified the buffer frame during cool validation, "
              "pageId={}",
              cooledBf->mHeader.mPageId);
    }
  }

  mEvictCandidateBfs.clear();
}

void PageEvictor::FlushAndRecycleBufferFrames(Partition& targetPartition) {
  LS_DLOG("Phase3: FlushAndRecycleBufferFrames begins");
  SCOPED_DEFER(LS_DLOG("Phase3: FlushAndRecycleBufferFrames ended"));

  auto result = mAsyncWriteBuffer.SubmitAll();
  if (!result) {
    Log::Error("Failed to submit IO, error={}", result.error().ToString());
    return;
  }

  result = mAsyncWriteBuffer.WaitAll();
  if (!result) {
    Log::Error("Failed to wait IO request to complete, error={}", result.error().ToString());
    return;
  }

  auto numFlushedBfs = result.value();
  mAsyncWriteBuffer.IterateFlushedBfs(
      [&](BufferFrame& writtenBf, uint64_t flushedGsn) {
        JUMPMU_TRY() {
          // When the written back page is being exclusively locked, we
          // should rather waste the write and move on to another page
          // Instead of waiting on its latch because of the likelihood that
          // a data structure implementation keeps holding a parent latch
          // while trying to acquire a new page
          BMOptimisticGuard optimisticGuard(writtenBf.mHeader.mLatch);
          BMExclusiveGuard exclusiveGuard(optimisticGuard);
          LS_DCHECK(writtenBf.mHeader.mIsBeingWrittenBack);
          LS_DCHECK(writtenBf.mHeader.mFlushedGsn < flushedGsn);

          // For recovery, so much has to be done here...
          writtenBf.mHeader.mFlushedGsn = flushedGsn;
          writtenBf.mHeader.mIsBeingWrittenBack = false;
          PPCounters::MyCounters().flushed_pages_counter++;
        }
        JUMPMU_CATCH() {
          writtenBf.mHeader.mCrc = 0;
          writtenBf.mHeader.mIsBeingWrittenBack.store(false, std::memory_order_release);
        }

        JUMPMU_TRY() {
          BMOptimisticGuard optimisticGuard(writtenBf.mHeader.mLatch);
          if (writtenBf.mHeader.mState == State::kCool && !writtenBf.mHeader.mIsBeingWrittenBack &&
              !writtenBf.IsDirty()) {
            evictFlushedBf(writtenBf, optimisticGuard, targetPartition);
          }
        }
        JUMPMU_CATCH() {
        }
      },
      numFlushedBfs);

  if (mFreeBfList.Size()) {
    mFreeBfList.PopTo(targetPartition);
  }
}

void PageEvictor::evictFlushedBf(BufferFrame& cooledBf, BMOptimisticGuard& optimisticGuard,
                                 Partition& targetPartition) {
  TREEID btreeId = cooledBf.mPage.mBTreeId;
  optimisticGuard.JumpIfModifiedByOthers();
  ParentSwipHandler parentHandler = mStore->mTreeRegistry->FindParent(btreeId, cooledBf);

  LS_DCHECK(parentHandler.mParentGuard.mState == GuardState::kOptimisticShared);
  BMExclusiveUpgradeIfNeeded parentWriteGuard(parentHandler.mParentGuard);
  optimisticGuard.mGuard.ToExclusiveMayJump();

  if (mStore->mStoreOption->mEnableBufferCrcCheck && cooledBf.mHeader.mCrc) {
    LS_DCHECK(cooledBf.mPage.CRC() == cooledBf.mHeader.mCrc);
  }
  LS_DCHECK(!cooledBf.IsDirty());
  LS_DCHECK(!cooledBf.mHeader.mIsBeingWrittenBack);
  LS_DCHECK(cooledBf.mHeader.mState == State::kCool);
  LS_DCHECK(parentHandler.mChildSwip.IsCool());

  parentHandler.mChildSwip.Evict(cooledBf.mHeader.mPageId);

  // Reclaim buffer frame
  cooledBf.Reset();
  cooledBf.mHeader.mLatch.UnlockExclusively();

  mFreeBfList.PushFront(cooledBf);
  if (mFreeBfList.Size() <= std::min<uint64_t>(mStore->mStoreOption->mWorkerThreads, 128)) {
    mFreeBfList.PopTo(targetPartition);
  }

  COUNTERS_BLOCK() {
    PPCounters::MyCounters().evicted_pages++;
  }
};

} // namespace leanstore::storage