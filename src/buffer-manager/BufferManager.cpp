#include "buffer-manager/BufferManager.hpp"

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/TreeRegistry.hpp"
#include "concurrency/CRManager.hpp"
#include "concurrency/GroupCommitter.hpp"
#include "concurrency/Recovery.hpp"
#include "leanstore/Exceptions.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "sync/HybridLatch.hpp"
#include "sync/ScopedHybridGuard.hpp"
#include "utils/AsyncIo.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/Error.hpp"
#include "utils/JumpMU.hpp"
#include "utils/Log.hpp"
#include "utils/Parallelize.hpp"
#include "utils/UserThread.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <format>
#include <vector>

#include <fcntl.h>
#include <linux/perf_event.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore::storage {

BufferManager::BufferManager(leanstore::LeanStore* store) : mStore(store) {
  auto bpSize = mStore->mStoreOption.mBufferPoolSize;
  auto bfSize = mStore->mStoreOption.mBufferFrameSize;
  mNumBfs = bpSize / bfSize;
  const uint64_t totalMemSize = bfSize * (mNumBfs + mNumSaftyBfs);

  // Init buffer pool with zero-initialized buffer frames. Use mmap with flags
  // MAP_PRIVATE and MAP_ANONYMOUS, no underlying file desciptor to allocate
  // totalmemSize buffer pool with zero-initialized contents.
  void* underlyingBuf = mmap(NULL, totalMemSize, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (underlyingBuf == MAP_FAILED) {
    Log::Fatal("Failed to allocate memory for the buffer pool, "
               "bufferPoolSize={}, totalMemSize={}",
               mStore->mStoreOption.mBufferPoolSize, totalMemSize);
  }

  mBufferPool = reinterpret_cast<uint8_t*>(underlyingBuf);
  madvise(mBufferPool, totalMemSize, MADV_HUGEPAGE);
  madvise(mBufferPool, totalMemSize, MADV_DONTFORK);

  // Initialize mPartitions
  mNumPartitions = mStore->mStoreOption.mNumPartitions;
  mPartitionsMask = mNumPartitions - 1;
  const uint64_t freeBfsLimitPerPartition = std::ceil(
      (mStore->mStoreOption.mFreePct * 1.0 * mNumBfs / 100.0) / mNumPartitions);
  for (uint64_t i = 0; i < mNumPartitions; i++) {
    mPartitions.push_back(std::make_unique<Partition>(
        i, mNumPartitions, freeBfsLimitPerPartition));
  }
  Log::Info(
      "Init buffer manager, IO partitions={}, freeBfsLimitPerPartition={}",
      mNumPartitions, freeBfsLimitPerPartition);

  // spread these buffer frames to all the partitions
  utils::Parallelize::ParallelRange(mNumBfs, [&](uint64_t begin, uint64_t end) {
    uint64_t partitionId = 0;
    for (uint64_t i = begin; i < end; i++) {
      auto& partition = GetPartition(partitionId);
      auto* bfAddr = &mBufferPool[i * mStore->mStoreOption.mBufferFrameSize];
      partition.mFreeBfList.PushFront(*new (bfAddr) BufferFrame());
      partitionId = (partitionId + 1) % mNumPartitions;
    }
  });
}

void BufferManager::StartPageEvictors() {
  auto numBufferProviders = mStore->mStoreOption.mNumBufferProviders;
  // make it optional for pure in-memory experiments
  if (numBufferProviders <= 0) {
    return;
  }

  LS_DCHECK(numBufferProviders <= mNumPartitions);
  mPageEvictors.reserve(numBufferProviders);
  for (auto i = 0u; i < numBufferProviders; ++i) {
    std::string threadName = "PageEvictor";
    if (numBufferProviders > 1) {
      threadName += std::to_string(i);
    }

    auto runningCPU = mStore->mStoreOption.mWorkerThreads +
                      mStore->mStoreOption.mEnableWal + i;
    mPageEvictors.push_back(std::make_unique<PageEvictor>(
        mStore, threadName, runningCPU, mNumBfs, mBufferPool, mNumPartitions,
        mPartitionsMask, mPartitions));
  }

  for (auto i = 0u; i < mPageEvictors.size(); ++i) {
    mPageEvictors[i]->Start();
  }
}

StringMap BufferManager::Serialize() {
  // TODO: correctly serialize ranges of used pages
  StringMap map;
  PID maxPageId = 0;
  for (uint64_t i = 0; i < mNumPartitions; i++) {
    maxPageId = std::max<PID>(GetPartition(i).mNextPageId, maxPageId);
  }
  map["max_pid"] = std::to_string(maxPageId);
  return map;
}

void BufferManager::Deserialize(StringMap map) {
  PID maxPageId = std::stoull(map["max_pid"]);
  maxPageId = (maxPageId + (mNumPartitions - 1)) & ~(mNumPartitions - 1);
  for (uint64_t i = 0; i < mNumPartitions; i++) {
    GetPartition(i).mNextPageId = maxPageId + i;
  }
}

Result<void> BufferManager::CheckpointAllBufferFrames() {
  auto startAt = std::chrono::steady_clock::now();
  Log::Info("CheckpointAllBufferFrames, mNumBfs={}", mNumBfs);
  SCOPED_DEFER({
    auto stoppedAt = std::chrono::steady_clock::now();
    auto elaspedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         stoppedAt - startAt)
                         .count();
    Log::Info("CheckpointAllBufferFrames finished, timeElasped={:.6f}s",
              elaspedNs / 1000000000.0);
  });

  LS_DEBUG_EXECUTE(mStore, "skip_CheckpointAllBufferFrames", {
    Log::Error("CheckpointAllBufferFrames skipped due to debug flag");
    return std::unexpected(utils::Error::General("skipped due to debug flag"));
  });

  StopPageEvictors();

  utils::Parallelize::ParallelRange(mNumBfs, [&](uint64_t begin, uint64_t end) {
    const auto bufferFrameSize = mStore->mStoreOption.mBufferFrameSize;
    const auto pageSize = mStore->mStoreOption.mPageSize;

    // the underlying batch for aio
    const auto batchCapacity = mStore->mStoreOption.mBufferWriteBatchSize;
    // const auto batchCapacity = 1;
    alignas(512) uint8_t buffer[pageSize * batchCapacity];
    auto batchSize = 0u;

    // the aio itself
    utils::AsyncIo aio(batchCapacity);

    for (uint64_t i = begin; i < end;) {
      // collect a batch of pages for async write
      for (; batchSize < batchCapacity && i < end; i++) {
        auto& bf =
            *reinterpret_cast<BufferFrame*>(&mBufferPool[i * bufferFrameSize]);
        if (!bf.IsFree()) {
          auto* tmpBuffer = buffer + batchSize * pageSize;
          auto pageOffset = bf.mHeader.mPageId * pageSize;
          mStore->mTreeRegistry->Checkpoint(bf.mPage.mBTreeId, bf, tmpBuffer);
          aio.PrepareWrite(mStore->mPageFd, tmpBuffer, pageSize, pageOffset);
          bf.mHeader.mFlushedGsn = bf.mPage.mGSN;
          batchSize++;
        }
      }

      // write the batch of pages
      if (auto res = aio.SubmitAll(); !res) {
        Log::Fatal("Failed to submit aio, error={}", res.error().ToString());
      }
      if (auto res = aio.WaitAll(); !res) {
        Log::Fatal("Failed to wait aio, error={}", res.error().ToString());
      }

      // reset batch size
      batchSize = 0;
    }
  });

  return {};
}

Result<void> BufferManager::CheckpointBufferFrame(BufferFrame& bf) {
  alignas(512) uint8_t buffer[mStore->mStoreOption.mPageSize];
  bf.mHeader.mLatch.LockExclusively();
  if (!bf.IsFree()) {
    mStore->mTreeRegistry->Checkpoint(bf.mPage.mBTreeId, bf, buffer);
    auto res = writePage(bf.mHeader.mPageId, buffer);
    if (!res) {
      return std::unexpected(std::move(res.error()));
    }
    bf.mHeader.mFlushedGsn = bf.mPage.mGSN;
  }
  bf.mHeader.mLatch.UnlockExclusively();
  return {};
}

void BufferManager::RecoverFromDisk() {
  auto recovery = std::make_unique<leanstore::cr::Recovery>(
      mStore, 0, mStore->mCRManager->mGroupCommitter->mWalSize);
  recovery->Run();
}

uint64_t BufferManager::ConsumedPages() {
  uint64_t totalUsedBfs = 0;
  uint64_t totalFreeBfs = 0;
  for (uint64_t i = 0; i < mNumPartitions; i++) {
    totalFreeBfs += GetPartition(i).NumReclaimedPages();
    totalUsedBfs += GetPartition(i).NumAllocatedPages();
  }
  return totalUsedBfs - totalFreeBfs;
}

BufferFrame& BufferManager::AllocNewPageMayJump(TREEID treeId) {
  Partition& partition = RandomPartition();
  BufferFrame& freeBf = partition.mFreeBfList.PopFrontMayJump();
  memset((void*)&freeBf, 0, mStore->mStoreOption.mBufferFrameSize);
  new (&freeBf) BufferFrame();
  freeBf.Init(partition.NextPageId());

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().allocate_operations_counter++;
  }

  freeBf.mPage.mBTreeId = treeId;
  freeBf.mPage.mGSN++; // mark as dirty
  LS_DLOG("Alloc new page, pageId={}, btreeId={}", freeBf.mHeader.mPageId,
          freeBf.mPage.mBTreeId);
  return freeBf;
}

void BufferManager::ReclaimPage(BufferFrame& bf) {
  Partition& partition = GetPartition(bf.mHeader.mPageId);
  if (mStore->mStoreOption.mEnableReclaimPageIds) {
    partition.ReclaimPageId(bf.mHeader.mPageId);
  }

  if (bf.mHeader.mIsBeingWrittenBack) {
    // Do nothing ! we have a garbage collector ;-)
    bf.mHeader.mLatch.UnlockExclusively();
  } else {
    bf.Reset();
    bf.mHeader.mLatch.UnlockExclusively();
    partition.mFreeBfList.PushFront(bf);
  }
}

BufferFrame* BufferManager::ResolveSwipMayJump(HybridGuard& nodeGuard,
                                               Swip& swipInNode) {
  LS_DCHECK(nodeGuard.mState == GuardState::kOptimisticShared);
  if (swipInNode.IsHot()) {
    // Resolve swip from hot state
    auto* bf = &swipInNode.AsBufferFrame();
    nodeGuard.JumpIfModifiedByOthers();
    return bf;
  }

  if (swipInNode.IsCool()) {
    // Resolve swip from cool state
    auto* bf = &swipInNode.AsBufferFrameMasked();
    nodeGuard.JumpIfModifiedByOthers();
    BMOptimisticGuard bfGuard(bf->mHeader.mLatch);
    BMExclusiveUpgradeIfNeeded swipXGuard(nodeGuard); // parent
    BMExclusiveGuard bfXGuard(bfGuard);               // child
    bf->mHeader.mState = State::kHot;
    swipInNode.MarkHOT();
    return bf;
  }

  // Resolve swip from evicted state
  //
  // 1. Allocate buffer frame from memory
  // 2. Read page content from disk and fill the buffer frame
  //

  // unlock the current node firstly to avoid deadlock: P->G, G->P
  nodeGuard.Unlock();

  const PID pageId = swipInNode.AsPageId();
  Partition& partition = GetPartition(pageId);

  JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
      partition.mInflightIOMutex);
  nodeGuard.JumpIfModifiedByOthers();

  auto frameHandler = partition.mInflightIOs.Lookup(pageId);

  // Create an IO frame to read page from disk.
  if (!frameHandler) {
    // 1. Randomly get a buffer frame from partitions
    BufferFrame& bf = RandomPartition().mFreeBfList.PopFrontMayJump();
    LS_DCHECK(!bf.mHeader.mLatch.IsLockedExclusively());
    LS_DCHECK(bf.mHeader.mState == State::kFree);

    // 2. Create an IO frame in the current partition
    IOFrame& ioFrame = partition.mInflightIOs.Insert(pageId);
    ioFrame.mState = IOFrame::State::kReading;
    ioFrame.mNumReaders = 1;
    JumpScoped<std::unique_lock<std::mutex>> ioFrameGuard(ioFrame.mMutex);
    inflightIOGuard->unlock();

    // 3. Read page at pageId to the target buffer frame
    ReadPageSync(pageId, &bf.mPage);
    LS_DLOG("Read page from disk, pageId={}, btreeId={}", pageId,
            bf.mPage.mBTreeId);
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_page_reads[bf.mPage.mBTreeId]++;
    }

    // 4. Intialize the buffer frame header
    LS_DCHECK(!bf.mHeader.mIsBeingWrittenBack);
    bf.mHeader.mFlushedGsn = bf.mPage.mGSN;
    bf.mHeader.mState = State::kLoaded;
    bf.mHeader.mPageId = pageId;
    if (mStore->mStoreOption.mEnableBufferCrcCheck) {
      bf.mHeader.mCrc = bf.mPage.CRC();
    }

    // 5. Publish the buffer frame
    JUMPMU_TRY() {
      nodeGuard.JumpIfModifiedByOthers();
      ioFrameGuard->unlock();
      JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
          partition.mInflightIOMutex);
      BMExclusiveUpgradeIfNeeded swipXGuard(nodeGuard);

      swipInNode.MarkHOT(&bf);
      bf.mHeader.mState = State::kHot;

      if (ioFrame.mNumReaders.fetch_add(-1) == 1) {
        partition.mInflightIOs.Remove(pageId);
      }

      JUMPMU_RETURN& bf;
    }
    JUMPMU_CATCH() {
      // Change state to ready if contention is encountered
      inflightIOGuard->lock();
      ioFrame.mBf = &bf;
      ioFrame.mState = IOFrame::State::kReady;
      inflightIOGuard->unlock();
      if (ioFrameGuard->owns_lock()) {
        ioFrameGuard->unlock();
      }
      jumpmu::Jump();
    }
  }

  IOFrame& ioFrame = frameHandler.Frame();
  switch (ioFrame.mState) {
  case IOFrame::State::kReading: {
    ioFrame.mNumReaders++; // incremented while holding partition lock
    inflightIOGuard->unlock();

    // wait untile the reading is finished
    JumpScoped<std::unique_lock<std::mutex>> ioFrameGuard(ioFrame.mMutex);
    ioFrameGuard->unlock(); // no need to hold the mutex anymore
    if (ioFrame.mNumReaders.fetch_add(-1) == 1) {
      inflightIOGuard->lock();
      if (ioFrame.mNumReaders == 0) {
        partition.mInflightIOs.Remove(pageId);
      }
      inflightIOGuard->unlock();
    }
    jumpmu::Jump(); // why jump?
    break;
  }
  case IOFrame::State::kReady: {
    BufferFrame* bf = ioFrame.mBf;
    {
      // We have to exclusively lock the bf because the page provider thread
      // will try to evict them when its IO is done
      LS_DCHECK(!bf->mHeader.mLatch.IsLockedExclusively());
      LS_DCHECK(bf->mHeader.mState == State::kLoaded);
      BMOptimisticGuard bfGuard(bf->mHeader.mLatch);
      BMExclusiveUpgradeIfNeeded swipXGuard(nodeGuard);
      BMExclusiveGuard bfXGuard(bfGuard);
      ioFrame.mBf = nullptr;
      swipInNode.MarkHOT(bf);
      LS_DCHECK(bf->mHeader.mPageId == pageId);
      LS_DCHECK(swipInNode.IsHot());
      LS_DCHECK(bf->mHeader.mState == State::kLoaded);
      bf->mHeader.mState = State::kHot;

      if (ioFrame.mNumReaders.fetch_add(-1) == 1) {
        partition.mInflightIOs.Remove(pageId);
      } else {
        ioFrame.mState = IOFrame::State::kToDelete;
      }
      inflightIOGuard->unlock();
      return bf;
    }
  }
  case IOFrame::State::kToDelete: {
    if (ioFrame.mNumReaders == 0) {
      partition.mInflightIOs.Remove(pageId);
    }
    inflightIOGuard->unlock();
    jumpmu::Jump();
    break;
  }
  default: {
    LS_DCHECK(false);
  }
  }
  assert(false);
  return nullptr;
}

void BufferManager::ReadPageSync(PID pageId, void* pageBuffer) {
  LS_DCHECK(uint64_t(pageBuffer) % 512 == 0);
  int64_t bytesLeft = mStore->mStoreOption.mPageSize;
  while (bytesLeft > 0) {
    auto totalRead = mStore->mStoreOption.mPageSize - bytesLeft;
    auto curOffset = pageId * mStore->mStoreOption.mPageSize + totalRead;
    auto* curBuffer = reinterpret_cast<uint8_t*>(pageBuffer) + totalRead;
    auto bytesRead = pread(mStore->mPageFd, curBuffer, bytesLeft, curOffset);

    // read error, return a zero-initialized pageBuffer frame
    if (bytesRead <= 0) {
      memset(pageBuffer, 0, mStore->mStoreOption.mPageSize);
      auto* page = new (pageBuffer) BufferFrame();
      page->Init(pageId);
      if (bytesRead == 0) {
        Log::Warn("Read empty page, pageId={}, fd={}, bytesRead={}, "
                  "bytesLeft={}, file={}",
                  pageId, mStore->mPageFd, bytesRead, bytesLeft,
                  mStore->mStoreOption.GetDbFilePath());
      } else {
        Log::Error("Failed to read page, errno={}, error={}, pageId={}, fd={}, "
                   "bytesRead={}, bytesLeft={}, file={}",
                   errno, strerror(errno), pageId, mStore->mPageFd, bytesRead,
                   bytesLeft, mStore->mStoreOption.GetDbFilePath());
      }
      return;
    }

    bytesLeft -= bytesRead;
  }

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().read_operations_counter++;
  }
}

BufferFrame& BufferManager::ReadPageSync(PID pageId) {
  HybridLatch dummyParentLatch;
  HybridGuard dummyParentGuard(&dummyParentLatch);
  dummyParentGuard.ToOptimisticSpin();

  Swip swip;
  swip.Evict(pageId);

  while (true) {
    JUMPMU_TRY() {
      swip = ResolveSwipMayJump(dummyParentGuard, swip);
      JUMPMU_RETURN swip.AsBufferFrame();
    }
    JUMPMU_CATCH() {
    }
  }
}

Result<void> BufferManager::WritePageSync(BufferFrame& bf) {
  ScopedHybridGuard guard(bf.mHeader.mLatch, LatchMode::kPessimisticExclusive);
  auto pageId = bf.mHeader.mPageId;
  auto& partition = GetPartition(pageId);

  writePage(pageId, &bf.mPage);

  bf.Reset();
  guard.Unlock();
  partition.mFreeBfList.PushFront(bf);
  return {};
}

void BufferManager::StopPageEvictors() {
  for (auto& pageEvictor : mPageEvictors) {
    pageEvictor->Stop();
  }
  mPageEvictors.clear();
}

BufferManager::~BufferManager() {
  StopPageEvictors();
  uint64_t totalMemSize =
      mStore->mStoreOption.mBufferFrameSize * (mNumBfs + mNumSaftyBfs);
  munmap(mBufferPool, totalMemSize);
}

void BufferManager::DoWithBufferFrameIf(
    std::function<bool(BufferFrame& bf)> condition,
    std::function<void(BufferFrame& bf)> action) {
  utils::Parallelize::ParallelRange(mNumBfs, [&](uint64_t begin, uint64_t end) {
    LS_DCHECK(condition != nullptr);
    LS_DCHECK(action != nullptr);
    for (uint64_t i = begin; i < end; i++) {
      auto* bfAddr = &mBufferPool[i * mStore->mStoreOption.mBufferFrameSize];
      auto& bf = *reinterpret_cast<BufferFrame*>(bfAddr);
      bf.mHeader.mLatch.LockExclusively();
      if (condition(bf)) {
        action(bf);
      }
      bf.mHeader.mLatch.UnlockExclusively();
    }
  });
}

} // namespace leanstore::storage
