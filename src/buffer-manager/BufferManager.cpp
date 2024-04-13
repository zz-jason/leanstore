#include "buffer-manager/BufferManager.hpp"

#include "buffer-manager/BufferFrame.hpp"
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
#include "utils/Error.hpp"
#include "utils/Log.hpp"
#include "utils/Misc.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"
#include "utils/UserThread.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <format>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore {
namespace storage {

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
  Log::FatalIf(underlyingBuf == MAP_FAILED,
               "Failed to allocate memory for the buffer pool, "
               "bufferPoolSize={}, totalMemSize={}",
               mStore->mStoreOption.mBufferPoolSize, totalMemSize);

  mBufferPool = reinterpret_cast<uint8_t*>(underlyingBuf);
  madvise(mBufferPool, totalMemSize, MADV_HUGEPAGE);
  madvise(mBufferPool, totalMemSize, MADV_DONTFORK);

  // Initialize mPartitions
  mNumPartitions = mStore->mStoreOption.mNumPartitions;
  mPartitionsMask = mNumPartitions - 1;
  const uint64_t freeBfsLimitPerPartition =
      std::ceil((mStore->mStoreOption.mFreePct * 1.0 * mNumBfs / 100.0) /
                static_cast<double>(mNumPartitions));
  for (uint64_t i = 0; i < mNumPartitions; i++) {
    mPartitions.push_back(std::make_unique<Partition>(
        i, mNumPartitions, freeBfsLimitPerPartition));
  }

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

void BufferManager::StartBufferFrameProviders() {
  auto numBufferProviders = mStore->mStoreOption.mNumBufferProviders;
  // make it optional for pure in-memory experiments
  if (numBufferProviders <= 0) {
    return;
  }

  DCHECK(numBufferProviders <= mNumPartitions);
  mBfProviders.reserve(numBufferProviders);
  for (auto i = 0u; i < numBufferProviders; ++i) {
    std::string threadName = "BuffProvider";
    if (numBufferProviders > 1) {
      threadName += std::to_string(i);
    }

    auto runningCPU = mStore->mStoreOption.mWorkerThreads +
                      mStore->mStoreOption.mEnableWal + i;
    mBfProviders.push_back(std::make_unique<BufferFrameProvider>(
        mStore, threadName, runningCPU, mNumBfs, mBufferPool, mNumPartitions,
        mPartitionsMask, mPartitions));
  }

  for (auto i = 0u; i < mBfProviders.size(); ++i) {
    mBfProviders[i]->Start();
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

void BufferManager::CheckpointAllBufferFrames() {
  LS_DEBUG_EXECUTE(mStore, "skip_CheckpointAllBufferFrames", {
    Log::Error("CheckpointAllBufferFrames skipped due to debug flag");
    return;
  });

  StopBufferFrameProviders();
  utils::Parallelize::ParallelRange(mNumBfs, [&](uint64_t begin, uint64_t end) {
    utils::AlignedBuffer<512> alignedBuffer(mStore->mStoreOption.mPageSize);
    auto* buffer = alignedBuffer.Get();
    utils::AsyncIo aio(1);

    for (uint64_t i = begin; i < end; i++) {
      auto* bfAddr = &mBufferPool[i * mStore->mStoreOption.mBufferFrameSize];
      auto& bf = *reinterpret_cast<BufferFrame*>(bfAddr);
      bf.mHeader.mLatch.LockExclusively();
      if (!bf.IsFree()) {
        mStore->mTreeRegistry->Checkpoint(bf.mPage.mBTreeId, bf, buffer);
        // wait the last write to finish
        if (auto res = aio.WaitAll(); !res) {
          Log::Fatal("failed to wait all IO to finish, error={}",
                     res.error().ToString());
        }
        aio.PrepareWrite(mStore->mPageFd, buffer,
                         mStore->mStoreOption.mPageSize,
                         bf.mHeader.mPageId * mStore->mStoreOption.mPageSize);
        if (auto res = aio.SubmitAll(); !res) {
          Log::Fatal("failed to submit all IO, error={}",
                     res.error().ToString());
        }
      }
      bf.mHeader.mLatch.UnlockExclusively();
    }
    // wait the last write to finish
    if (auto res = aio.WaitAll(); !res) {
      Log::Fatal("failed to wait all IO to finish, error={}",
                 res.error().ToString());
    }
  });
}

Result<void> BufferManager::CheckpointBufferFrame(BufferFrame& bf) {
  utils::AsyncIo aio(1);
  alignas(512) uint8_t buffer[mStore->mStoreOption.mPageSize];
  bf.mHeader.mLatch.LockExclusively();
  if (!bf.IsFree()) {
    mStore->mTreeRegistry->Checkpoint(bf.mPage.mBTreeId, bf, buffer);

    aio.PrepareWrite(mStore->mPageFd, buffer, mStore->mStoreOption.mPageSize,
                     bf.mHeader.mPageId * mStore->mStoreOption.mPageSize);
    if (auto res = aio.SubmitAll(); !res) {
      return std::unexpected(res.error());
    }
    if (auto res = aio.WaitAll(); !res) {
      return std::unexpected(res.error());
    }
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

// Buffer Frames Management

Partition& BufferManager::RandomPartition() {
  auto randOrdinal = utils::RandomGenerator::Rand<uint64_t>(0, mNumPartitions);
  return GetPartition(randOrdinal);
}

BufferFrame& BufferManager::RandomBufferFrame() {
  auto i = utils::RandomGenerator::Rand<uint64_t>(0, mNumBfs);
  auto* bfAddr = &mBufferPool[i * mStore->mStoreOption.mBufferFrameSize];
  return *reinterpret_cast<BufferFrame*>(bfAddr);
}

BufferFrame& BufferManager::AllocNewPage(TREEID treeId) {
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
  return freeBf;
}

// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
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

// Returns a non-latched BufguardedSwipferFrame, called by worker threads
BufferFrame* BufferManager::ResolveSwipMayJump(HybridGuard& parentNodeGuard,
                                               Swip& childSwip) {
  DCHECK(parentNodeGuard.mState == GuardState::kOptimisticShared);
  if (childSwip.IsHot()) {
    // Resolve swip from hot state
    auto* bf = &childSwip.AsBufferFrame();
    parentNodeGuard.JumpIfModifiedByOthers();
    return bf;
  }

  if (childSwip.IsCool()) {
    // Resolve swip from cool state
    auto* bf = &childSwip.AsBufferFrameMasked();
    parentNodeGuard.JumpIfModifiedByOthers();
    BMOptimisticGuard bfGuard(bf->mHeader.mLatch);
    BMExclusiveUpgradeIfNeeded swipXGuard(parentNodeGuard); // parent
    BMExclusiveGuard bfXGuard(bfGuard);                     // child
    bf->mHeader.mState = State::kHot;
    childSwip.MarkHOT();
    return bf;
  }

  // Resolve swip from evicted state
  //
  // 1. Allocate buffer frame from memory
  // 2. Read page content from disk and fill the buffer frame
  //

  // unlock the current node firstly to avoid deadlock: P->G, G->P
  parentNodeGuard.Unlock();

  const PID pageId = childSwip.AsPageId();
  Partition& partition = GetPartition(pageId);

  JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
      partition.mInflightIOMutex);
  parentNodeGuard.JumpIfModifiedByOthers();

  auto frameHandler = partition.mInflightIOs.Lookup(pageId);

  // Create an IO frame to read page from disk.
  if (!frameHandler) {
    // 1. Randomly get a buffer frame from partitions
    BufferFrame& bf = RandomPartition().mFreeBfList.PopFrontMayJump();
    DCHECK(!bf.mHeader.mLatch.IsLockedExclusively());
    DCHECK(bf.mHeader.mState == State::kFree);

    // 2. Create an IO frame in the current partition
    IOFrame& ioFrame = partition.mInflightIOs.Insert(pageId);
    ioFrame.state = IOFrame::State::kReading;
    ioFrame.readers_counter = 1;
    JumpScoped<std::unique_lock<std::mutex>> ioFrameGuard(ioFrame.mutex);
    inflightIOGuard->unlock();

    // 3. Read page at pageId to the target buffer frame
    ReadPageSync(pageId, &bf.mPage);
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_page_reads[bf.mPage.mBTreeId]++;
    }

    // 4. Intialize the buffer frame header
    DCHECK(!bf.mHeader.mIsBeingWrittenBack);
    bf.mHeader.mFlushedGsn = bf.mPage.mGSN;
    bf.mHeader.mState = State::kLoaded;
    bf.mHeader.mPageId = pageId;
    if (mStore->mStoreOption.mEnableBufferCrcCheck) {
      bf.mHeader.mCrc = bf.mPage.CRC();
    }

    // 5. Publish the buffer frame
    JUMPMU_TRY() {
      parentNodeGuard.JumpIfModifiedByOthers();
      ioFrameGuard->unlock();
      JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
          partition.mInflightIOMutex);
      BMExclusiveUpgradeIfNeeded swipXGuard(parentNodeGuard);

      childSwip.MarkHOT(&bf);
      bf.mHeader.mState = State::kHot;

      if (ioFrame.readers_counter.fetch_add(-1) == 1) {
        partition.mInflightIOs.Remove(pageId);
      }

      JUMPMU_RETURN& bf;
    }
    JUMPMU_CATCH() {
      // Change state to ready if contention is encountered
      inflightIOGuard->lock();
      ioFrame.bf = &bf;
      ioFrame.state = IOFrame::State::kReady;
      inflightIOGuard->unlock();
      ioFrameGuard->unlock();
      jumpmu::Jump();
    }
  }

  IOFrame& ioFrame = frameHandler.frame();
  switch (ioFrame.state) {
  case IOFrame::State::kReading: {
    ioFrame.readers_counter++; // incremented while holding partition lock
    inflightIOGuard->unlock();

    // wait untile the reading is finished
    JumpScoped<std::unique_lock<std::mutex>> ioFrameGuard(ioFrame.mutex);
    ioFrameGuard->unlock(); // no need to hold the mutex anymore
    if (ioFrame.readers_counter.fetch_add(-1) == 1) {
      inflightIOGuard->lock();
      if (ioFrame.readers_counter == 0) {
        partition.mInflightIOs.Remove(pageId);
      }
      inflightIOGuard->unlock();
    }
    jumpmu::Jump(); // why jump?
    break;
  }
  case IOFrame::State::kReady: {
    BufferFrame* bf = ioFrame.bf;
    {
      // We have to exclusively lock the bf because the page provider thread
      // will try to evict them when its IO is done
      DCHECK(!bf->mHeader.mLatch.IsLockedExclusively());
      DCHECK(bf->mHeader.mState == State::kLoaded);
      BMOptimisticGuard bfGuard(bf->mHeader.mLatch);
      BMExclusiveUpgradeIfNeeded swipXGuard(parentNodeGuard);
      BMExclusiveGuard bfXGuard(bfGuard);
      ioFrame.bf = nullptr;
      childSwip.MarkHOT(bf);
      DCHECK(bf->mHeader.mPageId == pageId);
      DCHECK(childSwip.IsHot());
      DCHECK(bf->mHeader.mState == State::kLoaded);
      bf->mHeader.mState = State::kHot;

      if (ioFrame.readers_counter.fetch_add(-1) == 1) {
        partition.mInflightIOs.Remove(pageId);
      } else {
        ioFrame.state = IOFrame::State::kToDelete;
      }
      inflightIOGuard->unlock();
      return bf;
    }
  }
  case IOFrame::State::kToDelete: {
    if (ioFrame.readers_counter == 0) {
      partition.mInflightIOs.Remove(pageId);
    }
    inflightIOGuard->unlock();
    jumpmu::Jump();
    break;
  }
  default: {
    DCHECK(false);
  }
  }
  assert(false);
  return nullptr;
}

void BufferManager::ReadPageSync(PID pageId, void* pageBuffer) {
  DCHECK(uint64_t(pageBuffer) % 512 == 0);
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
      Log::Error(
          "Failed to read page, error={}, fileName={}, fd={}, pageId={}, "
          "bytesRead={}, bytesLeft={}",
          strerror(errno), mStore->mStoreOption.GetDbFilePath(),
          mStore->mPageFd, pageId, bytesRead, bytesLeft);
      return;
    }

    bytesLeft -= bytesRead;
  };

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
  utils::AsyncIo aio(1);
  aio.PrepareWrite(mStore->mPageFd, &bf.mPage, mStore->mStoreOption.mPageSize,
                   pageId * mStore->mStoreOption.mPageSize);
  if (auto res = aio.SubmitAll(); !res) {
    return std::unexpected(res.error());
  }
  if (auto res = aio.WaitAll(); !res) {
    return std::unexpected(res.error());
  }
  bf.Reset();
  guard.Unlock();
  partition.mFreeBfList.PushFront(bf);
  return {};
}

void BufferManager::SyncAllPageWrites() {
  fdatasync(mStore->mPageFd);
}

uint64_t BufferManager::GetPartitionID(PID pageId) {
  return pageId & mPartitionsMask;
}

Partition& BufferManager::GetPartition(PID pageId) {
  const uint64_t partitionId = GetPartitionID(pageId);
  return *mPartitions[partitionId];
}

void BufferManager::StopBufferFrameProviders() {
  mBfProviders.clear();
}

BufferManager::~BufferManager() {
  StopBufferFrameProviders();
  uint64_t totalMemSize =
      mStore->mStoreOption.mBufferFrameSize * (mNumBfs + mNumSaftyBfs);
  munmap(mBufferPool, totalMemSize);
}

void BufferManager::DoWithBufferFrameIf(
    std::function<bool(BufferFrame& bf)> condition,
    std::function<void(BufferFrame& bf)> action) {
  utils::Parallelize::ParallelRange(mNumBfs, [&](uint64_t begin, uint64_t end) {
    DCHECK(condition != nullptr);
    DCHECK(action != nullptr);
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

} // namespace storage
} // namespace leanstore
