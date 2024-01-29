#include "BufferManager.hpp"

#include "BufferFrame.hpp"
#include "Config.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "concurrency-recovery/GroupCommitter.hpp"
#include "concurrency-recovery/Recovery.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Exceptions.hpp"
#include "shared-headers/Units.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Error.hpp"
#include "utils/Misc.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>
#include <cstring>
#include <expected>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore {
namespace storage {

BufferManager::BufferManager(leanstore::LeanStore* store) : mStore(store) {
  mNumBfs = mStore->mStoreOption.mBufferPoolSize / BufferFrame::Size();
  const u64 totalMemSize = BufferFrame::Size() * (mNumBfs + mNumSaftyBfs);

  // Init buffer pool with zero-initialized buffer frames. Use mmap with flags
  // MAP_PRIVATE and MAP_ANONYMOUS, no underlying file desciptor to allocate
  // totalmemSize buffer pool with zero-initialized contents.
  void* underlyingBuf = mmap(NULL, totalMemSize, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  LOG_IF(FATAL, underlyingBuf == MAP_FAILED)
      << "Failed to allocate memory for the buffer pool"
      << ", FLAGS_buffer_pool_size=" << mStore->mStoreOption.mBufferPoolSize
      << ", totalMemSize=" << totalMemSize;

  mBufferPool = reinterpret_cast<u8*>(underlyingBuf);
  madvise(mBufferPool, totalMemSize, MADV_HUGEPAGE);
  madvise(mBufferPool, totalMemSize, MADV_DONTFORK);

  // Initialize mPartitions
  mNumPartitions = mStore->mStoreOption.mNumPartitions;
  mPartitionsMask = mNumPartitions - 1;
  const u64 freeBfsLimitPerPartition =
      std::ceil((FLAGS_free_pct * 1.0 * mNumBfs / 100.0) /
                static_cast<double>(mNumPartitions));
  for (u64 i = 0; i < mNumPartitions; i++) {
    mPartitions.push_back(std::make_unique<Partition>(
        i, mNumPartitions, freeBfsLimitPerPartition));
  }

  // spread these buffer frames to all the partitions
  utils::Parallelize::parallelRange(mNumBfs, [&](u64 begin, u64 end) {
    u64 partitionId = 0;
    for (u64 i = begin; i < end; i++) {
      auto& partition = GetPartition(partitionId);
      auto* bfAddr = &mBufferPool[i * BufferFrame::Size()];
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

    int runningCPU = 0;
    if (FLAGS_enable_pin_worker_threads) {
      runningCPU = FLAGS_worker_threads + FLAGS_wal + i;
    } else {
      runningCPU = FLAGS_wal + i;
    }

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
  for (u64 i = 0; i < mNumPartitions; i++) {
    maxPageId = std::max<PID>(GetPartition(i).mNextPageId, maxPageId);
  }
  map["max_pid"] = std::to_string(maxPageId);
  return map;
}

void BufferManager::Deserialize(StringMap map) {
  PID maxPageId = std::stoull(map["max_pid"]);
  maxPageId = (maxPageId + (mNumPartitions - 1)) & ~(mNumPartitions - 1);
  for (u64 i = 0; i < mNumPartitions; i++) {
    GetPartition(i).mNextPageId = maxPageId + i;
  }
}

void BufferManager::CheckpointAllBufferFrames() {
  LS_DEBUG_EXECUTE(mStore, "skip_CheckpointAllBufferFrames", {
    LOG(ERROR) << "CheckpointAllBufferFrames skipped due to debug flag";
    return;
  });

  StopBufferFrameProviders();
  utils::Parallelize::parallelRange(mNumBfs, [&](u64 begin, u64 end) {
    utils::AlignedBuffer<512> alignedBuffer(FLAGS_page_size);
    auto* buffer = alignedBuffer.Get();
    for (u64 i = begin; i < end; i++) {
      auto* bfAddr = &mBufferPool[i * BufferFrame::Size()];
      auto& bf = *reinterpret_cast<BufferFrame*>(bfAddr);
      bf.header.mLatch.LockExclusively();
      if (!bf.isFree()) {
        mStore->mTreeRegistry->Checkpoint(bf.page.mBTreeId, bf, buffer);
        auto ret = pwrite(mStore->mPageFd, buffer, FLAGS_page_size,
                          bf.header.mPageId * FLAGS_page_size);
        DCHECK_EQ(ret, FLAGS_page_size);
      }
      bf.header.mLatch.UnlockExclusively();
    }
  });
}

auto BufferManager::CheckpointBufferFrame(BufferFrame& bf)
    -> std::expected<void, utils::Error> {
  alignas(512) u8 buffer[FLAGS_page_size];
  bf.header.mLatch.LockExclusively();
  if (!bf.isFree()) {
    mStore->mTreeRegistry->Checkpoint(bf.page.mBTreeId, bf, buffer);
    auto ret = pwrite(mStore->mPageFd, buffer, FLAGS_page_size,
                      bf.header.mPageId * FLAGS_page_size);
    if (ret < 0) {
      return std::unexpected<utils::Error>(
          utils::Error::FileWrite(GetDBFilePath(), errno, strerror(errno)));
    }
    if (ret < FLAGS_page_size) {
      return std::unexpected<utils::Error>(utils::Error::General(
          "Write incomplete, only " + std::to_string(ret) + " bytes written"));
    }
  }
  bf.header.mLatch.UnlockExclusively();
  return {};
}

void BufferManager::RecoverFromDisk() {
  auto recovery = std::make_unique<leanstore::cr::Recovery>(
      mStore, mStore->mCRManager->mGroupCommitter->mWalFd, 0,
      mStore->mCRManager->mGroupCommitter->mWalSize);
  recovery->Run();
}

u64 BufferManager::ConsumedPages() {
  u64 totalUsedBfs = 0;
  u64 totalFreeBfs = 0;
  for (u64 i = 0; i < mNumPartitions; i++) {
    totalFreeBfs += GetPartition(i).NumReclaimedPages();
    totalUsedBfs += GetPartition(i).NumAllocatedPages();
  }
  return totalUsedBfs - totalFreeBfs;
}

// Buffer Frames Management

Partition& BufferManager::RandomPartition() {
  auto randOrdinal = utils::RandomGenerator::Rand<u64>(0, mNumPartitions);
  return GetPartition(randOrdinal);
}

BufferFrame& BufferManager::RandomBufferFrame() {
  auto i = utils::RandomGenerator::Rand<u64>(0, mNumBfs);
  auto* bfAddr = &mBufferPool[i * BufferFrame::Size()];
  return *reinterpret_cast<BufferFrame*>(bfAddr);
}

BufferFrame& BufferManager::AllocNewPage(TREEID treeId) {
  Partition& partition = RandomPartition();
  BufferFrame& freeBf = partition.mFreeBfList.PopFrontMayJump();
  memset((void*)&freeBf, 0, BufferFrame::Size());
  new (&freeBf) BufferFrame();
  freeBf.Init(partition.NextPageId());

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().allocate_operations_counter++;
  }

  freeBf.page.mBTreeId = treeId;
  freeBf.page.mPSN++; // mark as dirty
  return freeBf;
}

// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
void BufferManager::ReclaimPage(BufferFrame& bf) {
  Partition& partition = GetPartition(bf.header.mPageId);
  if (FLAGS_reclaim_page_ids) {
    partition.ReclaimPageId(bf.header.mPageId);
  }

  if (bf.header.mIsBeingWrittenBack) {
    // Do nothing ! we have a garbage collector ;-)
    bf.header.mLatch.UnlockExclusively();
  } else {
    bf.Reset();
    bf.header.mLatch.UnlockExclusively();
    partition.mFreeBfList.PushFront(bf);
  }
}

// Returns a non-latched BufguardedSwipferFrame, called by worker threads
BufferFrame* BufferManager::ResolveSwipMayJump(HybridGuard& swipGuard,
                                               Swip<BufferFrame>& swipValue) {
  if (swipValue.isHOT()) {
    // Resolve swip from hot state
    auto* bf = &swipValue.AsBufferFrame();
    swipGuard.JumpIfModifiedByOthers();
    return bf;
  }
  if (swipValue.isCOOL()) {
    // Resolve swip from cool state
    auto* bf = &swipValue.asBufferFrameMasked();
    swipGuard.JumpIfModifiedByOthers();
    BMOptimisticGuard bfGuard(bf->header.mLatch);
    BMExclusiveUpgradeIfNeeded swipXGuard(swipGuard); // parent
    BMExclusiveGuard bfXGuard(bfGuard);               // child
    bf->header.state = STATE::HOT;
    swipValue.MarkHOT();
    return bf;
  }

  // Resolve swip from evicted state
  //
  // 1. Allocate buffer frame from memory
  // 2. Read page content from disk and fill the buffer frame
  //

  // unlock the current node firstly to avoid deadlock: P->G, G->P
  swipGuard.unlock();

  const PID pageId = swipValue.asPageID();
  Partition& partition = GetPartition(pageId);
  JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
      partition.mInflightIOMutex);
  swipGuard.JumpIfModifiedByOthers();

  auto frameHandler = partition.mInflightIOs.Lookup(pageId);

  // Create an IO frame to read page from disk.
  if (!frameHandler) {
    // 1. Randomly get a buffer frame from partitions
    BufferFrame& bf = RandomPartition().mFreeBfList.PopFrontMayJump();
    DCHECK(!bf.header.mLatch.IsLockedExclusively());
    DCHECK(bf.header.state == STATE::FREE);

    // 2. Create an IO frame in the current partition
    IOFrame& ioFrame = partition.mInflightIOs.Insert(pageId);
    ioFrame.state = IOFrame::STATE::READING;
    ioFrame.readers_counter = 1;
    JumpScoped<std::unique_lock<std::mutex>> ioFrameGuard(ioFrame.mutex);
    inflightIOGuard->unlock();

    // 3. Read page at pageId to the target buffer frame
    ReadPageSync(pageId, &bf.page);
    // DLOG_IF(FATAL, bf.page.mMagicDebuging != pageId)
    //     << "Failed to read page, page corrupted";
    COUNTERS_BLOCK() {
      WorkerCounters::MyCounters().dt_page_reads[bf.page.mBTreeId]++;
      if (FLAGS_trace_dt_id >= 0 &&
          bf.page.mBTreeId == static_cast<TREEID>(FLAGS_trace_dt_id) &&
          utils::RandomGenerator::Rand<u64>(
              0, FLAGS_trace_trigger_probability) == 0) {
        utils::PrintBackTrace();
      }
    }

    // 4. Intialize the buffer frame header
    DCHECK(!bf.header.mIsBeingWrittenBack);
    bf.header.mFlushedPSN = bf.page.mPSN;
    bf.header.state = STATE::LOADED;
    bf.header.mPageId = pageId;
    if (FLAGS_crc_check) {
      bf.header.crc = bf.page.CRC();
    }

    // 5. Publish the buffer frame
    JUMPMU_TRY() {
      swipGuard.JumpIfModifiedByOthers();
      ioFrameGuard->unlock();
      JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
          partition.mInflightIOMutex);
      BMExclusiveUpgradeIfNeeded swipXGuard(swipGuard);

      swipValue.MarkHOT(&bf);
      bf.header.state = STATE::HOT;

      if (ioFrame.readers_counter.fetch_add(-1) == 1) {
        partition.mInflightIOs.Remove(pageId);
      }

      JUMPMU_RETURN& bf;
    }
    JUMPMU_CATCH() {
      // Change state to ready if contention is encountered
      inflightIOGuard->lock();
      ioFrame.bf = &bf;
      ioFrame.state = IOFrame::STATE::READY;
      inflightIOGuard->unlock();
      ioFrameGuard->unlock();
      jumpmu::Jump();
    }
  }

  IOFrame& ioFrame = frameHandler.frame();
  switch (ioFrame.state) {
  case IOFrame::STATE::READING: {
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
  case IOFrame::STATE::READY: {
    BufferFrame* bf = ioFrame.bf;
    {
      // We have to exclusively lock the bf because the page provider thread
      // will try to evict them when its IO is done
      DCHECK(!bf->header.mLatch.IsLockedExclusively());
      DCHECK(bf->header.state == STATE::LOADED);
      BMOptimisticGuard bfGuard(bf->header.mLatch);
      BMExclusiveUpgradeIfNeeded swipXGuard(swipGuard);
      BMExclusiveGuard bfXGuard(bfGuard);
      ioFrame.bf = nullptr;
      swipValue.MarkHOT(bf);
      DCHECK(bf->header.mPageId == pageId);
      DCHECK(swipValue.isHOT());
      DCHECK(bf->header.state == STATE::LOADED);
      bf->header.state = STATE::HOT;

      if (ioFrame.readers_counter.fetch_add(-1) == 1) {
        partition.mInflightIOs.Remove(pageId);
      } else {
        ioFrame.state = IOFrame::STATE::TO_DELETE;
      }
      inflightIOGuard->unlock();
      return bf;
    }
  }
  case IOFrame::STATE::TO_DELETE: {
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
  DCHECK(u64(pageBuffer) % 512 == 0);
  s64 bytesLeft = FLAGS_page_size;
  while (bytesLeft > 0) {
    auto totalRead = FLAGS_page_size - bytesLeft;
    auto curOffset = pageId * FLAGS_page_size + totalRead;
    auto* curBuffer = reinterpret_cast<u8*>(pageBuffer) + totalRead;
    auto bytesRead = pread(mStore->mPageFd, curBuffer, bytesLeft, curOffset);

    // read error, return a zero-initialized pageBuffer frame
    if (bytesRead <= 0) {
      memset(pageBuffer, 0, FLAGS_page_size);
      auto* page = new (pageBuffer) BufferFrame();
      page->Init(pageId);
      LOG(ERROR) << "Failed to read page"
                 << ", error="
                 << utils::Error::FileRead(GetDBFilePath(), errno,
                                           strerror(errno))
                        .ToString()              // error
                 << ", fd=" << mStore->mPageFd   // file descriptor
                 << ", pageId=" << pageId        // page id
                 << ", bytesRead=" << bytesRead  // bytes read
                 << ", bytesLeft=" << bytesLeft; // bytes left
      return;
    }

    bytesLeft -= bytesRead;
  };

  COUNTERS_BLOCK() {
    WorkerCounters::MyCounters().read_operations_counter++;
  }
}

BufferFrame& BufferManager::ReadPageSync(PID pageId) {
  HybridLatch dummyLatch;
  HybridGuard dummyGuard(&dummyLatch);
  dummyGuard.toOptimisticSpin();

  Swip<BufferFrame> swip;
  swip.evict(pageId);

  for (auto failCounter = 100; failCounter > 0; failCounter--) {
    JUMPMU_TRY() {
      swip = ResolveSwipMayJump(dummyGuard, swip);
      JUMPMU_RETURN swip.AsBufferFrame();
    }
    JUMPMU_CATCH() {
    }
  }

  LOG(FATAL) << "Failed to read page, pageId=" << pageId;
}

void BufferManager::WritePageSync(BufferFrame& bf) {
  HybridGuard guardedBf(&bf.header.mLatch);
  guardedBf.ToExclusiveMayJump();
  auto pageId = bf.header.mPageId;
  auto& partition = GetPartition(pageId);
  pwrite(mStore->mPageFd, &bf.page, FLAGS_page_size, pageId * FLAGS_page_size);
  bf.Reset();
  guardedBf.unlock();
  partition.mFreeBfList.PushFront(bf);
}

void BufferManager::SyncAllPageWrites() {
  fdatasync(mStore->mPageFd);
}

u64 BufferManager::GetPartitionID(PID pageId) {
  return pageId & mPartitionsMask;
}

Partition& BufferManager::GetPartition(PID pageId) {
  const u64 partitionId = GetPartitionID(pageId);
  return *mPartitions[partitionId];
}

void BufferManager::StopBufferFrameProviders() {
  mBfProviders.clear();
}

BufferManager::~BufferManager() {
  StopBufferFrameProviders();
  u64 totalMemSize = BufferFrame::Size() * (mNumBfs + mNumSaftyBfs);
  munmap(mBufferPool, totalMemSize);
}

void BufferManager::DoWithBufferFrameIf(
    std::function<bool(BufferFrame& bf)> condition,
    std::function<void(BufferFrame& bf)> action) {
  utils::Parallelize::parallelRange(mNumBfs, [&](u64 begin, u64 end) {
    DCHECK(condition != nullptr);
    DCHECK(action != nullptr);
    for (u64 i = begin; i < end; i++) {
      auto* bfAddr = &mBufferPool[i * BufferFrame::Size()];
      auto& bf = *reinterpret_cast<BufferFrame*>(bfAddr);
      bf.header.mLatch.LockExclusively();
      if (condition(bf)) {
        action(bf);
      }
      bf.header.mLatch.UnlockExclusively();
    }
  });
}

} // namespace storage
} // namespace leanstore
