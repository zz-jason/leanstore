#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Config.hpp"
#include "Exceptions.hpp"
#include "concurrency-recovery/Recovery.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/PPCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/FVector.hpp"
#include "utils/Misc.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <iomanip>
#include <set>

namespace leanstore {
namespace storage {

thread_local BufferFrame* BufferManager::sTlsLastReadBf = nullptr;
std::unique_ptr<BufferManager> BufferManager::sInstance = nullptr;

BufferManager::BufferManager(s32 fd) : mPageFd(fd) {
  mNumBfs = FLAGS_buffer_pool_size / sizeof(BufferFrame);
  const u64 totalMemSize = sizeof(BufferFrame) * (mNumBfs + mNumSaftyBfs);

  // Init buffer pool with zero-initialized buffer frames. Use mmap with flags
  // MAP_PRIVATE and MAP_ANONYMOUS, no underlying file desciptor to allocate
  // totalmemSize buffer pool with zero-initialized contents. See:
  //  1. https://man7.org/linux/man-pages/man2/mmap.2.html
  //  2.
  //  https://stackoverflow.com/questions/34042915/what-is-the-purpose-of-map-anonymous-flag-in-mmap-system-call
  {
    void* underlyingBuf = mmap(/* addr= */ NULL, /* length= */ totalMemSize,
                               /* prot= */ PROT_READ | PROT_WRITE,
                               /* flags= */ MAP_PRIVATE | MAP_ANONYMOUS,
                               /* fd= */ -1, /* offset= */ 0);
    LOG_IF(FATAL, underlyingBuf == MAP_FAILED)
        << "Failed to allocate memory for the buffer pool"
        << ", FLAGS_buffer_pool_size=" << FLAGS_buffer_pool_size
        << ", totalMemSize=" << totalMemSize;

    mBfs = reinterpret_cast<BufferFrame*>(underlyingBuf);
    madvise(mBfs, totalMemSize, MADV_HUGEPAGE);
    madvise(mBfs, totalMemSize, MADV_DONTFORK);
  }

  // Initialize mPartitions
  mNumPartitions = (1 << FLAGS_partition_bits);
  mPartitionsMask = mNumPartitions - 1;
  const u64 freeBfsLimitPerPartition =
      std::ceil((FLAGS_free_pct * 1.0 * mNumBfs / 100.0) /
                static_cast<double>(mNumPartitions));
  for (u64 i = 0; i < mNumPartitions; i++) {
    mPartitions.push_back(std::make_unique<Partition>(
        i, mNumPartitions, freeBfsLimitPerPartition));
  }

  // zerofill all the buffer frames
  utils::Parallelize::parallelRange(totalMemSize, [&](u64 begin, u64 end) {
    memset(reinterpret_cast<u8*>(mBfs) + begin, 0, end - begin);
  });

  // spread these buffer frames to all the partitions
  utils::Parallelize::parallelRange(mNumBfs, [&](u64 begin, u64 end) {
    u64 partitionId = 0;
    for (u64 i = begin; i < end; i++) {
      auto& partition = getPartition(partitionId);
      partition.mFreeBfList.PushFront(*new (mBfs + i) BufferFrame());
      partitionId = (partitionId + 1) % mNumPartitions;
    }
  });
}

void BufferManager::StartBufferFrameProviders() {
  // make it optional for pure in-memory experiments
  if (FLAGS_pp_threads <= 0) {
    return;
  }

  DCHECK(FLAGS_pp_threads <= mNumPartitions);
  mBfProviders.reserve(FLAGS_pp_threads);
  for (auto i = 0u; i < FLAGS_pp_threads; ++i) {
    mBfProviders.push_back(std::move(std::make_unique<BufferFrameProvider>(
        i, "leanstore_bf_provider_" + std::to_string(i), mNumBfs, mBfs,
        mNumPartitions, mPartitionsMask, mPartitions, mPageFd)));
  }

  for (auto i = 0u; i < mBfProviders.size(); ++i) {
    mBfProviders[i]->Start();
  }
}

StringMap BufferManager::serialize() {
  // TODO: correctly serialize ranges of used pages
  StringMap map;
  PID maxPageId = 0;
  for (u64 i = 0; i < mNumPartitions; i++) {
    maxPageId = std::max<PID>(getPartition(i).mNextPageId, maxPageId);
  }
  map["max_pid"] = std::to_string(maxPageId);
  return map;
}

void BufferManager::deserialize(StringMap map) {
  PID maxPageId = std::stoull(map["max_pid"]);
  maxPageId = (maxPageId + (mNumPartitions - 1)) & ~(mNumPartitions - 1);
  for (u64 i = 0; i < mNumPartitions; i++) {
    getPartition(i).mNextPageId = maxPageId + i;
  }
}

void BufferManager::writeAllBufferFrames() {
  LS_DEBUG_EXECUTE("skip_writeAllBufferFrames", {
    LOG(ERROR) << "writeAllBufferFrames skipped due to debug flag";
    return;
  });

  StopBufferFrameProviders();
  utils::Parallelize::parallelRange(mNumBfs, [&](u64 begin, u64 end) {
    Page page;
    for (u64 i = begin; i < end; i++) {
      auto& bf = mBfs[i];
      bf.header.mLatch.mutex.lock();
      if (!bf.isFree()) {
        page.mBTreeId = bf.page.mBTreeId;
        page.mMagicDebuging = bf.header.mPageId;
        TreeRegistry::sInstance->checkpoint(bf.page.mBTreeId, bf,
                                            page.mPayload);
        s64 ret = pwrite(mPageFd, &page, PAGE_SIZE, bf.header.mPageId * PAGE_SIZE);
        ENSURE(ret == PAGE_SIZE);
      }
      bf.header.mLatch.mutex.unlock();
    }
  });
}

void BufferManager::WriteBufferFrame(BufferFrame& bf) {
  Page page;
  bf.header.mLatch.mutex.lock();
  if (!bf.isFree()) {
    page.mBTreeId = bf.page.mBTreeId;
    page.mMagicDebuging = bf.header.mPageId;
    TreeRegistry::sInstance->checkpoint(bf.page.mBTreeId, bf, page.mPayload);
    s64 ret = pwrite(mPageFd, &page, PAGE_SIZE, bf.header.mPageId * PAGE_SIZE);
    ENSURE(ret == PAGE_SIZE);
  }
  bf.header.mLatch.mutex.unlock();
}

void BufferManager::RecoveryFromDisk() {
  auto recovery = std::make_unique<leanstore::cr::Recovery>(
      leanstore::cr::CRManager::sInstance->mWalFd, 0,
      leanstore::cr::CRManager::sInstance->mWalSize);
  recovery->Run();
}

BufferFrame& BufferManager::ReadPageToRecover(PID pageId) {
  HybridLatch dummyLatch;
  Guard dummyGuard(&dummyLatch);
  dummyGuard.toOptimisticSpin();

  Swip<BufferFrame> swip;
  swip.evict(pageId);

  for (auto failCounter = 100; failCounter > 0; failCounter--) {
    JUMPMU_TRY() {
      swip = ResolveSwipMayJump(dummyGuard, swip);
      swip.AsBufferFrame().header.mKeepInMemory = true;
      JUMPMU_RETURN swip.AsBufferFrame();
    }
    JUMPMU_CATCH() {
    }
  }

  LOG(FATAL) << "Failed to read page, pageId=" << pageId;
}

u64 BufferManager::consumedPages() {
  u64 totalUsedBfs = 0;
  u64 totalFreeBfs = 0;
  for (u64 i = 0; i < mNumPartitions; i++) {
    totalFreeBfs += getPartition(i).NumReclaimedPages();
    totalUsedBfs += getPartition(i).NumAllocatedPages();
  }
  return totalUsedBfs - totalFreeBfs;
}

// Buffer Frames Management

Partition& BufferManager::randomPartition() {
  auto randOrdinal = utils::RandomGenerator::getRand<u64>(0, mNumPartitions);
  return getPartition(randOrdinal);
}

BufferFrame& BufferManager::randomBufferFrame() {
  auto randOrdinal = utils::RandomGenerator::getRand<u64>(0, mNumBfs);
  return mBfs[randOrdinal];
}

BufferFrame& BufferManager::AllocNewPage() {
  Partition& partition = randomPartition();
  BufferFrame& free_bf = partition.mFreeBfList.PopFrontMayJump();
  free_bf.Init(partition.NextPageId());

  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().allocate_operations_counter++;
  }

  return free_bf;
}

// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
void BufferManager::reclaimPage(BufferFrame& bf) {
  Partition& partition = getPartition(bf.header.mPageId);
  if (FLAGS_reclaim_page_ids) {
    partition.ReclaimPageId(bf.header.mPageId);
  }

  if (bf.header.mIsBeingWrittenBack) {
    // DO NOTHING ! we have a garbage collector ;-)
    bf.header.mLatch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
    bf.header.mLatch.mutex.unlock();
  } else {
    bf.reset();
    bf.header.mLatch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
    bf.header.mLatch.mutex.unlock();
    partition.mFreeBfList.PushFront(bf);
  }
}

// Returns a non-latched BufguardedSwipferFrame, called by worker threads
BufferFrame* BufferManager::ResolveSwipMayJump(Guard& swipGuard,
                                               Swip<BufferFrame>& swipValue) {
  if (swipValue.isHOT()) {
    // Resolve swip from hot state
    auto bf = &swipValue.AsBufferFrame();
    swipGuard.JumpIfModifiedByOthers();
    return bf;
  } else if (swipValue.isCOOL()) {
    // Resolve swip from cool state
    auto bf = &swipValue.asBufferFrameMasked();
    swipGuard.JumpIfModifiedByOthers();
    BMOptimisticGuard bf_guard(bf->header.mLatch);
    BMExclusiveUpgradeIfNeeded swip_x_guard(swipGuard); // parent
    BMExclusiveGuard bf_x_guard(bf_guard);              // child
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
  Partition& partition = getPartition(pageId);
  JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
      partition.mInflightIOMutex);
  swipGuard.JumpIfModifiedByOthers();

  auto frameHandler = partition.mInflightIOs.Lookup(pageId);

  // Create an IO frame to read page from disk.
  if (!frameHandler) {
    // 1. Randomly get a buffer frame from partitions
    BufferFrame& bf = randomPartition().mFreeBfList.PopFrontMayJump();
    DCHECK(!bf.header.mLatch.isExclusivelyLatched());
    DCHECK(bf.header.state == STATE::FREE);

    // 2. Create an IO frame in the current partition
    IOFrame& ioFrame = partition.mInflightIOs.insert(pageId);
    ioFrame.state = IOFrame::STATE::READING;
    ioFrame.readers_counter = 1;
    JumpScoped<std::unique_lock<std::mutex>> ioFrameGuard(ioFrame.mutex);
    inflightIOGuard->unlock();

    // 3. Read page at pageId to the target buffer frame
    readPageSync(pageId, &bf.page);
    // DLOG_IF(FATAL, bf.page.mMagicDebuging != pageId)
    //     << "Failed to read page, page corrupted";
    COUNTERS_BLOCK() {
      WorkerCounters::myCounters().dt_page_reads[bf.page.mBTreeId]++;
      if (FLAGS_trace_dt_id >= 0 &&
          bf.page.mBTreeId == static_cast<TREEID>(FLAGS_trace_dt_id) &&
          utils::RandomGenerator::getRand<u64>(
              0, FLAGS_trace_trigger_probability) == 0) {
        utils::printBackTrace();
      }
    }

    // 4. Intialize the buffer frame header
    DCHECK(!bf.header.mIsBeingWrittenBack);
    bf.header.mFlushedPSN = bf.page.mPSN;
    bf.header.state = STATE::LOADED;
    bf.header.mPageId = pageId;
    if (FLAGS_crc_check) {
      bf.header.crc = utils::CRC(bf.page.mPayload, EFFECTIVE_PAGE_SIZE);
    }

    // 5. Publish the buffer frame
    JUMPMU_TRY() {
      swipGuard.JumpIfModifiedByOthers();
      JumpScoped<std::unique_lock<std::mutex>> inflightIOGuard(
          partition.mInflightIOMutex);
      BMExclusiveUpgradeIfNeeded swip_x_guard(swipGuard);
      ioFrameGuard->unlock();

      swipValue.MarkHOT(&bf);
      bf.header.state = STATE::HOT;

      if (ioFrame.readers_counter.fetch_add(-1) == 1) {
        partition.mInflightIOs.remove(pageId);
      }

      sTlsLastReadBf = &bf;
      JUMPMU_RETURN& bf;
    }
    JUMPMU_CATCH() {
      // Change state to ready if contention is encountered
      inflightIOGuard->lock();
      ioFrame.bf = &bf;
      ioFrame.state = IOFrame::STATE::READY;
      inflightIOGuard->unlock();
      ioFrameGuard->unlock();
      jumpmu::jump();
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
        partition.mInflightIOs.remove(pageId);
      }
      inflightIOGuard->unlock();
    }
    jumpmu::jump(); // why jump?
    break;
  }
  case IOFrame::STATE::READY: {
    BufferFrame* bf = ioFrame.bf;
    {
      // We have to exclusively lock the bf because the page provider thread
      // will try to evict them when its IO is done
      DCHECK(!bf->header.mLatch.isExclusivelyLatched());
      DCHECK(bf->header.state == STATE::LOADED);
      BMOptimisticGuard bf_guard(bf->header.mLatch);
      BMExclusiveUpgradeIfNeeded swip_x_guard(swipGuard);
      BMExclusiveGuard bf_x_guard(bf_guard);
      ioFrame.bf = nullptr;
      swipValue.MarkHOT(bf);
      DCHECK(bf->header.mPageId == pageId);
      DCHECK(swipValue.isHOT());
      DCHECK(bf->header.state == STATE::LOADED);
      bf->header.state = STATE::HOT;

      if (ioFrame.readers_counter.fetch_add(-1) == 1) {
        partition.mInflightIOs.remove(pageId);
      } else {
        ioFrame.state = IOFrame::STATE::TO_DELETE;
      }
      inflightIOGuard->unlock();
      sTlsLastReadBf = bf;
      return bf;
    }
  }
  case IOFrame::STATE::TO_DELETE: {
    if (ioFrame.readers_counter == 0) {
      partition.mInflightIOs.remove(pageId);
    }
    inflightIOGuard->unlock();
    jumpmu::jump();
    break;
  }
  default: {
    DCHECK(false);
  }
  }
  assert(false);
}

void BufferManager::readPageSync(PID pageId, void* destination) {
  DCHECK(u64(destination) % 512 == 0);
  s64 bytesLeft = PAGE_SIZE;
  do {
    auto bytesRead = pread(mPageFd, destination, bytesLeft,
                           pageId * PAGE_SIZE + (PAGE_SIZE - bytesLeft));
    if (bytesRead < 0) {
      LOG(ERROR) << "pread failed"
                 << ", error= " << bytesRead << ", pageId=" << pageId;
      return;
    }
    bytesLeft -= bytesRead;
  } while (bytesLeft > 0);

  COUNTERS_BLOCK() {
    WorkerCounters::myCounters().read_operations_counter++;
  }
}

void BufferManager::fDataSync() {
  fdatasync(mPageFd);
}

u64 BufferManager::getPartitionID(PID pageId) {
  return pageId & mPartitionsMask;
}

Partition& BufferManager::getPartition(PID pageId) {
  const u64 partitionId = getPartitionID(pageId);
  return *mPartitions[partitionId];
}

void BufferManager::StopBufferFrameProviders() {
  for (auto i = 0u; i < mBfProviders.size(); ++i) {
    mBfProviders[i]->Stop();
  }
  mBfProviders.clear();
}

BufferManager::~BufferManager() {
  StopBufferFrameProviders();
  u64 totalMemSize = sizeof(BufferFrame) * (mNumBfs + mNumSaftyBfs);
  munmap(mBfs, totalMemSize);
}

} // namespace storage
} // namespace leanstore
