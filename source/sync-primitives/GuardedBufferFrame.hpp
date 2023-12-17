#pragma once

#include "Exceptions.hpp"
#include "HybridGuard.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/Tracing.hpp"

#include <glog/logging.h>

namespace leanstore {
namespace storage {

template <typename T> class ExclusivePageGuard;
template <typename T> class SharedPageGuard;

/// A lock guarded buffer frame
template <typename T> class GuardedBufferFrame {
public:
  /// The guarded buffer frame. Latch mode is determined by mGuard.
  BufferFrame* mBf = nullptr;

  /// The latch guard of this buffer frame
  HybridGuard mGuard;

  bool mKeepAlive = true;

public:
  /// Construct an empty GuardedBufferFrame, nothing to guard.
  GuardedBufferFrame() : mBf(nullptr), mGuard(nullptr) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Construct a GuardedBufferFrame from an existing latch guard.
  /// @param hybridGuard the latch guard of the buffer frame
  /// @param bf the latch guarded buffer frame
  GuardedBufferFrame(HybridGuard&& hybridGuard, BufferFrame* bf)
      : mBf(bf), mGuard(std::move(hybridGuard)) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  GuardedBufferFrame(GuardedBufferFrame& other) = delete;  // Copy constructor
  GuardedBufferFrame(GuardedBufferFrame&& other) = delete; // Move constructor

  /// Allocate a new page, latch the buffer frame exclusively. The newly created
  ///
  /// @param treeId The tree ID which this page belongs to.
  /// @param keepAlive
  GuardedBufferFrame(TREEID treeId, bool keepAlive = true)
      : mBf(&BufferManager::sInstance->AllocNewPage()),
        mGuard(mBf->header.mLatch, GUARD_STATE::EXCLUSIVE),
        mKeepAlive(keepAlive) {
    mBf->page.mBTreeId = treeId;
    markAsDirty();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Construct a GuardedBufferFrame from a HOT swip, usually used for latching
  /// the meta node of a BTree.
  GuardedBufferFrame(
      Swip<BufferFrame> hotSwip,
      const LATCH_FALLBACK_MODE ifContended = LATCH_FALLBACK_MODE::SPIN)
      : mBf(&hotSwip.AsBufferFrame()), mGuard(&mBf->header.mLatch) {
    latchMayJump(mGuard, ifContended);
    syncGSN();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Construct a GuardedBufferFrame from the guarded parent and the child swip,
  /// usually used for lock coupling which locks the parent firstly then lock
  /// the child.
  ///
  /// @param guardedParent The guarded parent node, which protects everyting in
  /// the parent node, including childSwip.
  /// @param childSwip The swip to the child node. The child page is loaded to
  /// memory if it is evicted.
  /// @param ifContended Lock fall back mode if contention happens.
  template <typename T2>
  GuardedBufferFrame(
      GuardedBufferFrame<T2>& guardedParent, Swip<T>& childSwip,
      const LATCH_FALLBACK_MODE ifContended = LATCH_FALLBACK_MODE::SPIN)
      : mBf(BufferManager::sInstance->tryFastResolveSwip(
            guardedParent.mGuard, childSwip.template CastTo<BufferFrame>())),
        mGuard(&mBf->header.mLatch) {
    latchMayJump(mGuard, ifContended);
    syncGSN();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();

    PARANOID_BLOCK() {
      TREEID parentTreeId = guardedParent.mBf->page.mBTreeId;
      TREEID treeId = mBf->page.mBTreeId;
      PID pageId = mBf->header.mPageId;
      guardedParent.JumpIfModifiedByOthers();
      JumpIfModifiedByOthers();
      if (parentTreeId != treeId) {
        cout << "parentTreeId != treeId" << endl;
        leanstore::storage::Tracing::printStatus(pageId);
      }
    }

    guardedParent.JumpIfModifiedByOthers();
  }

  /// Downgrade an exclusive guard
  GuardedBufferFrame(ExclusivePageGuard<T>&&) = delete;
  GuardedBufferFrame& operator=(ExclusivePageGuard<T>&&) {
    mGuard.unlock();
    return *this;
  }

  /// Downgrade a shared guard
  GuardedBufferFrame(SharedPageGuard<T>&&) = delete;
  GuardedBufferFrame& operator=(SharedPageGuard<T>&&) {
    mGuard.unlock();
    return *this;
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(GuardedBufferFrame)

  ~GuardedBufferFrame() {
    if (mGuard.mState == GUARD_STATE::EXCLUSIVE) {
      if (!mKeepAlive) {
        reclaim();
      }
    }
    mGuard.unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }

  /// Assignment operator
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame& other) = delete;

  /// Move assignment
  template <typename T2>
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame<T2>&& other) {
    mBf = other.mBf;
    mGuard = std::move(other.mGuard);
    mKeepAlive = other.mKeepAlive;
    return *this;
  }

public:
  //---------------------------------------------------------------------------
  // Object Utils
  //---------------------------------------------------------------------------
  inline void markAsDirty() {
    mBf->page.mPSN++;
  }

  inline void incrementGSN() {
    DCHECK(mBf != nullptr);
    DCHECK(mBf->page.mGSN <= cr::Worker::my().mLogging.GetCurrentGsn());

    mBf->page.mPSN++;
    mBf->page.mGSN = cr::Worker::my().mLogging.GetCurrentGsn() + 1;
    mBf->header.mLastWriterWorker = cr::Worker::my().mWorkerId; // RFA

    const auto currentGsn = cr::Worker::my().mLogging.GetCurrentGsn();
    const auto pageGsn = mBf->page.mGSN;
    if (currentGsn < pageGsn) {
      cr::Worker::my().mLogging.SetCurrentGsn(pageGsn);
    }
  }

  // WAL
  inline void syncGSN() {
    if (!FLAGS_wal) {
      return;
    }

    // TODO: don't sync on temporary table pages like HistoryTree
    if (FLAGS_wal_rfa) {
      if (mBf->page.mGSN > cr::Worker::my().mLogging.mMinFlushedGsn &&
          mBf->header.mLastWriterWorker != cr::Worker::my().mWorkerId) {
        cr::Worker::my().mLogging.mHasRemoteDependency = true;
      }
    }

    const auto currentGsn = cr::Worker::my().mLogging.GetCurrentGsn();
    const auto pageGsn = mBf->page.mGSN;
    if (currentGsn < pageGsn) {
      cr::Worker::my().mLogging.SetCurrentGsn(pageGsn);
    }
  }

  template <typename WT, typename... Args>
  cr::WALPayloadHandler<WT> ReserveWALPayload(u64 payloadSize, Args&&... args) {
    DCHECK(FLAGS_wal);
    DCHECK(mGuard.mState == GUARD_STATE::EXCLUSIVE);

    if (!FLAGS_wal_tuple_rfa) {
      incrementGSN();
    }

    const auto pageId = mBf->header.mPageId;
    const auto treeId = mBf->page.mBTreeId;
    // TODO: verify
    auto handler =
        cr::Worker::my().mLogging.ReserveWALEntryComplex<WT, Args...>(
            sizeof(WT) + payloadSize, pageId,
            cr::Worker::my().mLogging.GetCurrentGsn(), treeId,
            std::forward<Args>(args)...);
    return handler;
  }

  inline void submitWALEntry(u64 total_size) {
    cr::Worker::my().mLogging.SubmitWALEntryComplex(total_size);
  }

  inline bool EncounteredContention() {
    return mGuard.mEncounteredContention;
  }
  inline void unlock() {
    mGuard.unlock();
  }

  inline void JumpIfModifiedByOthers() {
    mGuard.JumpIfModifiedByOthers();
  }

  inline T& ref() {
    return *reinterpret_cast<T*>(mBf->page.mPayload);
  }
  inline T* ptr() {
    return reinterpret_cast<T*>(mBf->page.mPayload);
  }
  inline Swip<T> swip() {
    return Swip<T>(mBf);
  }
  inline T* operator->() {
    return reinterpret_cast<T*>(mBf->page.mPayload);
  }

  // Use with caution!
  void ToSharedMayJump() {
    mGuard.ToSharedMayJump();
  }
  void ToExclusiveMayJump() {
    mGuard.ToExclusiveMayJump();
  }

  void TryToSharedMayJump() {
    mGuard.TryToSharedMayJump();
  }

  void TryToExclusiveMayJump() {
    mGuard.TryToExclusiveMayJump();
  }

  void reclaim() {
    BufferManager::sInstance->reclaimPage(*(mBf));
    mGuard.mState = GUARD_STATE::MOVED;
  }

protected:
  void latchMayJump(HybridGuard& guard, const LATCH_FALLBACK_MODE ifContended) {
    switch (ifContended) {
    case LATCH_FALLBACK_MODE::SPIN: {
      guard.toOptimisticSpin();
      break;
    }
    case LATCH_FALLBACK_MODE::EXCLUSIVE: {
      guard.toOptimisticOrExclusive();
      break;
    }
    case LATCH_FALLBACK_MODE::SHARED: {
      guard.toOptimisticOrShared();
      break;
    }
    case LATCH_FALLBACK_MODE::JUMP: {
      guard.toOptimisticOrJump();
      break;
    }
    default: {
      DCHECK(false) << "Unhandled LATCH_FALLBACK_MODE: "
                    << std::to_string(static_cast<u64>(ifContended));
    }
    }
  }
};

template <typename T> class ExclusivePageGuard {
private:
  GuardedBufferFrame<T>& mRefGuard;

public:
  // I: Upgrade
  ExclusivePageGuard(GuardedBufferFrame<T>&& o_guard) : mRefGuard(o_guard) {
    mRefGuard.mGuard.ToExclusiveMayJump();
  }

  template <typename WT, typename... Args>
  cr::WALPayloadHandler<WT> ReserveWALPayload(u64 payloadSize, Args&&... args) {
    return mRefGuard.template ReserveWALPayload<WT>(
        payloadSize, std::forward<Args>(args)...);
  }

  inline void submitWALEntry(u64 total_size) {
    mRefGuard.submitWALEntry(total_size);
  }

  template <typename... Args> void init(Args&&... args) {
    new (mRefGuard.mBf->page.mPayload) T(std::forward<Args>(args)...);
  }

  void keepAlive() {
    mRefGuard.mKeepAlive = true;
  }

  void incrementGSN() {
    mRefGuard.incrementGSN();
  }

  void markAsDirty() {
    mRefGuard.markAsDirty();
  }

  ~ExclusivePageGuard() {
    if (!mRefGuard.mKeepAlive &&
        mRefGuard.mGuard.mState == GUARD_STATE::EXCLUSIVE) {
      mRefGuard.reclaim();
    } else {
      mRefGuard.unlock();
    }
  }

  inline T& ref() {
    return *reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }

  inline T* PageData() {
    return reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }

  inline Swip<T> swip() {
    return Swip<T>(mRefGuard.mBf);
  }

  inline T* operator->() {
    return reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }

  inline BufferFrame* bf() {
    return mRefGuard.mBf;
  }

  inline void reclaim() {
    mRefGuard.reclaim();
  }
};

template <typename T> class SharedPageGuard {
public:
  GuardedBufferFrame<T>& mRefGuard;

  // I: Upgrade
  SharedPageGuard(GuardedBufferFrame<T>&& h_guard) : mRefGuard(h_guard) {
    mRefGuard.ToSharedMayJump();
  }

  ~SharedPageGuard() {
    mRefGuard.unlock();
  }

  inline T& ref() {
    return *reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }

  inline T* ptr() {
    return reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }

  inline Swip<T> swip() {
    return Swip<T>(mRefGuard.mBf);
  }

  inline T* operator->() {
    return reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }
};

} // namespace storage
} // namespace leanstore
