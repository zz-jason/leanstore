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

template <typename T> class HybridPageGuard {
public:
  BufferFrame* mBf = nullptr;

  HybridGuard mGuard;

  bool mKeepAlive = true;

public:
  HybridPageGuard() : mBf(nullptr), mGuard(nullptr) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  HybridPageGuard(HybridGuard&& hybridGuard, BufferFrame* bf)
      : mBf(bf), mGuard(std::move(hybridGuard)) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  HybridPageGuard(HybridPageGuard& other) = delete;  // Copy constructor
  HybridPageGuard(HybridPageGuard&& other) = delete; // Move constructor

  /// Used to allocate a new page and create a HybridGuard on it.
  ///
  /// @param treeId The tree which this page belongs to.
  /// @param keepAlive
  HybridPageGuard(TREEID treeId, bool keepAlive = true)
      : mBf(&BufferManager::sInstance->AllocNewPage()),
        mGuard(mBf->header.mLatch, GUARD_STATE::EXCLUSIVE),
        mKeepAlive(keepAlive) {
    mBf->page.mBTreeId = treeId;
    markAsDirty();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Used to for root node.
  HybridPageGuard(
      Swip<BufferFrame> swip,
      const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
      : mBf(&swip.AsBufferFrame()), mGuard(&mBf->header.mLatch) {
    latchAccordingToFallbackMode(mGuard, if_contended);
    syncGSN();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Used for lock coupling.
  ////
  /// @param parentGuard The guarded parent node, which protects everyting in
  /// the parent node, including childSwip.
  /// @param childSwip The swip to the child node.
  /// @param if_contended Lock fall back mode if contention happens.
  template <typename T2>
  HybridPageGuard(
      HybridPageGuard<T2>& parentGuard, Swip<T>& childSwip,
      const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
      : mBf(BufferManager::sInstance->tryFastResolveSwip(
            parentGuard.mGuard, childSwip.template CastTo<BufferFrame>())),
        mGuard(&mBf->header.mLatch) {
    latchAccordingToFallbackMode(mGuard, if_contended);
    syncGSN();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();

    PARANOID_BLOCK() {
      TREEID parentTreeId = parentGuard.mBf->page.mBTreeId;
      TREEID treeId = mBf->page.mBTreeId;
      PID pageId = mBf->header.mPageId;
      parentGuard.JumpIfModifiedByOthers();
      JumpIfModifiedByOthers();
      if (parentTreeId != treeId) {
        cout << "parentTreeId != treeId" << endl;
        leanstore::storage::Tracing::printStatus(pageId);
      }
    }

    parentGuard.JumpIfModifiedByOthers();
  }

  // I: Downgrade exclusive
  HybridPageGuard(ExclusivePageGuard<T>&&) = delete;
  HybridPageGuard& operator=(ExclusivePageGuard<T>&&) {
    mGuard.unlock();
    return *this;
  }

  // I: Downgrade shared
  HybridPageGuard(SharedPageGuard<T>&&) = delete;
  HybridPageGuard& operator=(SharedPageGuard<T>&&) {
    mGuard.unlock();
    return *this;
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(HybridPageGuard)

  ~HybridPageGuard() {
    if (mGuard.mState == GUARD_STATE::EXCLUSIVE) {
      if (!mKeepAlive) {
        reclaim();
      }
    }
    mGuard.unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }

  // Assignment operator
  constexpr HybridPageGuard& operator=(HybridPageGuard& other) = delete;
  template <typename T2>
  constexpr HybridPageGuard& operator=(HybridPageGuard<T2>&& other) {
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
    assert(mBf != nullptr);
    assert(mBf->page.mGSN <= cr::Worker::my().mLogging.GetCurrentGsn());

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
  void latchAccordingToFallbackMode(HybridGuard& guard,
                                    const LATCH_FALLBACK_MODE if_contended) {
    if (if_contended == LATCH_FALLBACK_MODE::SPIN) {
      guard.toOptimisticSpin();
    } else if (if_contended == LATCH_FALLBACK_MODE::EXCLUSIVE) {
      guard.toOptimisticOrExclusive();
    } else if (if_contended == LATCH_FALLBACK_MODE::SHARED) {
      guard.toOptimisticOrShared();
    } else if (if_contended == LATCH_FALLBACK_MODE::JUMP) {
      guard.toOptimisticOrJump();
    } else {
      UNREACHABLE();
    }
  }
};

template <typename T> class ExclusivePageGuard {
private:
  HybridPageGuard<T>& mRefGuard;

public:
  // I: Upgrade
  ExclusivePageGuard(HybridPageGuard<T>&& o_guard) : mRefGuard(o_guard) {
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
  HybridPageGuard<T>& mRefGuard;

  // I: Upgrade
  SharedPageGuard(HybridPageGuard<T>&& h_guard) : mRefGuard(h_guard) {
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
