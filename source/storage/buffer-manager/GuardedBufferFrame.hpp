#pragma once

#include "concurrency-recovery/WALEntry.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "shared-headers/Exceptions.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/Tracing.hpp"
#include "sync-primitives/HybridGuard.hpp"

#include <glog/logging.h>

namespace leanstore {
namespace storage {

template <typename T> class ExclusiveGuardedBufferFrame;
template <typename T> class SharedGuardedBufferFrame;

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
      : mBf(bf),
        mGuard(std::move(hybridGuard)) {
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
    MarkAsDirty();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Construct a GuardedBufferFrame from a HOT swip, usually used for latching
  /// the meta node of a BTree.
  GuardedBufferFrame(
      Swip<BufferFrame> hotSwip,
      const LATCH_FALLBACK_MODE ifContended = LATCH_FALLBACK_MODE::SPIN)
      : mBf(&hotSwip.AsBufferFrame()),
        mGuard(&mBf->header.mLatch) {
    latchMayJump(mGuard, ifContended);
    SyncGSNBeforeRead();
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
    SyncGSNBeforeRead();
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
  GuardedBufferFrame(ExclusiveGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(ExclusiveGuardedBufferFrame<T>&&) {
    mGuard.unlock();
    return *this;
  }

  /// Downgrade a shared guard
  GuardedBufferFrame(SharedGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(SharedGuardedBufferFrame<T>&&) {
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
  inline void MarkAsDirty() {
    mBf->page.mPSN++;
  }

  inline void SyncGSNBeforeWrite() {
    DCHECK(mBf != nullptr);
    DCHECK(mBf->page.mGSN <= cr::Worker::my().mLogging.GetCurrentGsn());

    MarkAsDirty();
    mBf->header.mLastWriterWorker = cr::Worker::my().mWorkerId;

    auto const workerGSN = cr::Worker::my().mLogging.GetCurrentGsn();
    mBf->page.mGSN = workerGSN + 1;
    cr::Worker::my().mLogging.SetCurrentGsn(workerGSN + 1);
  }

  // TODO: don't sync on temporary table pages like HistoryTree
  inline void SyncGSNBeforeRead() {
    if (!cr::Worker::my().mLogging.mHasRemoteDependency &&
        mBf->page.mGSN > cr::Worker::my().mLogging.mTxReadSnapshot &&
        mBf->header.mLastWriterWorker != cr::Worker::my().mWorkerId) {
      cr::Worker::my().mLogging.mHasRemoteDependency = true;
      DLOG(INFO) << "detect remote dependency"
                 << ", workerId=" << cr::Worker::my().mWorkerId
                 << ", txReadSnapshot(GSN)="
                 << cr::Worker::my().mLogging.mTxReadSnapshot
                 << ", pageLastWriterWorker=" << mBf->header.mLastWriterWorker
                 << ", pageGSN=" << mBf->page.mGSN;
    }

    auto const workerGSN = cr::Worker::my().mLogging.GetCurrentGsn();
    auto const pageGSN = mBf->page.mGSN;
    if (workerGSN < pageGSN) {
      cr::Worker::my().mLogging.SetCurrentGsn(pageGSN);
    }
  }

  template <typename WT, typename... Args>
  cr::WALPayloadHandler<WT> ReserveWALPayload(u64 payloadSize, Args&&... args) {
    DCHECK(FLAGS_wal);
    DCHECK(mGuard.mState == GUARD_STATE::EXCLUSIVE);

    SyncGSNBeforeWrite();

    auto const pageId = mBf->header.mPageId;
    auto const treeId = mBf->page.mBTreeId;
    payloadSize = ((payloadSize - 1) / 8 + 1) * 8;
    // TODO: verify
    auto handler =
        cr::Worker::my().mLogging.ReserveWALEntryComplex<WT, Args...>(
            sizeof(WT) + payloadSize, pageId, mBf->page.mPSN, treeId,
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

template <typename PayloadType> class ExclusiveGuardedBufferFrame {
private:
  GuardedBufferFrame<PayloadType>& mRefGuard;

public:
  // I: Upgrade
  ExclusiveGuardedBufferFrame(GuardedBufferFrame<PayloadType>&& guardedBf)
      : mRefGuard(guardedBf) {
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

  void keepAlive() {
    mRefGuard.mKeepAlive = true;
  }

  void MarkAsDirty() {
    mRefGuard.MarkAsDirty();
  }

  void SyncGSNBeforeWrite() {
    mRefGuard.SyncGSNBeforeWrite();
  }

  ~ExclusiveGuardedBufferFrame() {
    if (!mRefGuard.mKeepAlive &&
        mRefGuard.mGuard.mState == GUARD_STATE::EXCLUSIVE) {
      mRefGuard.reclaim();
    } else {
      mRefGuard.unlock();
    }
  }

  /// Initialize the payload data structure (i.e. BTreeNode), usually used when
  /// the buffer frame is used to serve a new page.
  template <typename... Args> void InitPayload(Args&&... args) {
    new (mRefGuard.mBf->page.mPayload) PayloadType(std::forward<Args>(args)...);
  }

  inline u8* GetPagePayloadPtr() {
    return reinterpret_cast<u8*>(mRefGuard.mBf->page.mPayload);
  }

  inline PayloadType* GetPagePayload() {
    return reinterpret_cast<PayloadType*>(mRefGuard.mBf->page.mPayload);
  }

  inline PayloadType* operator->() {
    return GetPagePayload();
  }

  inline Swip<PayloadType> swip() {
    return Swip<PayloadType>(mRefGuard.mBf);
  }

  inline BufferFrame* bf() {
    return mRefGuard.mBf;
  }

  inline void reclaim() {
    mRefGuard.reclaim();
  }
};

/// Shared lock guarded buffer frame.
/// TODO(jian.z): Seems it hasen't been used in the codebase, can we remove it?
template <typename T> class SharedGuardedBufferFrame {
public:
  GuardedBufferFrame<T>& mRefGuard;

  // I: Upgrade
  SharedGuardedBufferFrame(GuardedBufferFrame<T>&& h_guard)
      : mRefGuard(h_guard) {
    mRefGuard.ToSharedMayJump();
  }

  ~SharedGuardedBufferFrame() {
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
