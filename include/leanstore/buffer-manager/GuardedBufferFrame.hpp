#pragma once

#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/LoggingImpl.hpp"
#include "leanstore/concurrency/WalPayloadHandler.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/sync/HybridGuard.hpp"
#include "leanstore/sync/HybridLatch.hpp"
#include "leanstore/utils/Log.hpp"

#include <utility>

namespace leanstore::storage {

template <typename T>
class ExclusiveGuardedBufferFrame;
template <typename T>
class SharedGuardedBufferFrame;

//! A lock guarded buffer frame
template <typename T>
class GuardedBufferFrame {
public:
  //! The buffer manager who manages the guarded buffer frame. Used to reclaim the buffer frame,
  //! buffer manager must be set when keep alive is false.
  BufferManager* mBufferManager = nullptr;

  //! The guarded buffer frame. Latch mode is determined by mGuard.
  BufferFrame* mBf = nullptr;

  //! The latch guard of this buffer frame
  HybridGuard mGuard;

  //! Whether to keep the buffer frame alive after the guard is released. If false, the buffer frame
  //! will be reclaimed by the buffer manager.
  bool mKeepAlive = true;

public:
  //! Construct an empty GuardedBufferFrame, nothing to guard.
  GuardedBufferFrame() : mBufferManager(nullptr), mBf(nullptr), mGuard(nullptr), mKeepAlive(true) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  //! Construct a GuardedBufferFrame from an existing latch guard.
  //! @param hybridGuard the latch guard of the buffer frame
  //! @param bf the latch guarded buffer frame
  GuardedBufferFrame(BufferManager* bufferManager, HybridGuard&& hybridGuard, BufferFrame* bf)
      : mBufferManager(bufferManager),
        mBf(bf),
        mGuard(std::move(hybridGuard)),
        mKeepAlive(true) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  GuardedBufferFrame(GuardedBufferFrame& other) = delete; // Copy constructor

  GuardedBufferFrame(GuardedBufferFrame&& other) {
    // call the move assignment
    *this = std::move(other);
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  GuardedBufferFrame(BufferManager* bufferManager, BufferFrame* bf, bool keepAlive = true)
      : mBufferManager(bufferManager),
        mBf(bf),
        mGuard(mBf->mHeader.mLatch, GuardState::kUninitialized),
        mKeepAlive(keepAlive) {
    LS_DCHECK(!HasExclusiveMark(mBf->mHeader.mLatch.GetOptimisticVersion()));
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  //! Guard a single page, usually used for latching the meta node of a BTree.
  GuardedBufferFrame(BufferManager* bufferManager, Swip& hotSwip,
                     const LatchMode latchMode = LatchMode::kOptimisticSpin)
      : mBufferManager(bufferManager),
        mBf(&hotSwip.AsBufferFrame()),
        mGuard(&mBf->mHeader.mLatch),
        mKeepAlive(true) {
    latchMayJump(mGuard, latchMode);
    CheckRemoteDependency();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  //! Guard the childSwip when holding the guarded parent, usually used for lock
  //! coupling which locks the parent firstly then lock the child.
  //! @param guardedParent The guarded parent node, which protects everything in
  //! the parent node, including childSwip.
  //! @param childSwip The swip to the child node. The child page is loaded to
  //! memory if it is evicted.
  template <typename T2>
  GuardedBufferFrame(BufferManager* bufferManager, GuardedBufferFrame<T2>& guardedParent,
                     Swip& childSwip, const LatchMode latchMode = LatchMode::kOptimisticSpin)
      : mBufferManager(bufferManager),
        mBf(bufferManager->ResolveSwipMayJump(guardedParent.mGuard, childSwip)),
        mGuard(&mBf->mHeader.mLatch),
        mKeepAlive(true) {
    latchMayJump(mGuard, latchMode);
    CheckRemoteDependency();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();

    guardedParent.JumpIfModifiedByOthers();
  }

  //! Downgrade from an exclusive guard
  GuardedBufferFrame(ExclusiveGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(ExclusiveGuardedBufferFrame<T>&&) {
    mGuard.Unlock();
    return *this;
  }

  //! Downgrade from a shared guard
  GuardedBufferFrame(SharedGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(SharedGuardedBufferFrame<T>&&) {
    mGuard.Unlock();
    return *this;
  }

  JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(GuardedBufferFrame)

  ~GuardedBufferFrame() {
    if (mGuard.mState == GuardState::kPessimisticExclusive) {
      if (!mKeepAlive) {
        Reclaim();
      }
    }
    mGuard.Unlock();
    JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
  }

  //! Assignment operator
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame& other) = delete;

  //! Move assignment
  template <typename T2>
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame<T2>&& other) {
    mBufferManager = other.mBufferManager;
    mBf = other.mBf;
    mGuard = std::move(other.mGuard);
    mKeepAlive = other.mKeepAlive;
    return *this;
  }

public:
  //! Mark the page as dirty after modification by a user or system transaction.
  void MarkPageAsDirty() {
    LS_DCHECK(mBf != nullptr);
    mBf->mPage.mPsn++;
  }

  //! Sync the system transaction id to the page. Page system transaction id is updated during the
  //! execution of a system transaction.
  void SyncSystemTxId(TXID sysTxId) {
    LS_DCHECK(mBf != nullptr);

    // update last writer worker
    mBf->mHeader.mLastWriterWorker = cr::WorkerContext::My().mWorkerId;

    // update system transaction id
    mBf->mPage.mSysTxId = sysTxId;

    // update the maximum system transaction id written by the worker
    cr::WorkerContext::My().mLogging.UpdateSysTxWrittern(sysTxId);
  }

  //! Check remote dependency
  //! TODO: don't sync on temporary table pages like history trees
  void CheckRemoteDependency() {
    // skip if not running inside a worker
    if (!cr::WorkerContext::InWorker()) {
      return;
    }

    if (mBf->mHeader.mLastWriterWorker != cr::WorkerContext::My().mWorkerId &&
        mBf->mPage.mSysTxId > cr::ActiveTx().mMaxObservedSysTxId) {
      cr::ActiveTx().mMaxObservedSysTxId = mBf->mPage.mSysTxId;
      cr::ActiveTx().mHasRemoteDependency = true;
    }
  }

  template <typename WT, typename... Args>
  cr::WalPayloadHandler<WT> ReserveWALPayload(uint64_t walSize, Args&&... args) {
    LS_DCHECK(cr::ActiveTx().mIsDurable);
    LS_DCHECK(mGuard.mState == GuardState::kPessimisticExclusive);

    const auto pageId = mBf->mHeader.mPageId;
    const auto treeId = mBf->mPage.mBTreeId;
    walSize = ((walSize - 1) / 8 + 1) * 8;
    auto handler = cr::WorkerContext::My().mLogging.ReserveWALEntryComplex<WT, Args...>(
        sizeof(WT) + walSize, pageId, mBf->mPage.mPsn, treeId, std::forward<Args>(args)...);

    return handler;
  }

  template <typename WT, typename... Args>
  void WriteWal(uint64_t walSize, Args&&... args) {
    auto handle = ReserveWALPayload<WT>(walSize, std::forward<Args>(args)...);
    handle.SubmitWal();
  }

  bool EncounteredContention() {
    return mGuard.mEncounteredContention;
  }

  // NOLINTBEGIN

  void unlock() {
    mGuard.Unlock();
  }

  void JumpIfModifiedByOthers() {
    mGuard.JumpIfModifiedByOthers();
  }

  T& ref() {
    return *reinterpret_cast<T*>(mBf->mPage.mPayload);
  }

  T* ptr() {
    return reinterpret_cast<T*>(mBf->mPage.mPayload);
  }

  Swip swip() {
    return Swip(mBf);
  }

  T* operator->() {
    return reinterpret_cast<T*>(mBf->mPage.mPayload);
  }

  // Use with caution!
  void ToSharedMayJump() {
    mGuard.ToSharedMayJump();
  }

  void ToOptimisticOrShared() {
    mGuard.ToOptimisticOrShared();
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

  void Reclaim() {
    mBufferManager->ReclaimPage(*(mBf));
    mGuard.mState = GuardState::kMoved;
  }

protected:
  void latchMayJump(HybridGuard& guard, const LatchMode latchMode) {
    switch (latchMode) {
    case LatchMode::kOptimisticSpin: {
      guard.ToOptimisticSpin();
      break;
    }
    case LatchMode::kPessimisticExclusive: {
      guard.ToOptimisticOrExclusive();
      break;
    }
    case LatchMode::kPessimisticShared: {
      guard.ToOptimisticOrShared();
      break;
    }
    case LatchMode::kOptimisticOrJump: {
      guard.ToOptimisticOrJump();
      break;
    }
    default: {
      LS_DCHECK(false, "Unhandled LatchMode: {}", std::to_string(static_cast<uint64_t>(latchMode)));
    }
    }
  }
};

template <typename PayloadType>
class ExclusiveGuardedBufferFrame {
private:
  GuardedBufferFrame<PayloadType>& mRefGuard;

public:
  // I: Upgrade
  ExclusiveGuardedBufferFrame(GuardedBufferFrame<PayloadType>&& guardedBf) : mRefGuard(guardedBf) {
    mRefGuard.mGuard.ToExclusiveMayJump();
  }

  template <typename WT, typename... Args>
  cr::WalPayloadHandler<WT> ReserveWALPayload(uint64_t payloadSize, Args&&... args) {
    return mRefGuard.template ReserveWALPayload<WT>(payloadSize, std::forward<Args>(args)...);
  }

  template <typename WT, typename... Args>
  void WriteWal(uint64_t payloadSize, Args&&... args) {
    auto walPayloadHandler =
        mRefGuard.template ReserveWALPayload<WT>(payloadSize, std::forward<Args>(args)...);
    walPayloadHandler.SubmitWal();
  }

  void keepAlive() {
    mRefGuard.mKeepAlive = true;
  }

  void SyncSystemTxId(TXID sysTxId) {
    mRefGuard.SyncSystemTxId(sysTxId);
  }

  ~ExclusiveGuardedBufferFrame() {
    if (!mRefGuard.mKeepAlive && mRefGuard.mGuard.mState == GuardState::kPessimisticExclusive) {
      mRefGuard.Reclaim();
    } else {
      mRefGuard.unlock();
    }
  }

  //! Initialize the payload data structure (i.e. BTreeNode), usually used when
  //! the buffer frame is used to serve a new page.
  template <typename... Args>
  void InitPayload(Args&&... args) {
    new (mRefGuard.mBf->mPage.mPayload) PayloadType(std::forward<Args>(args)...);
  }

  uint8_t* GetPagePayloadPtr() {
    return reinterpret_cast<uint8_t*>(mRefGuard.mBf->mPage.mPayload);
  }

  PayloadType* GetPagePayload() {
    return reinterpret_cast<PayloadType*>(mRefGuard.mBf->mPage.mPayload);
  }

  PayloadType* operator->() {
    return GetPagePayload();
  }

  Swip swip() {
    return Swip(mRefGuard.mBf);
  }

  BufferFrame* bf() {
    return mRefGuard.mBf;
  }

  void Reclaim() {
    mRefGuard.Reclaim();
  }
};

//! Shared lock guarded buffer frame.
//! TODO(jian.z): Seems it hasn't been used in the codebase, can we remove it?
template <typename T>
class SharedGuardedBufferFrame {
public:
  GuardedBufferFrame<T>& mRefGuard;

  SharedGuardedBufferFrame(GuardedBufferFrame<T>&& guardedBf) : mRefGuard(guardedBf) {
    mRefGuard.ToSharedMayJump();
  }

  ~SharedGuardedBufferFrame() {
    mRefGuard.unlock();
  }

  T& ref() {
    return *reinterpret_cast<T*>(mRefGuard.mBf->mPage.mPayload);
  }

  T* ptr() {
    return reinterpret_cast<T*>(mRefGuard.mBf->mPage.mPayload);
  }

  Swip swip() {
    return Swip(mRefGuard.mBf);
  }

  // NOLINTEND

  T* operator->() {
    return reinterpret_cast<T*>(mRefGuard.mBf->mPage.mPayload);
  }
};

} // namespace leanstore::storage
