#pragma once

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency-recovery/WALEntry.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "sync/HybridGuard.hpp"
#include "sync/HybridLatch.hpp"

#include <glog/logging.h>

#include <utility>

namespace leanstore {
namespace storage {

template <typename T> class ExclusiveGuardedBufferFrame;
template <typename T> class SharedGuardedBufferFrame;

/// A lock guarded buffer frame
template <typename T> class GuardedBufferFrame {
public:
  /// The buffer manager who manages the guarded buffer frame. Used to reclaim
  /// the buffer frame, buffer manager must be set when keep alive is false.
  BufferManager* mBufferManager = nullptr;

  /// The guarded buffer frame. Latch mode is determined by mGuard.
  BufferFrame* mBf = nullptr;

  /// The latch guard of this buffer frame
  HybridGuard mGuard;

  bool mKeepAlive = true;

public:
  /// Construct an empty GuardedBufferFrame, nothing to guard.
  GuardedBufferFrame()
      : mBufferManager(nullptr),
        mBf(nullptr),
        mGuard(nullptr),
        mKeepAlive(true) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Construct a GuardedBufferFrame from an existing latch guard.
  /// @param hybridGuard the latch guard of the buffer frame
  /// @param bf the latch guarded buffer frame
  GuardedBufferFrame(BufferManager* bufferManager, HybridGuard&& hybridGuard,
                     BufferFrame* bf)
      : mBufferManager(bufferManager),
        mBf(bf),
        mGuard(std::move(hybridGuard)),
        mKeepAlive(true) {
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  GuardedBufferFrame(GuardedBufferFrame& other) = delete;  // Copy constructor
  GuardedBufferFrame(GuardedBufferFrame&& other) = delete; // Move constructor

  GuardedBufferFrame(BufferManager* bufferManager, BufferFrame* bf,
                     bool keepAlive = true)
      : mBufferManager(bufferManager),
        mBf(bf),
        mGuard(mBf->header.mLatch, GuardState::kUninitialized),
        mKeepAlive(keepAlive) {
    DCHECK(!HasExclusiveMark(mBf->header.mLatch.GetOptimisticVersion()));
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  // GuardedBufferFrame(BufferManager* bufferManager, TREEID treeId,
  //                    bool keepAlive = true)
  //     : mBufferManager(bufferManager),
  //       mBf(&bufferManager->AllocNewPage(treeId)),
  //       mGuard(&mBf->header.mLatch),
  //       mKeepAlive(keepAlive) {
  //   latchMayJump(mGuard, LatchMode::kOptimisticSpin);
  //   SyncGSNBeforeRead();
  //   DCHECK(HasExclusiveMark(mBf->header.mLatch.GetOptimisticVersion()));
  //   JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  // }

  /// Guard a single page, usually used for latching the meta node of a BTree.
  GuardedBufferFrame(BufferManager* bufferManager, Swip& hotSwip,
                     const LatchMode latchMode = LatchMode::kOptimisticSpin)
      : mBufferManager(bufferManager),
        mBf(&hotSwip.AsBufferFrame()),
        mGuard(&mBf->header.mLatch),
        mKeepAlive(true) {
    latchMayJump(mGuard, latchMode);
    SyncGSNBeforeRead();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();
  }

  /// Guard the childSwip when holding the guarded parent, usually used for lock
  /// coupling which locks the parent firstly then lock the child.
  /// @param guardedParent The guarded parent node, which protects everyting in
  /// the parent node, including childSwip.
  /// @param childSwip The swip to the child node. The child page is loaded to
  /// memory if it is evicted.
  template <typename T2>
  GuardedBufferFrame(BufferManager* bufferManager,
                     GuardedBufferFrame<T2>& guardedParent, Swip& childSwip,
                     const LatchMode latchMode = LatchMode::kOptimisticSpin)
      : mBufferManager(bufferManager),
        mBf(bufferManager->ResolveSwipMayJump(guardedParent.mGuard, childSwip)),
        mGuard(&mBf->header.mLatch),
        mKeepAlive(true) {
    latchMayJump(mGuard, latchMode);
    SyncGSNBeforeRead();
    JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP();

    guardedParent.JumpIfModifiedByOthers();
  }

  /// Downgrade from an exclusive guard
  GuardedBufferFrame(ExclusiveGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(ExclusiveGuardedBufferFrame<T>&&) {
    mGuard.Unlock();
    return *this;
  }

  /// Downgrade from a shared guard
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

  /// Assignment operator
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame& other) = delete;

  /// Move assignment
  template <typename T2>
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame<T2>&& other) {
    mBufferManager = other.mBufferManager;
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
    DCHECK(mBf->page.mGSN <= cr::Worker::My().mLogging.GetCurrentGsn())
        << "Page GSN should <= worker GSN"
        << ", pageGSN=" << mBf->page.mGSN
        << ", workerGSN=" << cr::Worker::My().mLogging.GetCurrentGsn();

    // update last writer worker
    mBf->header.mLastWriterWorker = cr::Worker::My().mWorkerId;

    // increase GSN
    const auto workerGSN = cr::Worker::My().mLogging.GetCurrentGsn();
    mBf->page.mGSN = workerGSN + 1;
    cr::Worker::My().mLogging.SetCurrentGsn(workerGSN + 1);
  }

  // TODO: don't sync on temporary table pages like HistoryTree
  inline void SyncGSNBeforeRead() {
    if (!cr::Worker::My().mLogging.mHasRemoteDependency &&
        mBf->page.mGSN > cr::Worker::My().mLogging.mTxReadSnapshot &&
        mBf->header.mLastWriterWorker != cr::Worker::My().mWorkerId) {
      cr::Worker::My().mLogging.mHasRemoteDependency = true;
      DLOG(INFO) << "Detected remote dependency"
                 << ", workerId=" << cr::Worker::My().mWorkerId
                 << ", txReadSnapshot(GSN)="
                 << cr::Worker::My().mLogging.mTxReadSnapshot
                 << ", pageLastWriterWorker=" << mBf->header.mLastWriterWorker
                 << ", pageGSN=" << mBf->page.mGSN;
    }

    const auto workerGSN = cr::Worker::My().mLogging.GetCurrentGsn();
    const auto pageGSN = mBf->page.mGSN;
    if (workerGSN < pageGSN) {
      cr::Worker::My().mLogging.SetCurrentGsn(pageGSN);
    }
  }

  template <typename WT, typename... Args>
  inline cr::WALPayloadHandler<WT> ReserveWALPayload(uint64_t walSize,
                                                     Args&&... args) {
    DCHECK(cr::ActiveTx().mIsDurable);
    DCHECK(mGuard.mState == GuardState::kPessimisticExclusive);

    const auto pageId = mBf->header.mPageId;
    const auto treeId = mBf->page.mBTreeId;
    walSize = ((walSize - 1) / 8 + 1) * 8;
    auto handler =
        cr::Worker::My().mLogging.ReserveWALEntryComplex<WT, Args...>(
            sizeof(WT) + walSize, pageId, mBf->page.mPSN, treeId,
            std::forward<Args>(args)...);

    SyncGSNBeforeWrite();
    MarkAsDirty();
    return handler;
  }

  template <typename WT, typename... Args>
  inline void WriteWal(uint64_t walSize, Args&&... args) {
    auto handle = ReserveWALPayload<WT>(walSize, std::forward<Args>(args)...);
    handle.SubmitWal();
  }

  inline bool EncounteredContention() {
    return mGuard.mEncounteredContention;
  }

  inline void unlock() {
    mGuard.Unlock();
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

  inline Swip swip() {
    return Swip(mBf);
  }

  inline T* operator->() {
    return reinterpret_cast<T*>(mBf->page.mPayload);
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
      DCHECK(false) << "Unhandled LatchMode: "
                    << std::to_string(static_cast<uint64_t>(latchMode));
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
  cr::WALPayloadHandler<WT> ReserveWALPayload(uint64_t payloadSize,
                                              Args&&... args) {
    return mRefGuard.template ReserveWALPayload<WT>(
        payloadSize, std::forward<Args>(args)...);
  }

  template <typename WT, typename... Args>
  void WriteWal(uint64_t payloadSize, Args&&... args) {
    auto walPayloadHandler = mRefGuard.template ReserveWALPayload<WT>(
        payloadSize, std::forward<Args>(args)...);
    walPayloadHandler.SubmitWal();
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
        mRefGuard.mGuard.mState == GuardState::kPessimisticExclusive) {
      mRefGuard.Reclaim();
    } else {
      mRefGuard.unlock();
    }
  }

  /// Initialize the payload data structure (i.e. BTreeNode), usually used when
  /// the buffer frame is used to serve a new page.
  template <typename... Args> void InitPayload(Args&&... args) {
    new (mRefGuard.mBf->page.mPayload) PayloadType(std::forward<Args>(args)...);
  }

  inline uint8_t* GetPagePayloadPtr() {
    return reinterpret_cast<uint8_t*>(mRefGuard.mBf->page.mPayload);
  }

  inline PayloadType* GetPagePayload() {
    return reinterpret_cast<PayloadType*>(mRefGuard.mBf->page.mPayload);
  }

  inline PayloadType* operator->() {
    return GetPagePayload();
  }

  inline Swip swip() {
    return Swip(mRefGuard.mBf);
  }

  inline BufferFrame* bf() {
    return mRefGuard.mBf;
  }

  inline void Reclaim() {
    mRefGuard.Reclaim();
  }
};

/// Shared lock guarded buffer frame.
/// TODO(jian.z): Seems it hasen't been used in the codebase, can we remove it?
template <typename T> class SharedGuardedBufferFrame {
public:
  GuardedBufferFrame<T>& mRefGuard;

  SharedGuardedBufferFrame(GuardedBufferFrame<T>&& guardedBf)
      : mRefGuard(guardedBf) {
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

  inline Swip swip() {
    return Swip(mRefGuard.mBf);
  }

  inline T* operator->() {
    return reinterpret_cast<T*>(mRefGuard.mBf->page.mPayload);
  }
};

} // namespace storage
} // namespace leanstore
