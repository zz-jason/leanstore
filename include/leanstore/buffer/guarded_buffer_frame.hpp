#pragma once

#include "leanstore/base/likely.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_guard.hpp"
#include "leanstore/sync/hybrid_mutex.hpp"
#include "leanstore/tx/tx_manager.hpp"
#include "leanstore/wal/logging.hpp"

#include <utility>

namespace leanstore {

template <typename T>
class ExclusiveGuardedBufferFrame;
template <typename T>
class SharedGuardedBufferFrame;

/// A lock guarded buffer frame
template <typename T>
class GuardedBufferFrame {
public:
  /// The buffer manager who manages the guarded buffer frame. Used to reclaim the buffer frame,
  /// buffer manager must be set when keep alive is false.
  BufferManager* buffer_manager_ = nullptr;

  /// The guarded buffer frame. Latch mode is determined by guard_.
  BufferFrame* bf_ = nullptr;

  /// The latch guard of this buffer frame
  HybridGuard guard_;

  /// Whether to keep the buffer frame alive after the guard is released. If false, the buffer frame
  /// will be reclaimed by the buffer manager.
  bool keep_alive_ = true;

  /// Construct an empty GuardedBufferFrame, nothing to guard.
  GuardedBufferFrame()
      : buffer_manager_(nullptr),
        bf_(nullptr),
        guard_(nullptr),
        keep_alive_(true) {
    JUMPMU_REGISTER_STACK_OBJECT(this);
  }

  /// Construct a GuardedBufferFrame from an existing latch guard.
  /// @param hybridGuard the latch guard of the buffer frame
  /// @param bf the latch guarded buffer frame
  GuardedBufferFrame(BufferManager* buffer_manager, HybridGuard&& hybrid_guard, BufferFrame* bf)
      : buffer_manager_(buffer_manager),
        bf_(bf),
        guard_(std::move(hybrid_guard)),
        keep_alive_(true) {
    JUMPMU_REGISTER_STACK_OBJECT(this);
  }

  GuardedBufferFrame(GuardedBufferFrame& other) = delete; // Copy constructor

  GuardedBufferFrame(BufferManager* buffer_manager, BufferFrame* bf, bool keep_alive = true)
      : buffer_manager_(buffer_manager),
        bf_(bf),
        guard_(bf_->header_.latch_, GuardState::kUninitialized),
        keep_alive_(keep_alive) {
    LEAN_DCHECK(!HasExclusiveMark(bf_->header_.latch_.GetVersion()));
    JUMPMU_REGISTER_STACK_OBJECT(this);
  }

  /// Guard a single page, usually used for latching the meta node of a BTree.
  GuardedBufferFrame(BufferManager* buffer_manager, Swip& hot_swip,
                     const LatchMode latch_mode = LatchMode::kOptimisticSpin)
      : buffer_manager_(buffer_manager),
        bf_(&hot_swip.AsBufferFrame()),
        guard_(&bf_->header_.latch_),
        keep_alive_(true) {
    latch_may_jump(guard_, latch_mode);
    CheckRemoteDependency();
    JUMPMU_REGISTER_STACK_OBJECT(this);
  }

  /// Guard the childSwip when holding the guarded parent, usually used for lock
  /// coupling which locks the parent firstly then lock the child.
  /// @param guardedParent The guarded parent node, which protects everything in
  /// the parent node, including childSwip.
  /// @param childSwip The swip to the child node. The child page is loaded to
  /// memory if it is evicted.
  template <typename T2>
  GuardedBufferFrame(BufferManager* buffer_manager, GuardedBufferFrame<T2>& guarded_parent,
                     Swip& child_swip, const LatchMode latch_mode = LatchMode::kOptimisticSpin)
      : buffer_manager_(buffer_manager),
        bf_(buffer_manager->ResolveSwipMayJump(guarded_parent.guard_, child_swip)),
        guard_(&bf_->header_.latch_),
        keep_alive_(true) {
    latch_may_jump(guard_, latch_mode);
    CheckRemoteDependency();
    JUMPMU_REGISTER_STACK_OBJECT(this);

    guarded_parent.JumpIfModifiedByOthers();
  }

  /// Downgrade from an exclusive guard
  GuardedBufferFrame(ExclusiveGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(ExclusiveGuardedBufferFrame<T>&&) {
    guard_.Unlock();
    return *this;
  }

  /// Downgrade from a shared guard
  GuardedBufferFrame(SharedGuardedBufferFrame<T>&&) = delete;
  GuardedBufferFrame& operator=(SharedGuardedBufferFrame<T>&&) {
    guard_.Unlock();
    return *this;
  }

  JUMPMU_DEFINE_DESTRUCTOR(GuardedBufferFrame)

  ~GuardedBufferFrame() {
    if (!keep_alive_ && guard_.state_ != GuardState::kExclusivePessimistic) {
      Reclaim();
    } else {
      unlock();
    }
    JUMPMU_UNREGISTER_STACK_OBJECT(this)
  }

  /// Assignment operator
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame& other) = delete;

  /// Move assignment
  template <typename T2>
  constexpr GuardedBufferFrame& operator=(GuardedBufferFrame<T2>&& other) {
    guard_.Unlock();

    buffer_manager_ = other.buffer_manager_;
    bf_ = other.bf_;
    guard_ = std::move(other.guard_);
    keep_alive_ = other.keep_alive_;
    return *this;
  }

  /// Update page version to a new GSN. Should be called when the page is
  /// modified, which makes the page dirty.
  void UpdatePageVersion() {
    LEAN_DCHECK(bf_ != nullptr);
    bf_->page_.page_version_ = CoroEnv::CurStore().AllocWalGsn();
  }

  /// Sync the system transaction id to the page. Page system transaction id is updated during the
  /// execution of a system transaction.
  void SyncSystemTxId(lean_txid_t sys_tx_id) {
    if (LEAN_UNLIKELY(bf_ == nullptr)) {
      LEAN_DCHECK(bf_ != nullptr);
      return;
    }

    // update system transaction id
    bf_->page_.sys_tx_id_ = sys_tx_id;

    // update the maximum system transaction id written by the worker
    CoroEnv::CurLogging().UpdateBufferedSysTx(sys_tx_id);
    if (CoroEnv::HasTxMgr()) {
      bf_->header_.last_writer_worker_ = CoroEnv::CurTxMgr().worker_id_;
    } else {
      bf_->header_.last_writer_worker_ = BufferFrameHeader::kInvalidWorkerId;
    }
  }

  /// Check remote dependency
  /// TODO: don't sync on temporary table pages like history trees
  void CheckRemoteDependency() {
    // skip if not running inside a worker
    if (!CoroEnv::HasTxMgr()) {
      return;
    }

    if (bf_->header_.last_writer_worker_ != CoroEnv::CurTxMgr().worker_id_ &&
        bf_->page_.sys_tx_id_ > CoroEnv::CurTxMgr().ActiveTx().max_observed_sys_tx_id_) {
      CoroEnv::CurTxMgr().ActiveTx().max_observed_sys_tx_id_ = bf_->page_.sys_tx_id_;
      CoroEnv::CurTxMgr().ActiveTx().has_remote_dependency_ = true;
    }
  }

  bool EncounteredContention() {
    return guard_.contented_;
  }

  // NOLINTBEGIN

  void unlock() {
    guard_.Unlock();
  }

  void JumpIfModifiedByOthers() {
    guard_.JumpIfModifiedByOthers();
  }

  T& ref() {
    return *reinterpret_cast<T*>(bf_->page_.payload_);
  }

  T* ptr() {
    return reinterpret_cast<T*>(bf_->page_.payload_);
  }

  Swip swip() {
    return Swip(bf_);
  }

  T* operator->() {
    return reinterpret_cast<T*>(bf_->page_.payload_);
  }

  // Use with caution!
  void ToSharedMayJump() {
    guard_.ToSharedMayJump();
  }

  void ToOptimisticOrShared() {
    guard_.ToOptimisticOrShared();
  }

  void ToExclusiveMayJump() {
    guard_.ToExclusiveMayJump();
  }

  void TryToSharedMayJump() {
    guard_.TryToSharedMayJump();
  }

  void TryToExclusiveMayJump() {
    guard_.TryToExclusiveMayJump();
  }

  void Reclaim() {
    buffer_manager_->ReclaimPage(*(bf_));
    guard_.state_ = GuardState::kMoved;
  }

protected:
  void latch_may_jump(HybridGuard& guard, const LatchMode latch_mode) {
    switch (latch_mode) {
    case LatchMode::kOptimisticSpin: {
      guard.ToOptimisticSpin();
      break;
    }
    case LatchMode::kExclusivePessimistic: {
      guard.ToOptimisticOrExclusive();
      break;
    }
    case LatchMode::kSharedPessimistic: {
      guard.ToOptimisticOrShared();
      break;
    }
    case LatchMode::kOptimisticOrJump: {
      guard.ToOptimisticOrJump();
      break;
    }
    default: {
      LEAN_DCHECK(false, "Unhandled LatchMode: {}",
                  std::to_string(static_cast<uint64_t>(latch_mode)));
    }
    }
  }
};

template <typename PayloadType>
class ExclusiveGuardedBufferFrame {
private:
  GuardedBufferFrame<PayloadType>& ref_guard_;

public:
  // I: Upgrade
  ExclusiveGuardedBufferFrame(GuardedBufferFrame<PayloadType>&& guarded_bf)
      : ref_guard_(guarded_bf) {
    ref_guard_.guard_.ToExclusiveMayJump();
  }

  void SyncSystemTxId(lean_txid_t sys_tx_id) {
    ref_guard_.SyncSystemTxId(sys_tx_id);
  }

  ~ExclusiveGuardedBufferFrame() {
    if (!ref_guard_.keep_alive_ && ref_guard_.guard_.state_ == GuardState::kExclusivePessimistic) {
      ref_guard_.Reclaim();
    } else {
      ref_guard_.unlock();
    }
  }

  /// Initialize the payload data structure (i.e. BTreeNode), usually used when
  /// the buffer frame is used to serve a new page.
  template <typename... Args>
  void InitPayload(Args&&... args) {
    new (ref_guard_.bf_->page_.payload_) PayloadType(std::forward<Args>(args)...);
  }

  uint8_t* GetPagePayloadPtr() {
    return reinterpret_cast<uint8_t*>(ref_guard_.bf_->page_.payload_);
  }

  PayloadType* GetPagePayload() {
    return reinterpret_cast<PayloadType*>(ref_guard_.bf_->page_.payload_);
  }

  void UpdatePageVersion() {
    ref_guard_.UpdatePageVersion();
  }

  PayloadType* operator->() {
    return GetPagePayload();
  }

  Swip swip() {
    return Swip(ref_guard_.bf_);
  }

  BufferFrame* bf() {
    return ref_guard_.bf_;
  }

  void Reclaim() {
    ref_guard_.Reclaim();
  }
};

/// Shared lock guarded buffer frame.
/// TODO(jian.z): Seems it hasn't been used in the codebase, can we remove it?
template <typename T>
class SharedGuardedBufferFrame {
public:
  GuardedBufferFrame<T>& ref_guard_;

  SharedGuardedBufferFrame(GuardedBufferFrame<T>&& guarded_bf) : ref_guard_(guarded_bf) {
    ref_guard_.ToSharedMayJump();
  }

  ~SharedGuardedBufferFrame() {
    ref_guard_.unlock();
  }

  T& ref() {
    return *reinterpret_cast<T*>(ref_guard_.bf_->page_.payload_);
  }

  T* ptr() {
    return reinterpret_cast<T*>(ref_guard_.bf_->page_.payload_);
  }

  Swip swip() {
    return Swip(ref_guard_.bf_);
  }

  // NOLINTEND

  T* operator->() {
    return reinterpret_cast<T*>(ref_guard_.bf_->page_.payload_);
  }
};

} // namespace leanstore
