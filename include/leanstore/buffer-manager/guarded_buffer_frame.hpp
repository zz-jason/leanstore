#pragma once

#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/concurrency/logging_impl.hpp"
#include "leanstore/concurrency/wal_payload_handler.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/sync/hybrid_guard.hpp"
#include "leanstore/sync/hybrid_latch.hpp"
#include "leanstore/utils/log.hpp"

#include <utility>

namespace leanstore::storage {

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

public:
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
    LS_DCHECK(!HasExclusiveMark(bf_->header_.latch_.GetVersion()));
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

public:
  /// Mark the page as dirty after modification by a user or system transaction.
  void MarkPageAsDirty() {
    LS_DCHECK(bf_ != nullptr);
    bf_->page_.psn_++;
  }

  /// Sync the system transaction id to the page. Page system transaction id is updated during the
  /// execution of a system transaction.
  void SyncSystemTxId(TXID sys_tx_id) {
    LS_DCHECK(bf_ != nullptr);

    // update last writer worker
    bf_->header_.last_writer_worker_ = cr::WorkerContext::My().worker_id_;

    // update system transaction id
    bf_->page_.sys_tx_id_ = sys_tx_id;

    // update the maximum system transaction id written by the worker
    cr::WorkerContext::My().logging_.UpdateSysTxWrittern(sys_tx_id);
  }

  /// Check remote dependency
  /// TODO: don't sync on temporary table pages like history trees
  void CheckRemoteDependency() {
    // skip if not running inside a worker
    if (!cr::WorkerContext::InWorker()) {
      return;
    }

    if (bf_->header_.last_writer_worker_ != cr::WorkerContext::My().worker_id_ &&
        bf_->page_.sys_tx_id_ > cr::ActiveTx().max_observed_sys_tx_id_) {
      cr::ActiveTx().max_observed_sys_tx_id_ = bf_->page_.sys_tx_id_;
      cr::ActiveTx().has_remote_dependency_ = true;
    }
  }

  template <typename WT, typename... Args>
  cr::WalPayloadHandler<WT> ReserveWALPayload(uint64_t wal_size, Args&&... args) {
    LS_DCHECK(cr::ActiveTx().is_durable_);
    LS_DCHECK(guard_.state_ == GuardState::kExclusivePessimistic);

    const auto page_id = bf_->header_.page_id_;
    const auto tree_id = bf_->page_.btree_id_;
    wal_size = ((wal_size - 1) / 8 + 1) * 8;
    auto handler = cr::WorkerContext::My().logging_.ReserveWALEntryComplex<WT, Args...>(
        sizeof(WT) + wal_size, page_id, bf_->page_.psn_, tree_id, std::forward<Args>(args)...);

    return handler;
  }

  template <typename WT, typename... Args>
  void WriteWal(uint64_t wal_size, Args&&... args) {
    auto handle = ReserveWALPayload<WT>(wal_size, std::forward<Args>(args)...);
    handle.SubmitWal();
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
      LS_DCHECK(false, "Unhandled LatchMode: {}",
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

  template <typename WT, typename... Args>
  cr::WalPayloadHandler<WT> ReserveWALPayload(uint64_t payload_size, Args&&... args) {
    return ref_guard_.template ReserveWALPayload<WT>(payload_size, std::forward<Args>(args)...);
  }

  template <typename WT, typename... Args>
  void WriteWal(uint64_t payload_size, Args&&... args) {
    auto wal_payload_handler =
        ref_guard_.template ReserveWALPayload<WT>(payload_size, std::forward<Args>(args)...);
    wal_payload_handler.SubmitWal();
  }

  void keep_alive() {
    ref_guard_.keep_alive_ = true;
  }

  void SyncSystemTxId(TXID sys_tx_id) {
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

} // namespace leanstore::storage
