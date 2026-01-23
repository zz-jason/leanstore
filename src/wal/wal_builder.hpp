#pragma once

#include "leanstore/base/slice.hpp"
#include "leanstore/btree/b_tree_node.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/c/types.h"
#include "leanstore/c/wal_record.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/tx/tx_manager.hpp"
#include "leanstore/wal/logging.hpp"
#include "leanstore/wal/wal_serde.hpp"
#include "leanstore/wal/wal_traits.hpp"

#include <cstdint>

namespace leanstore {

//------------------------------------------------------------------------------
// All forward declarations
//------------------------------------------------------------------------------

template <typename T>
class WalSmoBuilder; // for SMO-related WAL records
template <typename T>
class WalBuilder; // for atomic btree operations
template <typename T>
class WalTxBuilder; // for mvcc btree operations

template <typename T>
class WalSmoBuilder {
public:
  WalSmoBuilder(lean_treeid_t btree_id, lean_txid_t sys_txid);

  WalSmoBuilder& SetPageInfo(BufferFrame* bf);

  WalSmoBuilder& BuildPageNew(bool is_leaf);

  WalSmoBuilder& BuildSplitRoot(lean_pid_t parent, lean_pid_t new_lhs, lean_pid_t new_rhs,
                                const BTreeNodeHeader::SeparatorInfo& sep_info);

  WalSmoBuilder& BuildSplitNonRoot(lean_pid_t parent, lean_pid_t new_lhs,
                                   const BTreeNodeHeader::SeparatorInfo& sep_info);

  T* GetWal() {
    return wal_;
  }

  WalSmoBuilder& Submit() {
    wal_->base_.crc32_ = WalSerde::Crc32Masked(*reinterpret_cast<lean_wal_record*>(wal_));
    CoroEnv::CurLogging().AdvanceWalBuffer(wal_->base_.size_);
    CoroEnv::CurLogging().PublishWalFlushReq(0);
    return *this;
  }

private:
  T* wal_;
  lean_wal_size_t wal_size_;
};

/// A helper class to build different types of WAL records.
/// - Memory to hold the WAL record is allocated from the current coroutine's logging buffer.
/// - The WAL record is zeroed out for safety.
/// - The CRC32 field is set to 0 and should be computed later after all fields are set.
template <typename T>
class WalBuilder {
public:
  WalBuilder(lean_treeid_t btree_id, lean_wal_size_t payload_size);

  WalBuilder& SetPageInfo(BufferFrame* bf);

  WalBuilder& BuildInsert(Slice key, Slice value);

  WalBuilder& BuildRemove(Slice key, Slice value);

  WalBuilder& BuildUpdate(Slice key, const UpdateDesc& update_desc);

  T* GetWal() {
    return wal_;
  }

  WalBuilder& Submit() {
    wal_->base_.crc32_ = WalSerde::Crc32Masked(*reinterpret_cast<lean_wal_record*>(wal_));
    CoroEnv::CurLogging().AdvanceWalBuffer(wal_->base_.size_);
    CoroEnv::CurLogging().PublishWalFlushReq(0);
    return *this;
  }

private:
  T* wal_;
  lean_wal_size_t wal_size_;
};

/// A helper class to build different types of transaction WAL records.
/// - Memory to hold the WAL record is allocated from the current coroutine's logging buffer.
/// - The WAL record is zeroed out for safety.
/// - The CRC32 field is set to 0 and should be computed later after all fields are set.
template <typename T>
class WalTxBuilder {
public:
  WalTxBuilder(lean_treeid_t btree_id, lean_wal_size_t payload_size);

  WalTxBuilder& SetPageInfo(BufferFrame* bf) {
    return SetPageInfo(bf->header_.page_id_, bf->page_.page_version_);
  }

  WalTxBuilder& SetPageInfo(lean_pid_t page_id, lean_lid_t page_version);

  WalTxBuilder& SetPrevVersion(lean_wid_t prev_wid, lean_txid_t prev_txid,
                               lean_cmdid_t prev_cmd_id);

  WalTxBuilder& BuildTxAbort() {
    static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_ABORT,
                  "WalTxBuilder: Invalid type for TxAbort");
    return *this;
  }

  WalTxBuilder& BuildTxComplete() {
    static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_COMPLETE,
                  "WalTxBuilder: Invalid type for TxComplete");
    return *this;
  }

  WalTxBuilder& BuildTxInsert(Slice key, Slice value);

  WalTxBuilder& BuildTxRemove(Slice key, Slice value);

  WalTxBuilder& BuildTxUpdate(Slice key, const UpdateDesc& update_desc);

  T* GetWal() {
    return wal_;
  }

  WalTxBuilder& Submit() {
    wal_->tx_base_.base_.crc32_ = WalSerde::Crc32Masked(*reinterpret_cast<lean_wal_record*>(wal_));
    CoroEnv::CurLogging().AdvanceWalBuffer(wal_->tx_base_.base_.size_);
    CoroEnv::CurLogging().PublishWalFlushReq(wal_->tx_base_.txid_);
    CoroEnv::CurTxMgr().ActiveTx().prev_wal_lsn_ = wal_->tx_base_.base_.lsn_;
    return *this;
  }

private:
  T* wal_;
  lean_wal_size_t wal_size_;
};

//------------------------------------------------------------------------------
// WalSmoBuilder
//------------------------------------------------------------------------------

template <typename T>
inline WalSmoBuilder<T>::WalSmoBuilder(lean_treeid_t btree_id, lean_txid_t sys_txid) {
  assert(IsSmoBTreeWalRecordType(WalRecordTraits<T>::kType));

  auto total_size = sizeof(T);
  auto* buf = CoroEnv::CurLogging().ReserveWalBuffer(total_size);
  memset(buf, 0, total_size); // zero out for safety

  wal_ = reinterpret_cast<T*>(buf);
  wal_size_ = static_cast<lean_wal_size_t>(total_size);
  wal_->sys_txid_ = sys_txid;

  // init record base
  auto* wal_base = reinterpret_cast<lean_wal_record*>(wal_);
  wal_base->type_ = WalRecordTraits<T>::kType;
  wal_base->lsn_ = CoroEnv::CurLogging().GetLsn();
  wal_base->size_ = wal_size_;
  wal_base->btree_id_ = btree_id;
  wal_base->crc32_ = 0; // Should be computed later after all fields are set
}

template <typename T>
inline WalSmoBuilder<T>& WalSmoBuilder<T>::SetPageInfo(BufferFrame* bf) {
  if constexpr (WalRecordTraits<T>::kType == LEAN_WAL_TYPE_SMO_PAGENEW ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT) {
    wal_->page_id_ = bf->header_.page_id_;
    wal_->page_version_ = bf->page_.page_version_;
  }
  return *this;
}

template <typename T>
inline WalSmoBuilder<T>& WalSmoBuilder<T>::BuildPageNew(bool is_leaf) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_SMO_PAGENEW,
                "WalSmoBuilder: Invalid type for PageNew");
  wal_->is_leaf_ = is_leaf;
  return *this;
}

template <typename T>
inline WalSmoBuilder<T>& WalSmoBuilder<T>::BuildSplitRoot(
    lean_pid_t parent, lean_pid_t new_lhs, lean_pid_t new_rhs,
    const BTreeNodeHeader::SeparatorInfo& sep_info) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT,
                "WalSmoBuilder: Invalid type for SplitRoot");
  wal_->parent_ = parent;
  wal_->new_lhs_ = new_lhs;
  wal_->new_rhs_ = new_rhs;
  wal_->sep_slot_ = sep_info.slot_id_;
  wal_->sep_size_ = sep_info.size_;
  wal_->sep_truncated_ = sep_info.trunc_;
  return *this;
}

template <typename T>
inline WalSmoBuilder<T>& WalSmoBuilder<T>::BuildSplitNonRoot(
    lean_pid_t parent, lean_pid_t new_lhs, const BTreeNodeHeader::SeparatorInfo& sep_info) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT,
                "WalSmoBuilder: Invalid type for SplitNonRoot");
  wal_->parent_ = parent;
  wal_->new_lhs_ = new_lhs;
  wal_->sep_slot_ = sep_info.slot_id_;
  wal_->sep_size_ = sep_info.size_;
  wal_->sep_truncated_ = sep_info.trunc_;
  return *this;
}

//------------------------------------------------------------------------------
// WalBuilder
//------------------------------------------------------------------------------

template <typename T>
inline WalBuilder<T>::WalBuilder(lean_treeid_t btree_id, lean_wal_size_t payload_size) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_CARRIAGE_RETURN ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_INSERT ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_UPDATE ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_REMOVE,
                "WalBuilder: Invalid type");
  auto total_size = sizeof(T) + payload_size;
  constexpr bool kIsCarriageReturn = std::is_same_v<T, lean_wal_carriage_return>;
  auto* buf = CoroEnv::CurLogging().ReserveWalBuffer(total_size, !kIsCarriageReturn);
  memset(buf, 0, total_size); // zero out for safety

  wal_ = reinterpret_cast<T*>(buf);
  wal_size_ = static_cast<lean_wal_size_t>(total_size);

  // init record base
  auto* wal_base = reinterpret_cast<lean_wal_record*>(wal_);
  wal_base->type_ = WalRecordTraits<T>::kType;
  wal_base->lsn_ = CoroEnv::CurLogging().GetLsn();
  wal_base->size_ = wal_size_;
  wal_base->btree_id_ = btree_id;
  wal_base->crc32_ = 0; // Should be computed later after all fields are set
}

template <typename T>
inline WalBuilder<T>& WalBuilder<T>::SetPageInfo(BufferFrame* bf) {
  if constexpr (WalRecordTraits<T>::kType == LEAN_WAL_TYPE_INSERT ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_REMOVE ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_UPDATE) {
    wal_->page_id_ = bf->header_.page_id_;
    wal_->page_version_ = bf->page_.page_version_;
  }
  return *this;
}

template <typename T>
inline WalBuilder<T>& WalBuilder<T>::BuildInsert(Slice key, Slice value) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_INSERT,
                "WalBuilder: Invalid type for Insert");
  wal_->key_size_ = static_cast<lean_wal_size_t>(key.size());
  wal_->val_size_ = static_cast<lean_wal_size_t>(value.size());
  uint8_t* payload = wal_->payload_;
  if (key.size() > 0) {
    std::memcpy(payload, key.data(), key.size());
    payload += key.size();
  }
  if (value.size() > 0) {
    std::memcpy(payload, value.data(), value.size());
  }
  return *this;
}

template <typename T>
inline WalBuilder<T>& WalBuilder<T>::BuildRemove(Slice key, Slice value) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_REMOVE,
                "WalBuilder: Invalid type for Remove");
  wal_->key_size_ = static_cast<lean_wal_size_t>(key.size());
  wal_->val_size_ = static_cast<lean_wal_size_t>(value.size());
  uint8_t* payload = wal_->payload_;
  if (key.size() > 0) {
    std::memcpy(payload, key.data(), key.size());
    payload += key.size();
  }
  if (value.size() > 0) {
    std::memcpy(payload, value.data(), value.size());
  }
  return *this;
}

template <typename T>
inline WalBuilder<T>& WalBuilder<T>::BuildUpdate(Slice key, const UpdateDesc& update_desc) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_UPDATE,
                "WalBuilder: Invalid type for Update");
  wal_->key_size_ = static_cast<lean_wal_size_t>(key.size());
  wal_->update_desc_size_ = static_cast<lean_wal_size_t>(update_desc.Size());
  wal_->delta_size_ =
      static_cast<lean_wal_size_t>(update_desc.SizeWithDelta() - update_desc.Size());
  uint8_t* payload = wal_->payload_;
  if (key.size() > 0) {
    std::memcpy(payload, key.data(), key.size());
    payload += key.size();
  }
  if (update_desc.Size() > 0) {
    std::memcpy(payload, &update_desc, update_desc.Size());
    payload += update_desc.Size();
  }
  return *this;
}

//------------------------------------------------------------------------------
// WalTxBuilder
//------------------------------------------------------------------------------

template <typename T>
inline WalTxBuilder<T>::WalTxBuilder(lean_treeid_t btree_id, lean_wal_size_t payload_size) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_ABORT ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_COMPLETE ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_INSERT ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_REMOVE ||
                    WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_UPDATE,
                "WalTxBuilder: Invalid type");
  auto total_size = sizeof(T) + payload_size;
  auto* buf = CoroEnv::CurLogging().ReserveWalBuffer(total_size);
  memset(buf, 0, total_size); // zero out for safety

  wal_ = reinterpret_cast<T*>(buf);
  wal_size_ = static_cast<lean_wal_size_t>(total_size);

  // init record base
  auto* wal_base = reinterpret_cast<lean_wal_record*>(wal_);
  wal_base->type_ = WalRecordTraits<T>::kType;
  wal_base->lsn_ = CoroEnv::CurLogging().GetLsn();
  wal_base->size_ = wal_size_;
  wal_base->btree_id_ = btree_id;
  wal_base->crc32_ = 0; // Should be computed later after all fields are set

  // init tx base
  auto& tx_base = wal_->tx_base_;
  tx_base.wid_ = CoroEnv::CurTxMgr().worker_id_;
  tx_base.txid_ = CoroEnv::CurTxMgr().ActiveTx().start_ts_;
  tx_base.prev_lsn_ = CoroEnv::CurTxMgr().ActiveTx().prev_wal_lsn_;
}

template <typename T>
inline WalTxBuilder<T>& WalTxBuilder<T>::SetPageInfo(lean_pid_t page_id, lean_lid_t page_version) {
  if constexpr (WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_INSERT ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_REMOVE ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_UPDATE) {
    wal_->page_id_ = page_id;
    wal_->page_version_ = page_version;
  }
  return *this;
}

template <typename T>
inline WalTxBuilder<T>& WalTxBuilder<T>::SetPrevVersion(lean_wid_t prev_wid, lean_txid_t prev_txid,
                                                        lean_cmdid_t prev_cmd_id) {
  if constexpr (WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_INSERT ||
                WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_REMOVE) {
    wal_->prev_wid_ = prev_wid;
    wal_->prev_txid_ = prev_txid;
    wal_->prev_cmd_id_ = prev_cmd_id;
  } else if constexpr (WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_UPDATE) {
    wal_->prev_wid_ = prev_wid;
    wal_->prev_txid_ = prev_txid;
    wal_->xor_cmd_id_ = prev_cmd_id;
  }
  return *this;
}

template <typename T>
inline WalTxBuilder<T>& WalTxBuilder<T>::BuildTxInsert(Slice key, Slice value) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_INSERT,
                "WalTxBuilder: Invalid type for TxInsert");
  wal_->key_size_ = static_cast<lean_wal_size_t>(key.size());
  wal_->val_size_ = static_cast<lean_wal_size_t>(value.size());
  uint8_t* payload = wal_->payload_;
  if (key.size() > 0) {
    std::memcpy(payload, key.data(), key.size());
    payload += key.size();
  }
  if (value.size() > 0) {
    std::memcpy(payload, value.data(), value.size());
  }
  return *this;
}

template <typename T>
inline WalTxBuilder<T>& WalTxBuilder<T>::BuildTxRemove(Slice key, Slice value) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_REMOVE,
                "WalTxBuilder: Invalid type for TxRemove");
  wal_->key_size_ = static_cast<lean_wal_size_t>(key.size());
  wal_->val_size_ = static_cast<lean_wal_size_t>(value.size());
  uint8_t* payload = wal_->payload_;
  if (key.size() > 0) {
    std::memcpy(payload, key.data(), key.size());
    payload += key.size();
  }
  if (value.size() > 0) {
    std::memcpy(payload, value.data(), value.size());
  }
  return *this;
}

template <typename T>
inline WalTxBuilder<T>& WalTxBuilder<T>::BuildTxUpdate(Slice key, const UpdateDesc& update_desc) {
  static_assert(WalRecordTraits<T>::kType == LEAN_WAL_TYPE_TX_UPDATE,
                "WalTxBuilder: Invalid type for TxUpdate");
  wal_->key_size_ = static_cast<lean_wal_size_t>(key.size());
  wal_->update_desc_size_ = static_cast<lean_wal_size_t>(update_desc.Size());
  wal_->delta_size_ =
      static_cast<lean_wal_size_t>(update_desc.SizeWithDelta() - update_desc.Size());
  uint8_t* payload = wal_->payload_;
  if (key.size() > 0) {
    std::memcpy(payload, key.data(), key.size());
    payload += key.size();
  }
  if (update_desc.Size() > 0) {
    std::memcpy(payload, &update_desc, update_desc.Size());
    payload += update_desc.Size();
  }
  return *this;
}

} // namespace leanstore
