#include "leanstore/concurrency/recovery.hpp"

#include "btree/core/b_tree_wal_payload.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/guarded_buffer_frame.hpp"
#include "leanstore/concurrency/wal_entry.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/sync/hybrid_guard.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"

#include <cstdint>
#include <expected>
#include <utility>

namespace leanstore::cr {

using namespace leanstore::storage;
using namespace leanstore::utils;
using namespace leanstore::storage::btree;

bool Recovery::Run() {
  bool error(false);

  analysis();
  Log::Info("[Recovery] resolved page size: {}", resolved_pages_.size());
  for (auto it = resolved_pages_.begin(); it != resolved_pages_.end(); ++it) {
    if (it->second->IsFree()) {
      continue;
    }
    LEAN_DLOG("Resolved page after analysis"
              ", address={}, pageId={}, btreeId={}",
              (void*)it->second, it->first, it->second->page_.btree_id_);
  }

  // print resulting active transaction table
  Log::Info("[Recovery] active transaction table size: {}", active_tx_table_.size());
  for (auto it = active_tx_table_.begin(); it != active_tx_table_.end(); ++it) {
    LEAN_DLOG("Active transaction table after analysis, txId={}, offset={}", it->first, it->second);
  }

  // print dirty page table
  Log::Info("[Recovery] dirty page table size: {}", dirty_page_table_.size());
  for (auto it = dirty_page_table_.begin(); it != dirty_page_table_.end(); ++it) {
    LEAN_DLOG("Dirty page table after analysis, pageId: {}, offset: {}", it->first, it->second);
  }

  redo();

  undo();

  return error;
}

Result<void> Recovery::analysis() {
  Log::Info("[Recovery] analysis phase begins");
  SCOPED_DEFER(Log::Info("[Recovery] analysis phase ends"))

  // asume that each WalEntry is smaller than the page size
  utils::AlignedBuffer<512> aligned_buffer(store_->store_option_->page_size_);
  uint8_t* wal_entry_ptr = aligned_buffer.Get();
  for (auto offset = wal_start_offset_; offset < wal_size_;) {
    auto start_offset = offset;
    if (auto res = read_wal_entry(offset, wal_entry_ptr); !res) {
      return std::unexpected(std::move(res.error()));
    }
    auto* wal_entry = reinterpret_cast<WalEntry*>(wal_entry_ptr);
    switch (wal_entry->type_) {
    case WalEntry::Type::kTxAbort: {
      auto* wal = reinterpret_cast<WalTxAbort*>(wal_entry_ptr);
      LEAN_DCHECK(active_tx_table_.find(wal->tx_id_) != active_tx_table_.end());
      active_tx_table_[wal->tx_id_] = offset;
      continue;
    }
    case WalEntry::Type::kTxFinish: {
      auto* wal = reinterpret_cast<WalTxFinish*>(wal_entry_ptr);
      LEAN_DCHECK(active_tx_table_.find(wal->tx_id_) != active_tx_table_.end());
      active_tx_table_.erase(wal->tx_id_);
      continue;
    }
    case WalEntry::Type::kCarriageReturn: {
      // do nothing
      continue;
    }
    case WalEntry::Type::kComplex: {
      auto* wal = reinterpret_cast<WalEntryComplex*>(wal_entry_ptr);
      active_tx_table_[wal->tx_id_] = offset;
      auto& bf = resolve_page(wal->page_id_);
      if (wal->psn_ >= bf.page_.psn_ &&
          dirty_page_table_.find(wal->page_id_) == dirty_page_table_.end()) {
        // record the first WalEntry that makes the page dirty
        auto page_id = wal->page_id_;
        dirty_page_table_.emplace(page_id, offset);
      }
      continue;
    }
    default: {
      Log::Fatal("Unrecognized WalEntry type: {}, offset={}, walFd={}",
                 static_cast<uint8_t>(wal_entry->type_), start_offset, store_->wal_fd_);
    }
    }
  }
  return {};
}

Result<void> Recovery::redo() {
  Log::Info("[Recovery] redo phase begins");
  SCOPED_DEFER(Log::Info("[Recovery] redo phase ends"))

  // asume that each WalEntry is smaller than the page size
  utils::AlignedBuffer<512> aligned_buffer(store_->store_option_->page_size_);
  auto* complex_entry = reinterpret_cast<WalEntryComplex*>(aligned_buffer.Get());

  for (auto offset = wal_start_offset_; offset < wal_size_;) {
    auto start_offset = offset;
    auto res = next_wal_complex_to_redo(offset, complex_entry);
    if (!res) {
      // met error
      Log::Error("[Recovery] failed to get next WalComplex, offset={}, error={}", start_offset,
                 res.error().ToString());
      return std::unexpected(res.error());
    }

    if (!res.value()) {
      // no more complex entry to redo
      break;
    }

    // get a buffer frame for the corresponding dirty page
    auto& bf = resolve_page(complex_entry->page_id_);
    SCOPED_DEFER(bf.header_.keep_in_memory_ = false);

    auto* wal_payload = reinterpret_cast<WalPayload*>(complex_entry->payload_);
    switch (wal_payload->type_) {
    case WalPayload::Type::kWalInsert: {
      redo_insert(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalTxInsert: {
      redo_tx_insert(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalUpdate: {
      redo_update(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalTxUpdate: {
      redo_tx_update(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalRemove: {
      redo_remove(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalTxRemove: {
      redo_tx_remove(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalInitPage: {
      redo_init_page(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalSplitRoot: {
      redo_split_root(bf, complex_entry);
      break;
    }
    case WalPayload::Type::kWalSplitNonRoot: {
      redo_split_non_root(bf, complex_entry);
      break;
    }
    default: {
      LEAN_DCHECK(false, "Unhandled WalPayload::Type: {}",
                  std::to_string(static_cast<uint64_t>(wal_payload->type_)));
    }
    }
  }

  // Write all the resolved pages to disk
  for (auto it = resolved_pages_.begin(); it != resolved_pages_.end(); it++) {
    auto res = store_->buffer_manager_->WritePageSync(*it->second);
    if (!res) {
      return res;
    }
  }

  return {};
}

Result<bool> Recovery::next_wal_complex_to_redo(uint64_t& offset, WalEntryComplex* complex_entry) {
  auto* buff = reinterpret_cast<uint8_t*>(complex_entry);
  while (offset < wal_size_) {
    // read a WalEntry
    if (auto res = read_wal_entry(offset, buff); !res) {
      return std::unexpected(res.error());
    }

    // skip if not a complex entry
    auto* wal_entry = reinterpret_cast<WalEntry*>(buff);
    if (wal_entry->type_ != WalEntry::Type::kComplex) {
      continue;
    }

    // skip if the page is not dirty
    if (dirty_page_table_.find(complex_entry->page_id_) == dirty_page_table_.end() ||
        offset < dirty_page_table_[complex_entry->page_id_]) {
      continue;
    }

    // found a complex entry to redo
    return true;
  }

  // no more complex entry to redo
  return false;
}

void Recovery::redo_insert(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal_insert = reinterpret_cast<WalInsert*>(complex_entry->payload_);
  HybridGuard guard(&bf.header_.latch_);
  GuardedBufferFrame<BTreeNode> guarded_node(store_->buffer_manager_.get(), std::move(guard), &bf);

  int32_t slot_id = -1;
  TransactionKV::InsertToNode(guarded_node, wal_insert->GetKey(), wal_insert->GetVal(),
                              complex_entry->worker_id_, complex_entry->tx_id_, slot_id);
}

void Recovery::redo_tx_insert(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal_insert = reinterpret_cast<WalTxInsert*>(complex_entry->payload_);
  HybridGuard guard(&bf.header_.latch_);
  GuardedBufferFrame<BTreeNode> guarded_node(store_->buffer_manager_.get(), std::move(guard), &bf);

  int32_t slot_id = -1;
  TransactionKV::InsertToNode(guarded_node, wal_insert->GetKey(), wal_insert->GetVal(),
                              complex_entry->worker_id_, complex_entry->tx_id_, slot_id);
}

void Recovery::redo_update(storage::BufferFrame& bf [[maybe_unused]],
                           WalEntryComplex* complex_entry [[maybe_unused]]) {
  Log::Fatal("Unsupported");
}

void Recovery::redo_tx_update(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal = reinterpret_cast<WalTxUpdate*>(complex_entry->payload_);
  HybridGuard guard(&bf.header_.latch_);
  GuardedBufferFrame<BTreeNode> guarded_node(store_->buffer_manager_.get(), std::move(guard), &bf);
  auto* update_desc = wal->GetUpdateDesc();
  auto key = wal->GetKey();
  auto slot_id = guarded_node->LowerBound<true>(key);
  LEAN_DCHECK(slot_id != -1, "Key not found in WalTxUpdate");

  auto* mut_raw_val = guarded_node->ValData(slot_id);
  LEAN_DCHECK(Tuple::From(mut_raw_val)->format_ == TupleFormat::kChained,
              "Only chained tuple is supported");
  auto* chained_tuple = ChainedTuple::From(mut_raw_val);

  // update the chained tuple
  chained_tuple->worker_id_ = complex_entry->worker_id_;
  chained_tuple->tx_id_ = complex_entry->tx_id_;
  chained_tuple->command_id_ ^= wal->xor_command_id_;

  // 1. copy xor result to buff
  auto delta_size = wal->GetDeltaSize();
  uint8_t buff[delta_size];
  std::memcpy(buff, wal->GetDeltaPtr(), delta_size);

  // 2. calculate new value based on xor result and old value
  BasicKV::XorToBuffer(*update_desc, chained_tuple->payload_, buff);

  // 3. replace with the new value
  BasicKV::CopyToValue(*update_desc, buff, chained_tuple->payload_);
}

void Recovery::redo_remove(storage::BufferFrame& bf [[maybe_unused]],
                           WalEntryComplex* complex_entry [[maybe_unused]]) {
  Log::Fatal("Unsupported");
}

void Recovery::redo_tx_remove(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal = reinterpret_cast<WalTxRemove*>(complex_entry->payload_);
  HybridGuard guard(&bf.header_.latch_);
  GuardedBufferFrame<BTreeNode> guarded_node(store_->buffer_manager_.get(), std::move(guard), &bf);
  auto key = wal->RemovedKey();
  auto slot_id = guarded_node->LowerBound<true>(key);
  LEAN_DCHECK(slot_id != -1, "Key not found, key={}", key.ToString());

  auto* mut_raw_val = guarded_node->ValData(slot_id);
  LEAN_DCHECK(Tuple::From(mut_raw_val)->format_ == TupleFormat::kChained,
              "Only chained tuple is supported");
  auto* chained_tuple = ChainedTuple::From(mut_raw_val);

  // remove the chained tuple
  if (guarded_node->ValSize(slot_id) > sizeof(ChainedTuple)) {
    guarded_node->ShortenPayload(slot_id, sizeof(ChainedTuple));
  }
  chained_tuple->worker_id_ = complex_entry->worker_id_;
  chained_tuple->tx_id_ = complex_entry->tx_id_;
  chained_tuple->command_id_ ^= wal->prev_command_id_;
  chained_tuple->is_tombstone_ = true;
}

void Recovery::redo_init_page(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal_init_page = reinterpret_cast<WalInitPage*>(complex_entry->payload_);
  HybridGuard guard(&bf.header_.latch_);
  GuardedBufferFrame<BTreeNode> guarded_node(store_->buffer_manager_.get(), std::move(guard), &bf);
  auto x_guarded_node = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_node));
  x_guarded_node.InitPayload(wal_init_page->is_leaf_);
  bf.page_.btree_id_ = complex_entry->tree_id_;
}

void Recovery::redo_split_root(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal = reinterpret_cast<WalSplitRoot*>(complex_entry->payload_);

  // Resolve the old root
  auto old_root_guard = HybridGuard(&bf.header_.latch_);
  auto guarded_old_root =
      GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), std::move(old_root_guard), &bf);
  auto x_guarded_old_root = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_old_root));

  // Resolve the new left
  auto new_left_page_id = wal->new_left_;
  auto& new_left_bf = resolve_page(new_left_page_id);
  auto new_left_guard = HybridGuard(&new_left_bf.header_.latch_);
  auto guarded_new_left = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(),
                                                        std::move(new_left_guard), &new_left_bf);
  auto x_guarded_new_left = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_new_left));

  // Resolve the new root
  auto new_root_page_id = wal->new_root_;
  auto& new_root_bf = resolve_page(new_root_page_id);
  auto new_root_guard = HybridGuard(&new_root_bf.header_.latch_);
  auto guarded_new_root = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(),
                                                        std::move(new_root_guard), &new_root_bf);
  auto x_guarded_new_root = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_new_root));

  // Resolve the meta node
  auto meta_page_id = wal->meta_node_;
  auto& meta_bf = resolve_page(meta_page_id);
  auto meta_guard = HybridGuard(&meta_bf.header_.latch_);
  auto guarded_meta =
      GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), std::move(meta_guard), &meta_bf);
  auto x_guarded_meta = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_meta));

  // Resolve sepInfo
  auto sep_info =
      BTreeNode::SeparatorInfo(wal->separator_size_, wal->split_slot_, wal->separator_truncated_);

  // move half of the old root to the new left,
  // insert separator key into new root,
  // update meta node to point to new root
  x_guarded_new_root->right_most_child_swip_ = x_guarded_old_root.bf();
  x_guarded_old_root->Split(x_guarded_new_root, x_guarded_new_left, sep_info);
  x_guarded_meta->right_most_child_swip_ = x_guarded_new_root.bf();
}

void Recovery::redo_split_non_root(storage::BufferFrame& bf, WalEntryComplex* complex_entry) {
  auto* wal = reinterpret_cast<WalSplitNonRoot*>(complex_entry->payload_);

  // Resolve the old root
  auto child_guard = HybridGuard(&bf.header_.latch_);
  auto guarded_child =
      GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(), std::move(child_guard), &bf);
  auto x_guarded_child = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_child));

  // Resolve the new left
  auto new_left_page_id = wal->new_left_;
  auto& new_left_bf = resolve_page(new_left_page_id);
  auto new_left_guard = HybridGuard(&new_left_bf.header_.latch_);
  auto guarded_new_left = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(),
                                                        std::move(new_left_guard), &new_left_bf);
  auto x_guarded_new_left = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_new_left));

  // Resolve the parent node
  auto parent_page_id = wal->parent_page_id_;
  auto& parent_bf = resolve_page(parent_page_id);
  auto parent_guard = HybridGuard(&parent_bf.header_.latch_);
  auto guarded_parent = GuardedBufferFrame<BTreeNode>(store_->buffer_manager_.get(),
                                                      std::move(parent_guard), &parent_bf);
  auto x_guarded_parent = ExclusiveGuardedBufferFrame<BTreeNode>(std::move(guarded_parent));

  // Resolve sepInfo
  auto sep_info =
      BTreeNode::SeparatorInfo(wal->separator_size_, wal->split_slot_, wal->separator_truncated_);

  const uint16_t space_needed_for_separator =
      guarded_parent->SpaceNeeded(sep_info.size_, sizeof(Swip));
  x_guarded_parent->RequestSpaceFor(space_needed_for_separator);
  x_guarded_child->Split(x_guarded_parent, x_guarded_new_left, sep_info);
}

/// Read a WalEntry from the WAL file
Result<void> Recovery::read_wal_entry(uint64_t& offset, uint8_t* dest) {
  // read the WalEntry
  auto wal_entry_size = sizeof(WalEntry);
  if (auto res = read_from_wal_file(offset, wal_entry_size, dest); !res) {
    return std::unexpected(res.error());
  }

  switch (reinterpret_cast<WalEntry*>(dest)->type_) {
  case leanstore::cr::WalEntry::Type::kTxAbort: {
    auto left = sizeof(WalTxAbort) - wal_entry_size;
    auto res = read_from_wal_file(offset + wal_entry_size, left, dest + wal_entry_size);
    if (!res) {
      return std::unexpected(res.error());
    }
    offset += sizeof(WalTxAbort);
    return {};
  }
  case leanstore::cr::WalEntry::Type::kTxFinish: {
    auto left = sizeof(WalTxFinish) - wal_entry_size;
    auto res = read_from_wal_file(offset + wal_entry_size, left, dest + wal_entry_size);
    if (!res) {
      return std::unexpected(res.error());
    }
    offset += sizeof(WalTxFinish);
    return {};
  }
  case leanstore::cr::WalEntry::Type::kCarriageReturn: {
    auto left = sizeof(WalCarriageReturn) - wal_entry_size;
    auto res = read_from_wal_file(offset + wal_entry_size, left, dest + wal_entry_size);
    if (!res) {
      return std::unexpected(res.error());
    }
    offset += reinterpret_cast<WalCarriageReturn*>(dest)->size_;
    return {};
  }
  case leanstore::cr::WalEntry::Type::kComplex: {
    // read the body of WalEntryComplex
    auto left = sizeof(WalEntryComplex) - wal_entry_size;
    auto res = read_from_wal_file(offset + wal_entry_size, left, dest + wal_entry_size);
    if (!res) {
      return std::unexpected(res.error());
    }

    // read the payload of WalEntryComplex
    left = reinterpret_cast<WalEntryComplex*>(dest)->size_ - sizeof(WalEntryComplex);
    res =
        read_from_wal_file(offset + sizeof(WalEntryComplex), left, dest + sizeof(WalEntryComplex));
    if (!res) {
      return std::unexpected(res.error());
    }

    // advance the offset
    offset += reinterpret_cast<WalEntryComplex*>(dest)->size_;
    return {};
  }
  }
  return {};
}

storage::BufferFrame& Recovery::resolve_page(PID page_id) {
  auto it = resolved_pages_.find(page_id);
  if (it != resolved_pages_.end()) {
    return *it->second;
  }

  auto& bf = store_->buffer_manager_->ReadPageSync(page_id);
  // prevent the buffer frame from being evicted by buffer frame providers
  bf.header_.keep_in_memory_ = true;
  resolved_pages_.emplace(page_id, &bf);
  return bf;
}

// TODO(zz-jason): refactor with aio
Result<void> Recovery::read_from_wal_file(int64_t offset, size_t nbytes, void* destination) {
  auto file_name = store_->GetWalFilePath();
  FILE* fp = fopen(file_name.c_str(), "rb");
  if (fp == nullptr) {
    return std::unexpected(utils::Error::FileOpen(file_name, errno, strerror(errno)));
  }
  SCOPED_DEFER(fclose(fp));

  if (fseek(fp, offset, SEEK_SET) != 0) {
    return std::unexpected(utils::Error::FileSeek(file_name, errno, strerror(errno)));
  }

  if (fread(destination, 1, nbytes, fp) != nbytes) {
    return std::unexpected(utils::Error::FileRead(file_name, errno, strerror(errno)));
  }

  return {};
}

} // namespace leanstore::cr