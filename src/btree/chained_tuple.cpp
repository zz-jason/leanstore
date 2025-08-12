#include "leanstore/btree/chained_tuple.hpp"

#include "btree/core/b_tree_wal_payload.hpp"
#include "leanstore/utils/log.hpp"

namespace leanstore::storage::btree {

std::tuple<OpCode, uint16_t> ChainedTuple::GetVisibleTuple(Slice payload,
                                                           ValCallback callback) const {
  if (cr::TxManager::My().cc_.VisibleForMe(worker_id_, tx_id_)) {
    if (is_tombstone_) {
      return {OpCode::kNotFound, 1};
    }

    auto val_size = payload.length() - sizeof(ChainedTuple);
    callback(GetValue(val_size));
    return {OpCode::kOK, 1};
  }

  if (command_id_ == kInvalidCommandid) {
    return {OpCode::kNotFound, 1};
  }

  // Head is not visible
  uint16_t value_size = payload.length() - sizeof(ChainedTuple);
  auto value_buf = std::make_unique<uint8_t[]>(value_size);
  std::memcpy(value_buf.get(), this->payload_, value_size);

  WORKERID newer_worker_id = worker_id_;
  TXID newer_tx_id = tx_id_;
  COMMANDID newer_command_id = command_id_;

  uint16_t versions_read = 1;
  while (true) {
    bool found = cr::TxManager::My().cc_.GetVersion(
        newer_worker_id, newer_tx_id, newer_command_id,
        [&](const uint8_t* version_buf, uint64_t version_size) {
          auto& version = *reinterpret_cast<const Version*>(version_buf);
          switch (version.type_) {
          case VersionType::kUpdate: {
            auto& update_version = *UpdateVersion::From(version_buf);
            if (update_version.is_delta_) {
              // Apply delta
              auto& update_desc = *UpdateDesc::From(update_version.payload_);
              auto* old_val_of_slots = update_version.payload_ + update_desc.Size();
              BasicKV::CopyToValue(update_desc, old_val_of_slots, value_buf.get());
            } else {
              value_size = version_size - sizeof(UpdateVersion);
              value_buf = std::make_unique<uint8_t[]>(value_size);
              std::memcpy(value_buf.get(), update_version.payload_, value_size);
            }
            break;
          }
          case VersionType::kRemove: {
            auto& remove_version = *RemoveVersion::From(version_buf);
            auto removed_val = remove_version.RemovedVal();
            value_size = remove_version.val_size_;
            value_buf = std::make_unique<uint8_t[]>(removed_val.size());
            std::memcpy(value_buf.get(), removed_val.data(), removed_val.size());
            break;
          }
          case VersionType::kInsert: {
            auto& insert_version = *InsertVersion::From(version_buf);
            value_size = insert_version.val_size_;
            value_buf = std::make_unique<uint8_t[]>(value_size);
            std::memcpy(value_buf.get(), insert_version.payload_, value_size);
            break;
          }
          }

          newer_worker_id = version.worker_id_;
          newer_tx_id = version.tx_id_;
          newer_command_id = version.command_id_;
        });
    if (!found) {
      Log::Error("Not found in the version tree, workerId={}, startTs={}, "
                 "versionsRead={}, newerWorkerId={}, newerTxId={}, "
                 "newerCommandId={}",
                 cr::TxManager::My().worker_id_, cr::ActiveTx().start_ts_, versions_read,
                 newer_worker_id, newer_tx_id, newer_command_id);
      return {OpCode::kNotFound, versions_read};
    }

    if (cr::TxManager::My().cc_.VisibleForMe(newer_worker_id, newer_tx_id)) {
      callback(Slice(value_buf.get(), value_size));
      return {OpCode::kOK, versions_read};
    }
    versions_read++;
  }
  return {OpCode::kNotFound, versions_read};
}

void ChainedTuple::Update(BTreeIterMut* x_iter, Slice key, MutValCallback update_call_back,
                          UpdateDesc& update_desc) {
  auto size_of_desc_and_delta = update_desc.SizeWithDelta();
  auto version_size = size_of_desc_and_delta + sizeof(UpdateVersion);

  // Move the newest tuple to the history version tree.
  auto tree_id = x_iter->btree_.tree_id_;
  auto curr_command_id =
      cr::TxManager::My().cc_.PutVersion(tree_id, false, version_size, [&](uint8_t* version_buf) {
        auto& update_version =
            *new (version_buf) UpdateVersion(worker_id_, tx_id_, command_id_, true);
        std::memcpy(update_version.payload_, &update_desc, update_desc.Size());
        auto* dest = update_version.payload_ + update_desc.Size();
        BasicKV::CopyToBuffer(update_desc, payload_, dest);
      });

  auto perform_update = [&]() {
    auto mut_raw_val = x_iter->MutableVal();
    auto user_val_size = mut_raw_val.Size() - sizeof(ChainedTuple);
    update_call_back(MutableSlice(payload_, user_val_size));
    worker_id_ = cr::TxManager::My().worker_id_;
    tx_id_ = cr::ActiveTx().start_ts_;
    command_id_ = curr_command_id;
  };

  SCOPED_DEFER({
    WriteUnlock();
    x_iter->UpdateContentionStats();
  });

  if (!x_iter->btree_.config_.enable_wal_) {
    perform_update();
    return;
  }

  auto prev_worker_id = worker_id_;
  auto prev_tx_id = tx_id_;
  auto prev_command_id = command_id_;
  auto wal_handler = x_iter->guarded_leaf_.ReserveWALPayload<WalTxUpdate>(
      key.size() + size_of_desc_and_delta, key, update_desc, size_of_desc_and_delta, prev_worker_id,
      prev_tx_id, prev_command_id ^ curr_command_id);
  auto* wal_buf = wal_handler->GetDeltaPtr();

  // 1. copy old value to wal buffer
  BasicKV::CopyToBuffer(update_desc, payload_, wal_buf);

  // 2. update the value in-place
  perform_update();

  // 3. xor with the updated new value and store to wal buffer
  BasicKV::XorToBuffer(update_desc, payload_, wal_buf);

  wal_handler.SubmitWal();
}

} // namespace leanstore::storage::btree