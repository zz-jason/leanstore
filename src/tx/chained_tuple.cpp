#include "leanstore/tx/chained_tuple.hpp"

#include "leanstore/base/constants.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/c/types.h"
#include "leanstore/c/wal_record.h"
#include "wal/wal_builder.hpp"

namespace leanstore {

std::tuple<OpCode, uint16_t> ChainedTuple::GetVisibleTuple(Slice payload,
                                                           ValCallback callback) const {
  if (CoroEnv::CurTxMgr().cc_.VisibleForMe(worker_id_, tx_id_)) {
    if (is_tombstone_) {
      return {OpCode::kNotFound, 1};
    }

    auto val_size = payload.size() - sizeof(ChainedTuple);
    callback(GetValue(val_size));
    return {OpCode::kOK, 1};
  }

  if (cmd_id_ == kCmdInvalid) {
    return {OpCode::kNotFound, 1};
  }

  // Head is not visible
  uint16_t value_size = payload.size() - sizeof(ChainedTuple);
  auto value_buf = std::make_unique<uint8_t[]>(value_size);
  std::memcpy(value_buf.get(), this->payload_, value_size);

  lean_wid_t newer_worker_id = worker_id_;
  lean_txid_t newer_tx_id = tx_id_;
  lean_cmdid_t newer_command_id = cmd_id_;

  uint16_t versions_read = 1;
  while (true) {
    bool found = CoroEnv::CurTxMgr().cc_.GetVersion(
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
          newer_command_id = version.cmd_id_;
        });
    if (!found) {
      Log::Error("Not found in the version tree, workerId={}, startTs={}, "
                 "versionsRead={}, newerWorkerId={}, newerTxId={}, "
                 "newerCommandId={}",
                 CoroEnv::CurTxMgr().worker_id_, CoroEnv::CurTxMgr().ActiveTx().start_ts_,
                 versions_read, newer_worker_id, newer_tx_id, newer_command_id);
      return {OpCode::kNotFound, versions_read};
    }

    if (CoroEnv::CurTxMgr().cc_.VisibleForMe(newer_worker_id, newer_tx_id)) {
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
      CoroEnv::CurTxMgr().cc_.PutVersion(tree_id, false, version_size, [&](uint8_t* version_buf) {
        auto& update_version = *new (version_buf) UpdateVersion(worker_id_, tx_id_, cmd_id_, true);
        std::memcpy(update_version.payload_, &update_desc, update_desc.Size());
        auto* dest = update_version.payload_ + update_desc.Size();
        BasicKV::CopyToBuffer(update_desc, payload_, dest);
      });

  auto perform_update = [&]() {
    auto mut_raw_val = x_iter->MutableVal();
    auto user_val_size = mut_raw_val.size() - sizeof(ChainedTuple);
    update_call_back(MutableSlice(payload_, user_val_size));
    worker_id_ = CoroEnv::CurTxMgr().worker_id_;
    tx_id_ = CoroEnv::CurTxMgr().ActiveTx().start_ts_;
    cmd_id_ = curr_command_id;
  };

  LEAN_DEFER({
    WriteUnlock();
    x_iter->UpdateContentionStats();
  });

  if (!x_iter->btree_.config_.enable_wal_) {
    perform_update();
    return;
  }

  auto prev_worker_id = worker_id_;
  auto prev_tx_id = tx_id_;
  auto prev_command_id = cmd_id_;

  WalTxBuilder<lean_wal_tx_update> builder(tree_id, key.size() + size_of_desc_and_delta);
  x_iter->guarded_leaf_.UpdatePageVersion();
  builder.SetPageInfo(x_iter->guarded_leaf_.bf_)
      .SetPrevVersion(prev_worker_id, prev_tx_id, prev_command_id ^ curr_command_id)
      .BuildTxUpdate(key, update_desc);
  auto* delta_ptr = reinterpret_cast<uint8_t*>(lean_wal_tx_update_get_delta(builder.GetWal()));

  // 1. copy old value to wal buffer
  BasicKV::CopyToBuffer(update_desc, payload_, delta_ptr);

  // 2. update the value in-place
  perform_update();

  // 3. xor with the updated new value and store to wal buffer
  BasicKV::XorToBuffer(update_desc, payload_, delta_ptr);

  builder.Submit();

  CoroEnv::CurTxMgr().ActiveTx().has_wrote_ = true;
}

} // namespace leanstore