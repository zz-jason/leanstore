#include "leanstore/btree/tuple.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/chained_tuple.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/misc.hpp"

#include <unordered_map>

using namespace std;
using namespace leanstore::storage;
using OpCode = leanstore::OpCode;

namespace std {

template <>
class hash<leanstore::UpdateSlotInfo> {
public:
  size_t operator()(const leanstore::UpdateSlotInfo& slot) const {
    size_t result = (slot.size_ << 16) | slot.offset_;
    return result;
  }
};

} // namespace std

namespace leanstore::storage::btree {

static_assert(sizeof(FatTuple) >= sizeof(ChainedTuple));

// -----------------------------------------------------------------------------
// Tuple
// -----------------------------------------------------------------------------

static uint64_t MaxFatTupleLength() {
  return BTreeNode::Size() - 1000;
}

bool Tuple::ToFat(BTreeIterMut* x_iter) {
  // Process the chain tuple
  MutableSlice mut_raw_val = x_iter->MutableVal();
  auto& chained_tuple = *ChainedTuple::From(mut_raw_val.Data());
  LEAN_DCHECK(chained_tuple.IsWriteLocked());
  LEAN_DCHECK(chained_tuple.format_ == TupleFormat::kChained);

  auto tmp_buf_size = MaxFatTupleLength();
  auto tmp_buf = utils::JumpScopedArray<uint8_t>(tmp_buf_size);
  uint32_t payload_size = tmp_buf_size - sizeof(FatTuple);
  uint32_t val_size = mut_raw_val.Size() - sizeof(ChainedTuple);
  auto* fat_tuple = new (tmp_buf->get()) FatTuple(payload_size, val_size, chained_tuple);

  auto newer_worker_id = chained_tuple.worker_id_;
  auto newer_tx_id = chained_tuple.tx_id_;
  auto newer_command_id = chained_tuple.cmd_id_;

  // TODO: check for payload_size_ overflow
  bool abort_conversion = false;
  uint16_t num_deltas_to_replace = 0;
  while (!abort_conversion) {
    if (CoroEnv::CurTxMgr().cc_.VisibleForAll(newer_tx_id)) {
      // No need to convert versions that are visible for all to the FatTuple,
      // these old version can be GCed. Pruning versions space might get delayed
      break;
    }

    if (!CoroEnv::CurTxMgr().cc_.GetVersion(
            newer_worker_id, newer_tx_id, newer_command_id, [&](const uint8_t* version, uint64_t) {
              num_deltas_to_replace++;
              const auto& chained_delta = *UpdateVersion::From(version);
              LEAN_DCHECK(chained_delta.type_ == VersionType::kUpdate);
              LEAN_DCHECK(chained_delta.is_delta_);

              auto& update_desc = *UpdateDesc::From(chained_delta.payload_);
              auto size_of_desc_and_delta = update_desc.SizeWithDelta();
              auto delta_size = sizeof(FatTupleDelta) + size_of_desc_and_delta;

              if (!fat_tuple->HasSpaceFor(update_desc)) {
                fat_tuple->GarbageCollection();
                if (!fat_tuple->HasSpaceFor(update_desc)) {
                  abort_conversion = true;
                  return;
                }
              }

              // Add a FatTupleDelta
              fat_tuple->NewDelta(
                  delta_size, chained_delta.worker_id_, chained_delta.tx_id_, chained_delta.cmd_id_,
                  reinterpret_cast<const uint8_t*>(&update_desc), size_of_desc_and_delta);
              newer_worker_id = chained_delta.worker_id_;
              newer_tx_id = chained_delta.tx_id_;
              newer_command_id = chained_delta.cmd_id_;
            })) {
      // no more old versions
      break;
    }
  }

  if (abort_conversion) {
    chained_tuple.WriteUnlock();
    return false;
  }

  if (num_deltas_to_replace <= TransactionKV::ConvertToFatTupleThreshold()) {
    chained_tuple.WriteUnlock();
    return false;
  }

  // Reverse the deltas to make follow the O2N order.
  // NOTE: Deltas are added when visiting the chainedTuple in the N2O order.
  fat_tuple->ReverseDeltas();
  fat_tuple->GarbageCollection();

  if (fat_tuple->payload_size_ > MaxFatTupleLength()) {
    chained_tuple.WriteUnlock();
    return false;
  }

  LEAN_DCHECK(fat_tuple->payload_capacity_ >= fat_tuple->payload_size_);

  // Finalize the new FatTuple
  // TODO: corner cases, more careful about space usage
  const uint16_t fat_tuple_size = sizeof(FatTuple) + fat_tuple->payload_capacity_;
  if (x_iter->Val().size() < fat_tuple_size) {
    auto succeed = x_iter->ExtendPayload(fat_tuple_size);
    if (!succeed) {
      Log::Fatal("Failed to extend current value buffer to fit the FatTuple, "
                 "fatTupleSize={}, current value buffer size={}",
                 fat_tuple_size, x_iter->Val().size());
    }
  } else {
    x_iter->ShortenWithoutCompaction(fat_tuple_size);
  }

  // Copy the FatTuple back to the underlying value buffer.
  std::memcpy(mut_raw_val.Data(), tmp_buf->get(), fat_tuple_size);
  return true;
}

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

FatTuple::FatTuple(uint32_t payload_size, uint32_t val_size, const ChainedTuple& chained_tuple)
    : Tuple(TupleFormat::kFat, chained_tuple.worker_id_, chained_tuple.tx_id_),
      val_size_(val_size),
      payload_capacity_(payload_size),
      payload_size_(val_size),
      data_offset_(payload_size) {
  std::memcpy(payload_, chained_tuple.payload_, val_size_);
  cmd_id_ = chained_tuple.cmd_id_;
}

void FatTuple::UndoLastUpdate() {
  ENSURE(num_deltas_ >= 1);
  auto& delta = get_delta(num_deltas_ - 1);
  worker_id_ = delta.worker_id_;
  tx_id_ = delta.tx_id_;
  cmd_id_ = delta.cmd_id_;
  num_deltas_ -= 1;
  const uint32_t total_delta_size = delta.TotalSize();
  data_offset_ += total_delta_size;
  payload_size_ -= total_delta_size + sizeof(uint16_t);
  auto& update_desc = delta.GetUpdateDesc();
  auto* xor_data = delta.GetDeltaPtr();
  BasicKV::CopyToValue(update_desc, xor_data, GetValPtr());
}

// NOLINTBEGIN

// Attention: we have to disable garbage collection if the latest delta was from
// us and not committed yet! Otherwise we would crash during undo although the
// end result is the same if the transaction would commit (overwrite)
void FatTuple::GarbageCollection() {
  if (num_deltas_ == 0) {
    return;
  }
  auto append_delta = [](FatTuple& fat_tuple, uint8_t* delta, uint16_t delta_size) {
    assert(fat_tuple.payload_capacity_ >=
           (fat_tuple.payload_size_ + delta_size + sizeof(uint16_t)));
    const uint16_t i = fat_tuple.num_deltas_++;
    fat_tuple.payload_size_ += delta_size + sizeof(uint16_t);
    fat_tuple.data_offset_ -= delta_size;
    fat_tuple.get_delta_offsets()[i] = fat_tuple.data_offset_;
    std::memcpy(fat_tuple.payload_ + fat_tuple.data_offset_, delta, delta_size);
  };

  // Delete for all visible deltas, atm using cheap visibility check
  if (CoroEnv::CurTxMgr().cc_.VisibleForAll(tx_id_)) {
    num_deltas_ = 0;
    data_offset_ = payload_capacity_;
    payload_size_ = val_size_;
    return; // Done
  }

  uint16_t deltas_visible_for_all = 0;
  for (int32_t i = num_deltas_ - 1; i >= 1; i--) {
    auto& delta = get_delta(i);
    if (CoroEnv::CurTxMgr().cc_.VisibleForAll(delta.tx_id_)) {
      deltas_visible_for_all = i - 1;
      break;
    }
  }

  const lean_txid_t local_oldest_oltp =
      CoroEnv::CurTxMgr().store_->MvccManager()->GlobalWmkInfo().oldest_active_short_tx_.load();
  const lean_txid_t local_newest_olap =
      CoroEnv::CurTxMgr().store_->MvccManager()->GlobalWmkInfo().newest_active_long_tx_.load();
  if (deltas_visible_for_all == 0 && local_newest_olap > local_oldest_oltp) {
    return; // Nothing to do here
  }

  auto bin_search = [&](lean_txid_t upper_bound) {
    int64_t l = deltas_visible_for_all;
    int64_t r = num_deltas_ - 1;
    int64_t m = -1;
    while (l <= r) {
      m = l + (r - l) / 2;
      lean_txid_t it = get_delta(m).tx_id_;
      if (it == upper_bound) {
        return m;
      }

      if (it < upper_bound) {
        l = m + 1;
      } else {
        r = m - 1;
      }
    }
    return l;
  };

  // Identify tuples we should merge --> [zone_begin, zone_end)
  int32_t deltas_to_merge_counter = 0;
  int32_t zone_begin = -1, zone_end = -1; // [zone_begin, zone_end)
  int32_t res = bin_search(local_newest_olap);
  if (res < num_deltas_) {
    zone_begin = res;
    res = bin_search(local_oldest_oltp);
    if (res - 2 > zone_begin) { // 1 is enough but 2 is an easy fix for
                                // res=num_deltas_ case
      zone_end = res - 2;
      deltas_to_merge_counter = zone_end - zone_begin;
    }
  }
  if (deltas_visible_for_all == 0 && deltas_to_merge_counter <= 0) {
    return;
  }

  auto buffer_size = payload_capacity_ + sizeof(FatTuple);
  auto buffer = utils::JumpScopedArray<uint8_t>(buffer_size);
  auto& new_fat_tuple = *new (buffer->get()) FatTuple(payload_capacity_);
  new_fat_tuple.worker_id_ = worker_id_;
  new_fat_tuple.tx_id_ = tx_id_;
  new_fat_tuple.cmd_id_ = cmd_id_;
  new_fat_tuple.payload_size_ += val_size_;
  new_fat_tuple.val_size_ = val_size_;
  std::memcpy(new_fat_tuple.payload_, payload_, val_size_); // Copy value

  if (deltas_to_merge_counter <= 0) {
    for (uint32_t i = deltas_visible_for_all; i < num_deltas_; i++) {
      auto& delta = get_delta(i);
      append_delta(new_fat_tuple, reinterpret_cast<uint8_t*>(&delta), delta.TotalSize());
    }
  } else {
    for (int32_t i = deltas_visible_for_all; i < zone_begin; i++) {
      auto& delta = get_delta(i);
      append_delta(new_fat_tuple, reinterpret_cast<uint8_t*>(&delta), delta.TotalSize());
    }

    // TODO: Optimize
    // Merge from newest to oldest, i.e., from end to beginning
    std::unordered_map<UpdateSlotInfo, std::basic_string<uint8_t>> diff_slots_map;
    for (int32_t i = zone_end - 1; i >= zone_begin; i--) {
      auto& delta_id = get_delta(i);
      auto& update_desc = delta_id.GetUpdateDesc();
      auto* delta_ptr = delta_id.GetDeltaPtr();
      for (int16_t i = 0; i < update_desc.num_slots_; i++) {
        diff_slots_map[update_desc.update_slots_[i]] =
            std::basic_string<uint8_t>(delta_ptr, update_desc.update_slots_[i].size_);
        delta_ptr += update_desc.update_slots_[i].size_;
      }
    }
    uint32_t total_new_delta_size = sizeof(FatTupleDelta) + sizeof(UpdateDesc) +
                                    (sizeof(UpdateSlotInfo) * diff_slots_map.size());
    for (auto& slot_itr : diff_slots_map) {
      total_new_delta_size += slot_itr.second.size();
    }
    auto& merge_delta = new_fat_tuple.NewDelta(total_new_delta_size);
    merge_delta = get_delta(zone_begin);
    auto& update_desc = merge_delta.GetUpdateDesc();
    update_desc.num_slots_ = diff_slots_map.size();
    auto* merge_delta_ptr = merge_delta.GetDeltaPtr();
    uint32_t s_i = 0;
    for (auto& slot_itr : diff_slots_map) {
      update_desc.update_slots_[s_i++] = slot_itr.first;
      std::memcpy(merge_delta_ptr, slot_itr.second.c_str(), slot_itr.second.size());
      merge_delta_ptr += slot_itr.second.size();
    }

    for (uint32_t i = zone_end; i < num_deltas_; i++) {
      auto& delta = get_delta(i);
      append_delta(new_fat_tuple, reinterpret_cast<uint8_t*>(&delta), delta.TotalSize());
    }
  }

  std::memcpy(this, buffer->get(), buffer_size);
  LEAN_DCHECK(payload_capacity_ >= payload_size_);

  DEBUG_BLOCK() {
    uint32_t space_used [[maybe_unused]] = val_size_;
    for (uint32_t i = 0; i < num_deltas_; i++) {
      space_used += sizeof(uint16_t) + get_delta(i).TotalSize();
    }
    LEAN_DCHECK(payload_size_ == space_used);
  }
}

// NOLINTEND

bool FatTuple::HasSpaceFor(const UpdateDesc& update_desc) {
  const uint32_t space_needed =
      update_desc.SizeWithDelta() + sizeof(uint16_t) + sizeof(FatTupleDelta);
  return (data_offset_ - (val_size_ + (num_deltas_ * sizeof(uint16_t)))) >= space_needed;
}

template <typename... Args>
FatTupleDelta& FatTuple::NewDelta(uint32_t total_delta_size, Args&&... args) {
  LEAN_DCHECK((payload_capacity_ - payload_size_) >= (total_delta_size + sizeof(uint16_t)));
  payload_size_ += total_delta_size + sizeof(uint16_t);
  data_offset_ -= total_delta_size;
  const uint32_t delta_id = num_deltas_++;
  get_delta_offsets()[delta_id] = data_offset_;
  return *new (&get_delta(delta_id)) FatTupleDelta(std::forward<Args>(args)...);
}

// Caller should take care of WAL
void FatTuple::Append(UpdateDesc& update_desc) {
  const uint32_t total_delta_size = update_desc.SizeWithDelta() + sizeof(FatTupleDelta);
  auto& new_delta = NewDelta(total_delta_size);
  new_delta.worker_id_ = this->worker_id_;
  new_delta.tx_id_ = this->tx_id_;
  new_delta.cmd_id_ = this->cmd_id_;
  std::memcpy(new_delta.payload_, &update_desc, update_desc.Size());
  auto* delta_ptr = new_delta.payload_ + update_desc.Size();
  BasicKV::CopyToBuffer(update_desc, this->GetValPtr(), delta_ptr);
}

std::tuple<OpCode, uint16_t> FatTuple::GetVisibleTuple(ValCallback val_callback) const {

  // Latest version is visible
  if (CoroEnv::CurTxMgr().cc_.VisibleForMe(worker_id_, tx_id_)) {
    val_callback(GetValue());
    return {OpCode::kOK, 1};
  }

  LEAN_DCHECK(CoroEnv::CurTxMgr().ActiveTx().IsLongRunning());

  if (num_deltas_ > 0) {
    auto copied_val = utils::JumpScopedArray<uint8_t>(val_size_);
    std::memcpy(copied_val->get(), GetValPtr(), val_size_);
    // we have to apply the diffs
    uint16_t num_visited_versions = 2;
    for (int i = num_deltas_ - 1; i >= 0; i--) {
      const auto& delta = get_delta(i);
      const auto& update_desc = delta.GetUpdateDesc();
      auto* xor_data = delta.GetDeltaPtr();
      BasicKV::CopyToValue(update_desc, xor_data, copied_val->get());
      if (CoroEnv::CurTxMgr().cc_.VisibleForMe(delta.worker_id_, delta.tx_id_)) {
        val_callback(Slice(copied_val->get(), val_size_));
        return {OpCode::kOK, num_visited_versions};
      }

      num_visited_versions++;
    }

    return {OpCode::kNotFound, num_visited_versions};
  }

  return {OpCode::kNotFound, 1};
}

void FatTuple::resize(const uint32_t new_size) {
  auto tmp_page_size = new_size + sizeof(FatTuple);
  auto tmp_page = utils::JumpScopedArray<uint8_t>(tmp_page_size);
  auto& new_fat_tuple = *new (tmp_page->get()) FatTuple(new_size);
  new_fat_tuple.worker_id_ = worker_id_;
  new_fat_tuple.tx_id_ = tx_id_;
  new_fat_tuple.cmd_id_ = cmd_id_;
  new_fat_tuple.payload_size_ += val_size_;
  new_fat_tuple.val_size_ = val_size_;
  std::memcpy(new_fat_tuple.payload_, payload_, val_size_); // Copy value
  auto append_delta = [](FatTuple& fat_tuple, uint8_t* delta, uint16_t delta_size) {
    LEAN_DCHECK(fat_tuple.payload_capacity_ >=
                (fat_tuple.payload_size_ + delta_size + sizeof(uint16_t)));
    const uint16_t i = fat_tuple.num_deltas_++;
    fat_tuple.payload_size_ += delta_size + sizeof(uint16_t);
    fat_tuple.data_offset_ -= delta_size;
    fat_tuple.get_delta_offsets()[i] = fat_tuple.data_offset_;
    std::memcpy(fat_tuple.payload_ + fat_tuple.data_offset_, delta, delta_size);
  };
  for (uint64_t i = 0; i < num_deltas_; i++) {
    append_delta(new_fat_tuple, reinterpret_cast<uint8_t*>(&get_delta(i)),
                 get_delta(i).TotalSize());
  }
  std::memcpy(this, tmp_page->get(), tmp_page_size);
  LEAN_DCHECK(payload_capacity_ >= payload_size_);
}

void FatTuple::ConvertToChained(lean_treeid_t tree_id) {
  auto prev_worker_id = worker_id_;
  auto prev_tx_id = tx_id_;
  auto prev_command_id = cmd_id_;
  for (int64_t i = num_deltas_ - 1; i >= 0; i--) {
    auto& delta = get_delta(i);
    auto& update_desc = delta.GetUpdateDesc();
    auto size_of_desc_and_delta = update_desc.SizeWithDelta();
    auto version_size = size_of_desc_and_delta + sizeof(UpdateVersion);
    CoroEnv::CurTxMgr()
        .cc_.Other(prev_worker_id)
        .history_storage_.PutVersion(
            prev_tx_id, prev_command_id, tree_id, false, version_size,
            [&](uint8_t* version_buf) {
              new (version_buf) UpdateVersion(delta, size_of_desc_and_delta);
            },
            false);
    prev_worker_id = delta.worker_id_;
    prev_tx_id = delta.tx_id_;
    prev_command_id = delta.cmd_id_;
  }

  new (this) ChainedTuple(*this);
}

} // namespace leanstore::storage::btree
