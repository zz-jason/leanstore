#include "leanstore/wal/wal_serde.hpp"

#include "leanstore/c/wal_record.h"
#include "leanstore/utils/misc.hpp"
#include "leanstore/wal/wal_cast.hpp"
#include "leanstore/wal/wal_traits.hpp"
#include "leanstore/utils/json.hpp"

#include <cassert>
#include <string>

namespace leanstore {

namespace {

constexpr auto kWalType = "type";
constexpr auto kLsn = "lsn";
constexpr auto kSize = "size";
constexpr auto kSysTxId = "sys_tx";
constexpr auto kPageId = "page_id";
constexpr auto kPageVersion = "page_version";
constexpr auto kBtreeId = "btree_id";
constexpr auto kIsLeaf = "is_leaf";
constexpr auto kParent = "parent";
constexpr auto kNewLhs = "new_lhs";
constexpr auto kNewRhs = "new_rhs";
constexpr auto kSepSlot = "sep_slot";
constexpr auto kSepSize = "sep_size";
constexpr auto kSepTruncated = "sep_truncated";
constexpr auto kKeySize = "key_size";
constexpr auto kValSize = "val_size";
constexpr auto kUpdateDescSize = "update_desc_size";
constexpr auto kDeltaSize = "delta_size";
constexpr auto kWorkerId = "worker";
constexpr auto kTxId = "tx";
constexpr auto kPrevLsn = "prev_lsn";
constexpr auto kPrevWorkerId = "prev_worker";
constexpr auto kPrevTxId = "prev_tx";
constexpr auto kPrevCmd = "prev_cmd";
constexpr auto kXorCmdId = "xor_cmd_id";

} // namespace

std::string WalSerde::ToJson(const lean_wal_record& record) {
  auto serialize_wal_base = [](const lean_wal_record& record, std::string_view type_name,
                               utils::JsonObj& json_obj) {
    json_obj.AddUint64(kLsn, record.lsn_);
    json_obj.AddUint64(kSize, record.size_);
    json_obj.AddUint64(kBtreeId, record.btree_id_);
    json_obj.AddString(kWalType, type_name);
  };

  auto serialize_wal_tx_base = [&serialize_wal_base](const lean_wal_record& record,
                                                     std::string_view type_name,
                                                     utils::JsonObj& json_obj) {
    serialize_wal_base(record, type_name, json_obj);
    auto& record_tx_base = CastTo<lean_wal_tx_base>(record);
    json_obj.AddUint64(kWorkerId, record_tx_base.wid_);
    json_obj.AddUint64(kTxId, record_tx_base.txid_);
    json_obj.AddUint64(kPrevLsn, record_tx_base.prev_lsn_);
  };

  switch (record.type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN: {
    utils::JsonObj json_obj;
    serialize_wal_base(record, WalRecordTraits<lean_wal_carriage_return>::kName, json_obj);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_COMPLETE: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_smo_complete>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_complete>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl.sys_txid_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_PAGENEW: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_smo_pagenew>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_pagenew>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl.sys_txid_);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddBool(kIsLeaf, record_impl.is_leaf_);
    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_smo_pagesplit_root>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_pagesplit_root>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl.sys_txid_);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kParent, record_impl.parent_);
    json_obj.AddUint64(kNewLhs, record_impl.new_lhs_);
    json_obj.AddUint64(kNewRhs, record_impl.new_rhs_);
    json_obj.AddUint64(kSepSlot, record_impl.sep_slot_);
    json_obj.AddUint64(kSepSize, record_impl.sep_size_);
    json_obj.AddBool(kSepTruncated, record_impl.sep_truncated_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_smo_pagesplit_nonroot>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_pagesplit_nonroot>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl.sys_txid_);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kParent, record_impl.parent_);
    json_obj.AddUint64(kNewLhs, record_impl.new_lhs_);
    json_obj.AddUint64(kSepSlot, record_impl.sep_slot_);
    json_obj.AddUint64(kSepSize, record_impl.sep_size_);
    json_obj.AddBool(kSepTruncated, record_impl.sep_truncated_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_INSERT: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_insert>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_insert>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kKeySize, record_impl.key_size_);
    json_obj.AddUint64(kValSize, record_impl.val_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_UPDATE: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_update>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_update>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kKeySize, record_impl.key_size_);
    json_obj.AddUint64(kUpdateDescSize, record_impl.update_desc_size_);
    json_obj.AddUint64(kDeltaSize, record_impl.delta_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_REMOVE: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_remove>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_remove>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kKeySize, record_impl.key_size_);
    json_obj.AddUint64(kValSize, record_impl.val_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_ABORT: {
    utils::JsonObj json_obj;
    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_abort>::kName, json_obj);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_COMPLETE: {
    utils::JsonObj json_obj;
    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_complete>::kName, json_obj);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_INSERT: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_tx_insert>(record);

    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_insert>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kPrevWorkerId, record_impl.prev_wid_);
    json_obj.AddUint64(kPrevTxId, record_impl.prev_txid_);
    json_obj.AddUint64(kPrevCmd, record_impl.prev_cmd_id_);
    json_obj.AddUint64(kKeySize, record_impl.key_size_);
    json_obj.AddUint64(kValSize, record_impl.val_size_);
    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_tx_remove>(record);

    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_remove>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kPrevWorkerId, record_impl.prev_wid_);
    json_obj.AddUint64(kPrevTxId, record_impl.prev_txid_);
    json_obj.AddUint64(kPrevCmd, record_impl.prev_cmd_id_);
    json_obj.AddUint64(kKeySize, record_impl.key_size_);
    json_obj.AddUint64(kValSize, record_impl.val_size_);
    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    utils::JsonObj json_obj;
    auto& record_impl = CastTo<lean_wal_tx_update>(record);

    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_update>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl.page_id_);
    json_obj.AddUint64(kPageVersion, record_impl.page_version_);
    json_obj.AddUint64(kPrevWorkerId, record_impl.prev_wid_);
    json_obj.AddUint64(kPrevTxId, record_impl.prev_txid_);
    json_obj.AddUint64(kXorCmdId, record_impl.xor_cmd_id_);
    json_obj.AddUint64(kKeySize, record_impl.key_size_);
    json_obj.AddUint64(kUpdateDescSize, record_impl.update_desc_size_);
    json_obj.AddUint64(kDeltaSize, record_impl.delta_size_);

    return json_obj.Serialize();
  }
  default: {
    assert(false && "Unsupported WAL record type for text format");
    return "";
  }
  }
}

uint32_t WalSerde::Crc32Masked(const lean_wal_record& record) {
  auto* buffer = reinterpret_cast<const uint8_t*>(&record);
  auto header_size = sizeof(lean_wal_record);
  return utils::Crc32Calculator()
      .Update(buffer, header_size - 4)
      .Update(buffer + header_size, record.size_ - header_size)
      .Mask();
}

} // namespace leanstore