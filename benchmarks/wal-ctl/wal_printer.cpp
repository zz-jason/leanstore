#include "wal_printer.hpp"

#include "leanstore/common/wal_record.h"
#include "utils/json.hpp"
#include "utils/small_vector.hpp"
#include "utils/wal/wal_traits.hpp"

#include <cassert>
#include <cstring>
#include <format>
#include <iostream>
#include <sstream>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

namespace leanstore {

void WalPrinter::Run() {
  // open the file
  int fd = open(wal_path_.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    perror("open");
    exit(EXIT_FAILURE);
  }

  // read and print the records
  static constexpr size_t kHeaderSize = sizeof(lean_wal_record);
  uint8_t header_buf[kHeaderSize];
  uint64_t read_offset = 0ull;
  std::stringstream formatted_wal_record;

  while (true) {
    ssize_t bytes_read = pread(fd, header_buf, kHeaderSize, read_offset);

    // EOF
    if (bytes_read == 0) {
      break;
    }

    if (bytes_read < 0) {
      std::cerr << std::format("Error reading WAL record, {}: ", wal_path_);
      perror("read");
      exit(EXIT_FAILURE);
    }

    if (bytes_read < static_cast<ssize_t>(kHeaderSize)) {
      fprintf(stderr, "Incomplete WAL record header\n");
      exit(EXIT_FAILURE);
    }

    // parse header
    auto* tmp_record = reinterpret_cast<lean_wal_record*>(header_buf);
    assert(tmp_record->size_ >= kHeaderSize);

    // read the full record
    size_t full_size = tmp_record->size_;
    SmallBuffer256 full_record_buf(full_size);
    auto* record_buf = full_record_buf.Data();
    memcpy(record_buf, header_buf, kHeaderSize);
    bytes_read = pread(fd,
                       record_buf + kHeaderSize, // buffer to store the remaining data
                       full_size - kHeaderSize,  // size of the remaining data
                       read_offset + kHeaderSize // offset to read from
    );
    if (bytes_read < 0) {
      std::cerr << std::format("Error reading WAL record data, {}: ", wal_path_);
      perror("read");
      exit(EXIT_FAILURE);
    }

    // TODO: compute and compare crc32

    // print the record
    auto* current_record = reinterpret_cast<lean_wal_record*>(record_buf);
    std::cout << FormatWalRecord(current_record, print_format_) << std::endl;

    // advance the offset
    read_offset += full_size;
  }
}

WalPrinter::Format WalPrinter::FormatFromString(std::string_view format) {
  static const constexpr char* kWalPrintFormatNames[] = {
      "unknown",
      "text",
      "json",
  };

  std::string lower_format;
  lower_format.resize(format.size());
  std::transform(format.begin(), format.end(), lower_format.begin(), ::tolower);

  if (lower_format == kWalPrintFormatNames[static_cast<int>(Format::kText)]) {
    return Format::kText;
  }
  if (lower_format == kWalPrintFormatNames[static_cast<int>(Format::kJson)]) {
    return Format::kJson;
  }
  return Format::kUnknown;
}

std::string WalPrinter::FormatWalRecord(const lean_wal_record* record, Format format) {
  switch (format) {
  case Format::kText: {
    return FormatWalRecordAsText(record);
  }
  case Format::kJson: {
    return FormatWalRecordAsJson(record);
  }
  default: {
    assert(false && "Unsupported print format");
  }
  }
}

static constexpr auto kWalType = "type";
static constexpr auto kLsn = "lsn";
static constexpr auto kSize = "size";
static constexpr auto kSysTxId = "sys_tx";
static constexpr auto kPageId = "page_id";
static constexpr auto kPageVersion = "page_version";
static constexpr auto kBtreeId = "btree_id";
static constexpr auto kIsLeaf = "is_leaf";
static constexpr auto kParent = "parent";
static constexpr auto kNewLhs = "new_lhs";
static constexpr auto kNewRhs = "new_rhs";
static constexpr auto kSepSlot = "sep_slot";
static constexpr auto kSepSize = "sep_size";
static constexpr auto kSepTruncated = "sep_truncated";
static constexpr auto kKeySize = "key_size";
static constexpr auto kValSize = "val_size";
static constexpr auto kUpdateDescSize = "update_desc_size";
static constexpr auto kDeltaSize = "delta_size";
static constexpr auto kWorkerId = "worker";
static constexpr auto kTxId = "tx";
static constexpr auto kPrevLsn = "prev_lsn";
static constexpr auto kPrevWorkerId = "prev_worker";
static constexpr auto kPrevTxId = "prev_tx";
static constexpr auto kPrevCmd = "prev_cmd";
static constexpr auto kXorCmdId = "xor_cmd_id";

std::string WalPrinter::FormatWalRecordAsText(const lean_wal_record* record) {
  switch (record->type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN:
  case LEAN_WAL_TYPE_SMO_COMPLETE:
  case LEAN_WAL_TYPE_SMO_PAGENEW:
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT:
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT:
  case LEAN_WAL_TYPE_INSERT:
  case LEAN_WAL_TYPE_UPDATE:
  case LEAN_WAL_TYPE_REMOVE:
  case LEAN_WAL_TYPE_TX_ABORT:
  case LEAN_WAL_TYPE_TX_COMPLETE:
  case LEAN_WAL_TYPE_TX_INSERT:
  case LEAN_WAL_TYPE_TX_REMOVE:
  case LEAN_WAL_TYPE_TX_UPDATE:
  default: {
    assert(false && "Unsupported WAL record type for JSON format");
    return "";
  }
  }
}

std::string WalPrinter::FormatWalRecordAsJson(const lean_wal_record* record) {
  auto serialize_wal_base = [](const lean_wal_record* record, std::string_view type_name,
                               utils::JsonObj& json_obj) {
    json_obj.AddUint64(kLsn, record->lsn_);
    json_obj.AddUint64(kSize, record->size_);
    json_obj.AddString(kWalType, type_name);
  };

  auto serialize_wal_tx_base = [&serialize_wal_base](const lean_wal_record* record,
                                                     std::string_view type_name,
                                                     utils::JsonObj& json_obj) {
    serialize_wal_base(reinterpret_cast<const lean_wal_record*>(record), type_name, json_obj);
    auto* record_tx_base = reinterpret_cast<const lean_wal_tx_base*>(record);
    json_obj.AddUint64(kWorkerId, record_tx_base->wid_);
    json_obj.AddUint64(kTxId, record_tx_base->txid_);
    json_obj.AddUint64(kPrevLsn, record_tx_base->prev_lsn_);
  };

  switch (record->type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN: {
    utils::JsonObj json_obj;
    serialize_wal_base(record, WalRecordTraits<lean_wal_carriage_return>::kName, json_obj);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_COMPLETE: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_smo_complete*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_complete>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl->sys_txid_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_PAGENEW: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_smo_pagenew*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_pagenew>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl->sys_txid_);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kBtreeId, record_impl->btree_id_);
    json_obj.AddBool(kIsLeaf, record_impl->is_leaf_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_smo_pagesplit_root*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_pagesplit_root>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl->sys_txid_);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kParent, record_impl->parent_);
    json_obj.AddUint64(kNewLhs, record_impl->new_lhs_);
    json_obj.AddUint64(kNewRhs, record_impl->new_rhs_);
    json_obj.AddUint64(kSepSlot, record_impl->sep_slot_);
    json_obj.AddUint64(kSepSize, record_impl->sep_size_);
    json_obj.AddBool(kSepTruncated, record_impl->sep_truncated_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_smo_pagesplit_nonroot*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_smo_pagesplit_nonroot>::kName, json_obj);
    json_obj.AddUint64(kSysTxId, record_impl->sys_txid_);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kParent, record_impl->parent_);
    json_obj.AddUint64(kNewLhs, record_impl->new_lhs_);
    json_obj.AddUint64(kSepSlot, record_impl->sep_slot_);
    json_obj.AddUint64(kSepSize, record_impl->sep_size_);
    json_obj.AddBool(kSepTruncated, record_impl->sep_truncated_);

    return json_obj.Serialize();
  }

  case LEAN_WAL_TYPE_INSERT: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_insert*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_insert>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kKeySize, record_impl->key_size_);
    json_obj.AddUint64(kValSize, record_impl->val_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_UPDATE: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_update*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_update>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kKeySize, record_impl->key_size_);
    json_obj.AddUint64(kUpdateDescSize, record_impl->update_desc_size_);
    json_obj.AddUint64(kDeltaSize, record_impl->delta_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_REMOVE: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_remove*>(record);

    serialize_wal_base(record, WalRecordTraits<lean_wal_remove>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kKeySize, record_impl->key_size_);
    json_obj.AddUint64(kValSize, record_impl->val_size_);

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
    auto* record_impl = reinterpret_cast<const lean_wal_tx_insert*>(record);

    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_insert>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kPrevWorkerId, record_impl->prev_wid_);
    json_obj.AddUint64(kPrevTxId, record_impl->prev_txid_);
    json_obj.AddUint64(kPrevCmd, record_impl->prev_cmd_id_);
    json_obj.AddUint64(kKeySize, record_impl->key_size_);
    json_obj.AddUint64(kValSize, record_impl->val_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_REMOVE: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_tx_remove*>(record);

    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_remove>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kPrevWorkerId, record_impl->prev_wid_);
    json_obj.AddUint64(kPrevTxId, record_impl->prev_txid_);
    json_obj.AddUint64(kPrevCmd, record_impl->prev_cmd_id_);
    json_obj.AddUint64(kKeySize, record_impl->key_size_);
    json_obj.AddUint64(kValSize, record_impl->val_size_);

    return json_obj.Serialize();
  }
  case LEAN_WAL_TYPE_TX_UPDATE: {
    utils::JsonObj json_obj;
    auto* record_impl = reinterpret_cast<const lean_wal_tx_update*>(record);

    serialize_wal_tx_base(record, WalRecordTraits<lean_wal_tx_update>::kName, json_obj);
    json_obj.AddUint64(kPageId, record_impl->page_id_);
    json_obj.AddUint64(kPageVersion, record_impl->page_version_);
    json_obj.AddUint64(kPrevWorkerId, record_impl->prev_wid_);
    json_obj.AddUint64(kPrevTxId, record_impl->prev_txid_);
    json_obj.AddUint64(kXorCmdId, record_impl->xor_cmd_id_);
    json_obj.AddUint64(kKeySize, record_impl->key_size_);
    json_obj.AddUint64(kUpdateDescSize, record_impl->update_desc_size_);
    json_obj.AddUint64(kDeltaSize, record_impl->delta_size_);

    return json_obj.Serialize();
  }
  default: {
    assert(false && "Unsupported WAL record type for text format");
    return "";
  }
  }
}

} // namespace leanstore