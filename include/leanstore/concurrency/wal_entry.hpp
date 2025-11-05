#pragma once

#include "leanstore/common/portable.h"
#include "leanstore/common/types.h"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/misc.hpp"

#include <cstdint>
#include <string>

namespace leanstore {

#define DO_WITH_WAL_ENTRY_TYPES(ACTION, ...)                                                       \
  ACTION(kTxAbort, "kTxAbort", __VA_ARGS__)                                                        \
  ACTION(kTxFinish, "kTxFinish", __VA_ARGS__)                                                      \
  ACTION(kComplex, "kComplex", __VA_ARGS__)                                                        \
  ACTION(kCarriageReturn, "kCarriageReturn", __VA_ARGS__)

#define DECR_WAL_ENTRY_TYPE(type, type_name, ...) type,
#define WAL_ENTRY_TYPE_NAME(type, type_name, ...)                                                  \
  case Type::type:                                                                                 \
    return type_name;

// forward declaration
class WalTxAbort;
class WalTxFinish;
class WalCarriageReturn;
class WalEntryComplex;

/// The basic WAL record representation, there are two kinds of WAL entries: 1. WalEntryComplex,
/// whose type is kComplex
///
/// EalEntry size is critical to the write performance, packed attribute is used and virtual
/// functions are avoided to make the size as small as possible.
class PACKED WalEntry {
public:
  enum class Type : uint8_t { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

  /// Type of the WAL entry.
  Type type_;

public:
  WalEntry() = default;

  WalEntry(Type type) : type_(type) {
  }

  std::string TypeName() const;

  /// Returns the size of the WalEntry, including all the payloads.
  static size_t Size(const WalEntry* entry);
};

// maybe it can also be removed, we can use the first compensation log to
// indicate transaction abort
class PACKED WalTxAbort : public WalEntry {
public:
  lean_txid_t tx_id_;

  explicit WalTxAbort(lean_txid_t tx_id) : WalEntry(Type::kTxAbort), tx_id_(tx_id) {
  }
};

class PACKED WalTxFinish : public WalEntry {
public:
  lean_txid_t tx_id_;

  explicit WalTxFinish(lean_txid_t tx_id) : WalEntry(Type::kTxFinish), tx_id_(tx_id) {
  }
};

class PACKED WalCarriageReturn : public WalEntry {
public:
  uint16_t size_;

  explicit WalCarriageReturn(uint16_t size) : WalEntry(Type::kCarriageReturn), size_(size) {
  }
};

class PACKED WalEntryComplex : public WalEntry {
public:
  /// Crc of the whole WalEntry, including all the payloads.
  uint32_t crc32_;

  /// The log sequence number of this WalEntry. The number is globally and monotonically increased.
  lean_lid_t lsn_;

  /// Log sequence number for the previous WalEntry of the same transaction. 0 if it's the first WAL
  /// entry in the transaction.
  lean_lid_t prev_lsn_;

  // Size of the whole WalEntry, including all the payloads. The entire WAL entry stays in the WAL
  // ring buffer of the current worker thread.
  uint16_t size_;

  /// ID of the worker who executes the transaction and records the WalEntry.
  lean_wid_t worker_id_;

  /// ID of the transaction who creates this WalEntry.
  lean_txid_t tx_id_;

  /// Page sequence number of the WalEntry.
  uint64_t psn_;

  /// The page ID of the WalEntry, used to identify the btree node together with
  /// btree ID
  lean_pid_t page_id_;

  /// The btree ID of the WalEntry, used to identify the btree node together
  /// with page ID.
  lean_treeid_t tree_id_;

  /// Payload of the operation on the btree node, for example, insert,
  /// remove, update, etc.
  uint8_t payload_[];

  WalEntryComplex() = default;

  WalEntryComplex(lean_lid_t lsn, lean_lid_t prev_lsn, uint64_t size, lean_wid_t worker_id,
                  lean_txid_t txid, lean_lid_t psn, lean_pid_t page_id, lean_treeid_t tree_id)
      : WalEntry(Type::kComplex),
        crc32_(0),
        lsn_(lsn),
        prev_lsn_(prev_lsn),
        size_(size),
        worker_id_(worker_id),
        tx_id_(txid),
        psn_(psn),
        page_id_(page_id),
        tree_id_(tree_id) {
  }

  uint32_t ComputeCRC32() const {
    auto type_field_size = sizeof(Type);
    auto crc_field_size = sizeof(uint32_t);
    auto crc_skip_size = type_field_size + crc_field_size;
    const auto* src = reinterpret_cast<const uint8_t*>(this) + crc_skip_size;
    auto src_size = size_ - crc_skip_size;
    return utils::CRC(src, src_size);
  }

  void CheckCRC() const {
    auto actual_crc = ComputeCRC32();
    if (crc32_ != actual_crc) {
      Log::Fatal("CRC32 mismatch, actual={}, expected={}", actual_crc, crc32_);
    }
  }
};

// -----------------------------------------------------------------------------
// WalEntry
// -----------------------------------------------------------------------------

inline std::string WalEntry::TypeName() const {
  switch (type_) {
    DO_WITH_WAL_ENTRY_TYPES(WAL_ENTRY_TYPE_NAME);
  default:
    return "Unknow WAL entry type";
  }
}

#undef DECR_WAL_ENTRY_TYPE
#undef WAL_ENTRY_TYPE_NAME

inline size_t WalEntry::Size(const WalEntry* entry) {
  switch (entry->type_) {
  case WalEntry::Type::kComplex:
    return static_cast<const WalEntryComplex*>(entry)->size_;
  case WalEntry::Type::kTxAbort:
    return sizeof(WalTxAbort);
  case WalEntry::Type::kTxFinish:
    return sizeof(WalTxFinish);
  case WalEntry::Type::kCarriageReturn:
    return static_cast<const WalCarriageReturn*>(entry)->size_;
  default:
    Log::Fatal("Unknown WalEntry type: {}", static_cast<int>(entry->type_));
  }
  return 0;
}

} // namespace leanstore
