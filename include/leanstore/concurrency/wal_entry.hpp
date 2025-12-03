#pragma once

#include "leanstore/common/portable.h"
#include "leanstore/common/types.h"
#include "leanstore/cpp/base/enum_traits.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/utils/misc.hpp"

#include <cstdint>

namespace leanstore {

#define LEAN_WAL_ENTRY_TYPE_LIST(ACTION)                                                           \
  ACTION(kTxAbort)                                                                                 \
  ACTION(kTxFinish)                                                                                \
  ACTION(kComplex)                                                                                 \
  ACTION(kCarriageReturn)

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
  enum class Type : uint8_t {
#define ACTION(type) type,
    LEAN_WAL_ENTRY_TYPE_LIST(ACTION)
#undef ACTION
  };

  /// Type of the WAL entry.
  Type type_;

public:
  WalEntry() = default;

  WalEntry(Type type) : type_(type) {
  }

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
  uint64_t page_version_;

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
        page_version_(psn),
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

/// Specialization of EnumTraits for WalEntry::Type
template <>
struct EnumTraits<WalEntry::Type> {
  /// Converts the enum value to its string representation.
  static std::string_view ToString(WalEntry::Type enum_item) {
#define ACTION(enum_item)                                                                          \
  case WalEntry::Type::enum_item: {                                                                \
    return #enum_item;                                                                             \
  }

    switch (enum_item) {
      LEAN_WAL_ENTRY_TYPE_LIST(ACTION);
    default:
      return "Unknown WalEntry::Type";
    }

#undef ACTION
  }

  /// Converts a string representation to its enum value.
  static Optional<WalEntry::Type> FromString(std::string_view str) {
#define ACTION(enum_item)                                                                          \
  if (str == #enum_item) {                                                                         \
    return WalEntry::Type::enum_item;                                                              \
  }

    LEAN_WAL_ENTRY_TYPE_LIST(ACTION);
    return std::nullopt;

#undef ACTION
  }

  static_assert(EnumTraitsRequired<WalEntry::Type>,
                "WalEntry::Type must satisfy EnumTraitsRequired");
};

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
