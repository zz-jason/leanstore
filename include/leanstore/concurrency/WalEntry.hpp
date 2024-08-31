#pragma once

#include "leanstore/Units.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"

#include <cstdint>
#include <string>

namespace leanstore::cr {

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

//! The basic WAL record representation, there are two kinds of WAL entries: 1. WalEntryComplex,
//! whose type is kComplex
///
//! EalEntry size is critical to the write performance, packed attribute is used and virtual
//! functions are avoided to make the size as small as possible.
class __attribute__((packed)) WalEntry {
public:
  enum class Type : uint8_t { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

  //! Type of the WAL entry.
  Type mType;

public:
  WalEntry() = default;

  WalEntry(Type type) : mType(type) {
  }

  std::string TypeName() const;

  //! Returns the size of the WalEntry, including all the payloads.
  static size_t Size(const WalEntry* entry);
};

// maybe it can also be removed, we can use the first compensation log to
// indicate transaction abort
class __attribute__((packed)) WalTxAbort : public WalEntry {
public:
  TXID mTxId;

  explicit WalTxAbort(TXID txId) : WalEntry(Type::kTxAbort), mTxId(txId) {
  }
};

class __attribute__((packed)) WalTxFinish : public WalEntry {
public:
  TXID mTxId;

  explicit WalTxFinish(TXID txId) : WalEntry(Type::kTxFinish), mTxId(txId) {
  }
};

class __attribute__((packed)) WalCarriageReturn : public WalEntry {
public:
  uint16_t mSize;

  explicit WalCarriageReturn(uint16_t size) : WalEntry(Type::kCarriageReturn), mSize(size) {
  }
};

class __attribute__((packed)) WalEntryComplex : public WalEntry {
public:
  //! Crc of the whole WalEntry, including all the payloads.
  uint32_t mCrc32;

  //! The log sequence number of this WalEntry. The number is globally and monotonically increased.
  LID mLsn;

  //! Log sequence number for the previous WalEntry of the same transaction. 0 if it's the first WAL
  //! entry in the transaction.
  LID mPrevLSN;

  // Size of the whole WalEntry, including all the payloads. The entire WAL entry stays in the WAL
  // ring buffer of the current worker thread.
  uint16_t mSize;

  //! ID of the worker who executes the transaction and records the WalEntry.
  WORKERID mWorkerId;

  //! ID of the transaction who creates this WalEntry.
  TXID mTxId;

  //! Page sequence number of the WalEntry.
  uint64_t mPsn;

  //! The page ID of the WalEntry, used to identify the btree node together with
  //! btree ID
  PID mPageId;

  //! The btree ID of the WalEntry, used to identify the btree node together
  //! with page ID.
  TREEID mTreeId;

  //! Payload of the operation on the btree node, for example, insert,
  //! remove, update, etc.
  uint8_t mPayload[];

  WalEntryComplex() = default;

  WalEntryComplex(LID lsn, LID prevLsn, uint64_t size, WORKERID workerId, TXID txid, LID psn,
                  PID pageId, TREEID treeId)
      : WalEntry(Type::kComplex),
        mCrc32(0),
        mLsn(lsn),
        mPrevLSN(prevLsn),
        mSize(size),
        mWorkerId(workerId),
        mTxId(txid),
        mPsn(psn),
        mPageId(pageId),
        mTreeId(treeId) {
  }

  uint32_t ComputeCRC32() const {
    auto typeFieldSize = sizeof(Type);
    auto crc32FieldSize = sizeof(uint32_t);
    auto crcSkipSize = typeFieldSize + crc32FieldSize;
    const auto* src = reinterpret_cast<const uint8_t*>(this) + crcSkipSize;
    auto srcSize = mSize - crcSkipSize;
    return utils::CRC(src, srcSize);
  }

  void CheckCRC() const {
    auto actualCRC = ComputeCRC32();
    if (mCrc32 != actualCRC) {
      Log::Fatal("CRC32 mismatch, actual={}, expected={}", actualCRC, mCrc32);
    }
  }
};

// -----------------------------------------------------------------------------
// WalEntry
// -----------------------------------------------------------------------------

inline std::string WalEntry::TypeName() const {
  switch (mType) {
    DO_WITH_WAL_ENTRY_TYPES(WAL_ENTRY_TYPE_NAME);
  default:
    return "Unknow WAL entry type";
  }
}

#undef DECR_WAL_ENTRY_TYPE
#undef WAL_ENTRY_TYPE_NAME

inline size_t WalEntry::Size(const WalEntry* entry) {
  switch (entry->mType) {
  case WalEntry::Type::kComplex:
    return static_cast<const WalEntryComplex*>(entry)->mSize;
  case WalEntry::Type::kTxAbort:
    return sizeof(WalTxAbort);
  case WalEntry::Type::kTxFinish:
    return sizeof(WalTxFinish);
  case WalEntry::Type::kCarriageReturn:
    return static_cast<const WalCarriageReturn*>(entry)->mSize;
  default:
    Log::Fatal("Unknown WalEntry type: {}", static_cast<int>(entry->mType));
  }
  return 0;
}

const char kCrc32[] = "mCrc32";
const char kLsn[] = "mLsn";
const char kSize[] = "mSize";
const char kType[] = "mType";
const char kTxId[] = "mTxId";
const char kWorkerId[] = "mWorkerId";
const char kPrevLsn[] = "mPrevLsn";
const char kPsn[] = "mPsn";
const char kTreeId[] = "mTreeId";
const char kPageId[] = "mPageId";

} // namespace leanstore::cr
