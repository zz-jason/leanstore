#pragma once

#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <format>
#include <string>

namespace leanstore::cr {

#define DO_WITH_WAL_ENTRY_TYPES(ACTION, ...)                                   \
  ACTION(kTxAbort, "kTxAbort", __VA_ARGS__)                                    \
  ACTION(kTxFinish, "kTxFinish", __VA_ARGS__)                                  \
  ACTION(kComplex, "kComplex", __VA_ARGS__)                                    \
  ACTION(kCarriageReturn, "kCarriageReturn", __VA_ARGS__)

#define DECR_WAL_ENTRY_TYPE(type, type_name, ...) type,
#define WAL_ENTRY_TYPE_NAME(type, type_name, ...)                              \
  case Type::type:                                                             \
    return type_name;

// forward declaration
class WalTxAbort;
class WalTxFinish;
class WalCarriageReturn;
class WalEntryComplex;

/// The basic WAL record representation, there are two kinds of WAL entries:
/// 1. WalEntryComplex, whose type is kComplex
///
/// EalEntry size is critical to the write performance, packed attribute is
/// used and virtual functions are avoided to make the size as small as
/// possible.
class __attribute__((packed)) WalEntry {
public:
  enum class Type : uint8_t { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

  /// Type of the WAL entry.
  Type mType;

public:
  WalEntry() = default;

  WalEntry(Type type) : mType(type) {
  }

  std::string TypeName() const;

  /// Returns the size of the WalEntry, including all the payloads.
  static size_t Size(const WalEntry* entry);

  /// Convert the WalEntry to a JSON document.
  static void ToJson(const WalEntry* entry, rapidjson::Document* resultDoc);

  /// Convert the WalEntry to a JSON string.
  static std::string ToJsonString(const WalEntry* entry);

private:
  static void toJson(const WalTxAbort* entry, rapidjson::Document* doc);
  static void toJson(const WalTxFinish* entry, rapidjson::Document* doc);
  static void toJson(const WalCarriageReturn* entry, rapidjson::Document* doc);
  static void toJson(const WalEntryComplex* entry, rapidjson::Document* doc);
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

  explicit WalCarriageReturn(uint16_t size)
      : WalEntry(Type::kCarriageReturn),
        mSize(size) {
  }
};

class __attribute__((packed)) WalEntryComplex : public WalEntry {
public:
  /// Crc of the whole WalEntry, including all the payloads.
  uint32_t mCrc32;

  /// The log sequence number of this WalEntry. The number is globally and
  /// monotonically increased.
  LID mLsn;

  /// Log sequence number for the previous WalEntry of the same transaction. 0
  /// if it's the first WAL entry in the transaction.
  LID mPrevLSN;

  // Size of the whole WalEntry, including all the payloads. The entire WAL
  // entry stays in the WAL ring buffer of the current worker thread.
  uint16_t mSize;

  /// ID of the worker who executes the transaction and records the WalEntry.
  WORKERID mWorkerId;

  /// ID of the transaction who creates this WalEntry.
  TXID mTxId;

  /// Global sequence number of the WalEntry, indicate the global order of the
  /// WAL entry.
  uint64_t mGsn;

  /// The page ID of the WalEntry, used to identify the btree node together with
  /// btree ID
  PID mPageId;

  /// The btree ID of the WalEntry, used to identify the btree node together
  /// with page ID.
  TREEID mTreeId;

  /// Payload of the operation on the btree node, for example, insert,
  /// remove, update, etc.
  uint8_t mPayload[];

  WalEntryComplex() = default;

  WalEntryComplex(LID lsn, LID prevLsn, uint64_t size, WORKERID workerId,
                  TXID txid, LID gsn, PID pageId, TREEID treeId)
      : WalEntry(Type::kComplex),
        mCrc32(0),
        mLsn(lsn),
        mPrevLSN(prevLsn),
        mSize(size),
        mWorkerId(workerId),
        mTxId(txid),
        mGsn(gsn),
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
      rapidjson::Document doc(rapidjson::kObjectType);
      WalEntry::ToJson(this, &doc);
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      doc.Accept(writer);
      LOG(FATAL) << std::format(
          "CRC32 mismatch, actual={}, expected={}, walJson={}", actualCRC,
          mCrc32, buffer.GetString());
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
    return reinterpret_cast<const WalEntryComplex*>(&entry)->mSize;
  case WalEntry::Type::kTxAbort:
    return sizeof(WalTxAbort);
  case WalEntry::Type::kTxFinish:
    return sizeof(WalTxFinish);
  case WalEntry::Type::kCarriageReturn:
    return sizeof(WalCarriageReturn);
  }
}

const char kCrc32[] = "mCrc32";
const char kLsn[] = "mLsn";
const char kSize[] = "mSize";
const char kType[] = "mType";
const char kTxId[] = "mTxId";
const char kWorkerId[] = "mWorkerId";
const char kPrevLsn[] = "mPrevLsn";
const char kGsn[] = "mGsn";
const char kTreeId[] = "mTreeId";
const char kPageId[] = "mPageId";

inline void WalEntry::ToJson(const WalEntry* entry, rapidjson::Document* doc) {
  // type
  {
    auto typeName = entry->TypeName();
    rapidjson::Value member;
    member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
    doc->AddMember(kType, member, doc->GetAllocator());
  }

  switch (entry->mType) {
  case Type::kTxAbort:
    return ToJson(reinterpret_cast<const WalTxAbort*>(entry), doc);
  case Type::kTxFinish:
    return ToJson(reinterpret_cast<const WalTxFinish*>(entry), doc);
  case Type::kCarriageReturn:
    return toJson(reinterpret_cast<const WalCarriageReturn*>(entry), doc);
  case Type::kComplex:
    return toJson(reinterpret_cast<const WalEntryComplex*>(entry), doc);
  }
}

inline std::string WalEntry::ToJsonString(const WalEntry* entry) {
  rapidjson::Document doc(rapidjson::kObjectType);
  ToJson(entry, &doc);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

inline void WalEntry::toJson(const WalTxAbort* entry,
                             rapidjson::Document* doc) {
  // txid
  {
    rapidjson::Value member;
    member.SetUint64(entry->mTxId);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

inline void WalEntry::toJson(const WalTxFinish* entry,
                             rapidjson::Document* doc) {
  // txid
  {
    rapidjson::Value member;
    member.SetUint64(entry->mTxId);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

inline void WalEntry::toJson(const WalCarriageReturn* entry,
                             rapidjson::Document* doc) {
  // size
  {
    rapidjson::Value member;
    member.SetUint64(entry->mSize);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

inline void WalEntry::toJson(const WalEntryComplex* entry,
                             rapidjson::Document* doc) {
  // crc
  {
    rapidjson::Value member;
    member.SetUint(entry->mCrc32);
    doc->AddMember(kCrc32, member, doc->GetAllocator());
  }

  // lsn
  {
    rapidjson::Value member;
    member.SetUint64(entry->mLsn);
    doc->AddMember(kLsn, member, doc->GetAllocator());
  }

  // size
  {
    rapidjson::Value member;
    member.SetUint64(entry->mSize);
    doc->AddMember(kSize, member, doc->GetAllocator());
  }

  // txId
  {
    rapidjson::Value member;
    member.SetUint64(entry->mTxId);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }

  // workerId
  {
    rapidjson::Value member;
    member.SetUint64(entry->mWorkerId);
    doc->AddMember(kWorkerId, member, doc->GetAllocator());
  }

  // prev_lsn_in_tx
  {
    rapidjson::Value member;
    member.SetUint64(entry->mPrevLSN);
    doc->AddMember(kPrevLsn, member, doc->GetAllocator());
  }

  // psn
  {
    rapidjson::Value member;
    member.SetUint64(entry->mGsn);
    doc->AddMember(kGsn, member, doc->GetAllocator());
  }

  // treeId
  {
    rapidjson::Value member;
    member.SetInt64(entry->mTreeId);
    doc->AddMember(kTreeId, member, doc->GetAllocator());
  }

  // pageId
  {
    rapidjson::Value member;
    member.SetUint64(entry->mPageId);
    doc->AddMember(kPageId, member, doc->GetAllocator());
  }
}

} // namespace leanstore::cr
