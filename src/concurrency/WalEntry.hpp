#pragma once

#include "concurrency/Transaction.hpp"
#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <iostream>
#include <string>

namespace leanstore {
namespace cr {

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
class WalEntrySimple;
class WalEntryComplex;

/// The basic WAL record representation, there are two kinds of WAL entries:
/// 1. WalEntrySimple, whose type might be: kTxAbort, kTxFinish, kCarriageReturn
/// 2. WalEntryComplex, whose type is kComplex
///
/// EalEntry size is critical to the write performance, packed attribute is
/// used and virtual functions are avoided to make the size as small as
/// possible.
class __attribute__((packed)) WalEntry {
public:
  enum class Type : uint8_t { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

  /// Crc of the whole WalEntry, including all the payloads.
  uint32_t mCrc32 = 0;

  /// The log sequence number of this WalEntry. The number is globally and
  /// monotonically increased.
  LID mLsn;

  /// Log sequence number for the previous WalEntry of the same transaction. 0
  /// if it's the first WAL entry in the transaction.
  LID mPrevLSN = 0;

  // Size of the whole WalEntry, including all the payloads. The entire WAL
  // entry stays in the WAL ring buffer of the current worker thread.
  uint16_t mSize;

  /// Type of the WAL entry.
  Type mType;

  /// ID of the worker who executes the transaction and records the WalEntry.
  WORKERID mWorkerId;

  /// ID of the transaction who creates this WalEntry.
  TXID mTxId;

public:
  WalEntry() = default;

  WalEntry(LID lsn, uint64_t size, Type type)
      : mCrc32(0),
        mLsn(lsn),
        mSize(size),
        mType(type) {
  }

  std::string TypeName() const;

  void InitTxInfo(Transaction* tx, WORKERID workerId) {
    mTxId = tx->mStartTs;
    mWorkerId = workerId;
  }

  uint32_t ComputeCRC32() const {
    auto crc32FieldSize = sizeof(uint32_t);
    const auto* src = reinterpret_cast<const uint8_t*>(this) + crc32FieldSize;
    auto srcSize = mSize - crc32FieldSize;
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
      LOG(FATAL) << "CRC32 mismatch"
                 << ", this=" << (void*)this << ", actual=" << actualCRC
                 << ", expected=" << mCrc32
                 << ", walJson=" << buffer.GetString();
    }
  }

  /// Convert the WalEntry to a JSON document.
  static void ToJson(const WalEntry* entry, rapidjson::Document* resultDoc);

  /// Convert the WalEntry to a JSON string.
  static std::string ToJsonString(const WalEntry* entry);

private:
  static void toJson(const WalEntrySimple* entry, rapidjson::Document* doc);

  static void toJson(const WalEntryComplex* entry, rapidjson::Document* doc);
};

class WalEntrySimple : public WalEntry {
public:
  WalEntrySimple(LID lsn, uint64_t size, Type type)
      : WalEntry(lsn, size, type) {
  }
};

class __attribute__((packed)) WalEntryComplex : public WalEntry {
public:
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

public:
  WalEntryComplex() = default;

  WalEntryComplex(LID lsn, uint64_t size, LID gsn, TREEID treeId, PID pageId)
      : WalEntry(lsn, size, Type::kComplex),
        mGsn(gsn),
        mPageId(pageId),
        mTreeId(treeId) {
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

inline void WalEntry::ToJson(const WalEntry* entry, rapidjson::Document* doc) {
  switch (entry->mType) {
  case Type::kTxAbort:
    [[fallthrough]];
  case Type::kTxFinish:
    [[fallthrough]];
  case Type::kCarriageReturn: {
    return toJson(reinterpret_cast<const WalEntrySimple*>(entry), doc);
  }
  case Type::kComplex: {
    return toJson(reinterpret_cast<const WalEntryComplex*>(entry), doc);
  }
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

inline void WalEntry::toJson(const WalEntrySimple* entry,
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

  // type
  {
    auto typeName = entry->TypeName();
    rapidjson::Value member;
    member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
    doc->AddMember(kType, member, doc->GetAllocator());
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
}

inline void WalEntry::toJson(const WalEntryComplex* entry,
                             rapidjson::Document* doc) {
  // collect the common fields
  auto* simpleEntry = reinterpret_cast<const WalEntrySimple*>(entry);
  toJson(simpleEntry, doc);

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

} // namespace cr
} // namespace leanstore
