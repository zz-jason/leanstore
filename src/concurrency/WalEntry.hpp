#pragma once

#include "concurrency/Transaction.hpp"
#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstddef>
#include <iostream>
#include <string>

namespace leanstore {
namespace cr {

#define DO_WITH_WAL_ENTRY_TYPES(ACTION, ...)                                   \
  ACTION(kTxStart, "kTxStart", __VA_ARGS__)                                    \
  ACTION(kTxCommit, "kTxCommit", __VA_ARGS__)                                  \
  ACTION(kTxAbort, "kTxAbort", __VA_ARGS__)                                    \
  ACTION(kTxFinish, "kTxFinish", __VA_ARGS__)                                  \
  ACTION(kComplex, "kComplex", __VA_ARGS__)                                    \
  ACTION(kCarriageReturn, "kCarriageReturn", __VA_ARGS__)

#define DECR_WAL_ENTRY_TYPE(type, type_name, ...) type,
#define WAL_ENTRY_TYPE_NAME(type, type_name, ...)                              \
  case Type::type:                                                             \
    return type_name;

/// The basic WAL record representation, there are two kinds of WAL entries:
/// 1. WalEntrySimple, whose type might be: kTxStart, kTxCommit, kTxAbort
/// 2. WalEntryComplex, whose type is kComplex
class WalEntry {
public:
  enum class Type : uint8_t { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

public:
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

public:
  std::string TypeName();

  void InitTxInfo(Transaction* tx, WORKERID workerId) {
    mTxId = tx->mStartTs;
    mWorkerId = workerId;
  }

  virtual std::unique_ptr<rapidjson::Document> ToJson();

  uint32_t ComputeCRC32() const {
    // auto startOffset = offsetof(WalEntry, lsn);
    auto startOffset = ptrdiff_t(&mLsn) - ptrdiff_t(this);
    const auto* src = reinterpret_cast<const uint8_t*>(this) + startOffset;
    auto srcSize = mSize - startOffset;
    auto crc32 = utils::CRC(src, srcSize);
    return crc32;
  }

  void CheckCRC() const {
    auto actualCRC = ComputeCRC32();
    if (mCrc32 != actualCRC) {
      auto doc = const_cast<WalEntry*>(this)->ToJson();
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      doc->Accept(writer);
      LOG(FATAL) << "CRC32 mismatch"
                 << ", this=" << (void*)this << ", actual=" << actualCRC
                 << ", expected=" << mCrc32
                 << ", walJson=" << buffer.GetString();
    }
  }
};

class WalEntrySimple : public WalEntry {
public:
  WalEntrySimple(LID lsn, uint64_t size, Type type)
      : WalEntry(lsn, size, type) {
  }
};

class WalEntryComplex : public WalEntry {
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

  virtual std::unique_ptr<rapidjson::Document> ToJson() override;
};

// -----------------------------------------------------------------------------
// WalEntry
// -----------------------------------------------------------------------------

inline std::string WalEntry::TypeName() {
  switch (mType) {
    DO_WITH_WAL_ENTRY_TYPES(WAL_ENTRY_TYPE_NAME);
  default:
    return "Unknow WAL entry type";
  }
}

inline std::unique_ptr<rapidjson::Document> WalEntry::ToJson() {
  auto doc = std::make_unique<rapidjson::Document>();
  doc->SetObject();

  // crc
  {
    rapidjson::Value member;
    member.SetUint(mCrc32);
    doc->AddMember("CRC", member, doc->GetAllocator());
  }

  // lsn
  {
    rapidjson::Value member;
    member.SetUint64(mLsn);
    doc->AddMember("LSN", member, doc->GetAllocator());
  }

  // size
  {
    rapidjson::Value member;
    member.SetUint64(mSize);
    doc->AddMember("size", member, doc->GetAllocator());
  }

  // type
  {
    auto typeName = TypeName();
    rapidjson::Value member;
    member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
    doc->AddMember("type", member, doc->GetAllocator());
  }

  // txId
  {
    rapidjson::Value member;
    member.SetUint64(mTxId);
    doc->AddMember("mTxId", member, doc->GetAllocator());
  }

  // workerId
  {
    rapidjson::Value member;
    member.SetUint64(mWorkerId);
    doc->AddMember("mWorkerId", member, doc->GetAllocator());
  }

  // prev_lsn_in_tx
  {
    rapidjson::Value member;
    member.SetUint64(mPrevLSN);
    doc->AddMember("mPrevLSN", member, doc->GetAllocator());
  }

  return doc;
}

#undef DECR_WAL_ENTRY_TYPE
#undef WAL_ENTRY_TYPE_NAME

// -----------------------------------------------------------------------------
// WalEntryComplex
// -----------------------------------------------------------------------------

inline std::unique_ptr<rapidjson::Document> WalEntryComplex::ToJson() {
  auto doc = WalEntry::ToJson();

  // psn
  {
    rapidjson::Value member;
    member.SetUint64(mGsn);
    doc->AddMember("mGsn", member, doc->GetAllocator());
  }

  // treeId
  {
    rapidjson::Value member;
    member.SetInt64(mTreeId);
    doc->AddMember("mTreeId", member, doc->GetAllocator());
  }

  // pageId
  {
    rapidjson::Value member;
    member.SetUint64(mPageId);
    doc->AddMember("mPageId", member, doc->GetAllocator());
  }

  return doc;
}

} // namespace cr
} // namespace leanstore
