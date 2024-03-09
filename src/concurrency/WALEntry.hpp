#pragma once

#include "concurrency/Transaction.hpp"
#include "leanstore/Units.hpp"
#include "utils/Misc.hpp"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <glog/logging.h>

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
  ACTION(kComplex, "kComplex", __VA_ARGS__)                                      \
  ACTION(kCarriageReturn, "kCarriageReturn", __VA_ARGS__)

#define DECR_WAL_ENTRY_TYPE(type, type_name, ...) type,
#define WAL_ENTRY_TYPE_NAME(type, type_name, ...)                              \
  case Type::type:                                                             \
    return type_name;

/// The basic WAL record representation, there are two kinds of WAL entries:
/// 1. WALEntrySimple, whose type might be: kTxStart, kTxCommit, kTxAbort
/// 2. WALEntryComplex, whose type is kComplex
class WALEntry {
public:
  enum class Type : uint8_t { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

public:
  /// Used for debuging purpose.
  uint32_t mCRC32 = 99;

  /// The log sequence number of this WALEntry. The number is globally and
  /// monotonically increased.
  LID mLsn;

  // Size of the whole WALEntry, including all the payloads. The entire WAL
  // entry stays in the WAL ring buffer of the current worker thread.
  uint16_t mSize;

  /// Type of the WAL entry.
  Type mType;

  /// ID of the transaction who creates this WALEntry.
  TXID mTxId;

  /// Transaction mode.
  TxMode mTxMode;

  /// ID of the worker who executes the transaction and records the WALEntry.
  WORKERID mWorkerId;

  /// Log sequence number for the previous WALEntry of the same transaction. 0
  /// if it's the first WAL entry in the transaction.
  LID mPrevLSN = 0;

public:
  WALEntry() = default;

  WALEntry(LID lsn, uint64_t size, Type type)
      : mCRC32(99),
        mLsn(lsn),
        mSize(size),
        mType(type) {
  }

public:
  std::string TypeName();

  void InitTxInfo(Transaction* tx, WORKERID workerId) {
    mTxId = tx->mStartTs;
    mTxMode = tx->mTxMode;
    mWorkerId = workerId;
  }

  virtual std::unique_ptr<rapidjson::Document> ToJson();

  uint32_t ComputeCRC32() const {
    // auto startOffset = offsetof(WALEntry, lsn);
    auto startOffset = ptrdiff_t(&mLsn) - ptrdiff_t(this);
    const auto* src = reinterpret_cast<const uint8_t*>(this) + startOffset;
    auto srcSize = mSize - startOffset;
    auto crc32 = utils::CRC(src, srcSize);
    return crc32;
  }

  void CheckCRC() const {
    auto actualCRC = ComputeCRC32();
    if (mCRC32 != actualCRC) {
      auto doc = const_cast<WALEntry*>(this)->ToJson();
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      doc->Accept(writer);
      LOG(FATAL) << "CRC32 mismatch"
                 << ", this=" << (void*)this << ", actual=" << actualCRC
                 << ", expected=" << mCRC32
                 << ", walJson=" << buffer.GetString();
    }
  }
};

class WALEntrySimple : public WALEntry {
public:
  WALEntrySimple(LID lsn, uint64_t size, Type type)
      : WALEntry(lsn, size, type) {
  }
};

class WALEntryComplex : public WALEntry {
public:
  /// Page sequence number of the WALEntry, indicate the page version this WAL
  /// entry is based on.
  LID mPSN;

  /// The btree ID of the WALEntry, used to identify the btree node together
  /// with page ID.
  TREEID mTreeId;

  /// The page ID of the WALEntry, used to identify the btree node together with
  /// btree ID
  PID mPageId;

  /// Payload of the operation on the btree node, for example, insert,
  /// remove, update, etc.
  uint8_t mPayload[];

public:
  WALEntryComplex() = default;

  WALEntryComplex(LID lsn, uint64_t size, LID psn, TREEID treeId, PID pageId)
      : WALEntry(lsn, size, Type::kComplex),
        mPSN(psn),
        mTreeId(treeId),
        mPageId(pageId) {
  }

  virtual std::unique_ptr<rapidjson::Document> ToJson() override;
};

// -----------------------------------------------------------------------------
// WALEntry
// -----------------------------------------------------------------------------

inline std::string WALEntry::TypeName() {
  switch (mType) {
    DO_WITH_WAL_ENTRY_TYPES(WAL_ENTRY_TYPE_NAME);
  default:
    return "Unknow WAL entry type";
  }
}

inline std::unique_ptr<rapidjson::Document> WALEntry::ToJson() {
  auto doc = std::make_unique<rapidjson::Document>();
  doc->SetObject();

  // crc
  {
    rapidjson::Value member;
    member.SetUint(mCRC32);
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

  // txMode
  {
    rapidjson::Value member;
    auto txModeStr = ToString(mTxMode);
    member.SetString(txModeStr.data(), txModeStr.size(), doc->GetAllocator());
    doc->AddMember("mTxMode", member, doc->GetAllocator());
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
// WALEntryComplex
// -----------------------------------------------------------------------------

inline std::unique_ptr<rapidjson::Document> WALEntryComplex::ToJson() {
  auto doc = WALEntry::ToJson();

  // psn
  {
    rapidjson::Value member;
    member.SetUint64(mPSN);
    doc->AddMember("mPSN", member, doc->GetAllocator());
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