#pragma once

#include "Units.hpp"
#include "Worker.hpp"
#include "utils/Misc.hpp"

#include "rapidjson/document.h"

#include <glog/logging.h>

#include <atomic>
#include <iostream>
#include <string>

namespace leanstore {
namespace cr {

#define DO_WITH_WAL_ENTRY_TYPES(ACTION, ...)                                   \
  ACTION(TX_START, "TX_START", __VA_ARGS__)                                    \
  ACTION(TX_COMMIT, "TX_COMMIT", __VA_ARGS__)                                  \
  ACTION(TX_ABORT, "TX_ABORT", __VA_ARGS__)                                    \
  ACTION(TX_FINISH, "TX_FINISH", __VA_ARGS__)                                  \
  ACTION(COMPLEX, "COMPLEX", __VA_ARGS__)                                      \
  ACTION(CARRIAGE_RETURN, "CARRIAGE_RETURN", __VA_ARGS__)

#define DECR_WAL_ENTRY_TYPE(type, type_name, ...) type,
#define WAL_ENTRY_TYPE_NAME(type, type_name, ...)                              \
  case TYPE::type:                                                             \
    return type_name;

/// The basic WAL record representation, there are two kinds of WAL entries:
/// 1. WALEntrySimple, whose type might be: TX_START, TX_COMMIT, TX_ABORT
/// 2. WALEntryComplex, whose type is COMPLEX
class WALEntry {
public:
  enum class TYPE : u8 { DO_WITH_WAL_ENTRY_TYPES(DECR_WAL_ENTRY_TYPE) };

public:
  /// Used for debuging purpose.
  u64 mMagicDebuging = 99;

  /// The log sequence number of this WALEntry. The number is globally and
  /// monotonically increased.
  ///
  /// TODO(jian.z): verify whether we can remove atomic value.
  std::atomic<LID> lsn;

  // Size of the whole WALEntry, including all the payloads. The entire WAL
  // entry stays in the WAL ring buffer of the current worker thread.
  u16 size;

  /// Type of the WAL entry.
  TYPE type;

  /// ID of the transaction who creates this WALEntry.
  TXID mTxId;

  /// Transaction mode.
  TX_MODE mTxMode;

  /// ID of the worker who executes the transaction and records the WALEntry.
  WORKERID mWorkerId;

  /// Log sequence number for the previous WALEntry of the same transaction. 0
  /// if it's the first WAL entry in the transaction.
  LID mPrevLSN = 0;

public:
  WALEntry() = default;

  WALEntry(LID lsn, u64 size, TYPE type)
      : mMagicDebuging(99), size(size), type(type) {
    this->lsn.store(lsn, std::memory_order_release);
  }

public:
  std::string TypeName();

  void InitTxInfo(Transaction* tx, WORKERID workerId) {
    mTxId = tx->mStartTs;
    mTxMode = tx->mTxMode;
    mWorkerId = workerId;
  }

  virtual std::unique_ptr<rapidjson::Document> ToJSON();

  void computeCRC() {
    mMagicDebuging = utils::CRC(reinterpret_cast<u8*>(this) + sizeof(u64),
                                size - sizeof(u64));
  }

  void checkCRC() const {
    auto actualCRC = utils::CRC(reinterpret_cast<const u8*>(this) + sizeof(u64),
                                size - sizeof(u64));
    LOG_IF(FATAL, mMagicDebuging != actualCRC)
        << "CRC checksum mismatch"
        << ", CRC recorded in the WALEntry: " << mMagicDebuging
        << ", CRC calculated based on the actual WALEntry: " << actualCRC;
  }
};

class WALEntrySimple : public WALEntry {
public:
  WALEntrySimple(LID lsn, u64 size, TYPE type) : WALEntry(lsn, size, type) {
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

  /// Payload of the operation on the btree node, for example, WALInsert,
  /// WALRemove, etc.
  u8 payload[];

public:
  WALEntryComplex() = default;

  WALEntryComplex(LID lsn, u64 size, LID psn, TREEID treeId, PID pageId)
      : WALEntry(lsn, size, TYPE::COMPLEX), mPSN(psn), mTreeId(treeId),
        mPageId(pageId) {
  }

  virtual std::unique_ptr<rapidjson::Document> ToJSON() override;
};

template <typename T> class WALPayloadHandler {
public:
  T* entry;       // payload of the active WAL
  u64 mTotalSize; // size of the whole WALEntry, including payloads
  u64 lsn;

public:
  WALPayloadHandler() = default;

  /// @brief Initialize a WALPayloadHandler
  /// @param entry the WALPayload object, should already being initialized
  /// @param size the total size of the WALEntry
  /// @param lsn the log sequence number of the WALEntry
  WALPayloadHandler(T* entry, u64 size, u64 lsn)
      : entry(entry), mTotalSize(size), lsn(lsn) {
  }

public:
  inline T* operator->() {
    return entry;
  }

  inline T& operator*() {
    return *entry;
  }

  void SubmitWal();
};

// -----------------------------------------------------------------------------
// WALEntry
// -----------------------------------------------------------------------------

inline std::string WALEntry::TypeName() {
  switch (type) {
    DO_WITH_WAL_ENTRY_TYPES(WAL_ENTRY_TYPE_NAME);
  default:
    return "Unknow WAL entry type";
  }
}

inline std::unique_ptr<rapidjson::Document> WALEntry::ToJSON() {
  auto doc = std::make_unique<rapidjson::Document>();
  doc->SetObject();

  // crc
  {
    rapidjson::Value member;
    member.SetUint64(mMagicDebuging);
    doc->AddMember("CRC", member, doc->GetAllocator());
  }

  // lsn
  {
    rapidjson::Value member;
    member.SetUint64(lsn);
    doc->AddMember("LSN", member, doc->GetAllocator());
  }

  // size
  {
    rapidjson::Value member;
    member.SetUint64(size);
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

inline std::unique_ptr<rapidjson::Document> WALEntryComplex::ToJSON() {
  auto doc = WALEntry::ToJSON();

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
