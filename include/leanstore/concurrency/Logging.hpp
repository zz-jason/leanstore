#pragma once

#include "leanstore/Units.hpp"
#include "leanstore/concurrency/WalBuffer.hpp"

#include <atomic>
#include <cstdint>

#include <unistd.h>

namespace leanstore::cr {

//! forward declarations
class WalEntryComplex;

template <typename T>
class WalPayloadHandler;

//! Helps to transaction concurrenct control and write-ahead logging.
class Logging {
public:
  WalBuffer mWalBuffer;

  //! The active complex WalEntry for the current transaction, usually used for insert, update,
  //! delete, or btree related operations.
  //! NOTE: only effective during transaction processing.
  WalEntryComplex* mActiveWALEntryComplex;

  //! Used to track the write order of wal entries.
  LID mLsnClock = 0;

  LID mPrevLSN;

  //! The maximum committed system transasction ID on the worker. A system transaction is committed
  //! only when all its WAL records are written and flushed to disk.
  std::atomic<TXID> mActiveSysTx = 0;

  inline static std::atomic<TXID> sGlobalMinCommittedSysTx = 0;

public:
  Logging(uint64_t walBufferCapacity, std::string walFilePath, uint64_t ioBatchSize,
          bool createFromScratch)
      : mWalBuffer(walBufferCapacity, std::move(walFilePath), ioBatchSize, createFromScratch) {
  }

  ~Logging() = default;

  void WriteWalTxAbort();
  void WriteWalTxFinish();

  template <typename T, typename... Args>
  WalPayloadHandler<T> ReserveWALEntryComplex(uint64_t payloadSize, PID pageId, LID psn,
                                              TREEID treeId, Args&&... args);
};

} // namespace leanstore::cr