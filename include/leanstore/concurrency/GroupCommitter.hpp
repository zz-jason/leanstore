#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/utils/AsyncIo.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <atomic>
#include <string>

#include <libaio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

namespace leanstore::cr {

class WorkerContext;
class WalFlushReq;

//! The group committer thread is responsible for committing transactions in batches. It collects
//! wal records from all the worker threads, writes them to the wal file with libaio, and determines
//! the commitable transactions based on the min flushed GSN and min flushed transaction ID.
class GroupCommitter : public leanstore::utils::UserThread {
public:
  leanstore::LeanStore* mStore;

  //! File descriptor of the underlying WAL file.
  const int32_t mWalFd;

  //! Start file offset of the next WalEntry.
  uint64_t mWalSize;

  //! The minimum flushed system transaction ID among all worker threads. User transactions whose
  //! max observed system transaction ID not larger than it can be committed safely.
  std::atomic<TXID> mGlobalMinFlushedSysTx;

  //! All the workers.
  std::vector<WorkerContext*>& mWorkerCtxs;

  //! The libaio wrapper.
  utils::AsyncIo mAIo;

public:
  GroupCommitter(leanstore::LeanStore* store, int32_t walFd, std::vector<WorkerContext*>& workers,
                 int cpu)
      : UserThread(store, "GroupCommitter", cpu),
        mStore(store),
        mWalFd(walFd),
        mWalSize(0),
        mGlobalMinFlushedSysTx(0),
        mWorkerCtxs(workers),
        mAIo(workers.size() * 2 + 2) {
  }

  virtual ~GroupCommitter() override = default;

protected:
  virtual void runImpl() override;

private:
  //! Phase 1: collect wal records from all the worker threads. Collected wal records are written to
  //! libaio IOCBs.
  //!
  //! @param[out] minFlushedSysTx the min flushed system transaction ID
  //! @param[out] minFlushedUsrTx the min flushed user transaction ID
  //! @param[out] numRfaTxs number of transactions without dependency
  //! @param[out] walFlushReqCopies snapshot of the flush requests
  void collectWalRecords(TXID& minFlushedSysTx, TXID& minFlushedUsrTx,
                         std::vector<uint64_t>& numRfaTxs,
                         std::vector<WalFlushReq>& walFlushReqCopies);

  //! Phase 2: write all the collected wal records to the wal file with libaio.
  void flushWalRecords();

  //! Phase 3: determine the commitable transactions based on minFlushedGSN and minFlushedTxId.
  //!
  //! @param[in] minFlushedSysTx the min flushed system transaction ID
  //! @param[in] minFlushedUsrTx the min flushed user transaction ID
  //! @param[in] numRfaTxs number of transactions without dependency
  //! @param[in] walFlushReqCopies snapshot of the flush requests
  void determineCommitableTx(TXID minFlushedSysTx, TXID minFlushedUsrTx,
                             const std::vector<uint64_t>& numRfaTxs,
                             const std::vector<WalFlushReq>& walFlushReqCopies);

  //! Append a wal entry to libaio IOCBs.
  //!
  //! @param[in] buf the wal entry buffer
  //! @param[in] lower the begin offset of the wal entry in the buffer
  //! @param[in] upper the end offset of the wal entry in the buffer
  void append(uint8_t* buf, uint64_t lower, uint64_t upper);
};

} // namespace leanstore::cr
