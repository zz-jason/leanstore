#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "utils/AsyncIo.hpp"
#include "utils/UserThread.hpp"

#include <string>

#include <libaio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

namespace leanstore::cr {

class Worker;
class WalFlushReq;

class GroupCommitter : public leanstore::utils::UserThread {
public:
  leanstore::LeanStore* mStore;

  //! File descriptor of the underlying WAL file.
  const int32_t mWalFd;

  //! Start file offset of the next WalEntry.
  uint64_t mWalSize;

  //! The minimum flushed GSN among all worker threads. Transactions whose max
  //! observed GSN not larger than it can be committed safely.
  std::atomic<uint64_t> mGlobalMinFlushedGSN;

  //! The maximum flushed GSN among all worker threads in each group commit
  //! round. It is updated by the group commit thread and used to update the GCN
  //! counter of the current worker thread to prevent GSN from skewing and
  //! undermining RFA.
  std::atomic<uint64_t> mGlobalMaxFlushedGSN;

  //! All the workers.
  std::vector<Worker*>& mWorkers;

  //! The libaio wrapper.
  utils::AsyncIo mAIo;

public:
  GroupCommitter(leanstore::LeanStore* store, int32_t walFd, std::vector<Worker*>& workers, int cpu)
      : UserThread(store, "GroupCommitter", cpu),
        mStore(store),
        mWalFd(walFd),
        mWalSize(0),
        mGlobalMinFlushedGSN(0),
        mGlobalMaxFlushedGSN(0),
        mWorkers(workers),
        mAIo(workers.size() * 2 + 2) {
  }

  virtual ~GroupCommitter() override = default;

protected:
  virtual void runImpl() override;

private:
  //! Phase 1: collect wal records from all the worker threads. Collected
  //! wal records are written to libaio IOCBs.
  //!
  //! @param[out] minFlushedGSN the min flushed GSN among all the wal records
  //! @param[out] maxFlushedGSN the max flushed GSN among all the wal records
  //! @param[out] minFlushedTxId the min flushed transaction ID
  //! @param[out] numRfaTxs number of transactions without dependency
  //! @param[out] walFlushReqCopies snapshot of the flush requests
  void collectWalRecords(uint64_t& minFlushedGSN, uint64_t& maxFlushedGSN, TXID& minFlushedTxId,
                         std::vector<uint64_t>& numRfaTxs,
                         std::vector<WalFlushReq>& walFlushReqCopies);

  //! Phase 2: write all the collected wal records to the wal file with libaio.
  void flushWalRecords();

  //! Phase 3: determine the commitable transactions based on minFlushedGSN
  //! and minFlushedTxId.
  //!
  //! @param[in] minFlushedGSN the min flushed GSN among all the wal records
  //! @param[in] maxFlushedGSN the max flushed GSN among all the wal records
  //! @param[in] minFlushedTxId the min flushed transaction ID
  //! @param[in] numRfaTxs number of transactions without dependency
  //! @param[in] walFlushReqCopies snapshot of the flush requests
  void determineCommitableTx(uint64_t minFlushedGSN, uint64_t maxFlushedGSN, TXID minFlushedTxId,
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
