#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "utils/AsyncIo.hpp"
#include "utils/UserThread.hpp"

#include <glog/logging.h>

#include <string>

#include <libaio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

namespace leanstore {
namespace cr {

class Worker;
class WalFlushReq;

class GroupCommitter : public leanstore::utils::UserThread {
public:
  leanstore::LeanStore* mStore;

  /// File descriptor of the underlying WAL file.
  const int32_t mWalFd;

  /// Start file offset of the next WalEntry.
  uint64_t mWalSize;

  /// The minimum flushed GSN among all worker threads. Transactions whose max
  /// observed GSN not larger than it can be committed safely.
  std::atomic<uint64_t> mGlobalMinFlushedGSN;

  /// The maximum flushed GSN among all worker threads in each group commit
  /// round. It is updated by the group commit thread and used to update the GCN
  /// counter of the current worker thread to prevent GSN from skewing and
  /// undermining RFA.
  std::atomic<uint64_t> mGlobalMaxFlushedGSN;

  /// All the workers.
  std::vector<Worker*>& mWorkers;

  utils::AsyncIo mAIo;

public:
  GroupCommitter(leanstore::LeanStore* store, int32_t walFd,
                 std::vector<Worker*>& workers, int cpu)
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
  /// Phase 1: Prepare IOCBs
  ///
  /// libaio is used to batch all log writes, these log writes are then
  /// submitted using a single system call.
  ///
  /// @param[out] minFlushedGSN the min flushed GSN among all the wal records
  /// @param[out] maxFlushedGSN the max flushed GSN among all the wal records
  /// @param[out] minFlushedTxId the min flushed transaction ID
  /// @param[out] numRfaTxs number of transactions without dependency
  /// @param[out] walFlushReqCopies snapshot of the flush requests
  void prepareIOCBs(uint64_t& minFlushedGSN, uint64_t& maxFlushedGSN,
                    TXID& minFlushedTxId, std::vector<uint64_t>& numRfaTxs,
                    std::vector<WalFlushReq>& walFlushReqCopies);

  /// Phase 2: write all the prepared IOCBs
  void writeIOCBs();

  /// Phase 3: commit transactions
  ///
  /// @param[in] minFlushedGSN the min flushed GSN among all the wal records
  /// @param[in] maxFlushedGSN the max flushed GSN among all the wal records
  /// @param[in] minFlushedTxId the min flushed transaction ID
  /// @param[in] numRfaTxs number of transactions without dependency
  /// @param[in] walFlushReqCopies snapshot of the flush requests
  void commitTXs(uint64_t minFlushedGSN, uint64_t maxFlushedGSN,
                 TXID minFlushedTxId, const std::vector<uint64_t>& numRfaTxs,
                 const std::vector<WalFlushReq>& walFlushReqCopies);

  void setUpIOCB(uint8_t* buf, uint64_t lower, uint64_t upper);
};

} // namespace cr
} // namespace leanstore
