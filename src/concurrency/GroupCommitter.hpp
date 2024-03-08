#pragma once

#include "leanstore/Units.hpp"
#include "utils/UserThread.hpp"

#include <glog/logging.h>

#include <memory>
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
  /// File descriptor of the underlying WAL file.
  const int32_t mWalFd;

  /// Start file offset of the next WALEntry.
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

  /// IO context, used by libaio.
  io_context_t mIOContext;

  /// IO control blocks, used by libaio.
  std::unique_ptr<iocb[]> mIOCBs;

  /// IO control block address in mIOCBs, used by libaio.
  std::unique_ptr<iocb*[]> mIOCBPtrs;

  /// IO events, used by libaio.
  std::unique_ptr<io_event[]> mIOEvents;

public:
  GroupCommitter(int32_t walFd, std::vector<Worker*>& workers, int cpu)
      : UserThread("GroupCommitter", cpu),
        mWalFd(walFd),
        mWalSize(0),
        mGlobalMinFlushedGSN(0),
        mGlobalMaxFlushedGSN(0),
        mWorkers(workers),
        mIOContext(nullptr),
        mIOCBs(new iocb[workers.size() * 2 + 2]),
        mIOCBPtrs(new iocb*[workers.size() * 2 + 2]),
        mIOEvents(new io_event[workers.size() * 2 + 2]) {
    // setup AIO context
    const auto error = io_setup(workers.size() * 2 + 2, &mIOContext);
    LOG_IF(FATAL, error != 0) << "io_setup failed, error=" << error;
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
  /// @param[out] numIOCBS number of prepared IOCBs
  /// @param[out] minFlushedGSN the min flushed GSN among all the wal records
  /// @param[out] maxFlushedGSN the max flushed GSN among all the wal records
  /// @param[out] minFlushedTxId the min flushed transaction ID
  /// @param[out] numRfaTxs number of transactions without dependency
  /// @param[out] walFlushReqCopies snapshot of the flush requests
  void prepareIOCBs(int32_t& numIOCBs, uint64_t& minFlushedGSN,
                    uint64_t& maxFlushedGSN, TXID& minFlushedTxId,
                    std::vector<uint64_t>& numRfaTxs,
                    std::vector<WalFlushReq>& walFlushReqCopies);

  /// Phase 2: write all the prepared IOCBs
  ///
  /// @param[in] numIOCBs number of IOCBs to write
  void writeIOCBs(int32_t numIOCBs);

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

  void setUpIOCB(int32_t ioSlot, uint8_t* buf, uint64_t lower, uint64_t upper);
};

} // namespace cr
} // namespace leanstore
