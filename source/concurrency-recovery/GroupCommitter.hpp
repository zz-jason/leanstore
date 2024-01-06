#pragma once

#include "Config.hpp"
#include "Exceptions.hpp"
#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "utils/Misc.hpp"
#include "utils/Timer.hpp"
#include "utils/UserThread.hpp"

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>

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
  const s32 mWalFd;

  /// Start file offset of the next WALEntry.
  u64 mWalSize;

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
  GroupCommitter(s32 walFd, std::vector<Worker*>& workers, int cpu)
      : UserThread("GroupCommitter", cpu), mWalFd(walFd), mWalSize(0),
        mWorkers(workers), mIOContext(nullptr),
        mIOCBs(new iocb[FLAGS_worker_threads * 2 + 2]),
        mIOCBPtrs(new iocb*[FLAGS_worker_threads * 2 + 2]),
        mIOEvents(new io_event[FLAGS_worker_threads * 2 + 2]) {
    // setup AIO context
    const auto error = io_setup(FLAGS_worker_threads * 2 + 2, &mIOContext);
    LOG_IF(FATAL, error != 0) << "io_setup failed, error=" << error;
  }

  virtual ~GroupCommitter() override = default;

protected:
  virtual void runImpl() override;

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
  void prepareIOCBs(s32& numIOCBs, u64& minFlushedGSN, u64& maxFlushedGSN,
                    TXID& minFlushedTxId, std::vector<u64>& numRfaTxs,
                    std::vector<WalFlushReq>& walFlushReqCopies);

  /// Phase 2: write all the prepared IOCBs
  ///
  /// @param[in] numIOCBs number of IOCBs to write
  void writeIOCBs(s32 numIOCBs);

  /// Phase 3: commit transactions
  ///
  /// @param[in] minFlushedGSN the min flushed GSN among all the wal records
  /// @param[in] maxFlushedGSN the max flushed GSN among all the wal records
  /// @param[in] minFlushedTxId the min flushed transaction ID
  /// @param[in] numRfaTxs number of transactions without dependency
  /// @param[in] walFlushReqCopies snapshot of the flush requests
  void commitTXs(u64 minFlushedGSN, u64 maxFlushedGSN, TXID minFlushedTxId,
                 const std::vector<u64>& numRfaTxs,
                 const std::vector<WalFlushReq>& walFlushReqCopies);

  void setUpIOCB(s32 ioSlot, u8* buf, u64 lower, u64 upper);
};

} // namespace cr
} // namespace leanstore
