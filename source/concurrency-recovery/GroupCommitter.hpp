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

  /// Phase 1: Prepare pwrite requests
  ///
  /// We use the asynchronous IO interface from libaio to batch all log writes
  /// and submit them using a single system call. Once the writes are done, we
  /// flush the block device with fsync to make sure that the log records we
  /// have just written are durable.
  ///
  /// @param[out] numIOCBS number of prepared IOCBs
  /// @param[out] minFlushedGSN
  /// @param[out] maxFlushedGSN
  /// @param[out] minFlushedCommitTs
  /// @param[out] numRfaTxs
  /// @param[out] walFlushReqCopies
  void prepareIOCBs(s32& numIOCBs, u64& minFlushedGSN, u64& maxFlushedGSN,
                    TXID& minFlushedCommitTs, std::vector<u64>& numRfaTxs,
                    std::vector<WalFlushReq>& walFlushReqCopies);

  /// Phase 2: write all the prepared IOCBs
  /// @param[in] numIOCBs number of IOCBs to write
  void writeIOCBs(s32 numIOCBs);

  /// Phase 3: calculate the new safe set of transactions that are hardened
  /// and ready, signal their commit to the client.
  ///
  /// With this information in hand, we can commit the pre-committed
  /// transactions in each worker that have their own log and their
  /// dependencies hardened.
  void commitTXs(u64 minFlushedGSN, u64 maxFlushedGSN, TXID minFlushedCommitTs,
                 const std::vector<u64>& numRfaTxs,
                 const std::vector<WalFlushReq>& walFlushReqCopies);

  void setUpIOCB(s32 ioSlot, u8* buf, u64 lower, u64 upper);
};

} // namespace cr
} // namespace leanstore
