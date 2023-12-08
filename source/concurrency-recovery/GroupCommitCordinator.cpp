#include "CRMG.hpp"

#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/CRCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/Misc.hpp"
#include "utils/Parallelize.hpp"

#include <libaio.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>

using namespace std::chrono_literals;
namespace leanstore {
namespace cr {

void CRManager::groupCommitCordinator() {
  std::thread log_cordinator([&]() {
    mRunningThreads++;
    std::string thread_name("leanstore_log_cordinator");
    pthread_setname_np(pthread_self(), thread_name.c_str());
    CPUCounters::registerThread(thread_name, false);

    LID minFlushedGsn; // For Remote Flush Avoidance
    LID maxFlushedGsn; // Sync all workers to this point
    TXID minFlushedCommitTs;

    while (mKeepRunning) {
      minFlushedGsn = std::numeric_limits<LID>::max();
      maxFlushedGsn = 0;
      minFlushedCommitTs = std::numeric_limits<TXID>::max();

      if (FLAGS_wal_fsync) {
        fdatasync(mWalFd);
      }

      sFsyncCounter++;
      sFsyncCounter.notify_all();

      for (WORKERID workerId = 0; workerId < mNumWorkerThreads; workerId++) {
        LID flushedGsn = mWorkers[workerId]->mLogging.mFlushedGsn;
        TXID flushedCommitTs = mWorkers[workerId]->mLogging.mFlushedCommitTs;

        minFlushedGsn = std::min<LID>(minFlushedGsn, flushedGsn);
        maxFlushedGsn = std::max<LID>(maxFlushedGsn, flushedGsn);
        minFlushedCommitTs =
            std::min<TXID>(minFlushedCommitTs, flushedCommitTs);
      }

      Logging::UpdateMinFlushedCommitTs(minFlushedCommitTs);
      Logging::UpdateMinFlushedGsn(minFlushedGsn);
      Logging::UpdateMaxFlushedGsn(maxFlushedGsn);

      CRCounters::myCounters().gct_rounds += 1;
    }

    mRunningThreads--;
  });

  log_cordinator.detach();
}

} // namespace cr
} // namespace leanstore
