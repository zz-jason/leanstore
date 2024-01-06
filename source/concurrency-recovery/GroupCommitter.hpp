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

class GroupCommitter : public leanstore::utils::UserThread {
public:
  /// File descriptor of the underlying WAL file.
  const s32 mWalFd;

  /// Start file offset of the next WALEntry.
  u64 mWalSize;

  /// All the workers.
  std::vector<Worker*>& mWorkers;

public:
  GroupCommitter(s32 walFd, std::vector<Worker*>& workers)
      : UserThread("group-committer"), mWalFd(walFd), mWalSize(0),
        mWorkers(workers) {
  }

  virtual ~GroupCommitter() override = default;

protected:
  virtual void runImpl() override;
};

} // namespace cr
} // namespace leanstore
