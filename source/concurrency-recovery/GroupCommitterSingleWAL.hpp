#pragma once

#include "Units.hpp"
#include "utils/Defer.hpp"

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <thread>

namespace leanstore {
namespace cr {

class GroupCommitterSingleWAL {
private:
  std::string mThreadName;
  std::unique_ptr<std::thread> mThread;
  std::atomic<bool> mKeepRunning;

  /// File descriptor of the WAL file
  u64 mFD;

public:
  GroupCommitterSingleWAL(u64 fd) : mFD(fd) {
  }

  ~GroupCommitterSingleWAL() {
    Stop();
  }

public:
  /// Start the daemon group commit thread
  void Start() {
    if (mThread == nullptr) {
      mKeepRunning = true;
      mThread =
          std::make_unique<std::thread>(&GroupCommitterSingleWAL::Run, this);
      mThread->detach();
    }
  }

  /// Stop the daemon group commit thead
  void Stop() {
    mKeepRunning = false;
    if (mThread && mThread->joinable()) {
      mThread->join();
    }
    mThread = nullptr;
  }

  /// Thread loop
  void Run();

  /// Thread initialization, e.g. set thread name.
  void InitThread();

  /// Phase 1: write WAL records from every worker thread on SSD.
  ///
  /// We use the asynchronous IO interface from libaio to batch all log writes
  /// and submit them using a single system call. Once the writes are done, we
  /// flush the block device with fsync to make sure that the log records we
  /// have just written are durable.
  void CollectWALs();

  /// Phase 2: submit all log writes using a single system call.
  void FlushWALs();

  /// Phase 3: calculate the new safe set of transactions that are hardened
  /// and ready, signal their commit to the client.
  ///
  /// With this information in hand, we can commit the pre-committed
  /// transactions in each worker that have their own log and their
  /// dependencies hardened.
  void CommitTxs();

public:
  static inline std::unique_ptr<GroupCommitterSingleWAL> sInstance =
      std::make_unique<GroupCommitterSingleWAL>();
};

using Time = decltype(std::chrono::high_resolution_clock::now());

} // namespace cr
} // namespace leanstore