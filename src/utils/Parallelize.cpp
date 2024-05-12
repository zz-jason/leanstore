#include "Parallelize.hpp"

#include "utils/Log.hpp"
#include "utils/UserThread.hpp"

#include <functional>
#include <thread>
#include <vector>

namespace leanstore::utils {

void Parallelize::Range(
    uint64_t numThreads, uint64_t numJobs,
    std::function<void(uint64_t threadId, uint64_t jobBegin, uint64_t jobEnd)>
        jobHandler) {
  const uint64_t jobsPerThread = numJobs / numThreads;
  LS_DCHECK(jobsPerThread > 0, "Jobs per thread must be > 0");

  for (uint64_t i = 0; i < numThreads; i++) {
    uint64_t begin = (i * jobsPerThread);
    uint64_t end = begin + (jobsPerThread);
    if (i == numThreads - 1) {
      end = numJobs;
    }
    jobHandler(i, begin, end);
  }
}

void Parallelize::ParallelRange(
    uint64_t numJobs,
    std::function<void(uint64_t jobBegin, uint64_t jobEnd)> jobHandler) {
  auto* store = tlsStore;
  std::vector<std::thread> threads;
  uint64_t numThread = std::thread::hardware_concurrency();
  uint64_t jobsPerThread = numJobs / numThread;
  uint64_t numRemaining = numJobs % numThread;
  uint64_t numProceedTasks = 0;
  if (jobsPerThread < numThread) {
    numThread = numRemaining;
  }

  // To balance the workload among all threads:
  // - the first numRemaining threads process jobsPerThread+1 tasks
  // - other threads process jobsPerThread tasks
  for (uint64_t i = 0; i < numThread; i++) {
    uint64_t begin = numProceedTasks;
    uint64_t end = begin + jobsPerThread;
    if (numRemaining > 0) {
      end++;
      numRemaining--;
    }
    numProceedTasks = end;
    threads.emplace_back(
        [&](uint64_t begin, uint64_t end) {
          tlsStore = store;
          jobHandler(begin, end);
        },
        begin, end);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

} // namespace leanstore::utils
