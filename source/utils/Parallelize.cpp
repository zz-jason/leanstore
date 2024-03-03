#include "Parallelize.hpp"

#include "shared-headers/Units.hpp"

#include <glog/logging.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace leanstore {
namespace utils {

void Parallelize::range(
    uint64_t numThreads, uint64_t n,
    std::function<void(uint64_t, uint64_t, uint64_t)> callback) {
  const uint64_t jobsPerThread = n / numThreads;
  DCHECK(jobsPerThread > 0) << "Jobs per thread must be > 0";
  for (uint64_t i = 0; i < numThreads; i++) {
    uint64_t begin = (i * jobsPerThread);
    uint64_t end = begin + (jobsPerThread);
    if (i == numThreads - 1) {
      end = n;
    }
    callback(i, begin, end);
  }
}

void Parallelize::parallelRange(
    uint64_t numTasks,
    std::function<void(uint64_t begin, uint64_t end)> callback) {
  std::vector<std::thread> threads;
  const uint64_t numAvailableThreads = std::thread::hardware_concurrency();
  const uint64_t numTasksPerThread = numTasks / numAvailableThreads;
  uint64_t numRemaining = numTasks % numAvailableThreads;
  uint64_t numProceedTasks = 0;
  DCHECK(numTasksPerThread > 0);

  // To balance the workload among all threads:
  // - the first numRemaining threads process numTasksPerThread+1 tasks
  // - other threads process numTasksPerThread tasks
  for (uint64_t i = 0; i < numAvailableThreads; i++) {
    uint64_t begin = numProceedTasks;
    uint64_t end = begin + numTasksPerThread;
    if (numRemaining > 0) {
      end++;
      numRemaining--;
    }
    numProceedTasks = end;
    threads.emplace_back(
        [&](uint64_t begin, uint64_t end) { callback(begin, end); }, begin,
        end);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

void Parallelize::parallelRange(uint64_t a, uint64_t b, uint64_t numThreads,
                                std::function<void(uint64_t i)> callback) {
  std::vector<std::thread> threads;
  std::mutex m;
  std::condition_variable cv;
  uint64_t numActiveThreads = 0;

  while (a <= b) {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return numActiveThreads < numThreads; });
    numActiveThreads++;
    threads.emplace_back(
        [&](uint64_t i) {
          callback(i);
          {
            std::unique_lock<std::mutex> lk(m);
            numActiveThreads--;
          }
          cv.notify_all();
        },
        a++);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

} // namespace utils
} // namespace leanstore
