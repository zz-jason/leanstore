#pragma once

#include <thread>

namespace leanstore {
namespace utils {

class ThreadHolder {
private:
  std::thread mThread;

public:
  template <class... Args>
  explicit ThreadHolder(Args&&... args) : mThread(std::forward<Args>(args)...) {
  }

  ThreadHolder(ThreadHolder&& other) = default;

  ThreadHolder& operator=(ThreadHolder&& other) = default;

  ~ThreadHolder() {
    if (mThread.joinable()) {
      mThread.join();
    }
  }
};

} // namespace utils
} // namespace leanstore