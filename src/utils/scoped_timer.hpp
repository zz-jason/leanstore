#pragma once

#include <chrono>

namespace leanstore {

template <typename Callback>
class ScopedTimer {
public:
  ScopedTimer(Callback&& cb)
      : start_(std::chrono::steady_clock::now()),
        callback_(std::forward<Callback>(cb)) {
  }

  ~ScopedTimer() {
    auto end = std::chrono::steady_clock::now();
    auto duration_ms =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count() / 1000.0;
    callback_(duration_ms);
  }

private:
  std::chrono::steady_clock::time_point start_;
  Callback callback_;
};

} // namespace leanstore