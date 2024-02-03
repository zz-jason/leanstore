#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <vector>

namespace leanstore {

class Histogram {
private:
  std::vector<double> mBounds;
  std::vector<std::atomic<uint64_t>> mCounts;

public:
  Histogram(std::vector<double> bounds)
      : mBounds(bounds),
        mCounts(bounds.size() + 1) {
    for (auto& count : mCounts) {
      count = 0;
    }
  }

  Histogram(double start, double end, double step)
      : mBounds(),
        mCounts((end - start) / step + 1) {
    for (double i = start; i < end; i += step) {
      mBounds.push_back(i);
    }
    for (auto& count : mCounts) {
      count = 0;
    }
  }

  void Observe(double value) {
    auto it = std::upper_bound(mBounds.begin(), mBounds.end(), value);
    mCounts[std::distance(mBounds.begin(), it)]++;
  }
};

} // namespace leanstore