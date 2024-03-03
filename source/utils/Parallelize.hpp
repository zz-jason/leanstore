#pragma once

#include "shared-headers/Units.hpp"

#include <functional>

namespace leanstore {
namespace utils {
class Parallelize {
public:
  static void range(
      uint64_t threads_count, uint64_t n,
      std::function<void(uint64_t t_i, uint64_t begin, uint64_t end)> callback);
  static void parallelRange(uint64_t n,
                            std::function<void(uint64_t, uint64_t)>);
  // [begin, end]
  static void parallelRange(uint64_t begin, uint64_t end, uint64_t n_threads,
                            std::function<void(uint64_t)>);
};

} // namespace utils
} // namespace leanstore
