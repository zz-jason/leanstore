#pragma once

#include <cstdint>

namespace leanstore {

class CoroEnv {
public:
  static constexpr int64_t kMaxCoroutinesPerThread = 256;
  static constexpr int64_t kStackSize = 8 << 20; // 8 MB
};

} // namespace leanstore