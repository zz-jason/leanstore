#include "leanstore/utils/Misc.hpp"

#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <crc32c/crc32c.h>

namespace leanstore::utils {

uint32_t CRC(const uint8_t* src, uint64_t size) {
  return crc32c::Crc32c(src, size);
}

Timer::Timer(std::atomic<uint64_t>& timeCounterUS) : mTimeCounterUS(timeCounterUS) {
  if (tlsStore->mStoreOption->mEnableTimeMeasure) {
    mStartTimePoint = std::chrono::high_resolution_clock::now();
  }
}

Timer::~Timer() {
  if (tlsStore->mStoreOption->mEnableTimeMeasure) {
    auto endTimePoint = std::chrono::high_resolution_clock::now();
    const uint64_t duration =
        std::chrono::duration_cast<std::chrono::microseconds>(endTimePoint - mStartTimePoint)
            .count();
    mTimeCounterUS += duration;
  }
}

} // namespace leanstore::utils
