#include "leanstore/utils/random_generator.hpp"

#include <atomic>
#include <cstdint>

namespace leanstore::utils {

namespace {
std::atomic<uint64_t> mt_counter = 0;
} // namespace

MersenneTwister::MersenneTwister(uint64_t seed) : mti_(s_nn + 1) {
  Init(seed + (mt_counter++));
}

void MersenneTwister::Init(uint64_t seed) {
  mt_[0] = seed;
  for (mti_ = 1; mti_ < s_nn; mti_++) {
    mt_[mti_] = (6364136223846793005ULL * (mt_[mti_ - 1] ^ (mt_[mti_ - 1] >> 62)) + mti_);
  }
}

uint64_t MersenneTwister::Rand() {
  uint64_t x;
  static const uint64_t kMag01[2] = {0ULL, s_matrix_a};

  if (mti_ >= s_nn) { /* generate s_nn words at one time */
    int i;
    for (i = 0; i < s_nn - s_mm; i++) {
      x = (mt_[i] & s_um) | (mt_[i + 1] & s_lm);
      mt_[i] = mt_[i + s_mm] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    for (; i < s_nn - 1; i++) {
      x = (mt_[i] & s_um) | (mt_[i + 1] & s_lm);
      mt_[i] = mt_[i + (s_mm - s_nn)] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    x = (mt_[s_nn - 1] & s_um) | (mt_[0] & s_lm);
    mt_[s_nn - 1] = mt_[s_mm - 1] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];

    mti_ = 0;
  }

  x = mt_[mti_++];

  x ^= (x >> 29) & 0x5555555555555555ULL;
  x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
  x ^= (x << 37) & 0xFFF7EEE000000000ULL;
  x ^= (x >> 43);

  return x;
}

} // namespace leanstore::utils
