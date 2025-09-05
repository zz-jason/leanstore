#include "leanstore/utils/random_generator.hpp"

#include <atomic>

namespace leanstore::utils {

static std::atomic<uint64_t> mt_counter = 0;

MersenneTwister::MersenneTwister(uint64_t seed) : mti_(sNn + 1) {
  init(seed + (mt_counter++));
}

void MersenneTwister::init(uint64_t seed) {
  mt_[0] = seed;
  for (mti_ = 1; mti_ < sNn; mti_++)
    mt_[mti_] = (6364136223846793005ULL * (mt_[mti_ - 1] ^ (mt_[mti_ - 1] >> 62)) + mti_);
}

uint64_t MersenneTwister::Rand() {
  uint64_t x;
  static const uint64_t kMag01[2] = {0ULL, sMatrixA};

  if (mti_ >= sNn) { /* generate sNn words at one time */
    int i;
    for (i = 0; i < sNn - sMm; i++) {
      x = (mt_[i] & sUm) | (mt_[i + 1] & sLm);
      mt_[i] = mt_[i + sMm] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    for (; i < sNn - 1; i++) {
      x = (mt_[i] & sUm) | (mt_[i + 1] & sLm);
      mt_[i] = mt_[i + (sMm - sNn)] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    x = (mt_[sNn - 1] & sUm) | (mt_[0] & sLm);
    mt_[sNn - 1] = mt_[sMm - 1] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];

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
