#include "leanstore/utils/RandomGenerator.hpp"

#include <atomic>

namespace leanstore {
namespace utils {

static std::atomic<uint64_t> sMtCounter = 0;

MersenneTwister::MersenneTwister(uint64_t seed) : mMti(sNn + 1) {
  init(seed + (sMtCounter++));
}

void MersenneTwister::init(uint64_t seed) {
  mMt[0] = seed;
  for (mMti = 1; mMti < sNn; mMti++)
    mMt[mMti] = (6364136223846793005ULL * (mMt[mMti - 1] ^ (mMt[mMti - 1] >> 62)) + mMti);
}

uint64_t MersenneTwister::Rand() {
  uint64_t x;
  static const uint64_t kMag01[2] = {0ULL, sMatrixA};

  if (mMti >= sNn) { /* generate sNn words at one time */
    int i;
    for (i = 0; i < sNn - sMm; i++) {
      x = (mMt[i] & sUm) | (mMt[i + 1] & sLm);
      mMt[i] = mMt[i + sMm] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    for (; i < sNn - 1; i++) {
      x = (mMt[i] & sUm) | (mMt[i + 1] & sLm);
      mMt[i] = mMt[i + (sMm - sNn)] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    x = (mMt[sNn - 1] & sUm) | (mMt[0] & sLm);
    mMt[sNn - 1] = mMt[sMm - 1] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];

    mMti = 0;
  }

  x = mMt[mMti++];

  x ^= (x >> 29) & 0x5555555555555555ULL;
  x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
  x ^= (x << 37) & 0xFFF7EEE000000000ULL;
  x ^= (x >> 43);

  return x;
}

} // namespace utils
} // namespace leanstore
