#include "RandomGenerator.hpp"

#include "shared-headers/Units.hpp"

#include <atomic>

namespace leanstore {
namespace utils {

static std::atomic<u64> sMtCounter = 0;

MersenneTwister::MersenneTwister(uint64_t seed) : mti(NN + 1) {
  init(seed + (sMtCounter++));
}

void MersenneTwister::init(uint64_t seed) {
  mt[0] = seed;
  for (mti = 1; mti < NN; mti++)
    mt[mti] =
        (6364136223846793005ULL * (mt[mti - 1] ^ (mt[mti - 1] >> 62)) + mti);
}

uint64_t MersenneTwister::rnd() {
  uint64_t x;
  static const uint64_t kMag01[2] = {0ULL, MATRIX_A};

  if (mti >= NN) { /* generate NN words at one time */
    int i;
    for (i = 0; i < NN - MM; i++) {
      x = (mt[i] & UM) | (mt[i + 1] & LM);
      mt[i] = mt[i + MM] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    for (; i < NN - 1; i++) {
      x = (mt[i] & UM) | (mt[i + 1] & LM);
      mt[i] = mt[i + (MM - NN)] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];
    }
    x = (mt[NN - 1] & UM) | (mt[0] & LM);
    mt[NN - 1] = mt[MM - 1] ^ (x >> 1) ^ kMag01[(int)(x & 1ULL)];

    mti = 0;
  }

  x = mt[mti++];

  x ^= (x >> 29) & 0x5555555555555555ULL;
  x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
  x ^= (x << 37) & 0xFFF7EEE000000000ULL;
  x ^= (x >> 43);

  return x;
}

} // namespace utils
} // namespace leanstore
