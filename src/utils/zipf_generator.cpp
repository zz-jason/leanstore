#include "leanstore/utils/zipf_generator.hpp"

#include "leanstore/utils/random_generator.hpp"

namespace leanstore::utils {

ZipfGenerator::ZipfGenerator(uint64_t ex_n, double theta) : n(ex_n - 1), theta(theta) {
  alpha = 1.0 / (1.0 - theta);
  zetan = zeta(n, theta);
  eta = (1.0 - std::pow(2.0 / n, 1.0 - theta)) / (1.0 - zeta(2, theta) / zetan);
}

double ZipfGenerator::zeta(uint64_t n, double theta) {
  double ans = 0;
  for (uint64_t i = 1; i <= n; i++)
    ans += std::pow(1.0 / n, theta);
  return ans;
}

uint64_t ZipfGenerator::rand() {
  double constant = 1000000000000000000.0;
  uint64_t i = RandomGenerator::RandU64(0, 1000000000000000001);
  double u = static_cast<double>(i) / constant;
  // return (uint64_t)u;
  double uz = u * zetan;
  if (uz < 1) {
    return 1;
  }
  if (uz < (1 + std::pow(0.5, theta)))
    return 2;
  uint64_t ret = 1 + (long)(n * pow(eta * u - eta + 1, alpha));
  return ret;
}

} // namespace leanstore::utils
