#include "leanstore/cpp/base/stacktrace.hpp"

#include <cpptrace/cpptrace.hpp>

#include <sstream>

namespace leanstore {

std::string Stacktrace() {
  std::stringstream ss;
  auto st = cpptrace::stacktrace::current();
  st.print(ss, false);
  return ss.str();
}

} // namespace leanstore