#pragma once

#include <cpptrace/cpptrace.hpp>

#include <sstream>

namespace leanstore {

inline std::string Stacktrace() {
  std::stringstream ss;
  auto st = cpptrace::stacktrace::current();
  st.print(ss, false);
  return ss.str();
}

} // namespace leanstore