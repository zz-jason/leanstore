#pragma once

#include <cstdlib>
#include <iostream>

namespace leanstore::ycsb {

inline void TerminalLogInfo(const std::string& msg) {
  std::cout << "\033[1;32m[INFO]\033[0m " << msg << std::endl;
}

inline void TerminalLogError(const std::string& msg) {
  std::cerr << "\033[1;31m[ERROR]\033[0m " << msg << std::endl;
}

inline void TerminalLogFatal(const std::string& msg) {
  std::cerr << "\033[1;31m[FATAL]\033[0m " << msg << std::endl;
  exit(EXIT_FAILURE);
}

} // namespace leanstore::ycsb