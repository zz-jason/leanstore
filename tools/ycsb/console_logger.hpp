#pragma once

#include <cstdlib>
#include <iostream>
#include <string_view>

namespace leanstore::ycsb {

inline void ConsoleInfo(std::string_view msg) {
  std::cout << "\033[1;32m[INFO]\033[0m " << msg << std::endl;
}

inline void ConsoleError(std::string_view msg) {
  std::cerr << "\033[1;31m[ERROR]\033[0m " << msg << std::endl;
}

inline void ConsoleFatal(std::string_view msg) {
  std::cerr << "\033[1;31m[FATAL]\033[0m " << msg << std::endl;
  exit(EXIT_FAILURE);
}

} // namespace leanstore::ycsb