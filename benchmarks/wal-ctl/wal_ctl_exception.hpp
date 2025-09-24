#pragma once

#include <stdexcept>

namespace leanstore {

class WalCtlException : public std::runtime_error {
public:
  explicit WalCtlException(const std::string& msg) : std::runtime_error(msg) {
  }
};

} // namespace leanstore