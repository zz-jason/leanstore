#pragma once

#include "leanstore/common/wal_format.h"

#include <algorithm>
#include <cassert>
#include <string>
#include <string_view>

namespace leanstore {

enum class WalPrintFormat {
  kUnknown = 0,
  kText,
  kJson,

};

WalPrintFormat WalPrintFormatFromString(const std::string& format_str);

/// Wal command line tool, currently only supports reading wal files and
/// printing their contents in human-readable format.
class WalPrinter {
public:
  WalPrinter(const std::string& wal_path, WalPrintFormat print_format)
      : wal_path_(wal_path),
        print_format_(print_format) {
    assert(print_format_ != WalPrintFormat::kUnknown && "Unknown format");
  }

  void Run();

private:
  std::string wal_path_;
  WalPrintFormat print_format_;

  lean_wal_record* current_record_ = nullptr;
};

inline WalPrintFormat WalPrintFormatFromString(std::string_view format) {
  static const constexpr char* kWalPrintFormatNames[] = {
      "unknown",
      "text",
      "json",
  };

  std::string lower_format;
  lower_format.resize(format.size());
  std::transform(format.begin(), format.end(), lower_format.begin(), ::tolower);

  if (lower_format == kWalPrintFormatNames[static_cast<int>(WalPrintFormat::kText)]) {
    return WalPrintFormat::kText;
  }

  if (lower_format == kWalPrintFormatNames[static_cast<int>(WalPrintFormat::kJson)]) {
    return WalPrintFormat::kJson;
  }

  return WalPrintFormat::kUnknown;
}

} // namespace leanstore