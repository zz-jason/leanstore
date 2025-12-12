#include "wal_dump.hpp"

#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/wal/wal_cursor.hpp"
#include "leanstore/cpp/wal/wal_serde.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

namespace {

constexpr auto kArgWalPath = "wal_path";
constexpr auto kFormat = "format";

cmdline::parser ArgParse(int argc, char** argv) {
  cmdline::parser args;
  args.add<std::string>(kArgWalPath, 0, "Path to the WAL file", true, "");
  args.add<std::string>(kFormat, 0, "Output format, available: text, json", false, "json");
  args.parse_check(argc, argv);
  return args;
}

} // namespace

/// Entry point for the wal command line tool.
int main(int argc, char** argv) {
  auto args = ArgParse(argc, argv);
  leanstore::WalDump(args.get<std::string>(kArgWalPath), args.get<std::string>(kFormat)).Run();
  return 0;
}

namespace leanstore {

void WalDump::Run() {
  // Lambda for error exit
  auto error_exit = [](const Error& error) {
    std::cerr << error << std::endl;
    exit(EXIT_FAILURE);
  };

  // Lambda for printing a wal record
  auto print_record = [&](const lean_wal_record& record) {
    std::cout << FormatWalRecord(record, print_format_) << std::endl;
    return true;
  };

  // open the file
  auto cursor_res = WalCursor::New(wal_path_);
  if (!cursor_res) {
    error_exit(cursor_res.error());
  }

  // iterate and print each record
  if (auto err = cursor_res.value()->Foreach(print_record); err) {
    error_exit(*err);
  }
}

WalDump::Format WalDump::FormatFromString(std::string_view format) {
  static constexpr const char* kWalPrintFormatNames[] = {
      "unknown",
      "text",
      "json",
  };

  auto lower_format = std::string(format.size(), '\0');
  std::ranges::transform(format, lower_format.begin(), ::tolower);

  if (lower_format == kWalPrintFormatNames[static_cast<int>(Format::kText)]) {
    return Format::kText;
  }
  if (lower_format == kWalPrintFormatNames[static_cast<int>(Format::kJson)]) {
    return Format::kJson;
  }
  return Format::kUnknown;
}

std::string WalDump::FormatWalRecord(const lean_wal_record& record, Format format) {
  switch (format) {
  case Format::kJson: {
    return WalSerde::ToJson(record);
  }
  case Format::kText:
  default: {
    assert(false && "Unsupported print format");
    return "";
  }
  }
}

} // namespace leanstore
