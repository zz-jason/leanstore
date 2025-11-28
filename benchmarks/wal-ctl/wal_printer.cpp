#include "wal_printer.hpp"

#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/wal/wal_cursor.hpp"
#include "wal/wal_serde.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

namespace leanstore {

void WalPrinter::Run() {
  auto error_exit = [](const Error& error) {
    std::cerr << error << std::endl;
    exit(EXIT_FAILURE);
  };

  // open the file
  auto cursor_res = WalCursor::New(wal_path_);
  if (!cursor_res) {
    error_exit(cursor_res.error());
  }

  // seek to the beginning
  auto wal_iter = std::move(cursor_res.value());
  if (auto err = wal_iter->Seek(0); err) {
    error_exit(*err);
  }

  while (wal_iter->Valid()) {
    const auto& record = wal_iter->CurrentRecord();
    std::cout << FormatWalRecord(record, print_format_) << std::endl;

    if (auto err = wal_iter->Next(); err) {
      error_exit(*err);
    }
  }
}

WalPrinter::Format WalPrinter::FormatFromString(std::string_view format) {
  static constexpr const char* kWalPrintFormatNames[] = {
      "unknown",
      "text",
      "json",
  };

  std::string lower_format;
  lower_format.resize(format.size());
  std::transform(format.begin(), format.end(), lower_format.begin(), ::tolower);

  if (lower_format == kWalPrintFormatNames[static_cast<int>(Format::kText)]) {
    return Format::kText;
  }
  if (lower_format == kWalPrintFormatNames[static_cast<int>(Format::kJson)]) {
    return Format::kJson;
  }
  return Format::kUnknown;
}

std::string WalPrinter::FormatWalRecord(const lean_wal_record& record, Format format) {
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