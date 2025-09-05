#include "wal_printer.hpp"

#include "leanstore/common/wal_record.h"
#include "wal/wal_iterator.hpp"
#include "wal/wal_serde.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

namespace leanstore {

void WalPrinter::Run() {
  // open the file
  auto wal_iter = WalIterator::New(wal_path_);
  if (wal_iter->HasError()) {
    std::cerr << wal_iter->GetError() << std::endl;
    exit(EXIT_FAILURE);
  }

  lean_wal_record* record = wal_iter->Next();
  while (record) {
    std::cout << FormatWalRecord(record, print_format_) << std::endl;
    record = wal_iter->Next();
  }

  if (wal_iter->HasError()) {
    std::cerr << wal_iter->GetError() << std::endl;
    exit(EXIT_FAILURE);
  }
}

WalPrinter::Format WalPrinter::FormatFromString(std::string_view format) {
  static const constexpr char* kWalPrintFormatNames[] = {
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

std::string WalPrinter::FormatWalRecord(const lean_wal_record* record, Format format) {
  switch (format) {
  case Format::kText: {
    return FormatWalRecordAsText(record);
  }
  case Format::kJson: {
    return WalSerde::ToJson(record);
  }
  default: {
    assert(false && "Unsupported print format");
    return "";
  }
  }
}

std::string WalPrinter::FormatWalRecordAsText(const lean_wal_record* record) {
  switch (record->type_) {
  case LEAN_WAL_TYPE_CARRIAGE_RETURN:
  case LEAN_WAL_TYPE_SMO_COMPLETE:
  case LEAN_WAL_TYPE_SMO_PAGENEW:
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT:
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT:
  case LEAN_WAL_TYPE_INSERT:
  case LEAN_WAL_TYPE_UPDATE:
  case LEAN_WAL_TYPE_REMOVE:
  case LEAN_WAL_TYPE_TX_ABORT:
  case LEAN_WAL_TYPE_TX_COMPLETE:
  case LEAN_WAL_TYPE_TX_INSERT:
  case LEAN_WAL_TYPE_TX_REMOVE:
  case LEAN_WAL_TYPE_TX_UPDATE:
  default: {
    assert(false && "Unsupported WAL record type for JSON format");
    return "";
  }
  }
}

} // namespace leanstore