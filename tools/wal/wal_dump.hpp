#pragma once

#include "leanstore/common/wal_record.h"

#include <tanakh-cmdline/cmdline.h>

#include <cassert>
#include <string>
#include <string_view>

namespace leanstore {

/// Wal command line tool, currently only supports reading wal files and
/// printing their contents in human-readable format.
class WalDump {
public:
  /// WAL print format
  enum class Format : uint8_t {
    kUnknown = 0,
    kText,
    kJson,

  };

  /// Constructor
  WalDump(std::string_view wal_path, std::string_view format)
      : wal_path_(wal_path),
        print_format_(FormatFromString(format)) {
    assert(!wal_path_.empty() && "WAL path is required");
    assert(print_format_ != Format::kUnknown && "Unknown format");
  }

  /// Run the wal printer
  void Run();

private:
  /// Convert string to Format enum
  static Format FormatFromString(std::string_view format);

  /// Print a WAL record
  static std::string FormatWalRecord(const lean_wal_record& record, Format format);

  /// Path to the WAL file
  std::string wal_path_;

  /// Print format
  Format print_format_;
};

} // namespace leanstore