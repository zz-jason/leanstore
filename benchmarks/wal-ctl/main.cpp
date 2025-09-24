#include "wal_printer.hpp"

#include <gflags/gflags.h>

/// Command line args
DEFINE_string(wal_path, "", "Path to the WAL file");
DEFINE_string(format, "text", "Output format, available: text, json");

/// Entry point for the wal command line tool.
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  assert(!FLAGS_wal_path.empty() && "WAL path is required");
  assert((FLAGS_format == "text" || FLAGS_format == "json") && "Unknown format");

  auto print_format = leanstore::WalPrintFormatFromString(FLAGS_format);
  assert(print_format != leanstore::WalPrintFormat::kUnknown && "Unknown format");

  leanstore::WalPrinter wal_ctl(FLAGS_wal_path, print_format);
  wal_ctl.Run();
  return 0;
}