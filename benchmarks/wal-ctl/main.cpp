#include "wal_printer.hpp"

#include <gflags/gflags.h>

/// Command line args
DEFINE_string(wal_path, "", "Path to the WAL file");
DEFINE_string(format, "json", "Output format, available: text, json");

/// Entry point for the wal command line tool.
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  leanstore::WalPrinter(FLAGS_wal_path, FLAGS_format).Run();
  return 0;
}