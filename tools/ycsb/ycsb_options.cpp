#include "tools/ycsb/ycsb_options.hpp"

#include "terminal_log.hpp"

#include <tanakh-cmdline/cmdline.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace leanstore::ycsb {

namespace {

constexpr auto kArgTarget = "target";
constexpr auto kArgCmd = "cmd";
constexpr auto kArgWorkload = "workload";
constexpr auto kArgThreads = "threads";
constexpr auto kArgClients = "clients";
constexpr auto kArgMemGb = "mem_gb";
constexpr auto kArgRunForSeconds = "run_for_seconds";
constexpr auto kArgDataDir = "data_dir";
constexpr auto kArgKeySize = "key_size";
constexpr auto kArgValSize = "val_size";
constexpr auto kArgRecordCount = "record_count";
constexpr auto kArgZipfFactor = "zipf_factor";

} // namespace

YcsbOptions YcsbOptions::FromCmdLine(int argc, char** argv) {
  cmdline::parser arg_parser;
  arg_parser.add<std::string>(kArgTarget, 0, "YCSB target", true, "");
  arg_parser.add<std::string>(kArgCmd, 0, "YCSB cmd, <load|run> ", false, "run");
  arg_parser.add<std::string>(kArgWorkload, 0, "YCSB workload, <a|b|c|d|e|f> ", false, "a");
  arg_parser.add<uint64_t>(kArgThreads, 0, "Number of worker threads", false, 4);
  arg_parser.add<uint64_t>(kArgClients, 0, "Number of clients", false, 4);
  arg_parser.add<uint64_t>(kArgMemGb, 0, "Memory size in GB", false, 1);
  arg_parser.add<uint64_t>(kArgRunForSeconds, 0, "Run for seconds", false, 300);
  arg_parser.add<std::string>(kArgDataDir, 0, "Data directory", false, "/tmp/leanstore/ycsb");
  arg_parser.add<uint64_t>(kArgKeySize, 0, "Key size in bytes", false, 16);
  arg_parser.add<uint64_t>(kArgValSize, 0, "Value size in bytes", false, 120);
  arg_parser.add<uint64_t>(kArgRecordCount, 0, "Number of records to load", false, 10'000);
  arg_parser.add<double>(kArgZipfFactor, 0, "Zipfian distribution factor", false, 0.99);
  arg_parser.parse_check(argc, argv);

  if (arg_parser.get<uint64_t>(kArgKeySize) < 8) {
    leanstore::ycsb::TerminalLogFatal("Key size must be >= 8");
  }

  auto to_lower = [](std::string_view str) {
    std::string lower_str(str);
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lower_str;
  };

  return leanstore::ycsb::YcsbOptions{
      .data_dir_ = arg_parser.get<std::string>(kArgDataDir),
      .target_ = to_lower(arg_parser.get<std::string>(kArgTarget)),
      .workload_ = to_lower(arg_parser.get<std::string>(kArgWorkload)),
      .cmd_ = to_lower(arg_parser.get<std::string>(kArgCmd)),
      .clients_ = arg_parser.get<uint64_t>(kArgClients),
      .threads_ = arg_parser.get<uint64_t>(kArgThreads),
      .mem_gb_ = arg_parser.get<uint64_t>(kArgMemGb),
      .run_for_seconds_ = arg_parser.get<uint64_t>(kArgRunForSeconds),
      .key_size_ = arg_parser.get<uint64_t>(kArgKeySize),
      .val_size_ = arg_parser.get<uint64_t>(kArgValSize),
      .record_count_ = arg_parser.get<uint64_t>(kArgRecordCount),
      .zipf_factor_ = arg_parser.get<double>(kArgZipfFactor),
  };
}
} // namespace leanstore::ycsb