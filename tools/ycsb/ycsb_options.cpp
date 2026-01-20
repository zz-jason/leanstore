#include "tools/ycsb/ycsb_options.hpp"

#include "console_logger.hpp"

#include <tanakh-cmdline/cmdline.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace leanstore::ycsb {

namespace {

constexpr auto kArgBackend = "backend";
constexpr auto kArgAction = "action";
constexpr auto kArgWorkload = "workload";
constexpr auto kArgWorkers = "workers";
constexpr auto kArgClients = "clients";
constexpr auto kArgDram = "dram";
constexpr auto kArgDuration = "duration";
constexpr auto kArgDir = "dir";
constexpr auto kArgKeySize = "key_size";
constexpr auto kArgValSize = "val_size";
constexpr auto kArgRows = "rows";
constexpr auto kArgZipfFactor = "zipf_factor";

} // namespace

YcsbOptions YcsbOptions::FromCmdLine(int argc, char** argv) {
  cmdline::parser arg_parser;
  arg_parser.add<std::string>(kArgBackend, 0, "Target backend", true, "");
  arg_parser.add<std::string>(kArgAction, 0, "Action <load|run> ", false, "run");
  arg_parser.add<std::string>(kArgWorkload, 0, "YCSB workload, <a|b|c|d|e|f> ", false, "a");
  arg_parser.add<uint64_t>(kArgWorkers, 0, "Number of backend worker threads", false, 4);
  arg_parser.add<uint64_t>(kArgClients, 0, "Number of client threads", false, 8);
  arg_parser.add<uint64_t>(kArgDram, 0, "DRAM size in GB", false, 1);
  arg_parser.add<uint64_t>(kArgDuration, 0, "Run duration in seconds", false, 30);
  arg_parser.add<std::string>(kArgDir, 0, "Database directory", false, "/tmp/leanstore/ycsb");
  arg_parser.add<uint64_t>(kArgKeySize, 0, "Key size in bytes", false, 16);
  arg_parser.add<uint64_t>(kArgValSize, 0, "Value size in bytes", false, 120);
  arg_parser.add<uint64_t>(kArgRows, 0, "Number of rows", false, 10'000);
  arg_parser.add<double>(kArgZipfFactor, 0, "Zipfian distribution factor", false, 0.99);
  arg_parser.parse_check(argc, argv);

  if (arg_parser.get<uint64_t>(kArgKeySize) < 8) {
    leanstore::ycsb::ConsoleFatal("Key size must be >= 8");
  }

  auto to_lower = [](std::string_view str) {
    std::string lower_str(str);
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lower_str;
  };

  return leanstore::ycsb::YcsbOptions{
      .dir_ = arg_parser.get<std::string>(kArgDir),
      .backend_ = to_lower(arg_parser.get<std::string>(kArgBackend)),
      .workload_ = to_lower(arg_parser.get<std::string>(kArgWorkload)),
      .action_ = to_lower(arg_parser.get<std::string>(kArgAction)),
      .clients_ = arg_parser.get<uint64_t>(kArgClients),
      .workers_ = arg_parser.get<uint64_t>(kArgWorkers),
      .dram_ = arg_parser.get<uint64_t>(kArgDram),
      .duration_ = arg_parser.get<uint64_t>(kArgDuration),
      .key_size_ = arg_parser.get<uint64_t>(kArgKeySize),
      .val_size_ = arg_parser.get<uint64_t>(kArgValSize),
      .rows_ = arg_parser.get<uint64_t>(kArgRows),
      .zipf_factor_ = arg_parser.get<double>(kArgZipfFactor),
  };
}
} // namespace leanstore::ycsb