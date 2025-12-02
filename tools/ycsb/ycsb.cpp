#include "tools/ycsb/ycsb.hpp"

#include "leanstore/utils/log.hpp"
#include "tools/ycsb/ycsb_leanstore.hpp"
#include "tools/ycsb/ycsb_rocksdb.hpp"
#include "tools/ycsb/ycsb_wired_tiger.hpp"
#include "ycsb_args.hpp"

#include <tanakh-cmdline/cmdline.h>

#include <algorithm>
#include <cctype>
#include <format>
#include <string>
#include <string_view>

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

constexpr auto kCmdLoad = "load";
constexpr auto kCmdRun = "run";
constexpr auto kTargetTransactionKv = "transactionkv";
constexpr auto kTargetBasicKv = "basickv";
constexpr auto kTargetRocksDb = "rocksdb";
constexpr auto kWiredTiger = "wiredtiger";

leanstore::ycsb::YcsbOptions ArgParse(int argc, char** argv) {
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
    leanstore::Log::Fatal("Key size must be >= 8");
  }

  auto to_lower = [](std::string_view str) {
    std::string lower_str(str);
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lower_str;
  };

  return leanstore::ycsb::YcsbOptions{
      .target_ = to_lower(arg_parser.get<std::string>(kArgTarget)),
      .cmd_ = to_lower(arg_parser.get<std::string>(kArgCmd)),
      .workload_ = to_lower(arg_parser.get<std::string>(kArgWorkload)),
      .threads_ = arg_parser.get<uint64_t>(kArgThreads),
      .clients_ = arg_parser.get<uint64_t>(kArgClients),
      .mem_gb_ = arg_parser.get<uint64_t>(kArgMemGb),
      .run_for_seconds_ = arg_parser.get<uint64_t>(kArgRunForSeconds),
      .data_dir_ = arg_parser.get<std::string>(kArgDataDir),
      .key_size_ = arg_parser.get<uint64_t>(kArgKeySize),
      .val_size_ = arg_parser.get<uint64_t>(kArgValSize),
      .record_count_ = arg_parser.get<uint64_t>(kArgRecordCount),
      .zipf_factor_ = arg_parser.get<double>(kArgZipfFactor),
  };
}

std::unique_ptr<leanstore::ycsb::YcsbExecutor> GetExecutor(
    leanstore::ycsb::YcsbOptions& ycsb_options) {
  const auto& ycsb_target = ycsb_options.target_;
  const auto& ycsb_cmd = ycsb_options.cmd_;
  if (ycsb_target == kTargetTransactionKv || ycsb_target == kTargetBasicKv) {
    bool bench_transaction_kv = ycsb_target == kTargetTransactionKv;
    bool create_from_scratch = ycsb_cmd == kCmdLoad;
    return std::make_unique<leanstore::ycsb::YcsbLeanStore>(ycsb_options, bench_transaction_kv,
                                                            create_from_scratch);
  }
  if (ycsb_target == kTargetRocksDb) {
    return std::make_unique<leanstore::ycsb::YcsbRocksDb>(ycsb_options);
  }
#ifdef ENABLE_WIRED_TIGER
  if (ycsb_target == kWiredTiger) {
    return std::make_unique<leanstore::ycsb::YcsbWiredTiger>(ycsb_options);
  }
#endif
  return nullptr;
}

void RunExecutor(leanstore::ycsb::YcsbExecutor& executor, std::string_view ycsb_cmd) {
  if (ycsb_cmd == kCmdLoad) {
    executor.HandleCmdLoad();
    return;
  }
  if (ycsb_cmd == kCmdRun) {
    executor.HandleCmdRun();
    return;
  }
  leanstore::Log::Fatal(std::format("Unknown command: {}", ycsb_cmd));
}

} // namespace

int main(int argc, char** argv) {
  auto ycsb_options = ArgParse(argc, argv);

  // Transform ycsb_target to lowercase
  auto executor = GetExecutor(ycsb_options);
  if (executor == nullptr) {
    leanstore::Log::Fatal(std::format("Unknown target: {}", ycsb_options.target_));
  }

  RunExecutor(*executor, ycsb_options.cmd_);
  return 0;
}
