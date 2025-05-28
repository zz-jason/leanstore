#include "ycsb.hpp"

#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "ycsb_lean_store.hpp"
#include "ycsb_rocks_db.hpp"
#include "ycsb_wired_tiger.hpp"

#include <gflags/gflags.h>

#include <algorithm>
#include <cctype>
#include <format>
#include <string>

// For data preparation
static std::string kCmdLoad = "load";
static std::string kCmdRun = "run";
static std::string kTargetTransactionKv = "transactionkv";
static std::string kTargetBasicKv = "basickv";
static std::string kTargetRocksDb = "rocksdb";
static std::string kWiredTiger = "wiredtiger";

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Ycsb Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Transform ycsb_target to lowercase
  std::transform(FLAGS_ycsb_target.begin(), FLAGS_ycsb_target.end(), FLAGS_ycsb_target.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Transform ycsb_cmd to lowercase
  std::transform(FLAGS_ycsb_cmd.begin(), FLAGS_ycsb_cmd.end(), FLAGS_ycsb_cmd.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Transform ycsb_workload to lowercase
  std::transform(FLAGS_ycsb_workload.begin(), FLAGS_ycsb_workload.end(),
                 FLAGS_ycsb_workload.begin(), [](unsigned char c) { return std::tolower(c); });

  if (FLAGS_ycsb_key_size < 8) {
    leanstore::Log::Fatal("Key size must be >= 8");
  }

  leanstore::ycsb::YcsbExecutor* executor = nullptr;
  SCOPED_DEFER(if (executor != nullptr) { delete executor; });

  if (FLAGS_ycsb_target == kTargetTransactionKv || FLAGS_ycsb_target == kTargetBasicKv) {
    bool bench_transaction_kv = FLAGS_ycsb_target == kTargetTransactionKv;
    bool create_from_scratch = FLAGS_ycsb_cmd == kCmdLoad;
    executor = new leanstore::ycsb::YcsbLeanStore(bench_transaction_kv, create_from_scratch);
  } else if (FLAGS_ycsb_target == kTargetRocksDb) {
    executor = new leanstore::ycsb::YcsbRocksDb();
  } else if (FLAGS_ycsb_target == kWiredTiger) {
    executor = new leanstore::ycsb::YcsbWiredTiger();
  }

  if (executor == nullptr) {
    leanstore::Log::Fatal(std::format("Unknown target: {}", FLAGS_ycsb_target));
  }

  if (FLAGS_ycsb_cmd == kCmdLoad) {
    executor->HandleCmdLoad();
    return 0;
  }

  if (FLAGS_ycsb_cmd == kCmdRun) {
    executor->HandleCmdRun();
    return 0;
  }

  leanstore::Log::Fatal(std::format("Unknown command: {}", FLAGS_ycsb_cmd));
  return 0;
}
