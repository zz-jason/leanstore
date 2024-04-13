#include "Ycsb.hpp"

#include "YcsbLeanStore.hpp"
#include "YcsbRocksDb.hpp"
#include "leanstore/Config.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <string>

// For data preparation
static std::string kCmdLoad = "load";
static std::string kCmdRun = "run";
static std::string kTargetTransactionKv = "transactionkv";
static std::string kTargetBasicKv = "basickv";
static std::string kTargetRocksDb = "rocksdb";

int main(int argc, char** argv) {
  FLAGS_create_from_scratch = true;
  FLAGS_enable_metrics = true;
  FLAGS_metrics_port = 8080;
  FLAGS_data_dir = "/tmp/ycsb/" + FLAGS_ycsb_workload;

  gflags::SetUsageMessage("Ycsb Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Transform ycsb_target to lowercase
  std::transform(FLAGS_ycsb_target.begin(), FLAGS_ycsb_target.end(),
                 FLAGS_ycsb_target.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Transform ycsb_cmd to lowercase
  std::transform(FLAGS_ycsb_cmd.begin(), FLAGS_ycsb_cmd.end(),
                 FLAGS_ycsb_cmd.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Transform ycsb_workload to lowercase
  std::transform(FLAGS_ycsb_workload.begin(), FLAGS_ycsb_workload.end(),
                 FLAGS_ycsb_workload.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (FLAGS_ycsb_key_size < 8) {
    LOG(FATAL) << "Key size must be >= 8";
  }

  leanstore::ycsb::YcsbExecutor* executor = nullptr;
  if (FLAGS_ycsb_target == kTargetTransactionKv ||
      FLAGS_ycsb_target == kTargetBasicKv) {
    bool benchTransactionKv = FLAGS_ycsb_target == kTargetTransactionKv;
    executor = new leanstore::ycsb::YcsbLeanStore(benchTransactionKv);
  } else if (FLAGS_ycsb_target == kTargetRocksDb) {
    executor = new leanstore::ycsb::YcsbRocksDb();
  }

  if (executor == nullptr) {
    LOG(FATAL) << "Unknown target: " << FLAGS_ycsb_target;
  }

  if (FLAGS_ycsb_cmd == kCmdLoad) {
    executor->HandleCmdLoad();
    return 0;
  }

  if (FLAGS_ycsb_cmd == kCmdRun) {
    executor->HandleCmdLoad();
    executor->HandleCmdRun();
    return 0;
  }

  LOG(FATAL) << "Unknown command: " << FLAGS_ycsb_cmd;
  return 0;
}
