#pragma once

#include "tools/ycsb/ycsb_workload_spec.hpp"

#include <cassert>
#include <cstdint>
#include <format>
#include <string>

namespace leanstore::ycsb {

struct YcsbOptions {
  static YcsbOptions FromCmdLine(int argc, char** argv);

  static constexpr std::string kCmdLoad = "load";
  static constexpr std::string kCmdRun = "run";
  static constexpr std::string kTargetTransactionKv = "transactionkv";
  static constexpr std::string kTargetBasicKv = "basickv";
  static constexpr std::string kTargetRocksDb = "rocksdb";
  static constexpr std::string kWiredTiger = "wiredtiger";

  bool IsCreateFromScratch() const {
    return IsCmdLoad();
  }

  bool IsCmdLoad() const {
    return cmd_ == kCmdLoad;
  }

  bool IsCmdRun() const {
    return cmd_ == kCmdRun;
  }

  std::string DataDir() const {
    return std::format("{}/{}/{}", data_dir_, target_, workload_);
  }

  bool IsBenchTransactionKv() const {
    return target_ == kTargetTransactionKv;
  }

  bool IsBenchBasicKv() const {
    return target_ == kTargetBasicKv;
  }

  bool IsBenchRocksDb() const {
    return target_ == kTargetRocksDb;
  }

  bool IsBenchWiredTiger() const {
    return target_ == kWiredTiger;
  }

  YcsbWorkloadType GetWorkloadType() const {
    uint8_t workload_char = workload_[0];
    assert(workload_char >= 'a' && workload_char <= 'f');
    return static_cast<YcsbWorkloadType>(workload_char - 'a');
  }

  YcsbWorkloadSpec GetWorkloadSpec() const {
    return kYcsbWorkloadSpecs[static_cast<uint8_t>(GetWorkloadType())];
  }

  uint64_t GetReportPeriodSec() const {
    return 1;
  }

  std::string data_dir_;
  std::string target_;
  std::string workload_;
  std::string cmd_;

  uint64_t clients_;
  uint64_t threads_;
  uint64_t mem_gb_;
  uint64_t run_for_seconds_;

  uint64_t key_size_;
  uint64_t val_size_;
  uint64_t record_count_;
  double zipf_factor_;
};

} // namespace leanstore::ycsb