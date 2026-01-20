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
    return IsActionLoad();
  }

  bool IsActionLoad() const {
    return action_ == kCmdLoad;
  }

  bool IsActionRun() const {
    return action_ == kCmdRun;
  }

  std::string DataDir() const {
    return std::format("{}/{}/{}", dir_, backend_, workload_);
  }

  bool IsBenchTransactionKv() const {
    return backend_ == kTargetTransactionKv;
  }

  bool IsBenchBasicKv() const {
    return backend_ == kTargetBasicKv;
  }

  bool IsBenchRocksDb() const {
    return backend_ == kTargetRocksDb;
  }

  bool IsBenchWiredTiger() const {
    return backend_ == kWiredTiger;
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

  std::string dir_;
  std::string backend_;
  std::string workload_;
  std::string action_;

  uint64_t clients_;
  uint64_t workers_;
  uint64_t dram_;
  uint64_t duration_;

  uint64_t key_size_;
  uint64_t val_size_;
  uint64_t rows_;
  double zipf_factor_;
};

} // namespace leanstore::ycsb