#pragma once

#include <cstdint>
#include <string>

namespace leanstore::ycsb {

struct YcsbOptions {
  std::string target_;
  std::string cmd_;
  std::string workload_;
  uint64_t threads_;
  uint64_t clients_;
  uint64_t mem_gb_;
  uint64_t run_for_seconds_;

  std::string data_dir_;
  uint64_t key_size_;
  uint64_t val_size_;
  uint64_t record_count_;
  double zipf_factor_;
};

} // namespace leanstore::ycsb