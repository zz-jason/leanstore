#include "leanstore-c/leanstore.h"
#include "leanstore-c/perf_counters.h"
#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/scrambled_zipf_generator.hpp"
#include "ycsb.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <unistd.h>

namespace leanstore::ycsb {

constexpr std::string kTableName = "ycsb_leanstore";

class YcsbLeanStore : public YcsbExecutor {
private:
  std::unique_ptr<LeanStore> store_;

  bool bench_transaction_kv_;

public:
  YcsbLeanStore(bool bench_transaction_kv, bool create_from_scratch)
      : bench_transaction_kv_(bench_transaction_kv) {
    auto data_dir_str = FLAGS_ycsb_data_dir + std::string("/leanstore");
    StoreOption* option = CreateStoreOption(data_dir_str.c_str());
    option->create_from_scratch_ = create_from_scratch;
    option->enable_eager_gc_ = true;
    option->worker_threads_ = FLAGS_ycsb_threads;
    option->buffer_pool_size_ = FLAGS_ycsb_mem_gb << 30;

    auto res = LeanStore::Open(option);
    if (!res) {
      std::cerr << "Failed to open leanstore: " << res.error().ToString() << std::endl;
      DestroyStoreOption(option);
      exit(res.error().Code());
    }

    store_ = std::move(res.value());

    // start metrics http exposer for cpu/mem profiling
    StartMetricsHttpExposer(8080);
  }

  ~YcsbLeanStore() override {
    std::cout << "~YcsbLeanStore" << std::endl;
    store_.reset(nullptr);
  }

  KVInterface* CreateTable() {
    // create table with transaction kv
    if (bench_transaction_kv_) {
      leanstore::storage::btree::TransactionKV* table;
      store_->ExecSync(0, [&]() {
        auto res = store_->CreateTransactionKV(kTableName);
        if (!res) {
          Log::Fatal("Failed to create table: name={}, error={}", kTableName,
                     res.error().ToString());
        }
        table = res.value();
      });
      return table;
    }

    // create table with basic kv
    leanstore::storage::btree::BasicKV* table;
    store_->ExecSync(0, [&]() {
      auto res = store_->CreateBasicKv(kTableName);
      if (!res) {
        Log::Fatal("Failed to create table: name={}, error={}", kTableName, res.error().ToString());
      }
      table = res.value();
    });
    return table;
  }

  KVInterface* GetTable() {
    if (bench_transaction_kv_) {
      leanstore::storage::btree::TransactionKV* table;
      store_->GetTransactionKV(kTableName, &table);
      return table;
    }
    leanstore::storage::btree::BasicKV* table;
    store_->GetBasicKV(kTableName, &table);
    return table;
  }

  void HandleCmdLoad() override {
    auto* table = CreateTable();

    // record the start and end time, calculating throughput in the end
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Inserting " << FLAGS_ycsb_record_count << " values" << std::endl;
    SCOPED_DEFER({
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      auto summary =
          std::format("Done inserting, time elapsed={:.2f} seconds, throughput={:.2f} tps",
                      duration / 1000000.0, CalculateTps(start, end, FLAGS_ycsb_record_count));
      std::cout << summary << std::endl;
    });

    auto num_workers = store_->store_option_->worker_threads_;
    auto avg = FLAGS_ycsb_record_count / num_workers;
    auto rem = FLAGS_ycsb_record_count % num_workers;
    for (auto worker_id = 0u, begin = 0u; worker_id < num_workers;) {
      auto end = begin + avg + (rem-- > 0 ? 1 : 0);
      store_->ExecAsync(worker_id, [&, begin, end]() {
        uint8_t key[FLAGS_ycsb_key_size];
        uint8_t val[FLAGS_ycsb_val_size];

        for (uint64_t i = begin; i < end; i++) {
          // generate key-value for insert
          GenKey(i, key);
          utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

          if (bench_transaction_kv_) {
            cr::WorkerContext::My().StartTx();
          }
          auto op_code =
              table->Insert(Slice(key, FLAGS_ycsb_key_size), Slice(val, FLAGS_ycsb_val_size));
          if (op_code != OpCode::kOK) {
            Log::Fatal("Failed to insert, opCode={}", static_cast<uint8_t>(op_code));
          }
          if (bench_transaction_kv_) {
            cr::WorkerContext::My().CommitTx();
          }
        }
      });
      worker_id++, begin = end;
    }
    store_->WaitAll();
  }

  void HandleCmdRun() override {
    auto* table = GetTable();
    auto workload_type = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workload_type);
    auto zipf_random =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keep_running = true;
    std::vector<std::atomic<uint64_t>> thread_committed(store_->store_option_->worker_threads_);
    std::vector<std::atomic<uint64_t>> thread_aborted(store_->store_option_->worker_threads_);
    // init counters
    for (auto& c : thread_committed) {
      c = 0;
    }
    for (auto& a : thread_aborted) {
      a = 0;
    }

    std::vector<PerfCounters*> worker_perf_counters;
    for (auto i = 0u; i < store_->store_option_->worker_threads_; i++) {
      store_->ExecSync(
          i, [&]() { worker_perf_counters.push_back(cr::WorkerContext::My().GetPerfCounters()); });
    }

    for (uint64_t worker_id = 0; worker_id < store_->store_option_->worker_threads_; worker_id++) {
      store_->ExecAsync(worker_id, [&]() {
        uint8_t key[FLAGS_ycsb_key_size];
        std::string val_read;
        auto copy_value = [&](Slice val) { val.CopyTo(val_read); };

        auto update_desc_buf_size = UpdateDesc::Size(1);
        uint8_t update_desc_buf[update_desc_buf_size];
        auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
        update_desc->num_slots_ = 1;
        update_desc->update_slots_[0].offset_ = 0;
        update_desc->update_slots_[0].size_ = FLAGS_ycsb_val_size;

        std::string val_gen;
        auto update_call_back = [&](MutableSlice to_update) {
          auto new_val_size = update_desc->update_slots_[0].size_;
          utils::RandomGenerator::RandAlphString(new_val_size, val_gen);
          std::memcpy(to_update.Data(), val_gen.data(), val_gen.size());
        };

        while (keep_running) {
          JUMPMU_TRY() {
            switch (workload_type) {
            case Workload::kA:
            case Workload::kB:
            case Workload::kC: {
              auto read_probability = utils::RandomGenerator::Rand(0, 100);
              if (read_probability <= workload.read_proportion_ * 100) {
                // generate key for read
                GenYcsbKey(zipf_random, key);
                if (bench_transaction_kv_) {
                  cr::WorkerContext::My().StartTx(TxMode::kShortRunning,
                                                  IsolationLevel::kSnapshotIsolation, true);
                  table->Lookup(Slice(key, FLAGS_ycsb_key_size), copy_value);
                  cr::WorkerContext::My().CommitTx();
                } else {
                  table->Lookup(Slice(key, FLAGS_ycsb_key_size), copy_value);
                }
              } else {
                // generate key for update
                GenYcsbKey(zipf_random, key);
                // generate val for update
                if (bench_transaction_kv_) {
                  cr::WorkerContext::My().StartTx();
                  table->UpdatePartial(Slice(key, FLAGS_ycsb_key_size), update_call_back,
                                       *update_desc);
                  cr::WorkerContext::My().CommitTx();
                } else {
                  table->UpdatePartial(Slice(key, FLAGS_ycsb_key_size), update_call_back,
                                       *update_desc);
                }
              }
              break;
            }
            default: {
              Log::Fatal("Unsupported workload type: {}", static_cast<uint8_t>(workload_type));
            }
            }
            thread_committed[cr::WorkerContext::My().worker_id_]++;
          }
          JUMPMU_CATCH() {
            thread_aborted[cr::WorkerContext::My().worker_id_]++;
          }
        }
      });
    }

    // init counters
    for (auto& c : thread_committed) {
      c = 0;
    }
    for (auto& a : thread_aborted) {
      a = 0;
    }

    std::thread perf_context_reporter([&]() {
      auto report_period = 1;
      const char* counter_file_path = "/tmp/leanstore/worker-counters.txt";
      std::ofstream ost;

      while (keep_running) {
        sleep(report_period);
        uint64_t tx_with_remote_dependencies = 0;
        uint64_t lcb_executed = 0;
        uint64_t lcb_total_lat_ns [[maybe_unused]] = 0;
        uint64_t gc_executed = 0;
        uint64_t gc_total_lat_ns [[maybe_unused]] = 0;
        uint64_t tx_commit_wait = 0;

        // collect counters
        for (auto* perf_counters : worker_perf_counters) {
          tx_with_remote_dependencies +=
              atomic_exchange(&perf_counters->tx_with_remote_dependencies_, 0);
          tx_commit_wait += atomic_exchange(&perf_counters->tx_commit_wait_, 0);

          lcb_executed += atomic_exchange(&perf_counters->lcb_executed_, 0);
          lcb_total_lat_ns += atomic_exchange(&perf_counters->lcb_total_lat_ns_, 0);

          gc_executed += atomic_exchange(&perf_counters->gc_executed_, 0);
          gc_total_lat_ns += atomic_exchange(&perf_counters->gc_total_lat_ns_, 0);
        }
        ost.open(counter_file_path, std::ios_base::app);
        ost << std::format("TxWithDep: {}, txCommitWait: {}, LcbExec: {}, GcExec: {}",
                           tx_with_remote_dependencies, tx_commit_wait, lcb_executed, gc_executed)
            << std::endl;
        ost.close();
      }
    });

    print_tps_summary(1, FLAGS_ycsb_run_for_seconds, store_->store_option_->worker_threads_,
                      thread_committed, thread_aborted);

    // Shutdown threads
    keep_running = false;
    perf_context_reporter.join();
    store_->WaitAll();
  }
};

} // namespace leanstore::ycsb
