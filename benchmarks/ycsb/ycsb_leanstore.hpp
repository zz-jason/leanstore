#include "benchmarks/ycsb/ycsb.hpp"
#include "benchmarks/ycsb/ycsb_leanstore_client.hpp"
#include "leanstore-c/leanstore.h"
#include "leanstore-c/perf_counters.h"
#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/jump_mu.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/scrambled_zipf_generator.hpp"
#include "utils/coroutine/coro_future.hpp"
#include "utils/scoped_timer.hpp"
#include "utils/small_vector.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include <atomic>
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

    auto datadir_str = std::format("{}/{}", FLAGS_ycsb_data_dir, kTableName);
    StoreOption* option = CreateStoreOption(datadir_str.c_str());
    option->create_from_scratch_ = create_from_scratch;
    option->enable_eager_gc_ = true;
    option->enable_wal_ = false;
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
      BTreeConfig config{.enable_wal_ = false, .use_bulk_insert_ = false};
      auto job = [&]() {
        auto res = store_->CreateTransactionKV(kTableName, config);
        if (!res) {
          Log::Fatal("Failed to create table: name={}, error={}", kTableName,
                     res.error().ToString());
        }
        table = res.value();
      };

#ifdef ENABLE_COROUTINE
      store_->Submit(std::move(job), 0)->Wait();
#else
      store_->ExecSync(0, std::move(job));
#endif

      return table;
    }

    // create table with basic kv
    leanstore::storage::btree::BasicKV* table;

    auto job = [&]() {
      BTreeConfig config{.enable_wal_ = false, .use_bulk_insert_ = false};
      auto res = store_->CreateBasicKv(kTableName, config);
      if (!res) {
        Log::Fatal("Failed to create table: name={}, error={}", kTableName, res.error().ToString());
      }
      table = res.value();
    };

#ifdef ENABLE_COROUTINE
    store_->Submit(std::move(job), 0)->Wait();
#else
    store_->ExecSync(0, std::move(job));
#endif

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
    std::cout << "Inserting " << FLAGS_ycsb_record_count << " values" << std::endl;

    // record the start and end time, calculating throughput in the end
    ScopedTimer timer([&](double elapsed_ms) {
      auto elapsed_sec = elapsed_ms / 1000.0;
      auto ops = FLAGS_ycsb_record_count / elapsed_sec;
      std::cout << std::format("Inserted values: {}, time_elapsed_sec: {:.2f}, tps: {:.2f}",
                               FLAGS_ycsb_record_count, elapsed_sec, ops)
                << std::endl;
    });

    auto num_workers = store_->store_option_->worker_threads_;
    auto avg = FLAGS_ycsb_record_count / num_workers;
    auto rem = FLAGS_ycsb_record_count % num_workers;

    std::vector<std::shared_ptr<CoroFuture<void>>> futures;
    for (auto i = 0u, begin = 0u; i < num_workers;) {
      auto end = begin + avg + (rem-- > 0 ? 1 : 0);
      auto insert_func = [&, begin, end]() {
        SmallBuffer<1024> key_buffer(FLAGS_ycsb_key_size);
        SmallBuffer<1024> val_buffer(FLAGS_ycsb_val_size);
        uint8_t* key = key_buffer.Data();
        uint8_t* val = val_buffer.Data();

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
      };

#ifdef ENABLE_COROUTINE
      futures.emplace_back(store_->Submit(std::move(insert_func), i));
#else
      store_->ExecAsync(i, std::move(insert_func));
#endif
      i++, begin = end;
    }

#ifdef ENABLE_COROUTINE
    for (const auto& future : futures) {
      future->Wait();
    }
#else
    store_->WaitAll();
#endif
  }

  void HandleCmdRun() override {
    if (FLAGS_ycsb_clients > 0) {
      return CmdRunWithMultiClients();
    }

    auto* table = GetTable();
    auto workload_type = GetWorkloadType();
    auto workload = GetWorkloadSpec(workload_type);
    auto zipf_random =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keep_running = true;

    std::vector<PerfCounters*> worker_perf_counters;
    auto job = [&]() { worker_perf_counters.push_back(GetTlsPerfCounters()); };
    for (auto i = 0u; i < store_->store_option_->worker_threads_; i++) {
#ifdef ENABLE_COROUTINE
      store_->Submit(std::move(job), i)->Wait();
#else
      store_->ExecSync(i, std::move(job));
#endif
    }

    std::vector<std::shared_ptr<CoroFuture<void>>> futures;
    for (uint64_t worker_id = 0; worker_id < store_->store_option_->worker_threads_; worker_id++) {
      auto job = [&]() {
        SmallBuffer<1024> key_buffer(FLAGS_ycsb_key_size);
        uint8_t* key = key_buffer.Data();

        std::string val_read;
        auto copy_value = [&](Slice val) { val.CopyTo(val_read); };

        auto update_desc_buf_size = UpdateDesc::Size(1);
        SmallBuffer<4096> update_desc_buffer(update_desc_buf_size);
        uint8_t* update_desc_buf = update_desc_buffer.Data();

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
            if (!bench_transaction_kv_) {
              GetTlsPerfCounters()->tx_committed_++;
            }
          }
          JUMPMU_CATCH() {
            if (!bench_transaction_kv_) {
              GetTlsPerfCounters()->tx_aborted_++;
            }
          }
        }
      };

#ifdef ENABLE_COROUTINE
      futures.emplace_back(store_->Submit(std::move(job), worker_id));
#else
      store_->ExecAsync(worker_id, std::move(job));
#endif
    }

    std::thread perf_context_reporter([&]() {
      auto report_period = 1;
      const char* counter_file_path = "/tmp/leanstore/worker-counters.txt";
      Log::Info("Perf counters written to {}", counter_file_path);

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

    uint64_t report_period = 1;
    for (uint64_t i = 0; i < FLAGS_ycsb_run_for_seconds; i += report_period) {
      sleep(report_period);

      uint64_t tx_committed = 0;
      uint64_t tx_aborted = 0;
      for (auto* perf_counters : worker_perf_counters) {
        tx_committed += atomic_exchange(&perf_counters->tx_committed_, 0);
        tx_aborted += atomic_exchange(&perf_counters->tx_aborted_, 0);
      }
      PrintTps(store_->store_option_->worker_threads_, i, tx_committed, tx_aborted, report_period);
    }

    // Shutdown threads
    keep_running = false;
    perf_context_reporter.join();

#ifdef ENABLE_COROUTINE
    for (const auto& future : futures) {
      future->Wait();
    }
#else
    store_->WaitAll();
#endif
  }

  void CmdRunWithMultiClients() {
    Log::Info("Running YCSB with {} clients", FLAGS_ycsb_clients);
    // collect perf counters
    auto all_perf_counters = GetPerfCounters();

    // create && start clients
    Log::Info("Starting YCSB clients, num_clients={}", FLAGS_ycsb_clients);
    std::vector<std::unique_ptr<YcsbLeanStoreClient>> clients;
    for (auto i = 0u; i < FLAGS_ycsb_clients; i++) {
      clients.emplace_back(YcsbLeanStoreClient::New(store_.get(), GetTable(), GetWorkloadType()));
    }
    for (auto& client : clients) {
      client->Start();
    }

    // report tps
    auto report_period = 1u;
    for (auto i = 0u; i < FLAGS_ycsb_run_for_seconds; i += report_period) {
      sleep(report_period);
      uint64_t tx_committed = 0;
      uint64_t tx_aborted = 0;
      for (auto* perf_counters : all_perf_counters) {
        tx_committed += atomic_exchange(&perf_counters->tx_committed_, 0);
        tx_aborted += atomic_exchange(&perf_counters->tx_aborted_, 0);
      }
      PrintTps(store_->store_option_->worker_threads_, i, tx_committed, tx_aborted, report_period);
    }

    // stop clients
    for (auto& client : clients) {
      client->Stop();
    }
  }

  Workload GetWorkloadType() const {
    return static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
  }

  std::vector<PerfCounters*> GetPerfCounters() {
    std::vector<PerfCounters*> perf_counters;
    auto job = [&]() { perf_counters.push_back(GetTlsPerfCounters()); };
    for (auto i = 0u; i < store_->store_option_->worker_threads_; i++) {
      SubmitJobSync(std::move(job), i);
    }

    Log::Info("Collected {} perf counters", perf_counters.size());
    return perf_counters;
  }

  void SubmitJobSync(std::function<void()>&& job, uint64_t worker_id) {
#ifdef ENABLE_COROUTINE
    store_->Submit(std::move(job), worker_id)->Wait();
#else
    store_->ExecSync(worker_id, std::move(job));
#endif
  }
};

} // namespace leanstore::ycsb
