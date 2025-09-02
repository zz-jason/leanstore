#pragma once

#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/parallelize.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "ycsb.hpp"

#include <chrono>
#include <cstring>
#include <filesystem>
#include <format>
#include <iostream>
#include <string>
#include <thread>

#ifdef ENABLE_WIRED_TIGER
#include <wiredtiger.h>
#endif

namespace leanstore::ycsb {

class YcsbWiredTiger : public YcsbExecutor {

#ifdef ENABLE_WIRED_TIGER

public:
  YcsbWiredTiger() : conn_(nullptr) {
    lean_metrics_exposer_start(8080);
  }

  ~YcsbWiredTiger() override {
    close_wired_tiger();
  }

  void HandleCmdLoad() override {
    open_wired_tiger(true);

    // load data with FLAGS_ycsb_threads
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Inserting " << FLAGS_ycsb_record_count << " values" << std::endl;
    SCOPED_DEFER({
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      std::cout << "Done inserting" << ", time elapsed: " << duration / 1000000.0 << " seconds"
                << ", throughput: " << CalculateTps(start, end, FLAGS_ycsb_record_count) << " tps"
                << std::endl;
    });

    // create a table for ycsb
    const char table_name[] = "table:ycsb";
    {
      WT_SESSION* session = open_session("isolation=snapshot");
      SCOPED_DEFER(close_session(session));
      create_table(session, table_name);
    }

    // insert data in parallel
    utils::Parallelize::Range(
        FLAGS_ycsb_threads, FLAGS_ycsb_record_count,
        [&](uint64_t thread_id [[maybe_unused]], uint64_t begin, uint64_t end) {
          // session and cursor for each thread
          WT_SESSION* session = open_session("isolation=snapshot");
          WT_CURSOR* cursor = open_cursor(session, table_name);

          // key value buf
          uint8_t key[FLAGS_ycsb_key_size];
          WT_ITEM key_item;
          key_item.data = key;
          key_item.size = FLAGS_ycsb_key_size;

          uint8_t val[FLAGS_ycsb_val_size];
          WT_ITEM val_item;
          val_item.data = val;
          val_item.size = FLAGS_ycsb_val_size;

          for (uint64_t i = begin; i < end; i++) {
            // generate key-value for insert
            GenKey(i, key);
            utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

            // insert into wiredtiger
            cursor->set_key(cursor, &key_item);
            cursor->set_value(cursor, &val_item);
            int ret = cursor->insert(cursor);
            if (ret != 0) {
              Log::Fatal("Failed to insert: {}", wiredtiger_strerror(ret));
            }
          }
        });
  }

  void HandleCmdRun() override {
    open_wired_tiger(false);

    // Run the benchmark in FLAGS_ycsb_threads
    auto workload_type = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workload_type);
    auto zipf_random =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keep_running = true;
    std::vector<std::atomic<uint64_t>> thread_committed(FLAGS_ycsb_threads);
    std::vector<std::atomic<uint64_t>> thread_aborted(FLAGS_ycsb_threads);

    std::vector<std::thread> threads;
    for (uint64_t worker_id = 0; worker_id < FLAGS_ycsb_threads; worker_id++) {
      threads.emplace_back(
          [&](uint64_t thread_id) {
            // session and cursor
            WT_SESSION* session = open_session("isolation=snapshot");
            WT_CURSOR* cursor = open_cursor(session, "table:ycsb");

            // key buffer
            uint8_t key[FLAGS_ycsb_key_size];
            WT_ITEM key_item;
            key_item.data = key;
            key_item.size = FLAGS_ycsb_key_size;

            // val buffer
            uint8_t val[FLAGS_ycsb_val_size];
            WT_ITEM val_item;
            val_item.data = val;
            val_item.size = FLAGS_ycsb_val_size;

            while (keep_running) {
              switch (workload_type) {
              case Workload::kA:
              case Workload::kB:
              case Workload::kC: {
                auto read_probability = utils::RandomGenerator::Rand(0, 100);
                if (read_probability <= workload.read_proportion_ * 100) {
                  // generate key for read
                  GenYcsbKey(zipf_random, key);

                  // read from wiredtiger
                  cursor->set_key(cursor, &key_item);
                  int ret = cursor->search(cursor);
                  if (ret != 0) {
                    Log::Fatal("Failed to search: {}", wiredtiger_strerror(ret));
                  }

                  // copy value out
                  cursor->get_value(cursor, &val_item);
                } else {
                  // generate key val for update
                  GenYcsbKey(zipf_random, key);
                  utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

                  cursor->set_key(cursor, &key_item);
                  cursor->set_value(cursor, &val_item);
                  int ret = cursor->update(cursor);
                  if (ret != 0) {
                    Log::Fatal("Failed to update: {}", wiredtiger_strerror(ret));
                  }
                }
                break;
              }
              default: {
                Log::Fatal("Unsupported workload type: {}", static_cast<uint8_t>(workload_type));
              }
              }
              thread_committed[thread_id]++;
            }
          },
          worker_id);
    }

    // init counters
    for (auto& c : thread_committed) {
      c = 0;
    }
    for (auto& a : thread_aborted) {
      a = 0;
    }

    PrintTpsSummary(1, FLAGS_ycsb_run_for_seconds, FLAGS_ycsb_threads, thread_committed,
                    thread_aborted);

    keep_running.store(false);
    for (auto& thread : threads) {
      thread.join();
    }
  }

private:
  /// open wiredtiger
  void open_wired_tiger(bool create_from_scratch = true) {
    std::string data_dir = FLAGS_ycsb_data_dir + "/wiredtiger";

    if (create_from_scratch) {
      // remove the existing data
      std::filesystem::remove_all(data_dir);

      // mkdir
      std::filesystem::create_directories(data_dir);
    }

    std::string config_string(
        "create, direct_io=[data, log, checkpoint], "
        "log=(enabled=true,archive=true), statistics_log=(wait=1), "
        "statistics=(all, clear), session_max=2000, eviction=(threads_max=4), cache_size=" +
        std::to_string(FLAGS_ycsb_mem_gb * 1024) + "M");
    int ret = wiredtiger_open(data_dir.c_str(), nullptr, config_string.c_str(), &conn_);
    if (ret != 0) {
      Log::Fatal("Failed to open wiredtiger: {}", wiredtiger_strerror(ret));
    }
  }

  /// open session
  WT_SESSION* open_session(const char* session_config) {
    if (conn_ == nullptr) {
      Log::Fatal("Wiredtiger connection is not opened");
    }

    WT_SESSION* session;
    int ret = conn_->open_session(conn_, nullptr, session_config, &session);
    if (ret != 0) {
      Log::Fatal("Failed to open session: {}", wiredtiger_strerror(ret));
    }
    return session;
  }

  /// create table
  void create_table(WT_SESSION* session, const char* table_name) {
    const char* config_string = "key_format=S,value_format=S";
    int ret = session->create(session, table_name, config_string);
    if (ret != 0) {
      Log::Fatal("Failed to create table: {}", wiredtiger_strerror(ret));
    }
  }

  /// open a cursor
  WT_CURSOR* open_cursor(WT_SESSION* session, const char* table_name) {
    WT_CURSOR* cursor;
    int ret = session->open_cursor(session, table_name, nullptr, "raw", &cursor);
    if (ret != 0) {
      Log::Fatal("Failed to open cursor: {}", wiredtiger_strerror(ret));
    }
    return cursor;
  }

  /// close cursor
  void close_cursor(WT_CURSOR* cursor) {
    int ret = cursor->close(cursor);
    if (ret != 0) {
      Log::Fatal("Failed to close cursor: {}", wiredtiger_strerror(ret));
    }
  }

  /// drop table
  void drop_table(WT_SESSION* session, const char* table_name) {
    int ret = session->drop(session, table_name, nullptr);
    if (ret != 0) {
      Log::Fatal("Failed to drop table: {}", wiredtiger_strerror(ret));
    }
  }

  /// close session
  void close_session(WT_SESSION* session) {
    int ret = session->close(session, nullptr);
    if (ret != 0) {
      Log::Fatal("Failed to close session: {}", wiredtiger_strerror(ret));
    }
  }

  /// close wiredtiger
  void close_wired_tiger() {
    if (conn_ != nullptr) {
      conn_->close(conn_, nullptr);
      conn_ = nullptr;
    }
  }

  WT_CONNECTION* conn_;
#endif
};

} // namespace leanstore::ycsb