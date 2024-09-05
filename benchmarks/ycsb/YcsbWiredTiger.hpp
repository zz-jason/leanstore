#pragma once

#include "Ycsb.hpp"
#include "leanstore-c/leanstore-c.h"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

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
  YcsbWiredTiger() : mConn(nullptr) {
    StartMetricsHttpExposer(8080);
  }

  ~YcsbWiredTiger() override {
    closeWiredTiger();
  }

  void HandleCmdLoad() override {
    openWiredTiger(true);

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
    const char tableName[] = "table:ycsb";
    {
      WT_SESSION* session = openSession("isolation=snapshot");
      SCOPED_DEFER(closeSession(session));
      createTable(session, tableName);
    }

    // insert data in parallel
    utils::Parallelize::Range(
        FLAGS_ycsb_threads, FLAGS_ycsb_record_count,
        [&](uint64_t threadId [[maybe_unused]], uint64_t begin, uint64_t end) {
          // session and cursor for each thread
          WT_SESSION* session = openSession("isolation=snapshot");
          WT_CURSOR* cursor = openCursor(session, tableName);

          // key value buf
          uint8_t key[FLAGS_ycsb_key_size];
          WT_ITEM keyItem;
          keyItem.data = key;
          keyItem.size = FLAGS_ycsb_key_size;

          uint8_t val[FLAGS_ycsb_val_size];
          WT_ITEM valItem;
          valItem.data = val;
          valItem.size = FLAGS_ycsb_val_size;

          for (uint64_t i = begin; i < end; i++) {
            // generate key-value for insert
            GenKey(i, key);
            utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

            // insert into wiredtiger
            cursor->set_key(cursor, &keyItem);
            cursor->set_value(cursor, &valItem);
            int ret = cursor->insert(cursor);
            if (ret != 0) {
              Log::Fatal("Failed to insert: {}", wiredtiger_strerror(ret));
            }
          }
        });
  }

  void HandleCmdRun() override {
    openWiredTiger(false);

    // Run the benchmark in FLAGS_ycsb_threads
    auto workloadType = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workloadType);
    auto zipfRandom =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keepRunning = true;
    std::vector<std::atomic<uint64_t>> threadCommitted(FLAGS_ycsb_threads);
    std::vector<std::atomic<uint64_t>> threadAborted(FLAGS_ycsb_threads);

    std::vector<std::thread> threads;
    for (uint64_t workerId = 0; workerId < FLAGS_ycsb_threads; workerId++) {
      threads.emplace_back(
          [&](uint64_t threadId) {
            // session and cursor
            WT_SESSION* session = openSession("isolation=snapshot");
            WT_CURSOR* cursor = openCursor(session, "table:ycsb");

            // key buffer
            uint8_t key[FLAGS_ycsb_key_size];
            WT_ITEM keyItem;
            keyItem.data = key;
            keyItem.size = FLAGS_ycsb_key_size;

            // val buffer
            uint8_t val[FLAGS_ycsb_val_size];
            WT_ITEM valItem;
            valItem.data = val;
            valItem.size = FLAGS_ycsb_val_size;

            while (keepRunning) {
              switch (workloadType) {
              case Workload::kA:
              case Workload::kB:
              case Workload::kC: {
                auto readProbability = utils::RandomGenerator::Rand(0, 100);
                if (readProbability <= workload.mReadProportion * 100) {
                  // generate key for read
                  GenYcsbKey(zipfRandom, key);

                  // read from wiredtiger
                  cursor->set_key(cursor, &keyItem);
                  int ret = cursor->search(cursor);
                  if (ret != 0) {
                    Log::Fatal("Failed to search: {}", wiredtiger_strerror(ret));
                  }

                  // copy value out
                  cursor->get_value(cursor, &valItem);
                } else {
                  // generate key val for update
                  GenYcsbKey(zipfRandom, key);
                  utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

                  cursor->set_key(cursor, &keyItem);
                  cursor->set_value(cursor, &valItem);
                  int ret = cursor->update(cursor);
                  if (ret != 0) {
                    Log::Fatal("Failed to update: {}", wiredtiger_strerror(ret));
                  }
                }
                break;
              }
              default: {
                Log::Fatal("Unsupported workload type: {}", static_cast<uint8_t>(workloadType));
              }
              }
              threadCommitted[threadId]++;
            }
          },
          workerId);
    }

    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    printTpsSummary(1, FLAGS_ycsb_run_for_seconds, FLAGS_ycsb_threads, threadCommitted,
                    threadAborted);

    keepRunning.store(false);
    for (auto& thread : threads) {
      thread.join();
    }
  }

private:
  //! open wiredtiger
  void openWiredTiger(bool createFromScratch = true) {
    std::string dataDir = FLAGS_ycsb_data_dir + "/wiredtiger";

    if (createFromScratch) {
      // remove the existing data
      std::filesystem::remove_all(dataDir);

      // mkdir
      std::filesystem::create_directories(dataDir);
    }

    std::string configString(
        "create, direct_io=[data, log, checkpoint], "
        "log=(enabled=true,archive=true), statistics_log=(wait=1), "
        "statistics=(all, clear), session_max=2000, eviction=(threads_max=4), cache_size=" +
        std::to_string(FLAGS_ycsb_mem_kb / 1024) + "M");
    int ret = wiredtiger_open(dataDir.c_str(), nullptr, configString.c_str(), &mConn);
    if (ret != 0) {
      Log::Fatal("Failed to open wiredtiger: {}", wiredtiger_strerror(ret));
    }
  }

  //! open session
  WT_SESSION* openSession(const char* sessionConfig) {
    if (mConn == nullptr) {
      Log::Fatal("Wiredtiger connection is not opened");
    }

    WT_SESSION* session;
    int ret = mConn->open_session(mConn, nullptr, sessionConfig, &session);
    if (ret != 0) {
      Log::Fatal("Failed to open session: {}", wiredtiger_strerror(ret));
    }
    return session;
  }

  //! create table
  void createTable(WT_SESSION* session, const char* tableName) {
    const char* configString = "key_format=S,value_format=S";
    int ret = session->create(session, tableName, configString);
    if (ret != 0) {
      Log::Fatal("Failed to create table: {}", wiredtiger_strerror(ret));
    }
  }

  //! open a cursor
  WT_CURSOR* openCursor(WT_SESSION* session, const char* tableName) {
    WT_CURSOR* cursor;
    int ret = session->open_cursor(session, tableName, nullptr, "raw", &cursor);
    if (ret != 0) {
      Log::Fatal("Failed to open cursor: {}", wiredtiger_strerror(ret));
    }
    return cursor;
  }

  //! close cursor
  void closeCursor(WT_CURSOR* cursor) {
    int ret = cursor->close(cursor);
    if (ret != 0) {
      Log::Fatal("Failed to close cursor: {}", wiredtiger_strerror(ret));
    }
  }

  //! drop table
  void dropTable(WT_SESSION* session, const char* tableName) {
    int ret = session->drop(session, tableName, nullptr);
    if (ret != 0) {
      Log::Fatal("Failed to drop table: {}", wiredtiger_strerror(ret));
    }
  }

  //! close session
  void closeSession(WT_SESSION* session) {
    int ret = session->close(session, nullptr);
    if (ret != 0) {
      Log::Fatal("Failed to close session: {}", wiredtiger_strerror(ret));
    }
  }

  //! close wiredtiger
  void closeWiredTiger() {
    if (mConn != nullptr) {
      mConn->close(mConn, nullptr);
      mConn = nullptr;
    }
  }

  WT_CONNECTION* mConn;
#endif
};

} // namespace leanstore::ycsb