#include "Config.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "shared-headers/Units.hpp"
#include "shared/LeanStoreAdapter.hpp"
#include "shared/Schema.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"
#include "utils/ScrambledZipfGenerator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_insert_threads, 0, "");
DEFINE_uint32(ycsb_threads, 0, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_bool(ycsb_warmup, true, "");
DEFINE_uint32(ycsb_sleepy_thread, 0, "");
DEFINE_uint32(ycsb_ops_per_tx, 1, "");

using namespace leanstore;

using YCSBKey = u64;
using YCSBPayload = BytesPayload<8>;
using KVTable = Relation<YCSBKey, YCSBPayload>;

double CalculateMTPS(chrono::high_resolution_clock::time_point begin,
                     chrono::high_resolution_clock::time_point end,
                     u64 factor) {
  double tps =
      ((factor * 1.0 /
        (chrono::duration_cast<chrono::microseconds>(end - begin).count() /
         1000000.0)));
  return (tps / 1000000.0);
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage("LeanStore YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  chrono::high_resolution_clock::time_point begin, end;

  // Always init with the maximum number of threads (FLAGS_worker_threads)
  LeanStore db;
  auto& crm = *cr::CRManager::sInstance;
  LeanStoreAdapter<KVTable> table;
  crm.ScheduleJobSync(0,
                      [&]() { table = LeanStoreAdapter<KVTable>(db, "YCSB"); });

  // db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
  // db.registerConfigEntry("ycsb_threads", FLAGS_ycsb_threads);
  // db.registerConfigEntry("ycsb_ops_per_tx", FLAGS_ycsb_ops_per_tx);

  auto isolationLevel = leanstore::ParseIsolationLevel(FLAGS_isolation_level);
  const TxMode txType = TxMode::kShortRunning;

  const u64 ycsb_tuple_count =
      (FLAGS_ycsb_tuple_count)
          ? FLAGS_ycsb_tuple_count
          : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 /
                (sizeof(YCSBKey) + sizeof(YCSBPayload));

  // Insert values
  const u64 n = ycsb_tuple_count;

  if (FLAGS_recover) {
    // Warmup
    if (FLAGS_ycsb_warmup) {
      LOG(INFO) << "Warmup: Scanning...";
      begin = chrono::high_resolution_clock::now();
      utils::Parallelize::range(
          FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
            crm.ScheduleJobAsync(t_i, [&, begin, end]() {
              for (u64 i = begin; i < end; i++) {
                YCSBPayload result;
                cr::Worker::My().StartTx(txType, isolationLevel);
                table.lookup1(
                    {static_cast<YCSBKey>(i)},
                    [&](const KVTable& record) { result = record.mValue; });
                cr::Worker::My().CommitTx();
              }
            });
          });
      crm.JoinAll();
      end = chrono::high_resolution_clock::now();

      LOG(INFO)
          << "time elapsed = "
          << (chrono::duration_cast<chrono::microseconds>(end - begin).count() /
              1000000.0)
          << " seconds";
      LOG(INFO) << CalculateMTPS(begin, end, n) << " M tps";
    }
  } else {
    LOG(INFO) << "Inserting " << ycsb_tuple_count << " values";
    begin = chrono::high_resolution_clock::now();
    utils::Parallelize::range(
        FLAGS_ycsb_insert_threads ? FLAGS_ycsb_insert_threads
                                  : FLAGS_worker_threads,
        n, [&](u64 t_i, u64 begin, u64 end) {
          crm.ScheduleJobAsync(t_i, [&, begin, end]() {
            for (u64 i = begin; i < end; i++) {
              YCSBPayload payload;
              utils::RandomGenerator::RandString(
                  reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
              YCSBKey key = i;
              cr::Worker::My().StartTx(
                  txType, leanstore::IsolationLevel::kSnapshotIsolation);
              table.insert({key}, {payload});
              cr::Worker::My().CommitTx();
            }
          });
        });
    crm.JoinAll();
    end = chrono::high_resolution_clock::now();
    LOG(INFO)
        << "time elapsed = "
        << (chrono::duration_cast<chrono::microseconds>(end - begin).count() /
            1000000.0)
        << " seconds";
    LOG(INFO) << CalculateMTPS(begin, end, n) << " M tps";

    const u64 written_pages = BufferManager::sInstance->consumedPages();
    const u64 mib = written_pages * FLAGS_page_size / 1024 / 1024;
    LOG(INFO) << "Inserted volume: (pages, MiB) = (" << written_pages << ", "
              << mib << ")";
  }

  auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(
      0, ycsb_tuple_count, FLAGS_zipf_factor);
  cout << setprecision(4);

  cout << "~Transactions" << endl;
  db.startProfilingThread();
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  const u32 exec_threads =
      FLAGS_ycsb_threads ? FLAGS_ycsb_threads : FLAGS_worker_threads;
  for (u64 t_i = 0; t_i < exec_threads - ((FLAGS_ycsb_sleepy_thread) ? 1 : 0);
       t_i++) {
    crm.ScheduleJobAsync(t_i, [&]() {
      running_threads_counter++;
      while (keep_running) {
        JUMPMU_TRY() {
          YCSBKey key;
          if (FLAGS_zipf_factor == 0) {
            key = utils::RandomGenerator::RandU64(0, ycsb_tuple_count);
          } else {
            key = zipf_random->rand();
          }
          DCHECK(key < ycsb_tuple_count);
          YCSBPayload result;
          cr::Worker::My().StartTx(txType, isolationLevel);
          for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
            if (FLAGS_ycsb_read_ratio == 100 ||
                utils::RandomGenerator::RandU64(0, 100) <
                    FLAGS_ycsb_read_ratio) {
              table.lookup1({key},
                            [&](const KVTable&) {}); // result = record.mValue;
            } else {
              const auto updateDescBufSize =
                  sizeof(leanstore::UpdateDesc) +
                  (sizeof(leanstore::UpdateSlotInfo) * 1);
              u8 updateDescBuf[updateDescBufSize];
              auto& updateDesc = *leanstore::UpdateDesc::From(updateDescBuf);
              updateDesc.mNumSlots = 1;
              updateDesc.mUpdateSlots[0].mOffset = offsetof(KVTable, mValue);
              updateDesc.mUpdateSlots[0].mSize = sizeof(KVTable::mValue);

              utils::RandomGenerator::RandString(
                  reinterpret_cast<u8*>(&result), sizeof(YCSBPayload));

              table.update1(
                  {key}, [&](KVTable& rec) { rec.mValue = result; },
                  updateDesc);
            }
          }
          cr::Worker::My().CommitTx();
          WorkerCounters::MyCounters().tx++;
        }
        JUMPMU_CATCH() {
          WorkerCounters::MyCounters().tx_abort++;
        }
      }
      running_threads_counter--;
    });
  }

  if (FLAGS_ycsb_sleepy_thread) {
    const leanstore::TxMode tx_type = FLAGS_enable_long_running_transaction
                                          ? leanstore::TxMode::kLongRunning
                                          : leanstore::TxMode::kShortRunning;
    crm.ScheduleJobAsync(exec_threads - 1, [&]() {
      running_threads_counter++;
      while (keep_running) {
        JUMPMU_TRY() {
          cr::Worker::My().StartTx(
              tx_type, leanstore::IsolationLevel::kSnapshotIsolation);
          sleep(FLAGS_ycsb_sleepy_thread);
          cr::Worker::My().CommitTx();
        }
        JUMPMU_CATCH() {
        }
      }
      running_threads_counter--;
    });
  }

  {
    // Shutdown threads
    sleep(FLAGS_run_for_seconds);
    keep_running = false;
    while (running_threads_counter) {
    }
    crm.JoinAll();
  }
  cout << "--------------------------------------------------------------------"
          "-----------------"
       << endl;
  return 0;
}
