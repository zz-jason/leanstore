#include "LeanStore.hpp"

#include "profiling/counters/CPUCounters.hpp"
#include "profiling/counters/PPCounters.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "profiling/tables/BMTable.hpp"
#include "profiling/tables/CPUTable.hpp"
#include "profiling/tables/CRTable.hpp"
#include "profiling/tables/DTTable.hpp"
#include "profiling/tables/LatencyTable.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/FVector.hpp"
#include "utils/ThreadLocalAggregator.hpp"

#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "tabulate/table.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <termios.h>
#include <unistd.h>

#include <sys/stat.h>

#include <locale>
#include <sstream>

using namespace tabulate;
using leanstore::utils::threadlocal::sum;

namespace leanstore {

// Static members
FlagListString LeanStore::sPersistedStringFlags = {};
FlagListS64 LeanStore::sPersistedS64Flags = {};

LeanStore::LeanStore() {
  // init logging
  if (!google::IsGoogleLoggingInitialized()) {
    FLAGS_logtostderr = 1;
    // FLAGS_log_dir = GetLogDir();
    google::InitGoogleLogging("leanstore");
    LOG(INFO) << "LeanStore starting ...";
  }
  SCOPED_DEFER(LOG(INFO) << "LeanStore started");

  // init and verify flags
  if (FLAGS_recover) {
    DeSerializeFlags();
  }
  if ((FLAGS_vi) && !FLAGS_wal) {
    LOG(FATAL) << "BTreeVI is enabled without WAL, please enable FLAGS_wal";
  }
  if (FLAGS_isolation_level == "si" && (!FLAGS_mv | !FLAGS_vi)) {
    LOG(FATAL) << "Snapshot Isolation is only supported on BTreeVI with "
                  "multi-version enabled, please enable FLAGS_mv and FLAGS_vi";
  }

  // open file
  if (FLAGS_recover) {
    // recover pages and WAL from disk
    int flags = O_RDWR | O_DIRECT;
    mPageFd = open(GetDBFilePath().c_str(), flags, 0666);
    if (mPageFd == -1) {
      LOG(FATAL) << "Recover failed, could not open file at: "
                 << GetDBFilePath()
                 << ". The data is lost, please create a new DB file and start "
                    "a new instance from it";
    }

    mWalFd = open(GetWALFilePath().c_str(), flags, 0666);
    if (mPageFd == -1) {
      LOG(FATAL)
          << "Recover failed, could not open file at: " << GetWALFilePath()
          << ". The data is lost, please create a new WAL File and start "
             "a new instance from it";
    }
  } else {
    // Create a new instance on the specified DB file
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    mPageFd = open(GetDBFilePath().c_str(), flags, 0666);
    if (mPageFd == -1) {
      LOG(FATAL) << "Could not open file at: " << GetDBFilePath();
    }

    mWalFd = open(GetWALFilePath().c_str(), flags, 0666);
    if (mPageFd == -1) {
      LOG(FATAL) << "Could not open file at: " << GetWALFilePath();
    }

    // Write FLAGS_db_file_prealloc_gib data to the SSD file
    if (FLAGS_db_file_prealloc_gib > 0) {
      size_t gib = 1 << 30;
      auto dummyData = (u8*)aligned_alloc(512, gib);
      SCOPED_DEFER({
        free(dummyData);
        fsync(mPageFd);
      });

      for (u64 i = 0; i < FLAGS_db_file_prealloc_gib; i++) {
        auto ret = pwrite(mPageFd, dummyData, gib, gib * i);
        LOG_IF(FATAL, ret != (ssize_t)gib)
            << "Failed to pre-allocated disk file"
            << ", FLAGS_db_file_prealloc_gib=" << FLAGS_db_file_prealloc_gib;
      }
    }
  }
  DCHECK(fcntl(mPageFd, F_GETFL) != -1);
  DCHECK(fcntl(mWalFd, F_GETFL) != -1);

  // create global btree catalog
  storage::TreeRegistry::sInstance = std::make_unique<storage::TreeRegistry>();

  // create global buffer manager and buffer frame providers
  {
    BufferManager::sInstance =
        std::make_unique<storage::BufferManager>(mPageFd);
    BufferManager::sInstance->StartBufferFrameProviders();
  }

  // create global concurrenct resource manager
  cr::CRManager::sInstance = std::make_unique<cr::CRManager>(mWalFd);

  if (FLAGS_recover) {
    // deserialize meta from disk
    DeSerializeMeta();
    BufferManager::sInstance->RecoveryFromDisk(mWalFd);
  }
}

LeanStore::~LeanStore() {
  LOG(INFO) << "LeanStore stopping ...";
  SCOPED_DEFER({
    LOG(INFO) << "LeanStore stopped";

    // deinit logging in the last
    if (google::IsGoogleLoggingInitialized()) {
      google::ShutdownGoogleLogging();
    }
  });

  // wait all concurrent jobs to finsh
  cr::CRManager::sInstance->joinAll();

  if (FLAGS_enable_print_btree_stats_on_exit) {
    // print trees
    for (auto& it : storage::TreeRegistry::sInstance->mTrees) {
      auto treeId = it.first;
      auto& [treePtr, treeName] = it.second;
      auto btree = dynamic_cast<storage::btree::BTreeGeneric*>(treePtr.get());

      u64 numEntries(0);
      cr::CRManager::sInstance->scheduleJobSync(
          0, [&]() { numEntries = btree->countEntries(); });

      LOG(INFO) << "[BTreeVI] name=" << treeName << ", btreeId=" << treeId
                << ", height=" << btree->mHeight
                << ", numEntries=" << numEntries;
    }
  }

  // persist all the metadata and pages on exit
  SerializeMeta();
  BufferManager::sInstance->CheckpointAllBufferFrames();
  BufferManager::sInstance->SyncAllPageWrites();

  // destroy and stop all foreground workers
  cr::CRManager::sInstance = nullptr;

  // destroy buffer manager (buffer frame providers)
  BufferManager::sInstance = nullptr;

  // destroy global btree catalog
  storage::TreeRegistry::sInstance = nullptr;

  // close open fds
  if (close(mPageFd) == -1) {
    perror("Failed to close page file: ");
  } else {
    LOG(INFO) << "page file closed";
  }

  {
    struct stat st;
    if (stat(GetWALFilePath().c_str(), &st) == 0) {
      DLOG(INFO) << "The size of " << GetWALFilePath() << " is " << st.st_size
                 << " bytes";
    }
  }
  if (close(mWalFd) == -1) {
    perror("Failed to close WAL file: ");
  } else {
    LOG(INFO) << "WAL file closed";
  }

  // // destroy and  profiling threads
  // mProfilingThreadKeepRunning = false;
  // while (mNumProfilingThreads) {
  // }
}

void LeanStore::startProfilingThread() {
  std::thread profiling_thread([&]() {
    utils::pinThisThread(
        ((FLAGS_enable_pin_worker_threads) ? FLAGS_worker_threads : 0) +
        FLAGS_wal + FLAGS_pp_threads);
    if (FLAGS_root) {
      POSIX_CHECK(setpriority(PRIO_PROCESS, 0, -20) == 0);
    }

    profiling::BMTable bm_table(*BufferManager::sInstance.get());
    profiling::DTTable dt_table(*BufferManager::sInstance.get());
    profiling::CPUTable cpu_table;
    profiling::CRTable cr_table;
    profiling::LatencyTable latency_table;
    std::vector<profiling::ProfilingTable*> tables = {
        &configs_table, &bm_table, &dt_table, &cpu_table, &cr_table};
    if (FLAGS_profile_latency) {
      tables.push_back(&latency_table);
    }

    std::vector<std::ofstream> csvs;
    std::ofstream::openmode open_flags;
    if (FLAGS_csv_truncate) {
      open_flags = ios::trunc;
    } else {
      open_flags = ios::app;
    }
    for (u64 t_i = 0; t_i < tables.size(); t_i++) {
      tables[t_i]->open();

      csvs.emplace_back();
      auto& csv = csvs.back();
      csv.open(FLAGS_csv_path + "_" + tables[t_i]->getName() + ".csv",
               open_flags);
      csv.seekp(0, ios::end);
      csv << std::setprecision(2) << std::fixed;
      if (csv.tellp() == 0) {
        csv << "t,c_hash";
        for (auto& c : tables[t_i]->getColumns()) {
          csv << "," << c.first;
        }
        csv << endl;
      }
    }

    config_hash = configs_table.hash();

    u64 seconds = 0;
    while (mProfilingThreadKeepRunning) {
      for (u64 t_i = 0; t_i < tables.size(); t_i++) {
        tables[t_i]->next();
        if (tables[t_i]->size() == 0)
          continue;

        // CSV
        auto& csv = csvs[t_i];
        for (u64 r_i = 0; r_i < tables[t_i]->size(); r_i++) {
          csv << seconds << "," << config_hash;
          for (auto& c : tables[t_i]->getColumns()) {
            csv << "," << c.second.values[r_i];
          }
          csv << endl;
        }

        // TODO: Websocket, CLI
      }

      const u64 tx = std::stoull(cr_table.get("0", "tx"));
      const u64 olap_tx = std::stoull(cr_table.get("0", "olap_tx"));
      const double tx_abort = std::stod(cr_table.get("0", "tx_abort"));
      const double tx_abort_pct = tx_abort * 100.0 / (tx_abort + tx);
      const double rfa_pct =
          std::stod(cr_table.get("0", "rfa_committed_tx")) * 100.0 / tx;
      const double remote_flushes_pct = 100.0 - rfa_pct;
      global_stats.accumulated_tx_counter += tx;

      // Console
      const double instr_per_tx = cpu_table.workers_agg_events["instr"] / tx;
      const double cycles_per_tx = cpu_table.workers_agg_events["cycle"] / tx;
      const double l1_per_tx = cpu_table.workers_agg_events["L1-miss"] / tx;
      const double llc_per_tx = cpu_table.workers_agg_events["LLC-miss"] / tx;
      // using RowType = std::vector<variant<std::string, const char*, Table>>;
      if (FLAGS_print_tx_console) {
        tabulate::Table table;
        table.add_row({"t", "OLTP TX", "RF %", "Abort%", "OLAP TX", "W MiB",
                       "R MiB", "Instrs/TX", "Cycles/TX", "CPUs", "L1/TX",
                       "LLC/TX", "GHz", "WAL GiB/s", "GCT GiB/s", "Space G",
                       "GCT Rounds"});
        table.add_row({std::to_string(seconds), std::to_string(tx),
                       std::to_string(remote_flushes_pct),
                       std::to_string(tx_abort_pct), std::to_string(olap_tx),
                       bm_table.get("0", "w_mib"), bm_table.get("0", "r_mib"),
                       std::to_string(instr_per_tx),
                       std::to_string(cycles_per_tx),
                       std::to_string(cpu_table.workers_agg_events["CPU"]),
                       std::to_string(l1_per_tx), std::to_string(llc_per_tx),
                       std::to_string(cpu_table.workers_agg_events["GHz"]),
                       cr_table.get("0", "wal_write_gib"),
                       cr_table.get("0", "gct_write_gib"),
                       bm_table.get("0", "space_usage_gib"),
                       cr_table.get("0", "gct_rounds")});

        table.format().width(10);
        table.column(0).format().width(5);
        table.column(1).format().width(12);

        auto print_table = [](tabulate::Table& table,
                              std::function<bool(u64)> predicate) {
          std::stringstream ss;
          table.print(ss);
          string str = ss.str();
          u64 line_n = 0;
          for (u64 i = 0; i < str.size(); i++) {
            if (str[i] == '\n') {
              line_n++;
            }
            if (predicate(line_n)) {
              cout << str[i];
            }
          }
        };
        if (seconds == 0) {
          print_table(table,
                      [](u64 line_n) { return (line_n < 3) || (line_n == 4); });
        } else {
          print_table(table, [](u64 line_n) { return line_n == 4; });
        }
        // -------------------------------------------------------------------------------------
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        seconds += 1;
        std::locale::global(std::locale::classic());
      }
    }
    mNumProfilingThreads--;
  });
  mNumProfilingThreads++;
  profiling_thread.detach();
}

#define META_KEY_CR_MANAGER "cr_manager"
#define META_KEY_BUFFER_MANAGER "buffer_manager"
#define META_KEY_REGISTERED_DATASTRUCTURES "registered_datastructures"
#define META_KEY_FLAGS "flags"

void LeanStore::SerializeMeta() {
  LOG(INFO) << "SerializeMeta started";
  SCOPED_DEFER(LOG(INFO) << "SerializeMeta ended");

  // Serialize data structure instances
  std::ofstream metaFile;
  metaFile.open(leanstore::GetMetaFilePath(), ios::trunc);

  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  doc.SetObject();

  // cr_manager
  {
    auto crMetaMap = cr::CRManager::sInstance->serialize();
    rapidjson::Value crJsonObj(rapidjson::kObjectType);
    for (const auto& [key, val] : crMetaMap) {
      rapidjson::Value k, v;
      k.SetString(key.data(), key.size(), allocator);
      v.SetString(val.data(), val.size(), allocator);
      crJsonObj.AddMember(k, v, allocator);
    }
    doc.AddMember(META_KEY_CR_MANAGER, crJsonObj, allocator);
  }

  // buffer_manager
  {
    auto bmMetaMap = BufferManager::sInstance->serialize();
    rapidjson::Value bmJsonObj(rapidjson::kObjectType);
    for (const auto& [key, val] : bmMetaMap) {
      rapidjson::Value k, v;
      k.SetString(key.data(), key.size(), allocator);
      v.SetString(val.data(), val.size(), allocator);
      bmJsonObj.AddMember(k, v, allocator);
    }
    doc.AddMember(META_KEY_BUFFER_MANAGER, bmJsonObj, allocator);
  }

  // registered_datastructures, i.e. btrees
  {
    rapidjson::Value btreeJsonArray(rapidjson::kArrayType);
    for (auto& it : TreeRegistry::sInstance->mTrees) {
      auto btreeId = it.first;
      auto& [treePtr, btreeName] = it.second;
      if (btreeName.substr(0, 1) == "_") {
        continue;
      }

      auto btree = dynamic_cast<storage::btree::BTreeGeneric*>(treePtr.get());
      rapidjson::Value btreeJsonObj(rapidjson::kObjectType);
      rapidjson::Value btreeJsonName;
      btreeJsonName.SetString(btreeName.data(), btreeName.size(), allocator);
      btreeJsonObj.AddMember("name", btreeJsonName, allocator);

      rapidjson::Value btreeJsonType(static_cast<u8>(btree->mTreeType));
      btreeJsonObj.AddMember("type", btreeJsonType, allocator);

      rapidjson::Value btreeJsonId(btreeId);
      btreeJsonObj.AddMember("id", btreeJsonId, allocator);

      auto btreeMetaMap = TreeRegistry::sInstance->serialize(btreeId);
      rapidjson::Value btreeMetaJsonObj(rapidjson::kObjectType);
      for (const auto& [key, val] : btreeMetaMap) {
        rapidjson::Value k, v;
        k.SetString(key.c_str(), key.length(), allocator);
        v.SetString(val.c_str(), val.length(), allocator);
        btreeMetaJsonObj.AddMember(k, v, allocator);
      }
      btreeJsonObj.AddMember("serialized", btreeMetaJsonObj, allocator);

      btreeJsonArray.PushBack(btreeJsonObj, allocator);
    }
    doc.AddMember(META_KEY_REGISTERED_DATASTRUCTURES, btreeJsonArray,
                  allocator);
  }

  // flags
  SerializeFlags(doc);

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  doc.Accept(writer);
  metaFile << sb.GetString();
}

void LeanStore::SerializeFlags(rapidjson::Document& doc) {
  LOG(INFO) << "SerializeFlags started";
  SCOPED_DEFER(LOG(INFO) << "SerializeFlags ended");

  rapidjson::Value flagsJsonObj(rapidjson::kObjectType);
  auto& allocator = doc.GetAllocator();

  for (auto flag : sPersistedStringFlags) {
    auto& flagName = std::get<0>(flag);
    auto& flagValue = *std::get<1>(flag);
    rapidjson::Value name(flagName.data(), flagName.size(), allocator);
    rapidjson::Value value(flagValue.data(), flagValue.size(), allocator);
    flagsJsonObj.AddMember(name, value, allocator);
  }

  for (auto flag : sPersistedS64Flags) {
    auto& flagName = std::get<0>(flag);
    auto flagValue = std::to_string(*std::get<1>(flag));
    rapidjson::Value name(flagName.data(), flagName.size(), allocator);
    rapidjson::Value value(flagValue.data(), flagValue.size(), allocator);
    flagsJsonObj.AddMember(name, value, allocator);
  }

  doc.AddMember(META_KEY_FLAGS, flagsJsonObj, allocator);
}

void LeanStore::DeSerializeMeta() {
  LOG(INFO) << "DeSerializeMeta started";
  SCOPED_DEFER(LOG(INFO) << "DeSerializeMeta ended");

  std::ifstream metaFile;
  metaFile.open(leanstore::GetMetaFilePath());
  rapidjson::IStreamWrapper isw(metaFile);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  // deserialize concurrent resource manager
  {
    auto& crJsonObj = doc[META_KEY_CR_MANAGER];
    StringMap crMetaMap;
    for (auto it = crJsonObj.MemberBegin(); it != crJsonObj.MemberEnd(); ++it) {
      crMetaMap[it->name.GetString()] = it->value.GetString();
    }
    cr::CRManager::sInstance->deserialize(crMetaMap);
  }

  // deserialize buffer manager
  {
    auto& bmJsonObj = doc[META_KEY_BUFFER_MANAGER];
    StringMap bmMetaMap;
    for (auto it = bmJsonObj.MemberBegin(); it != bmJsonObj.MemberEnd(); ++it) {
      bmMetaMap[it->name.GetString()] = it->value.GetString();
    }
    BufferManager::sInstance->deserialize(bmMetaMap);
  }

  auto& btreeJsonArray = doc[META_KEY_REGISTERED_DATASTRUCTURES];
  DCHECK(btreeJsonArray.IsArray());
  for (auto& btreeJsonObj : btreeJsonArray.GetArray()) {
    DCHECK(btreeJsonObj.IsObject());
    const TREEID btreeId = btreeJsonObj["id"].GetInt64();
    const auto btreeType = btreeJsonObj["type"].GetInt();
    const std::string btreeName = btreeJsonObj["name"].GetString();

    StringMap btreeMetaMap;
    auto& btreeMetaJsonObj = btreeJsonObj["serialized"];
    for (auto it = btreeMetaJsonObj.MemberBegin();
         it != btreeMetaJsonObj.MemberEnd(); ++it) {
      btreeMetaMap[it->name.GetString()] = it->value.GetString();
    }

    // create and register btrees
    switch (static_cast<leanstore::storage::btree::BTREE_TYPE>(btreeType)) {
    case leanstore::storage::btree::BTREE_TYPE::LL: {
      auto btree = std::make_unique<leanstore::storage::btree::BTreeLL>();
      TreeRegistry::sInstance->RegisterTree(btreeId, std::move(btree),
                                            btreeName);
      break;
    }
    case leanstore::storage::btree::BTREE_TYPE::VI: {
      auto btree = std::make_unique<leanstore::storage::btree::BTreeVI>();
      TreeRegistry::sInstance->RegisterTree(btreeId, std::move(btree),
                                            btreeName);
      break;
    }
    default: {
      LOG(FATAL) << "DeSerializeMeta failed"
                 << ", unsupported btree type=" << btreeType;
    }
    }
    TreeRegistry::sInstance->deserialize(btreeId, btreeMetaMap);
  }
}

void LeanStore::DeSerializeFlags() {
  LOG(INFO) << "DeSerializeFlags started";
  SCOPED_DEFER(LOG(INFO) << "DeSerializeFlags ended");

  std::ifstream jsonFile;
  jsonFile.open(leanstore::GetMetaFilePath());
  rapidjson::IStreamWrapper isw(jsonFile);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  const rapidjson::Value& flagsJsonObj = doc[META_KEY_FLAGS];
  StringMap serializedFlags;
  for (auto it = flagsJsonObj.MemberBegin(); it != flagsJsonObj.MemberEnd();
       ++it) {
    serializedFlags[it->name.GetString()] = it->value.GetString();
  }

  for (auto flags : sPersistedStringFlags) {
    auto& flagKey = std::get<0>(flags);
    *std::get<1>(flags) = serializedFlags[flagKey];
  }

  for (auto flags : sPersistedS64Flags) {
    auto& flagKey = std::get<0>(flags);
    *std::get<1>(flags) =
        strtoll(serializedFlags[flagKey].c_str(), nullptr, 10);
  }
}

} // namespace leanstore
