#include "LeanStore.hpp"

#include "Config.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "profiling/tables/BMTable.hpp"
#include "profiling/tables/CPUTable.hpp"
#include "profiling/tables/CRTable.hpp"
#include "profiling/tables/DTTable.hpp"
#include "profiling/tables/LatencyTable.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/TransactionKV.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/Defer.hpp"
#include "utils/UserThread.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <tabulate/table.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>

#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

namespace leanstore {

// Static members
FlagListString LeanStore::sPersistedStringFlags = {};
FlagListS64 LeanStore::sPersistedS64Flags = {};

std::expected<LeanStore*, utils::Error> LeanStore::Open() {
  static std::shared_mutex sMutex;
  static std::unique_ptr<LeanStore> sLeanstore = nullptr;

  std::shared_lock readGuard(sMutex);
  if (sLeanstore != nullptr) {
    return sLeanstore.get();
  }
  readGuard.unlock();

  std::unique_lock writeGuard(sMutex);
  if (sLeanstore != nullptr) {
    return sLeanstore.get();
  }

  if (FLAGS_init) {
    std::cout << "Clean data dir: " << FLAGS_data_dir << std::endl;
    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    std::filesystem::create_directories(GetLogDir());
    // for glog
    FLAGS_log_dir = GetLogDir();
  }
  sLeanstore = std::make_unique<LeanStore>();
  return sLeanstore.get();
}

LeanStore::LeanStore() {
  initStoreOption();
  initGoogleLog();

  LOG(INFO) << "LeanStore starting ...";
  SCOPED_DEFER(LOG(INFO) << "LeanStore started");

  initPageAndWalFd();

  // create global btree catalog
  mTreeRegistry = std::make_unique<storage::TreeRegistry>();

  // create global buffer manager and buffer frame providers
  mBufferManager = std::make_unique<storage::BufferManager>(this, mPageFd);
  mBufferManager->StartBufferFrameProviders();

  // create global transaction worker and group committer
  mCRManager = std::make_unique<cr::CRManager>(this, mWalFd);

  // recover from disk
  if (!mStoreOption.mCreateFromScratch) {
    deserializeMeta();
    mBufferManager->RecoverFromDisk();
  }
}

// TODO: abandon the usage of gflags, init a StoreOption before creating the
// store. StoreOption is designed to be used to create a store.
void LeanStore::initStoreOption() {
  mStoreOption.mStoreDir = FLAGS_data_dir;
  mStoreOption.mCreateFromScratch = FLAGS_init;
  mStoreOption.mNumPartitions = 1 << FLAGS_partition_bits;
  mStoreOption.mBufferPoolSize = FLAGS_buffer_pool_size;
  mStoreOption.mPageSize = FLAGS_page_size;
  mStoreOption.mWalRingBufferSize = FLAGS_wal_buffer_size;
  mStoreOption.mNumTxWorkers = FLAGS_worker_threads;
  mStoreOption.mNumBufferProviders = FLAGS_pp_threads;
  mStoreOption.mEnableGc = FLAGS_enable_garbage_collection;
  mStoreOption.mEnableEagerGc = FLAGS_enable_eager_garbage_collection;
}

void LeanStore::initGoogleLog() {
  if (google::IsGoogleLoggingInitialized()) {
    return;
  }

  auto customPrefixCallback = [](std::ostream& s,
                                 const google::LogMessageInfo& m, void*) {
    // severity
    s << "[" << std::string(m.severity) << "]";

    // YYYY-MM-DD hh:mm::ss.xxxxxx
    s << " [" << setw(4) << 1900 + m.time.year() << "-" << setw(2)
      << 1 + m.time.month() << "-" << setw(2) << m.time.day() << ' ' << setw(2)
      << m.time.hour() << ':' << setw(2) << m.time.min() << ':' << setw(2)
      << m.time.sec() << "." << setw(6) << m.time.usec() << ']';

    // thread id and name
    if (utils::tlsThreadName.size() > 0) {
      s << " [" << setfill(' ') << setw(5) << m.thread_id << setfill('0') << " "
        << utils::tlsThreadName << ']';
    } else {
      s << " [" << setfill(' ') << setw(5) << m.thread_id << setfill('0')
        << ']';
    }

    // filename and line number
    s << " [" << m.filename << ':' << m.line_number << "]";
  };
  google::InitGoogleLogging("leanstore", customPrefixCallback, nullptr);
}

void LeanStore::initPageAndWalFd() {
  SCOPED_DEFER({
    DCHECK(fcntl(mPageFd, F_GETFL) != -1);
    DCHECK(fcntl(mWalFd, F_GETFL) != -1);
  });

  // Create a new instance on the specified DB file
  if (mStoreOption.mCreateFromScratch) {
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    mPageFd = open(GetDBFilePath().c_str(), flags, 0666);
    if (mPageFd == -1) {
      LOG(FATAL) << "Could not open file at: " << GetDBFilePath();
    }

    mWalFd = open(GetWALFilePath().c_str(), flags, 0666);
    if (mPageFd == -1) {
      LOG(FATAL) << "Could not open file at: " << GetWALFilePath();
    }
    return;
  }

  // Recover pages and WAL from existing files
  deserializeFlags();
  int flags = O_RDWR | O_DIRECT;
  mPageFd = open(GetDBFilePath().c_str(), flags, 0666);
  if (mPageFd == -1) {
    LOG(FATAL) << "Recover failed, could not open file at: " << GetDBFilePath()
               << ". The data is lost, please create a new DB file and start "
                  "a new instance from it";
  }

  mWalFd = open(GetWALFilePath().c_str(), flags, 0666);
  if (mPageFd == -1) {
    LOG(FATAL) << "Recover failed, could not open file at: " << GetWALFilePath()
               << ". The data is lost, please create a new WAL File and start "
                  "a new instance from it";
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
  mCRManager->WaitAll();

  // print trees
  for (auto& it : mTreeRegistry->mTrees) {
    auto treeId = it.first;
    auto& [treePtr, treeName] = it.second;
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(treePtr.get());

    u64 numEntries(0);
    mCRManager->ExecSync(0, [&]() { numEntries = btree->CountEntries(); });

    LOG(INFO) << "[TransactionKV] name=" << treeName << ", btreeId=" << treeId
              << ", height=" << btree->mHeight << ", numEntries=" << numEntries;
  }

  // Stop transaction workers and group committer
  mCRManager->Stop();

  // persist all the metadata and pages before exit
  serializeMeta();
  mBufferManager->CheckpointAllBufferFrames();
  mBufferManager->SyncAllPageWrites();

  // destroy and Stop all foreground workers
  mCRManager = nullptr;

  // destroy buffer manager (buffer frame providers)
  mBufferManager = nullptr;

  // destroy global btree catalog
  mTreeRegistry = nullptr;

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

  // // destroy profiling threads
  // mProfilingThreadKeepRunning = false;
  // while (mNumProfilingThreads) {
  // }
}

void LeanStore::StartProfilingThread() {
  std::thread profilingThread([&]() {
    utils::PinThisThread(
        ((FLAGS_enable_pin_worker_threads) ? FLAGS_worker_threads : 0) +
        FLAGS_wal + FLAGS_pp_threads);
    if (FLAGS_root) {
      POSIX_CHECK(setpriority(PRIO_PROCESS, 0, -20) == 0);
    }

    profiling::BMTable bm_table(*mBufferManager);
    profiling::DTTable dt_table(*mBufferManager);
    profiling::CPUTable cpu_table;
    profiling::CRTable cr_table;
    profiling::LatencyTable latency_table;
    std::vector<profiling::ProfilingTable*> tables = {
        &mConfigsTable, &bm_table, &dt_table, &cpu_table, &cr_table};
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

    mConfigHash = mConfigsTable.hash();

    u64 seconds = 0;
    while (mProfilingThreadKeepRunning) {
      for (u64 t_i = 0; t_i < tables.size(); t_i++) {
        tables[t_i]->next();
        if (tables[t_i]->size() == 0)
          continue;

        // CSV
        auto& csv = csvs[t_i];
        for (u64 r_i = 0; r_i < tables[t_i]->size(); r_i++) {
          csv << seconds << "," << mConfigHash;
          for (auto& c : tables[t_i]->getColumns()) {
            csv << "," << c.second.values[r_i];
          }
          csv << endl;
        }

        // TODO: Websocket, CLI
      }

      const u64 tx = std::stoull(cr_table.get("0", "tx"));
      const u64 long_running_tx =
          std::stoull(cr_table.get("0", "long_running_tx"));
      const double tx_abort = std::stod(cr_table.get("0", "tx_abort"));
      const double tx_abort_pct = tx_abort * 100.0 / (tx_abort + tx);
      const double rfa_pct =
          std::stod(cr_table.get("0", "rfa_committed_tx")) * 100.0 / tx;
      const double remote_flushes_pct = 100.0 - rfa_pct;
      mGlobalStats.mAccumulatedTxCounter += tx;

      // Console
      const double instr_per_tx = cpu_table.workers_agg_events["instr"] / tx;
      const double cycles_per_tx = cpu_table.workers_agg_events["cycle"] / tx;
      const double l1_per_tx = cpu_table.workers_agg_events["L1-miss"] / tx;
      const double llc_per_tx = cpu_table.workers_agg_events["LLC-miss"] / tx;
      // using RowType = std::vector<variant<std::string, const char*, Table>>;
      if (FLAGS_print_tx_console) {
        tabulate::Table table;
        table.add_row({"t", "ShortRunning TX", "RF %", "Abort%",
                       "LongRunning TX", "W MiB", "R MiB", "Instrs/TX",
                       "Cycles/TX", "CPUs", "L1/TX", "LLC/TX", "GHz",
                       "WAL GiB/s", "GCT GiB/s", "Space G", "GCT Rounds"});
        table.add_row(
            {std::to_string(seconds), std::to_string(tx),
             std::to_string(remote_flushes_pct), std::to_string(tx_abort_pct),
             std::to_string(long_running_tx), bm_table.get("0", "w_mib"),
             bm_table.get("0", "r_mib"), std::to_string(instr_per_tx),
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
  profilingThread.detach();
}

#define META_KEY_CR_MANAGER "cr_manager"
#define META_KEY_BUFFER_MANAGER "buffer_manager"
#define META_KEY_REGISTERED_DATASTRUCTURES "registered_datastructures"
#define META_KEY_FLAGS "flags"

void LeanStore::serializeMeta() {
  LOG(INFO) << "serializeMeta started";
  SCOPED_DEFER(LOG(INFO) << "serializeMeta ended");

  // serialize data structure instances
  std::ofstream metaFile;
  metaFile.open(leanstore::GetMetaFilePath(), ios::trunc);

  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  doc.SetObject();

  // cr_manager
  {
    auto crMetaMap = mCRManager->Serialize();
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
    auto bmMetaMap = mBufferManager->Serialize();
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
    for (auto& it : mTreeRegistry->mTrees) {
      auto btreeId = it.first;
      auto& [treePtr, btreeName] = it.second;
      if (btreeName.substr(0, 1) == "_") {
        continue;
      }

      auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(treePtr.get());
      rapidjson::Value btreeJsonObj(rapidjson::kObjectType);
      rapidjson::Value btreeJsonName;
      btreeJsonName.SetString(btreeName.data(), btreeName.size(), allocator);
      btreeJsonObj.AddMember("name", btreeJsonName, allocator);

      rapidjson::Value btreeJsonType(static_cast<u8>(btree->mTreeType));
      btreeJsonObj.AddMember("type", btreeJsonType, allocator);

      rapidjson::Value btreeJsonId(btreeId);
      btreeJsonObj.AddMember("id", btreeJsonId, allocator);

      auto btreeMetaMap = mTreeRegistry->Serialize(btreeId);
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
  serializeFlags(doc);

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  doc.Accept(writer);
  metaFile << sb.GetString();
}

void LeanStore::serializeFlags(rapidjson::Document& doc) {
  LOG(INFO) << "serializeFlags started";
  SCOPED_DEFER(LOG(INFO) << "serializeFlags ended");

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

void LeanStore::deserializeMeta() {
  LOG(INFO) << "deserializeMeta started";
  SCOPED_DEFER(LOG(INFO) << "deserializeMeta ended");

  std::ifstream metaFile;
  metaFile.open(leanstore::GetMetaFilePath());
  rapidjson::IStreamWrapper isw(metaFile);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  // Deserialize concurrent resource manager
  {
    auto& crJsonObj = doc[META_KEY_CR_MANAGER];
    StringMap crMetaMap;
    for (auto it = crJsonObj.MemberBegin(); it != crJsonObj.MemberEnd(); ++it) {
      crMetaMap[it->name.GetString()] = it->value.GetString();
    }
    mCRManager->Deserialize(crMetaMap);
  }

  // Deserialize buffer manager
  {
    auto& bmJsonObj = doc[META_KEY_BUFFER_MANAGER];
    StringMap bmMetaMap;
    for (auto it = bmJsonObj.MemberBegin(); it != bmJsonObj.MemberEnd(); ++it) {
      bmMetaMap[it->name.GetString()] = it->value.GetString();
    }
    mBufferManager->Deserialize(bmMetaMap);
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
    switch (static_cast<leanstore::storage::btree::BTreeType>(btreeType)) {
    case leanstore::storage::btree::BTreeType::kBasicKV: {
      auto btree = std::make_unique<leanstore::storage::btree::BasicKV>();
      btree->mStore = this;
      mTreeRegistry->RegisterTree(btreeId, std::move(btree), btreeName);
      break;
    }
    case leanstore::storage::btree::BTreeType::kTransactionKV: {
      auto btree = std::make_unique<leanstore::storage::btree::TransactionKV>();
      btree->mStore = this;
      // create graveyard
      mCRManager->ExecSync(0, [&]() {
        auto graveyardName = "_" + btreeName + "_graveyard";
        auto graveyardConfig = storage::btree::BTreeConfig{
            .mEnableWal = false, .mUseBulkInsert = false};
        auto res = storage::btree::BasicKV::Create(this, graveyardName,
                                                   graveyardConfig);
        if (!res) {
          LOG(ERROR) << "Failed to create TransactionKV graveyard"
                     << ", btreeVI=" << btreeName
                     << ", graveyardName=" << graveyardName
                     << ", error=" << res.error().ToString();
          return;
        }
        btree->mGraveyard = res.value();
      });

      mTreeRegistry->RegisterTree(btreeId, std::move(btree), btreeName);
      break;
    }
    default: {
      LOG(FATAL) << "deserializeMeta failed"
                 << ", unsupported btree type=" << btreeType;
    }
    }
    mTreeRegistry->Deserialize(btreeId, btreeMetaMap);
  }
}

void LeanStore::deserializeFlags() {
  LOG(INFO) << "deserializeFlags started";
  SCOPED_DEFER(LOG(INFO) << "deserializeFlags ended");

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

void LeanStore::RegisterBasicKV(const std::string& name,
                                storage::btree::BTreeConfig& config,
                                storage::btree::BasicKV** btree) {
  DCHECK(cr::Worker::My().IsTxStarted());
  auto res = storage::btree::BasicKV::Create(this, name, config);
  if (!res) {
    LOG(ERROR) << "Failed to register BasicKV"
               << ", name=" << name << ", error=" << res.error().ToString();
    *btree = nullptr;
    return;
  }

  *btree = res.value();
}

void LeanStore::GetBasicKV(const std::string& name,
                           storage::btree::BasicKV** btree) {
  *btree = dynamic_cast<storage::btree::BasicKV*>(mTreeRegistry->GetTree(name));
}

void LeanStore::UnRegisterBasicKV(const std::string& name) {
  DCHECK(cr::Worker::My().IsTxStarted());
  auto* btree =
      dynamic_cast<btree::BTreeGeneric*>(mTreeRegistry->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = mTreeRegistry->UnregisterTree(name);
  if (!res) {
    LOG(ERROR) << "UnRegister BasicKV failed"
               << ", error=" << res.error().ToString();
  }
}

void LeanStore::RegisterTransactionKV(const std::string& name,
                                      storage::btree::BTreeConfig& config,
                                      storage::btree::TransactionKV** btree) {
  DCHECK(cr::Worker::My().IsTxStarted());
  *btree = nullptr;

  // create btree for graveyard
  auto graveyardName = "_" + name + "_graveyard";
  auto graveyardConfig =
      storage::btree::BTreeConfig{.mEnableWal = false, .mUseBulkInsert = false};
  auto res =
      storage::btree::BasicKV::Create(this, graveyardName, graveyardConfig);
  if (!res) {
    LOG(ERROR) << "Failed to create TransactionKV graveyard"
               << ", btreeVI=" << name << ", graveyardName=" << graveyardName
               << ", error=" << res.error().ToString();
    return;
  }
  auto* graveyard = res.value();

  // clean resource on failure
  SCOPED_DEFER(if (*btree == nullptr && graveyard != nullptr) {
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(
        *static_cast<leanstore::storage::btree::BTreeGeneric*>(graveyard));
    auto res = mTreeRegistry->UnRegisterTree(graveyard->mTreeId);
    if (!res) {
      LOG(ERROR) << "UnRegister graveyard failed"
                 << ", error=" << res.error().ToString();
    }
  });

  // create btree for main data
  *btree = storage::btree::TransactionKV::Create(this, name, config, graveyard);
}

void LeanStore::GetTransactionKV(const std::string& name,
                                 storage::btree::TransactionKV** btree) {
  *btree = dynamic_cast<storage::btree::TransactionKV*>(
      mTreeRegistry->GetTree(name));
}

void LeanStore::UnRegisterTransactionKV(const std::string& name) {
  DCHECK(cr::Worker::My().IsTxStarted());
  auto* btree =
      dynamic_cast<storage::btree::BTreeGeneric*>(mTreeRegistry->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = mTreeRegistry->UnregisterTree(name);
  if (!res) {
    LOG(ERROR) << "UnRegister TransactionKV failed"
               << ", error=" << res.error().ToString();
  }

  auto graveyardName = "_" + name + "_graveyard";
  btree = dynamic_cast<storage::btree::BTreeGeneric*>(
      mTreeRegistry->GetTree(graveyardName));
  DCHECK(btree != nullptr) << "graveyard not found";
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  res = mTreeRegistry->UnregisterTree(graveyardName);
  if (!res) {
    LOG(ERROR) << "UnRegister TransactionKV graveyard failed"
               << ", error=" << res.error().ToString();
  }
}

} // namespace leanstore
