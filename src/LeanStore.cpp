#include "leanstore/LeanStore.hpp"

#include "TxWorkerImpl.hpp"
#include "btree/BasicKV.hpp"
#include "btree/TransactionKV.hpp"
#include "btree/core/BTreeGeneric.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/CRManager.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/Store.hpp"
#include "profiling/tables/BMTable.hpp"
#include "utils/Defer.hpp"
#include "utils/Misc.hpp"
#include "utils/UserThread.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <tabulate/table.hpp>

#include <expected>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>

#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

namespace leanstore {

std::expected<std::unique_ptr<LeanStore>, utils::Error> LeanStore::Open() {
  if (FLAGS_init) {
    std::cout << "Clean data dir: " << FLAGS_data_dir << std::endl;
    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    std::filesystem::create_directories(GetLogDir());
  }
  // for glog
  FLAGS_log_dir = GetLogDir();
  return std::make_unique<LeanStore>();
}

LeanStore::LeanStore() : mMetricsManager() {
  initStoreOption();
  initGoogleLog();

  LOG(INFO) << "LeanStore starting ...";
  SCOPED_DEFER(LOG(INFO) << "LeanStore started");

  // Expose the metrics
  if (FLAGS_enable_metrics) {
    mMetricsManager.Expose();
  }

  initPageAndWalFd();

  // create global btree catalog
  mTreeRegistry = std::make_unique<storage::TreeRegistry>();

  // create global buffer manager and buffer frame providers
  mBufferManager = std::make_unique<storage::BufferManager>(this);
  mBufferManager->StartBufferFrameProviders();

  // create global transaction worker and group committer
  //
  // TODO(jian.z): Deserialize buffer manager before creating CRManager. We need
  // to initialize nextPageId for each buffer partition before creating history
  // tree in CRManager
  mCRManager = std::make_unique<cr::CRManager>(this);

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
  // google::InitGoogleLogging("leanstore");

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
    LOG(FATAL) << std::format(
        "Recover failed, could not open file at: {}. The data is lost, please "
        "create a new DB file and start a new instance from it",
        GetDBFilePath());
  }

  mWalFd = open(GetWALFilePath().c_str(), flags, 0666);
  if (mPageFd == -1) {
    LOG(FATAL) << std::format(
        "Recover failed, could not open file at: {}. The data is lost, please "
        "create a new WAL file and start a new instance from it",
        GetWALFilePath());
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
  WaitAll();

  // print trees
  for (auto& it : mTreeRegistry->mTrees) {
    auto treeId = it.first;
    auto& [treePtr, treeName] = it.second;
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(treePtr.get());

    uint64_t numEntries(0);
    ExecSync(0, [&]() { numEntries = btree->CountEntries(); });

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
}

std::expected<std::unique_ptr<TxWorker>, utils::Error> LeanStore::GetTxWorker(
    WORKERID workerId) {
  return std::make_unique<TxWorkerImpl>(this, workerId);
}

void LeanStore::ExecSync(uint64_t workerId, std::function<void()> job) {
  mCRManager->mWorkerThreads[workerId]->SetJob(job);
  mCRManager->mWorkerThreads[workerId]->Wait();
}

void LeanStore::ExecAsync(uint64_t workerId, std::function<void()> job) {
  mCRManager->mWorkerThreads[workerId]->SetJob(job);
}

/// Waits for the worker to complete.
void LeanStore::Wait(WORKERID workerId) {
  mCRManager->mWorkerThreads[workerId]->Wait();
}

void LeanStore::WaitAll() {
  for (uint32_t i = 0; i < mStoreOption.mNumTxWorkers; i++) {
    mCRManager->mWorkerThreads[i]->Wait();
  }
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

      rapidjson::Value btreeJsonType(static_cast<uint8_t>(btree->mTreeType));
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
      ExecSync(0, [&]() {
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
}

void LeanStore::CreateBasicKV(const std::string& name,
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

void LeanStore::DropBasicKV(const std::string& name) {
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

void LeanStore::CreateTransactionKV(const std::string& name,
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

void LeanStore::DropTransactionKV(const std::string& name) {
  DCHECK(cr::Worker::My().IsTxStarted());
  auto* btree =
      DownCast<storage::btree::BTreeGeneric*>(mTreeRegistry->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = mTreeRegistry->UnregisterTree(name);
  if (!res) {
    LOG(ERROR) << "UnRegister TransactionKV failed"
               << ", error=" << res.error().ToString();
  }

  auto graveyardName = "_" + name + "_graveyard";
  btree = DownCast<storage::btree::BTreeGeneric*>(
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
