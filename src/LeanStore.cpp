#include "leanstore/LeanStore.hpp"

#include "leanstore-c/StoreOption.h"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/btree/core/BTreeGeneric.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Error.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Result.hpp"
#include "leanstore/utils/UserThread.hpp"
#include "telemetry/MetricsManager.hpp"

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <tabulate/table.hpp>

#include <cstdint>
#include <expected>
#include <filesystem>
#include <format>
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

Result<std::unique_ptr<LeanStore>> LeanStore::Open(StoreOption* option) {
  if (option == nullptr) {
    return std::unexpected(leanstore::utils::Error::General("StoreOption should not be null"));
  }

  if (option->mCreateFromScratch) {
    std::cout << std::format("Clean store dir: {}", option->mStoreDir) << std::endl;
    std::filesystem::path dirPath = option->mStoreDir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
  }

  Log::Init(option);
  return std::make_unique<LeanStore>(option);
}

LeanStore::LeanStore(StoreOption* option) : mStoreOption(option), mMetricsManager(nullptr) {
  utils::tlsStore = this;

  Log::Info("LeanStore starting ...");
  SCOPED_DEFER(Log::Info("LeanStore started"));

  // Expose the metrics
  if (mStoreOption->mEnableMetrics) {
    mMetricsManager = std::make_unique<leanstore::telemetry::MetricsManager>(this);
    mMetricsManager->Expose();
  }

  initPageAndWalFd();

  // create global btree catalog
  mTreeRegistry = std::make_unique<storage::TreeRegistry>();

  // create global buffer manager and page evictors
  mBufferManager = std::make_unique<storage::BufferManager>(this);
  mBufferManager->StartPageEvictors();

  // create global transaction worker and group committer
  //
  // TODO(jian.z): Deserialize buffer manager before creating CRManager. We need to initialize
  // nextPageId for each buffer partition before creating history tree in CRManager
  mCRManager = std::make_unique<cr::CRManager>(this);

  // recover from disk
  if (!mStoreOption->mCreateFromScratch) {
    auto allPagesUpToDate = deserializeMeta();
    if (!allPagesUpToDate) {
      Log::Info("Not all pages up-to-date, recover from disk");
      mBufferManager->RecoverFromDisk();
    } else {
      Log::Info("All pages up-to-date, skip resovering");
      // TODO: truncate wal files
    }
  }
}

void LeanStore::initPageAndWalFd() {
  SCOPED_DEFER({
    LS_DCHECK(fcntl(mPageFd, F_GETFL) != -1);
    LS_DCHECK(fcntl(mWalFd, F_GETFL) != -1);
  });

  // Create a new instance on the specified DB file
  if (mStoreOption->mCreateFromScratch) {
    Log::Info("Create new page and wal files");
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    auto dbFilePath = GetDbFilePath();
    mPageFd = open(dbFilePath.c_str(), flags, 0666);
    if (mPageFd == -1) {
      Log::Fatal("Could not open file at: {}", dbFilePath);
    }
    Log::Info("Init page fd succeed, pageFd={}, pageFile={}", mPageFd, dbFilePath);

    auto walFilePath = GetWalFilePath();
    mWalFd = open(walFilePath.c_str(), flags, 0666);
    if (mWalFd == -1) {
      Log::Fatal("Could not open file at: {}", walFilePath);
    }
    Log::Info("Init wal fd succeed, walFd={}, walFile={}", mWalFd, walFilePath);
    return;
  }

  // Recover pages and WAL from existing files
  deserializeFlags();
  Log::Info("Reopen existing page and wal files");
  int flags = O_RDWR | O_DIRECT;
  auto dbFilePath = GetDbFilePath();
  mPageFd = open(dbFilePath.c_str(), flags, 0666);
  if (mPageFd == -1) {
    Log::Fatal("Recover failed, could not open file at: {}. The data is lost, "
               "please create a new DB file and start a new instance from it",
               dbFilePath);
  }
  Log::Info("Init page fd succeed, pageFd={}, pageFile={}", mPageFd, dbFilePath);

  auto walFilePath = GetWalFilePath();
  mWalFd = open(walFilePath.c_str(), flags, 0666);
  if (mWalFd == -1) {
    Log::Fatal("Recover failed, could not open file at: {}. The data is lost, "
               "please create a new WAL file and start a new instance from it",
               walFilePath);
  }
  Log::Info("Init wal fd succeed, walFd={}, walFile={}", mWalFd, walFilePath);
}

LeanStore::~LeanStore() {
  Log::Info("LeanStore stopping ...");
  SCOPED_DEFER({
    // stop metrics manager in the last
    if (mStoreOption->mEnableMetrics) {
      mMetricsManager = nullptr;
    }
    DestroyStoreOption(mStoreOption);
    Log::Info("LeanStore stopped");
  });

  // wait all concurrent jobs to finsh
  WaitAll();

  // print trees
  for (auto& it : mTreeRegistry->mTrees) {
    auto treeId = it.first;
    auto& [treePtr, treeName] = it.second;
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(treePtr.get());
    Log::Info("leanstore/btreeName={}, btreeId={}, btreeType={}, btreeHeight={}", treeName, treeId,
              static_cast<uint8_t>(btree->mTreeType), btree->mHeight.load());
  }

  // Stop transaction workers and group committer
  mCRManager->Stop();

  // persist all the metadata and pages before exit
  bool allPagesUpToDate = true;
  if (auto res = mBufferManager->CheckpointAllBufferFrames(); !res) {
    allPagesUpToDate = false;
  }
  serializeMeta(allPagesUpToDate);

  mBufferManager->SyncAllPageWrites();

  // destroy and Stop all foreground workers
  mCRManager = nullptr;

  // destroy buffer manager (buffer frame providers)
  mBufferManager->StopPageEvictors();
  mBufferManager = nullptr;

  // destroy global btree catalog
  mTreeRegistry = nullptr;

  // close open fds
  if (close(mPageFd) == -1) {
    perror("Failed to close page file: ");
  } else {
    Log::Info("Page file closed");
  }

  {
    auto walFilePath = GetWalFilePath();
    struct stat st;
    if (stat(walFilePath.c_str(), &st) == 0) {
      LS_DLOG("The size of {} is {} bytes", walFilePath, st.st_size);
    }
  }
  if (close(mWalFd) == -1) {
    perror("Failed to close WAL file: ");
  } else {
    Log::Info("WAL file closed");
  }
}

void LeanStore::ExecSync(uint64_t workerId, std::function<void()> job) {
  mCRManager->mWorkerThreads[workerId]->SetJob(job);
  mCRManager->mWorkerThreads[workerId]->Wait();
}

void LeanStore::ExecAsync(uint64_t workerId, std::function<void()> job) {
  mCRManager->mWorkerThreads[workerId]->SetJob(job);
}

void LeanStore::Wait(WORKERID workerId) {
  mCRManager->mWorkerThreads[workerId]->Wait();
}

void LeanStore::WaitAll() {
  for (auto i = 0u; i < mStoreOption->mWorkerThreads; i++) {
    mCRManager->mWorkerThreads[i]->Wait();
  }
}

constexpr char kMetaKeyCrManager[] = "cr_manager";
constexpr char kMetaKeyBufferManager[] = "buffer_manager";
constexpr char kMetaKeyBTrees[] = "leanstore/btrees";
constexpr char kMetaKeyFlags[] = "flags";

void LeanStore::serializeMeta(bool allPagesUpToDate) {
  Log::Info("serializeMeta started");
  SCOPED_DEFER(Log::Info("serializeMeta ended"));

  // serialize data structure instances
  std::ofstream metaFile;
  metaFile.open(GetMetaFilePath(), std::ios::trunc);

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
    doc.AddMember(kMetaKeyCrManager, crJsonObj, allocator);
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
    doc.AddMember(kMetaKeyBufferManager, bmJsonObj, allocator);
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
    doc.AddMember(kMetaKeyBTrees, btreeJsonArray, allocator);
  }

  // pages_up_to_date
  {
    rapidjson::Value updateToDate(allPagesUpToDate);
    doc.AddMember("pages_up_to_date", allPagesUpToDate, doc.GetAllocator());
  }

  // flags
  serializeFlags(reinterpret_cast<uint8_t*>(&doc));

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  doc.Accept(writer);
  metaFile << sb.GetString();
}

void LeanStore::serializeFlags(uint8_t* dest) {
  rapidjson::Document& doc = *reinterpret_cast<rapidjson::Document*>(dest);
  Log::Info("serializeFlags started");
  SCOPED_DEFER(Log::Info("serializeFlags ended"));

  rapidjson::Value flagsJsonObj(rapidjson::kObjectType);
  auto& allocator = doc.GetAllocator();

  doc.AddMember(kMetaKeyFlags, flagsJsonObj, allocator);
}

bool LeanStore::deserializeMeta() {
  Log::Info("deserializeMeta started");
  SCOPED_DEFER(Log::Info("deserializeMeta ended"));

  std::ifstream metaFile;
  metaFile.open(GetMetaFilePath());
  rapidjson::IStreamWrapper isw(metaFile);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  // Deserialize concurrent resource manager
  {
    auto& crJsonObj = doc[kMetaKeyCrManager];
    StringMap crMetaMap;
    for (auto it = crJsonObj.MemberBegin(); it != crJsonObj.MemberEnd(); ++it) {
      crMetaMap[it->name.GetString()] = it->value.GetString();
    }
    mCRManager->Deserialize(crMetaMap);
  }

  // Deserialize buffer manager
  {
    auto& bmJsonObj = doc[kMetaKeyBufferManager];
    StringMap bmMetaMap;
    for (auto it = bmJsonObj.MemberBegin(); it != bmJsonObj.MemberEnd(); ++it) {
      bmMetaMap[it->name.GetString()] = it->value.GetString();
    }
    mBufferManager->Deserialize(bmMetaMap);
  }

  // pages_up_to_date
  // rapidjson::Value updateToDate(allPagesUpToDate);
  // doc.AddMember("pages_up_to_date", allPagesUpToDate, doc.GetAllocator());
  auto& updateToDate = doc["pages_up_to_date"];
  auto allPagesUpToDate = updateToDate.GetBool();

  auto& btreeJsonArray = doc[kMetaKeyBTrees];
  LS_DCHECK(btreeJsonArray.IsArray());
  for (auto& btreeJsonObj : btreeJsonArray.GetArray()) {
    LS_DCHECK(btreeJsonObj.IsObject());
    const TREEID btreeId = btreeJsonObj["id"].GetInt64();
    const auto btreeType = btreeJsonObj["type"].GetInt();
    const std::string btreeName = btreeJsonObj["name"].GetString();

    StringMap btreeMetaMap;
    auto& btreeMetaJsonObj = btreeJsonObj["serialized"];
    for (auto it = btreeMetaJsonObj.MemberBegin(); it != btreeMetaJsonObj.MemberEnd(); ++it) {
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
        auto graveyardName = std::format("_{}_graveyard", btreeName);
        auto res = storage::btree::BasicKV::Create(
            this, graveyardName, BTreeConfig{.mEnableWal = false, .mUseBulkInsert = false});
        if (!res) {
          Log::Error("Failed to create TransactionKV graveyard"
                     ", btreeVI={}, graveyardName={}, error={}",
                     btreeName, graveyardName, res.error().ToString());
          return;
        }
        btree->mGraveyard = res.value();
      });

      mTreeRegistry->RegisterTree(btreeId, std::move(btree), btreeName);
      break;
    }
    default: {
      Log::Fatal("deserializeMeta failed, unsupported btree type={}", btreeType);
    }
    }
    mTreeRegistry->Deserialize(btreeId, btreeMetaMap);
  }

  return allPagesUpToDate;
}

void LeanStore::deserializeFlags() {
  Log::Info("deserializeFlags started");
  SCOPED_DEFER(Log::Info("deserializeFlags ended"));

  std::ifstream jsonFile;
  jsonFile.open(GetMetaFilePath());
  rapidjson::IStreamWrapper isw(jsonFile);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  const rapidjson::Value& flagsJsonObj = doc[kMetaKeyFlags];
  StringMap serializedFlags;
  for (auto it = flagsJsonObj.MemberBegin(); it != flagsJsonObj.MemberEnd(); ++it) {
    serializedFlags[it->name.GetString()] = it->value.GetString();
  }
}

Result<storage::btree::BasicKV*> LeanStore::CreateBasicKV(const std::string& name,
                                                          BTreeConfig config) {
  return storage::btree::BasicKV::Create(this, name, std::move(config));
}

void LeanStore::GetBasicKV(const std::string& name, storage::btree::BasicKV** btree) {
  *btree = dynamic_cast<storage::btree::BasicKV*>(mTreeRegistry->GetTree(name));
}

void LeanStore::DropBasicKV(const std::string& name) {
  LS_DCHECK(cr::Worker::My().IsTxStarted());
  auto* btree =
      dynamic_cast<leanstore::storage::btree::BTreeGeneric*>(mTreeRegistry->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = mTreeRegistry->UnregisterTree(name);
  if (!res) {
    Log::Error("UnRegister BasicKV failed, error={}", res.error().ToString());
  }
}

Result<storage::btree::TransactionKV*> LeanStore::CreateTransactionKV(const std::string& name,
                                                                      BTreeConfig config) {
  // create btree for graveyard
  auto graveyardName = std::format("_{}_graveyard", name);
  leanstore::storage::btree::BasicKV* graveyard;
  if (auto res = storage::btree::BasicKV::Create(
          this, graveyardName, BTreeConfig{.mEnableWal = false, .mUseBulkInsert = false});
      !res) {
    Log::Error("Failed to create TransactionKV graveyard"
               ", btreeVI={}, graveyardName={}, error={}",
               name, graveyardName, res.error().ToString());
    return std::unexpected(std::move(res.error()));
  } else {
    graveyard = res.value();
  }

  // create transaction btree
  auto res = storage::btree::TransactionKV::Create(this, name, std::move(config), graveyard);
  if (!res) {
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(
        *static_cast<leanstore::storage::btree::BTreeGeneric*>(graveyard));
    auto res2 = mTreeRegistry->UnRegisterTree(graveyard->mTreeId);
    if (!res2) {
      Log::Error("UnRegister TransactionKV graveyard failed, graveyardName={}, "
                 "error={}",
                 graveyardName, res2.error().ToString());
    }
  }
  return res;
}

void LeanStore::GetTransactionKV(const std::string& name, storage::btree::TransactionKV** btree) {
  *btree = dynamic_cast<storage::btree::TransactionKV*>(mTreeRegistry->GetTree(name));
}

void LeanStore::DropTransactionKV(const std::string& name) {
  LS_DCHECK(cr::Worker::My().IsTxStarted());
  auto* btree = DownCast<storage::btree::BTreeGeneric*>(mTreeRegistry->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = mTreeRegistry->UnregisterTree(name);
  if (!res) {
    Log::Error("UnRegister TransactionKV failed, error={}", res.error().ToString());
  }

  auto graveyardName = "_" + name + "_graveyard";
  btree = DownCast<storage::btree::BTreeGeneric*>(mTreeRegistry->GetTree(graveyardName));
  LS_DCHECK(btree != nullptr, "graveyard not found");
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  res = mTreeRegistry->UnregisterTree(graveyardName);
  if (!res) {
    Log::Error("UnRegister TransactionKV graveyard failed, error={}", res.error().ToString());
  }
}

} // namespace leanstore
