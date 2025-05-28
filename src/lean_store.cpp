#include "leanstore/lean_store.hpp"

#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/error.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/result.hpp"
#include "leanstore/utils/user_thread.hpp"

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <cstdint>
#include <expected>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>

#include <linux/fs.h>
#include <resolv.h>
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

  if (option->create_from_scratch_) {
    std::cout << std::format("Clean store dir: {}", option->store_dir_) << std::endl;
    std::filesystem::path dir_path(option->store_dir_);
    std::filesystem::remove_all(dir_path);
    std::filesystem::create_directories(dir_path);
  }

  Log::Init(option);
  return std::make_unique<LeanStore>(option);
}

LeanStore::LeanStore(StoreOption* option) : store_option_(option) {
  utils::tls_store = this;

  Log::Info("LeanStore starting ...");
  SCOPED_DEFER(Log::Info("LeanStore started"));

  init_page_and_wal_fd();

  // create global btree catalog
  tree_registry_ = std::make_unique<storage::TreeRegistry>();

  // create global buffer manager and page evictors
  buffer_manager_ = std::make_unique<storage::BufferManager>(this);
  buffer_manager_->StartPageEvictors();

  // create global transaction worker and group committer
  //
  // TODO(jian.z): Deserialize buffer manager before creating CRManager. We need to initialize
  // nextPageId for each buffer partition before creating history tree in CRManager
  crmanager_ = new cr::CRManager(this);

  // recover from disk
  if (!store_option_->create_from_scratch_) {
    auto all_pages_up_to_date = deserialize_meta();
    if (!all_pages_up_to_date) {
      Log::Info("Not all pages up-to-date, recover from disk");
      buffer_manager_->RecoverFromDisk();
    } else {
      Log::Info("All pages up-to-date, skip resovering");
      // TODO: truncate wal files
    }
  }
}

void LeanStore::init_page_and_wal_fd() {
  SCOPED_DEFER({
    LS_DCHECK(fcntl(page_fd_, F_GETFL) != -1);
    LS_DCHECK(fcntl(wal_fd_, F_GETFL) != -1);
  });

  // Create a new instance on the specified DB file
  if (store_option_->create_from_scratch_) {
    Log::Info("Create new page and wal files");
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    auto db_file_path = GetDbFilePath();
    page_fd_ = open(db_file_path.c_str(), flags, 0666);
    if (page_fd_ == -1) {
      Log::Fatal("Could not open file at: {}", db_file_path);
    }
    Log::Info("Init page fd succeed, pageFd={}, pageFile={}", page_fd_, db_file_path);

    auto wal_file_path = GetWalFilePath();
    wal_fd_ = open(wal_file_path.c_str(), flags, 0666);
    if (wal_fd_ == -1) {
      Log::Fatal("Could not open file at: {}", wal_file_path);
    }
    Log::Info("Init wal fd succeed, walFd={}, walFile={}", wal_fd_, wal_file_path);
    return;
  }

  // Recover pages and WAL from existing files
  deserialize_flags();
  Log::Info("Reopen existing page and wal files");
  int flags = O_RDWR | O_DIRECT;
  auto db_file_path = GetDbFilePath();
  page_fd_ = open(db_file_path.c_str(), flags, 0666);
  if (page_fd_ == -1) {
    Log::Fatal("Recover failed, could not open file at: {}. The data is lost, "
               "please create a new DB file and start a new instance from it",
               db_file_path);
  }
  Log::Info("Init page fd succeed, pageFd={}, pageFile={}", page_fd_, db_file_path);

  auto wal_file_path = GetWalFilePath();
  wal_fd_ = open(wal_file_path.c_str(), flags, 0666);
  if (wal_fd_ == -1) {
    Log::Fatal("Recover failed, could not open file at: {}. The data is lost, "
               "please create a new WAL file and start a new instance from it",
               wal_file_path);
  }
  Log::Info("Init wal fd succeed, walFd={}, walFile={}", wal_fd_, wal_file_path);
}

LeanStore::~LeanStore() {
  Log::Info("LeanStore stopping ...");
  SCOPED_DEFER({
    DestroyStoreOption(store_option_);
    Log::Info("LeanStore stopped");
  });

  // print trees
  for (auto& it : tree_registry_->trees_) {
    auto tree_id = it.first;
    auto& [tree_ptr, tree_name] = it.second;
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(tree_ptr.get());
    Log::Info("leanstore/btreeName={}, btreeId={}, btreeType={}, btreeSummary={}", tree_name,
              tree_id, static_cast<uint8_t>(btree->tree_type_), btree->Summary());
  }

  // Stop transaction workers and group committer
  crmanager_->Stop();

  // persist all the metadata and pages before exit
  bool all_pages_up_to_date = true;
  if (auto res = buffer_manager_->CheckpointAllBufferFrames(); !res) {
    all_pages_up_to_date = false;
  }
  serialize_meta(all_pages_up_to_date);

  buffer_manager_->SyncAllPageWrites();

  // destroy and Stop all foreground workers
  if (crmanager_ != nullptr) {
    delete crmanager_;
    crmanager_ = nullptr;
  }

  // destroy buffer manager (buffer frame providers)
  buffer_manager_->StopPageEvictors();
  buffer_manager_ = nullptr;

  // destroy global btree catalog
  tree_registry_ = nullptr;

  // close open fds
  if (close(page_fd_) == -1) {
    perror("Failed to close page file: ");
  } else {
    Log::Info("Page file closed");
  }

  {
    auto wal_file_path = GetWalFilePath();
    struct stat st;
    if (stat(wal_file_path.c_str(), &st) == 0) {
      LS_DLOG("The size of {} is {} bytes", wal_file_path, st.st_size);
    }
  }
  if (close(wal_fd_) == -1) {
    perror("Failed to close WAL file: ");
  } else {
    Log::Info("WAL file closed");
  }
}

void LeanStore::ExecSync(uint64_t worker_id, std::function<void()> job) {
  crmanager_->worker_threads_[worker_id]->SetJob(std::move(job));
  crmanager_->worker_threads_[worker_id]->Wait();
}

void LeanStore::ExecAsync(uint64_t worker_id, std::function<void()> job) {
  crmanager_->worker_threads_[worker_id]->SetJob(std::move(job));
}

void LeanStore::Wait(WORKERID worker_id) {
  crmanager_->worker_threads_[worker_id]->Wait();
}

void LeanStore::WaitAll() {
  for (auto i = 0u; i < store_option_->worker_threads_; i++) {
    crmanager_->worker_threads_[i]->Wait();
  }
}

constexpr char kMetaKeyCrManager[] = "cr_manager";
constexpr char kMetaKeyBufferManager[] = "buffer_manager";
constexpr char kMetaKeyBTrees[] = "leanstore/btrees";
constexpr char kMetaKeyFlags[] = "flags";

void LeanStore::serialize_meta(bool all_pages_up_to_date) {
  Log::Info("serializeMeta started");
  SCOPED_DEFER(Log::Info("serializeMeta ended"));

  // serialize data structure instances
  std::ofstream meta_file;
  meta_file.open(GetMetaFilePath(), std::ios::trunc);

  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  doc.SetObject();

  // cr_manager
  {
    auto cr_meta_map = crmanager_->Serialize();
    rapidjson::Value cr_json_obj(rapidjson::kObjectType);
    for (const auto& [key, val] : cr_meta_map) {
      rapidjson::Value k, v;
      k.SetString(key.data(), key.size(), allocator);
      v.SetString(val.data(), val.size(), allocator);
      cr_json_obj.AddMember(k, v, allocator);
    }
    doc.AddMember(kMetaKeyCrManager, cr_json_obj, allocator);
  }

  // buffer_manager
  {
    auto bm_meta_map = buffer_manager_->Serialize();
    rapidjson::Value bm_json_obj(rapidjson::kObjectType);
    for (const auto& [key, val] : bm_meta_map) {
      rapidjson::Value k, v;
      k.SetString(key.data(), key.size(), allocator);
      v.SetString(val.data(), val.size(), allocator);
      bm_json_obj.AddMember(k, v, allocator);
    }
    doc.AddMember(kMetaKeyBufferManager, bm_json_obj, allocator);
  }

  // registered_datastructures, i.e. btrees
  {
    rapidjson::Value btree_json_array(rapidjson::kArrayType);
    for (auto& it : tree_registry_->trees_) {
      auto btree_id = it.first;
      auto& [tree_ptr, btree_name] = it.second;
      if (btree_name.substr(0, 1) == "_") {
        continue;
      }

      auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(tree_ptr.get());
      rapidjson::Value btree_json_obj(rapidjson::kObjectType);
      rapidjson::Value btree_json_name;
      btree_json_name.SetString(btree_name.data(), btree_name.size(), allocator);
      btree_json_obj.AddMember("name", btree_json_name, allocator);

      rapidjson::Value btree_json_type(static_cast<uint8_t>(btree->tree_type_));
      btree_json_obj.AddMember("type", btree_json_type, allocator);

      rapidjson::Value btree_json_id(btree_id);
      btree_json_obj.AddMember("id", btree_json_id, allocator);

      rapidjson::Value btree_enable_wal(btree->config_.enable_wal_);
      btree_json_obj.AddMember("enable_wal", btree_enable_wal, allocator);

      rapidjson::Value btree_use_bulk_insert(btree->config_.use_bulk_insert_);
      btree_json_obj.AddMember("use_bulk_insert", btree_use_bulk_insert, allocator);

      auto btree_meta_map = tree_registry_->Serialize(btree_id);
      rapidjson::Value btree_meta_json_obj(rapidjson::kObjectType);
      for (const auto& [key, val] : btree_meta_map) {
        rapidjson::Value k, v;
        k.SetString(key.c_str(), key.length(), allocator);
        v.SetString(val.c_str(), val.length(), allocator);
        btree_meta_json_obj.AddMember(k, v, allocator);
      }
      btree_json_obj.AddMember("serialized", btree_meta_json_obj, allocator);

      btree_json_array.PushBack(btree_json_obj, allocator);
    }
    doc.AddMember(kMetaKeyBTrees, btree_json_array, allocator);
  }

  // pages_up_to_date
  {
    rapidjson::Value update_to_date(all_pages_up_to_date);
    doc.AddMember("pages_up_to_date", all_pages_up_to_date, doc.GetAllocator());
  }

  // flags
  serialize_flags(reinterpret_cast<uint8_t*>(&doc));

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  doc.Accept(writer);
  meta_file << sb.GetString();
}

void LeanStore::serialize_flags(uint8_t* dest) {
  rapidjson::Document& doc = *reinterpret_cast<rapidjson::Document*>(dest);
  Log::Info("serializeFlags started");
  SCOPED_DEFER(Log::Info("serializeFlags ended"));

  rapidjson::Value flags_json_obj(rapidjson::kObjectType);
  auto& allocator = doc.GetAllocator();

  doc.AddMember(kMetaKeyFlags, flags_json_obj, allocator);
}

bool LeanStore::deserialize_meta() {
  Log::Info("deserializeMeta started");
  SCOPED_DEFER(Log::Info("deserializeMeta ended"));

  std::ifstream meta_file;
  meta_file.open(GetMetaFilePath());
  rapidjson::IStreamWrapper isw(meta_file);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  // Deserialize concurrent resource manager
  {
    auto& cr_json_obj = doc[kMetaKeyCrManager];
    StringMap cr_meta_map;
    for (auto it = cr_json_obj.MemberBegin(); it != cr_json_obj.MemberEnd(); ++it) {
      cr_meta_map[it->name.GetString()] = it->value.GetString();
    }
    crmanager_->Deserialize(cr_meta_map);
  }

  // Deserialize buffer manager
  {
    auto& bm_json_obj = doc[kMetaKeyBufferManager];
    StringMap bm_meta_map;
    for (auto it = bm_json_obj.MemberBegin(); it != bm_json_obj.MemberEnd(); ++it) {
      bm_meta_map[it->name.GetString()] = it->value.GetString();
    }
    buffer_manager_->Deserialize(bm_meta_map);
  }

  // pages_up_to_date
  // rapidjson::Value updateToDate(allPagesUpToDate);
  // doc.AddMember("pages_up_to_date", allPagesUpToDate, doc.GetAllocator());
  auto& update_to_date = doc["pages_up_to_date"];
  auto all_pages_up_to_date = update_to_date.GetBool();

  auto& btree_json_array = doc[kMetaKeyBTrees];
  LS_DCHECK(btree_json_array.IsArray());
  for (auto& btree_json_obj : btree_json_array.GetArray()) {
    LS_DCHECK(btree_json_obj.IsObject());
    const TREEID btree_id = btree_json_obj["id"].GetInt64();
    const auto btree_type = btree_json_obj["type"].GetInt();
    const std::string btree_name = btree_json_obj["name"].GetString();
    const auto btree_enable_wal = btree_json_obj["enable_wal"].GetBool();
    const auto btree_use_bulk_insert = btree_json_obj["use_bulk_insert"].GetBool();

    StringMap btree_meta_map;
    auto& btree_meta_json_obj = btree_json_obj["serialized"];
    for (auto it = btree_meta_json_obj.MemberBegin(); it != btree_meta_json_obj.MemberEnd(); ++it) {
      btree_meta_map[it->name.GetString()] = it->value.GetString();
    }

    // create and register btrees
    switch (static_cast<leanstore::storage::btree::BTreeType>(btree_type)) {
    case leanstore::storage::btree::BTreeType::kBasicKV: {
      auto btree = std::make_unique<leanstore::storage::btree::BasicKV>();
      btree->store_ = this;
      btree->config_ =
          BTreeConfig{.enable_wal_ = btree_enable_wal, .use_bulk_insert_ = btree_use_bulk_insert};
      tree_registry_->RegisterTree(btree_id, std::move(btree), btree_name);
      break;
    }
    case leanstore::storage::btree::BTreeType::kTransactionKV: {
      auto btree = std::make_unique<leanstore::storage::btree::TransactionKV>();
      btree->store_ = this;
      btree->config_ =
          BTreeConfig{.enable_wal_ = btree_enable_wal, .use_bulk_insert_ = btree_use_bulk_insert};
      // create graveyard
      ExecSync(0, [&]() {
        auto graveyard_name = std::format("_{}_graveyard", btree_name);
        auto res = storage::btree::BasicKV::Create(
            this, graveyard_name, BTreeConfig{.enable_wal_ = false, .use_bulk_insert_ = false});
        if (!res) {
          Log::Error("Failed to create TransactionKV graveyard"
                     ", btreeVI={}, graveyardName={}, error={}",
                     btree_name, graveyard_name, res.error().ToString());
          return;
        }
        btree->graveyard_ = res.value();
      });

      tree_registry_->RegisterTree(btree_id, std::move(btree), btree_name);
      break;
    }
    default: {
      Log::Fatal("deserializeMeta failed, unsupported btree type={}", btree_type);
    }
    }
    tree_registry_->Deserialize(btree_id, btree_meta_map);
  }

  return all_pages_up_to_date;
}

void LeanStore::deserialize_flags() {
  Log::Info("deserializeFlags started");
  SCOPED_DEFER(Log::Info("deserializeFlags ended"));

  std::ifstream json_file;
  json_file.open(GetMetaFilePath());
  rapidjson::IStreamWrapper isw(json_file);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  const rapidjson::Value& flags_json_obj = doc[kMetaKeyFlags];
  StringMap serialized_flags;
  for (auto it = flags_json_obj.MemberBegin(); it != flags_json_obj.MemberEnd(); ++it) {
    serialized_flags[it->name.GetString()] = it->value.GetString();
  }
}

Result<storage::btree::BasicKV*> LeanStore::CreateBasicKv(const std::string& name,
                                                          BTreeConfig config) {
  return storage::btree::BasicKV::Create(this, name, std::move(config));
}

void LeanStore::GetBasicKV(const std::string& name, storage::btree::BasicKV** btree) {
  *btree = dynamic_cast<storage::btree::BasicKV*>(tree_registry_->GetTree(name));
}

void LeanStore::DropBasicKV(const std::string& name) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  auto* btree =
      dynamic_cast<leanstore::storage::btree::BTreeGeneric*>(tree_registry_->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = tree_registry_->UnregisterTree(name);
  if (!res) {
    Log::Error("UnRegister BasicKV failed, error={}", res.error().ToString());
  }
}

Result<storage::btree::TransactionKV*> LeanStore::CreateTransactionKV(const std::string& name,
                                                                      BTreeConfig config) {
  // create btree for graveyard
  auto graveyard_name = std::format("_{}_graveyard", name);
  leanstore::storage::btree::BasicKV* graveyard;
  if (auto res = storage::btree::BasicKV::Create(
          this, graveyard_name, BTreeConfig{.enable_wal_ = false, .use_bulk_insert_ = false});
      !res) {
    Log::Error("Failed to create TransactionKV graveyard"
               ", btreeVI={}, graveyardName={}, error={}",
               name, graveyard_name, res.error().ToString());
    return std::unexpected(std::move(res.error()));
  } else {
    graveyard = res.value();
  }

  // create transaction btree
  auto res = storage::btree::TransactionKV::Create(this, name, std::move(config), graveyard);
  if (!res) {
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(
        *static_cast<leanstore::storage::btree::BTreeGeneric*>(graveyard));
    auto res2 = tree_registry_->UnRegisterTree(graveyard->tree_id_);
    if (!res2) {
      Log::Error("UnRegister TransactionKV graveyard failed, graveyardName={}, "
                 "error={}",
                 graveyard_name, res2.error().ToString());
    }
  }
  return res;
}

void LeanStore::GetTransactionKV(const std::string& name, storage::btree::TransactionKV** btree) {
  *btree = dynamic_cast<storage::btree::TransactionKV*>(tree_registry_->GetTree(name));
}

void LeanStore::DropTransactionKV(const std::string& name) {
  LS_DCHECK(cr::WorkerContext::My().IsTxStarted());
  auto* btree = DownCast<storage::btree::BTreeGeneric*>(tree_registry_->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = tree_registry_->UnregisterTree(name);
  if (!res) {
    Log::Error("UnRegister TransactionKV failed, error={}", res.error().ToString());
  }

  auto graveyard_name = "_" + name + "_graveyard";
  btree = DownCast<storage::btree::BTreeGeneric*>(tree_registry_->GetTree(graveyard_name));
  LS_DCHECK(btree != nullptr, "graveyard not found");
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  res = tree_registry_->UnregisterTree(graveyard_name);
  if (!res) {
    Log::Error("UnRegister TransactionKV graveyard failed, error={}", res.error().ToString());
  }
}

} // namespace leanstore
