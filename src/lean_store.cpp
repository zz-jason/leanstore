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
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/parallelize.hpp"
#include "leanstore/utils/result.hpp"
#include "utils/coroutine/mvcc_manager.hpp"
#include "utils/json.hpp"
#include "utils/scoped_timer.hpp"

#include <cassert>
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
    Log::Info("Create store from scratch, store_dir={}", option->store_dir_);
    std::filesystem::path dir_path(option->store_dir_);
    std::filesystem::remove_all(dir_path);
    std::filesystem::create_directories(dir_path);
  }
  Log::Init(option);
  return std::make_unique<LeanStore>(option);
}

LeanStore::LeanStore(StoreOption* option) : store_option_(option) {
  CoroEnv::SetCurStore(this);

  Log::Info("LeanStore starting ...");
  SCOPED_DEFER(Log::Info("LeanStore started"));

  InitDbFiles();

  // create global btree catalog
  tree_registry_ = std::make_unique<storage::TreeRegistry>();

  // create global buffer manager and page evictors
  buffer_manager_ = std::make_unique<storage::BufferManager>(this);

  auto num_tx_mgrs = store_option_->worker_threads_ * store_option_->max_concurrent_tx_per_worker_;
  mvcc_mgr_ = std::make_unique<leanstore::MvccManager>(num_tx_mgrs, this);

#ifdef ENABLE_COROUTINE
  coro_scheduler_ = new CoroScheduler(this, store_option_->worker_threads_);
  coro_scheduler_->Init();
  buffer_manager_->InitFreeBfLists();
  crmanager_ = nullptr;
#else
  buffer_manager_->InitFreeBfLists();
  buffer_manager_->StartPageEvictors();
  crmanager_ = new cr::CRManager(this);
#endif

  // recover from disk
  if (!store_option_->create_from_scratch_) {
    auto all_pages_up_to_date = DeserializeMeta();
    if (!all_pages_up_to_date) {
      Log::Info("Not all pages up-to-date, recover from disk");
      buffer_manager_->RecoverFromDisk();
    } else {
      Log::Info("All pages up-to-date, skip resovering");
      // TODO: truncate wal files
    }
  }
}

void LeanStore::InitDbFiles() {
  SCOPED_DEFER({
    LEAN_DCHECK(fcntl(page_fd_, F_GETFL) != -1);
    LEAN_DCHECK(fcntl(wal_fd_, F_GETFL) != -1);
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
    Log::Deinit();
  });

  // print trees
  tree_registry_->VisitAllTrees([](const storage::TreeMap& all_trees) {
    for (const auto& it : all_trees) {
      auto tree_id = it.first;
      auto& [tree_ptr, tree_name] = it.second;
      auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(tree_ptr.get());
      Log::Info("btree: name={}, id={}, type={}, summary={}", tree_name, tree_id,
                static_cast<uint8_t>(btree->tree_type_), btree->Summary());
    }
  });

  // Stop transaction workers and group committer
  if (crmanager_) {
    crmanager_->Stop();
  }

  // persist all the metadata and pages before exit
  bool all_pages_up_to_date = true;
  if (auto res = buffer_manager_->CheckpointAllBufferFrames(); !res) {
    all_pages_up_to_date = false;
  }
  SerializeMeta(all_pages_up_to_date);

  buffer_manager_->SyncAllPageWrites();

  StopBackgroundThreads();

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
      LEAN_DLOG("The size of {} is {} bytes", wal_file_path, st.st_size);
    }
  }
  if (close(wal_fd_) == -1) {
    perror("Failed to close WAL file: ");
  } else {
    Log::Info("WAL file closed");
  }
}

void LeanStore::StartBackgroundThreads() {
#ifdef ENABLE_COROUTINE
  coro_scheduler_ = new CoroScheduler(this, store_option_->worker_threads_);
  coro_scheduler_->Init();
#else
  buffer_manager_->StartPageEvictors();
  crmanager_ = new cr::CRManager(this);
#endif
}

void LeanStore::StopBackgroundThreads() {
#ifdef ENABLE_COROUTINE
  // destroy coro scheduler
  if (coro_scheduler_ != nullptr) {
    coro_scheduler_->Deinit();
    delete coro_scheduler_;
    coro_scheduler_ = nullptr;
  }
#else
  // destroy and Stop all foreground workers
  if (crmanager_ != nullptr) {
    delete crmanager_;
    crmanager_ = nullptr;
  }
  // destroy buffer manager (buffer frame providers)
  buffer_manager_->StopPageEvictors();
#endif
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

void LeanStore::ParallelRange(
    uint64_t num_jobs, std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler) {
#ifdef ENABLE_COROUTINE
  coro_scheduler_->ParallelRange(num_jobs, std::move(job_handler));
#else
  utils::Parallelize::ParallelRange(num_jobs, std::move(job_handler));
#endif
}

constexpr char kMetaKeyCrManager[] = "cr_manager";
constexpr char kMetaKeyMvcc[] = "mvcc";
constexpr char kMetaKeyBufferManager[] = "buffer_manager";
constexpr char kMetaKeyBTrees[] = "leanstore/btrees";
constexpr char kMetaKeyFlags[] = "flags";
constexpr char kName[] = "name";
constexpr char kType[] = "type";
constexpr char kId[] = "id";
constexpr char kEnableWal[] = "enable_wal";
constexpr char kUseBulkInsert[] = "use_bulk_insert";
constexpr char kSerialized[] = "serialized";
constexpr char kPagesUpToDate[] = "pages_up_to_date";

void LeanStore::SerializeMeta(bool all_pages_up_to_date) {
  Log::Info("serializeMeta started");
  ScopedTimer timer([](double elapsed_ms) {
    Log::Info("SerializeMeta finished, timeElapsed={:.2f}ms", elapsed_ms);
  });

  // serialize data structure instances
  utils::JsonObj meta_json_obj;
  std::ofstream meta_file;
  meta_file.open(GetMetaFilePath(), std::ios::trunc);

  // cr_manager
  if (crmanager_) {
    meta_json_obj.AddJsonObj(kMetaKeyCrManager, crmanager_->Serialize());
  }

  meta_json_obj.AddJsonObj(kMetaKeyMvcc, mvcc_mgr_->Serialize());

  // buffer_manager
  meta_json_obj.AddJsonObj(kMetaKeyBufferManager, buffer_manager_->Serialize());

  // registered_datastructures, i.e. btrees
  {
    utils::JsonArray btree_json_array;
    tree_registry_->VisitAllTrees([&](const storage::TreeMap& all_trees) {
      for (const auto& it : all_trees) {
        auto btree_id = it.first;
        auto& [tree_ptr, btree_name] = it.second;
        if (btree_name.substr(0, 1) == "_") {
          continue;
        }

        auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(tree_ptr.get());
        utils::JsonObj btree_meta_json_obj;
        auto btree_meta_map = btree->Serialize();
        for (const auto& [key, val] : btree_meta_map) {
          btree_meta_json_obj.AddString(key, val);
        }

        utils::JsonObj btree_json_obj;
        btree_json_obj.AddString(kName, btree_name);
        btree_json_obj.AddInt64(kType, static_cast<int64_t>(btree->tree_type_));
        btree_json_obj.AddInt64(kId, btree_id);
        btree_json_obj.AddBool(kEnableWal, btree->config_.enable_wal_);
        btree_json_obj.AddBool(kUseBulkInsert, btree->config_.use_bulk_insert_);
        btree_json_obj.AddJsonObj(kSerialized, btree_meta_json_obj);

        btree_json_array.AppendJsonObj(btree_json_obj);
      }
    });

    meta_json_obj.AddJsonArray(kMetaKeyBTrees, btree_json_array);
  }

  // pages_up_to_date
  meta_json_obj.AddBool(kPagesUpToDate, all_pages_up_to_date);

  meta_file << meta_json_obj.Serialize();
}

bool LeanStore::DeserializeMeta() {
  Log::Info("deserializeMeta started");
  ScopedTimer timer([](double elapsed_ms) {
    Log::Info("DeserializeMeta finished, timeElapsed={:.2f}ms", elapsed_ms);
  });

  std::ifstream meta_file;
  meta_file.open(GetMetaFilePath());

  utils::JsonObj meta_json_obj;
  meta_json_obj.Deserialize(
      std::string(std::istreambuf_iterator<char>(meta_file), std::istreambuf_iterator<char>()));

  // Deserialize concurrent resource manager
  if (crmanager_) {
    assert(meta_json_obj.HasMember(kMetaKeyCrManager));
    crmanager_->Deserialize(meta_json_obj.GetJsonObj(kMetaKeyCrManager).value());
  }

  mvcc_mgr_->Deserialize(meta_json_obj.GetJsonObj(kMetaKeyMvcc).value());

  // Deserialize buffer manager
  assert(meta_json_obj.HasMember(kMetaKeyBufferManager));
  buffer_manager_->Deserialize(meta_json_obj.GetJsonObj(kMetaKeyBufferManager).value());

  assert(meta_json_obj.HasMember(kMetaKeyBTrees));
  auto all_pages_up_to_date = meta_json_obj.GetBool("pages_up_to_date").value();

  assert(meta_json_obj.HasMember(kMetaKeyBTrees));
  auto btree_json_array = meta_json_obj.GetJsonArray(kMetaKeyBTrees).value();

  for (auto i = 0u; i < btree_json_array.Size(); ++i) {
    assert(btree_json_array.GetJsonObj(i).has_value());
    auto btree_json_obj = btree_json_array.GetJsonObj(i).value();

    const TREEID btree_id = btree_json_obj.GetInt64("id").value();
    const auto btree_type = btree_json_obj.GetInt64("type").value();
    const auto btree_name_ref = btree_json_obj.GetString("name").value();
    const auto btree_enable_wal = btree_json_obj.GetBool("enable_wal").value();
    const auto btree_use_bulk_insert = btree_json_obj.GetBool("use_bulk_insert").value();

    std::string btree_name(btree_name_ref.data(), btree_name_ref.size());

    StringMap btree_meta_map;
    auto btree_meta_json_obj = btree_json_obj.GetJsonObj("serialized").value();
    btree_meta_json_obj.Foreach([&](const std::string_view& key, const utils::JsonValue& value) {
      assert(value.IsString());
      auto meta_key = std::string(key.data(), key.size());
      auto meta_val = std::string(value.GetString(), value.GetStringLength());
      btree_meta_map[meta_key] = meta_val;
    });

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

Result<storage::btree::BasicKV*> LeanStore::CreateBasicKv(const std::string& name,
                                                          BTreeConfig config) {
  return storage::btree::BasicKV::Create(this, name, std::move(config));
}

void LeanStore::GetBasicKV(const std::string& name, storage::btree::BasicKV** btree) {
  *btree = dynamic_cast<storage::btree::BasicKV*>(tree_registry_->GetTree(name));
}

void LeanStore::DropBasicKV(const std::string& name) {
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
  auto* btree = DownCast<storage::btree::BTreeGeneric*>(tree_registry_->GetTree(name));
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  auto res = tree_registry_->UnregisterTree(name);
  if (!res) {
    Log::Error("UnRegister TransactionKV failed, error={}", res.error().ToString());
  }

  auto graveyard_name = "_" + name + "_graveyard";
  btree = DownCast<storage::btree::BTreeGeneric*>(tree_registry_->GetTree(graveyard_name));
  LEAN_DCHECK(btree != nullptr, "graveyard not found");
  leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
  res = tree_registry_->UnregisterTree(graveyard_name);
  if (!res) {
    Log::Error("UnRegister TransactionKV graveyard failed, error={}", res.error().ToString());
  }
}

} // namespace leanstore
