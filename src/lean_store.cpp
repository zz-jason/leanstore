#include "leanstore/lean_store.hpp"

#include "leanstore/base/defer.hpp"
#include "leanstore/base/error.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/c/types.h"
#include "leanstore/checkpoint/checkpoint_processor.hpp"
#include "leanstore/config/store_paths.hpp"
#include "leanstore/coro/coro_scheduler.hpp"
#include "leanstore/coro/coro_session.hpp"
#include "leanstore/coro/mvcc_manager.hpp"
#ifdef LEAN_ENABLE_CORO
#include "leanstore/lean_session.hpp"
#endif
#include "leanstore/table/table.hpp"
#include "leanstore/table/table_registry.hpp"
#include "leanstore/tx/cr_manager.hpp"
#include "leanstore/tx/transaction_kv.hpp"
#include "leanstore/utils/managed_thread.hpp"
#include "leanstore/utils/misc.hpp"
#ifndef LEAN_ENABLE_CORO
#include "leanstore/utils/parallelize.hpp"
#endif
#include "leanstore/utils/scoped_timer.hpp"
#include "utils/json.hpp"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <linux/fs.h>
#include <resolv.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

namespace leanstore {

Result<std::unique_ptr<LeanStore>> LeanStore::Open(lean_store_option* option) {
  if (option == nullptr) {
    return Error::General("lean_store_option should not be null");
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

LeanStore::LeanStore(lean_store_option* option) : store_option_(option) {
  CoroEnv::SetCurStore(this);

  Log::Info("LeanStore starting ...");
  LEAN_DEFER(Log::Info("LeanStore started"));

  InitDbFiles();

  // create global btree catalog
  tree_registry_ = std::make_unique<TreeRegistry>();
  table_registry_ = std::make_unique<TableRegistry>();

  // create global buffer manager and page evictors
  buffer_manager_ = std::make_unique<BufferManager>(this);

  mvcc_mgr_ = std::make_unique<leanstore::MvccManager>(this);

  StartBackgroundThreads();

  // recover from disk
  if (!store_option_->create_from_scratch_) {
    auto all_pages_up_to_date = DeserializeMeta();
    if (!all_pages_up_to_date) {
      Log::Info("Not all pages up-to-date, recover from disk");
      buffer_manager_->RecoverFromDisk();
    } else {
      Log::Info("All pages up-to-date, skip recover from disk");
      // TODO: truncate wal files
    }
  }
}

void LeanStore::InitDbFiles() {
  LEAN_DEFER({ LEAN_DCHECK(fcntl(page_fd_, F_GETFL) != -1); });
#ifndef LEAN_ENABLE_CORO
  LEAN_DEFER({ LEAN_DCHECK(fcntl(wal_fd_, F_GETFL) != -1); });
#endif
  // Create a new instance on the specified DB file
  if (store_option_->create_from_scratch_) {
    Log::Info("Create new page and wal files");
    int flags = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    auto db_file_path = StorePaths::PagesFilePath(store_option_->store_dir_);
    page_fd_ = open(db_file_path.c_str(), flags, 0666);
    if (page_fd_ == -1) {
      Log::Fatal("Could not open file at: {}", db_file_path);
    }
    Log::Info("Init page fd succeed, pageFd={}, pageFile={}", page_fd_, db_file_path);

#ifndef LEAN_ENABLE_CORO
    {
      auto wal_file_path = StorePaths::WalFilePath(store_option_->store_dir_);
      wal_fd_ = open(wal_file_path.c_str(), flags, 0666);
      if (wal_fd_ == -1) {
        Log::Fatal("Could not open file at: {}", wal_file_path);
      }
      Log::Info("Init wal fd succeed, walFd={}, walFile={}", wal_fd_, wal_file_path);
    }
#endif
    return;
  }

  // Recover pages and WAL from existing files
  Log::Info("Reopen existing page and wal files");
  int flags = O_RDWR | O_DIRECT;
  auto db_file_path = StorePaths::PagesFilePath(store_option_->store_dir_);
  page_fd_ = open(db_file_path.c_str(), flags, 0666);
  if (page_fd_ == -1) {
    Log::Fatal("Recover failed, could not open file at: {}. The data is lost, "
               "please create a new DB file and start a new instance from it",
               db_file_path);
  }
  Log::Info("Init page fd succeed, pageFd={}, pageFile={}", page_fd_, db_file_path);

#ifndef LEAN_ENABLE_CORO
  {
    auto wal_file_path = StorePaths::WalFilePath(store_option_->store_dir_);
    wal_fd_ = open(wal_file_path.c_str(), flags, 0666);
    if (wal_fd_ == -1) {
      Log::Fatal("Recover failed, could not open file at: {}. The data is lost, "
                 "please create a new WAL file and start a new instance from it",
                 wal_file_path);
    }
    Log::Info("Init wal fd succeed, walFd={}, walFile={}", wal_fd_, wal_file_path);
  }
#endif
}

LeanStore::~LeanStore() {
  Log::Info("LeanStore stopping ...");
  LEAN_DEFER({
    lean_store_option_destroy(store_option_);
    Log::Info("LeanStore stopped");
    Log::Deinit();
  });

  // print trees
  tree_registry_->VisitAllTrees([](const TreeMap& all_trees) {
    for (const auto& it : all_trees) {
      auto tree_id = it.first;
      auto& [tree_ptr, tree_name] = it.second;
      auto* btree = dynamic_cast<BTreeGeneric*>(tree_ptr.get());
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

#ifdef LEAN_ENABLE_CORO
  {
    CheckpointProcessor processor(*this, *store_option_);
    auto res = processor.CheckpointAll(buffer_manager_->buffer_pool_, buffer_manager_->num_bfs_);
    if (!res) {
      Log::Error("Failed to checkpoint all buffer frames: {}", res.error().ToString());
      all_pages_up_to_date = false;
    }
  }
#else
  if (auto res = buffer_manager_->CheckpointAllBufferFrames(); !res) {
    all_pages_up_to_date = false;
  }
#endif

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

#ifndef LEAN_ENABLE_CORO
  {
    auto wal_file_path = StorePaths::WalFilePath(store_option_->store_dir_);
    struct stat st;
    if (stat(wal_file_path.c_str(), &st) == 0) {
      LEAN_DLOG("The size of {} is {} bytes", wal_file_path, st.st_size);
    }

    if (close(wal_fd_) == -1) {
      perror("Failed to close WAL file: ");
    } else {
      Log::Info("WAL file closed");
    }
  }
#endif
}

void LeanStore::StartBackgroundThreads() {
#ifdef LEAN_ENABLE_CORO
  {
    coro_scheduler_ = std::make_unique<CoroScheduler>(this, store_option_->worker_threads_);
    coro_scheduler_->Init();
    buffer_manager_->InitFreeBfLists();

    crmanager_ = nullptr;

    auto* coro_session = coro_scheduler_->TryReserveCoroSession(0);
    assert(coro_session != nullptr && "Failed to reserve a CoroSession for coroutine execution");
    coro_scheduler_->Submit(coro_session, [&]() { mvcc_mgr_->InitHistoryStorage(); })->Wait();
    coro_scheduler_->ReleaseCoroSession(coro_session);
  }
#else
  {
    buffer_manager_->InitFreeBfLists();
    buffer_manager_->StartPageEvictors();

    crmanager_ = std::make_unique<CRManager>(this);
    crmanager_->worker_threads_[0]->SetJob([&]() { mvcc_mgr_->InitHistoryStorage(); });
    crmanager_->worker_threads_[0]->Wait();
  }
#endif
}

void LeanStore::StopBackgroundThreads() {
#ifdef LEAN_ENABLE_CORO
  {
    // destroy coro scheduler
    if (coro_scheduler_ != nullptr) {
      coro_scheduler_->Deinit();
      coro_scheduler_ = nullptr;
    }
  }
#else
  {
    // destroy and Stop all foreground workers
    if (crmanager_ != nullptr) {
      crmanager_ = nullptr;
    }
    // destroy buffer manager (buffer frame providers)
    buffer_manager_->StopPageEvictors();
  }
#endif
}

lean_lid_t LeanStore::AllocWalGsn() {
  return mvcc_mgr_->AllocWalGsn();
}

void LeanStore::ExecSync(uint64_t worker_id, std::function<void()> job) {
  crmanager_->worker_threads_[worker_id]->SetJob(std::move(job));
  crmanager_->worker_threads_[worker_id]->Wait();
}

void LeanStore::ExecAsync(uint64_t worker_id, std::function<void()> job) {
  crmanager_->worker_threads_[worker_id]->SetJob(std::move(job));
}

void LeanStore::Wait(lean_wid_t worker_id) {
  crmanager_->worker_threads_[worker_id]->Wait();
}

void LeanStore::WaitAll() {
  for (auto i = 0U; i < store_option_->worker_threads_; i++) {
    crmanager_->worker_threads_[i]->Wait();
  }
}

void LeanStore::ParallelRange(
    uint64_t num_jobs, std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler) {
#ifdef LEAN_ENABLE_CORO
  coro_scheduler_->ParallelRange(num_jobs, std::move(job_handler));
#else
  utils::Parallelize::ParallelRange(num_jobs, std::move(job_handler));
#endif
}

CoroSession* LeanStore::TryReserveSession(uint64_t worker_id) {
#ifdef LEAN_ENABLE_CORO
  return coro_scheduler_->TryReserveCoroSession(worker_id);
#else
  (void)worker_id;
  return nullptr;
#endif
}

CoroSession* LeanStore::ReserveSession(uint64_t worker_id) {
#ifdef LEAN_ENABLE_CORO
  return coro_scheduler_->ReserveCoroSession(worker_id);
#else
  (void)worker_id;
  return nullptr;
#endif
}

void LeanStore::ReleaseSession(CoroSession* session) {
#ifdef LEAN_ENABLE_CORO
  coro_scheduler_->ReleaseCoroSession(session);
#else
  (void)session;
#endif
}

void LeanStore::SubmitAndWait(CoroSession* session, std::function<void()> task) {
#ifdef LEAN_ENABLE_CORO
  coro_scheduler_->Submit(session, std::move(task))->Wait();
#else
  (void)session;
  (void)task;
#endif
}

#ifdef LEAN_ENABLE_CORO
auto LeanStore::Connect(uint64_t worker_id) -> LeanSession {
  CoroSession* session = ReserveSession(worker_id);
  return LeanSession(this, session);
}

auto LeanStore::TryConnect(uint64_t worker_id) -> std::optional<LeanSession> {
  CoroSession* session = TryReserveSession(worker_id);
  if (session == nullptr) {
    return std::nullopt;
  }
  return std::optional<LeanSession>(std::in_place, this, session);
}
#endif

constexpr char kMetaKeyCrManager[] = "cr_manager";
constexpr char kMetaKeyMvcc[] = "mvcc";
constexpr char kMetaKeyBufferManager[] = "buffer_manager";
constexpr char kMetaKeyBTrees[] = "leanstore/btrees";
constexpr char kMetaKeyTables[] = "leanstore/tables";
constexpr char kName[] = "name";
constexpr char kType[] = "type";
constexpr char kId[] = "id";
constexpr char kEnableWal[] = "enable_wal";
constexpr char kUseBulkInsert[] = "use_bulk_insert";
constexpr char kColumns[] = "columns";
constexpr char kPrimaryKey[] = "primary_key";
constexpr char kPrimaryIndexType[] = "primary_index_type";
constexpr char kSerialized[] = "serialized";
constexpr char kPagesUpToDate[] = "pages_up_to_date";
constexpr auto kGraveyardNameFormat = "_{}_graveyard";

void LeanStore::SerializeMeta(bool all_pages_up_to_date) {
  Log::Info("serializeMeta started");
  ScopedTimer timer([](double elapsed_ms) {
    Log::Info("SerializeMeta finished, timeElapsed={:.2f}ms", elapsed_ms);
  });

  // serialize data structure instances
  utils::JsonObj meta_json_obj;
  std::ofstream meta_file;
  meta_file.open(StorePaths::MetaFilePath(store_option_->store_dir_), std::ios::trunc);

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
    tree_registry_->VisitAllTrees([&](const TreeMap& all_trees) {
      for (const auto& it : all_trees) {
        auto btree_id = it.first;
        auto& [tree_ptr, btree_name] = it.second;
        if (btree_name.starts_with("_")) {
          continue;
        }

        auto* btree = dynamic_cast<BTreeGeneric*>(tree_ptr.get());
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

  // tables
  {
    utils::JsonArray table_json_array;
    table_registry_->Visit([&](const auto& tables) {
      for (const auto& [table_name, table_ptr] : tables) {
        const auto& def = table_ptr->Definition();
        utils::JsonObj table_obj;
        table_obj.AddString(kName, def.name_);
        table_obj.AddInt64(kPrimaryIndexType, static_cast<int64_t>(def.primary_index_type_));
        table_obj.AddBool(kEnableWal, def.primary_index_config_.enable_wal_);
        table_obj.AddBool(kUseBulkInsert, def.primary_index_config_.use_bulk_insert_);

        utils::JsonArray col_array;
        for (const auto& col : def.schema_.Columns()) {
          utils::JsonObj col_obj;
          col_obj.AddString(kName, col.name_);
          col_obj.AddInt64(kType, static_cast<int64_t>(col.type_));
          col_obj.AddBool("nullable", col.nullable_);
          col_obj.AddInt64("fixed_length", col.fixed_length_);
          col_array.AppendJsonObj(col_obj);
        }
        table_obj.AddJsonArray(kColumns, col_array);

        utils::JsonArray pk_array;
        for (auto pk : def.schema_.PrimaryKeyColumns()) {
          pk_array.AppendInt64(static_cast<int64_t>(pk));
        }
        table_obj.AddJsonArray(kPrimaryKey, pk_array);

        table_json_array.AppendJsonObj(table_obj);
      }
    });
    meta_json_obj.AddJsonArray(kMetaKeyTables, table_json_array);
  }

  // pages_up_to_date
  meta_json_obj.AddBool(kPagesUpToDate, all_pages_up_to_date);

  meta_file << meta_json_obj.Serialize();
}

bool LeanStore::DeserializeMeta() {
  Log::Info("DeserializeMeta started");
  ScopedTimer timer([](double elapsed_ms) {
    Log::Info("DeserializeMeta finished, timeElapsed={:.2f}ms", elapsed_ms);
  });

  std::ifstream meta_file;
  meta_file.open(StorePaths::MetaFilePath(store_option_->store_dir_));

  utils::JsonObj meta_json_obj;
  auto res = meta_json_obj.Deserialize(
      std::string(std::istreambuf_iterator<char>(meta_file), std::istreambuf_iterator<char>()));
  if (!res) {
    Log::Error("DeserializeMeta failed: {}", res.error().ToString());
    return false;
  }

  // Deserialize concurrent resource manager
  if (crmanager_ && meta_json_obj.HasMember(kMetaKeyCrManager)) {
    assert(meta_json_obj.HasMember(kMetaKeyCrManager));
    auto cr_manager_obj = meta_json_obj.GetJsonObj(kMetaKeyCrManager);
    crmanager_->Deserialize(*cr_manager_obj);
  }

  auto mvcc_mgr_obj = meta_json_obj.GetJsonObj(kMetaKeyMvcc);
  mvcc_mgr_->Deserialize(*mvcc_mgr_obj);

  // Deserialize buffer manager
  assert(meta_json_obj.HasMember(kMetaKeyBufferManager));
  auto buffer_manager_obj = meta_json_obj.GetJsonObj(kMetaKeyBufferManager);
  buffer_manager_->Deserialize(*buffer_manager_obj);

  assert(meta_json_obj.HasMember(kMetaKeyBTrees));
  auto all_pages_up_to_date = *meta_json_obj.GetBool(kPagesUpToDate);

  assert(meta_json_obj.HasMember(kMetaKeyBTrees));
  auto btree_json_array = meta_json_obj.GetJsonArray(kMetaKeyBTrees);
  for (auto i = 0U; i < btree_json_array->Size(); ++i) {
    assert(btree_json_array->GetJsonObj(i).has_value());
    auto btree_json_obj = btree_json_array->GetJsonObj(i);
    if (!btree_json_obj) {
      Log::Fatal("DeserializeMeta failed, invalid btree json object at index {}", i);
    }

    const lean_treeid_t btree_id = *btree_json_obj->GetInt64("id");
    const auto btree_type = *btree_json_obj->GetInt64("type");
    const auto btree_name_ref = *btree_json_obj->GetString("name");
    const auto btree_enable_wal = *btree_json_obj->GetBool("enable_wal");
    const auto btree_use_bulk_insert = *btree_json_obj->GetBool("use_bulk_insert");
    std::string btree_name(btree_name_ref.data(), btree_name_ref.size());

    std::unordered_map<std::string, std::string> btree_meta_map;
    auto btree_meta_json_obj = btree_json_obj->GetJsonObj("serialized");
    btree_meta_json_obj->Foreach([&](const std::string_view& key, const utils::JsonValue& value) {
      assert(value.IsString());
      auto meta_key = std::string(key.data(), key.size());
      auto meta_val = std::string(value.GetString(), value.GetStringLength());
      btree_meta_map[meta_key] = meta_val;
    });

    Log::Info("Deserialize btree meta, name={}, id={}, type={}", btree_name, btree_id, btree_type);

    // create and register btrees
    switch (static_cast<BTreeType>(btree_type)) {
    case BTreeType::kBasicKV: {
      auto btree = std::make_unique<BasicKV>();
      btree->store_ = this;
      btree->config_ = lean_btree_config{.enable_wal_ = btree_enable_wal,
                                         .use_bulk_insert_ = btree_use_bulk_insert};
      tree_registry_->RegisterTree(btree_id, std::move(btree), btree_name);
      break;
    }
    case BTreeType::kTransactionKV: {
      auto btree = std::make_unique<TransactionKV>();
      btree->store_ = this;
      btree->config_ = lean_btree_config{.enable_wal_ = btree_enable_wal,
                                         .use_bulk_insert_ = btree_use_bulk_insert};
      // create graveyard
      auto job = [&]() {
        auto graveyard_name = std::format(kGraveyardNameFormat, btree_name);
        auto res =
            BasicKV::Create(this, graveyard_name,
                            lean_btree_config{.enable_wal_ = false, .use_bulk_insert_ = false});
        if (!res) {
          Log::Error("Failed to create TransactionKV graveyard"
                     ", btree_name={}, graveyard_name={}, error={}",
                     btree_name, graveyard_name, res.error().ToString());
          return;
        }
        btree->graveyard_ = res.value();
      };

#ifdef LEAN_ENABLE_CORO
      auto* coro_session = GetCoroScheduler().TryReserveCoroSession(0);
      assert(coro_session != nullptr && "Failed to reserve a CoroSession for coroutine execution");
      GetCoroScheduler().Submit(coro_session, std::move(job))->Wait();
      GetCoroScheduler().ReleaseCoroSession(coro_session);
#else
      ExecSync(0, std::move(job));
#endif

      tree_registry_->RegisterTree(btree_id, std::move(btree), btree_name);
      break;
    }
    default: {
      Log::Fatal("deserializeMeta failed, unsupported btree type={}", btree_type);
    }
    }
    tree_registry_->Deserialize(btree_id, btree_meta_map);
  }

  // deserialize tables
  if (meta_json_obj.HasMember(kMetaKeyTables)) {
    auto table_json_array_opt = meta_json_obj.GetJsonArray(kMetaKeyTables);
    if (table_json_array_opt) {
      auto table_json_array = std::move(table_json_array_opt.value());
      for (uint64_t i = 0; i < table_json_array.Size(); ++i) {
        auto table_obj_opt = table_json_array.GetJsonObj(i);
        if (!table_obj_opt) {
          continue;
        }
        auto table_obj = std::move(table_obj_opt.value());
        auto name_sv_opt = table_obj.GetString(kName);
        if (!name_sv_opt) {
          continue;
        }
        auto name_sv = name_sv_opt.value();
        std::string name{name_sv.data(), name_sv.size()};
        lean_btree_type primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_MVCC;
        if (auto pit_opt = table_obj.GetInt64(kPrimaryIndexType); pit_opt) {
          primary_index_type = static_cast<lean_btree_type>(pit_opt.value());
        }
        lean_btree_config primary_index_config{
            .enable_wal_ = true,
            .use_bulk_insert_ = false,
        };
        if (auto wal_opt = table_obj.GetBool(kEnableWal); wal_opt) {
          primary_index_config.enable_wal_ = wal_opt.value();
        }
        if (auto bulk_opt = table_obj.GetBool(kUseBulkInsert); bulk_opt) {
          primary_index_config.use_bulk_insert_ = bulk_opt.value();
        }

        std::vector<ColumnDefinition> columns;
        if (auto cols_array_opt = table_obj.GetJsonArray(kColumns)) {
          auto cols_array = std::move(cols_array_opt.value());
          for (uint64_t c = 0; c < cols_array.Size(); ++c) {
            auto col_obj_opt = cols_array.GetJsonObj(c);
            if (!col_obj_opt) {
              continue;
            }
            auto col_obj = std::move(col_obj_opt.value());
            ColumnDefinition col_def;
            if (auto col_name_sv_opt = col_obj.GetString(kName)) {
              auto col_name_sv = col_name_sv_opt.value();
              col_def.name_.assign(col_name_sv.data(), col_name_sv.size());
            }
            if (auto type_opt = col_obj.GetInt64(kType)) {
              col_def.type_ = static_cast<ColumnType>(type_opt.value());
            }
            if (auto nullable_opt = col_obj.GetBool("nullable")) {
              col_def.nullable_ = nullable_opt.value();
            }
            if (auto fixed_opt = col_obj.GetInt64("fixed_length")) {
              col_def.fixed_length_ = static_cast<uint32_t>(fixed_opt.value());
            }
            columns.emplace_back(std::move(col_def));
          }
        }

        std::vector<uint32_t> pk_columns;
        if (auto pk_array_opt = table_obj.GetJsonArray(kPrimaryKey)) {
          auto pk_array = std::move(pk_array_opt.value());
          for (uint64_t p = 0; p < pk_array.Size(); ++p) {
            if (auto pk_val = pk_array.GetInt64(p)) {
              pk_columns.emplace_back(static_cast<uint32_t>(pk_val.value()));
            }
          }
        }
        auto schema_res = TableSchema::Create(std::move(columns), std::move(pk_columns));
        if (!schema_res) {
          Log::Error("Failed to deserialize table {}, invalid schema: {}", name,
                     schema_res.error().ToString());
          continue;
        }

        auto def_res = TableDefinition::Create(std::move(name), std::move(schema_res.value()),
                                               primary_index_type, primary_index_config);
        if (!def_res) {
          Log::Error("Failed to deserialize table, invalid definition: {}",
                     def_res.error().ToString());
          continue;
        }

        const auto& def = def_res.value();
        auto res = RegisterTableWithExisting(def);
        if (!res) {
          Log::Error("Failed to deserialize table {}, error={}", def.name_, res.error().ToString());
        }
      }
    }
  }

  return all_pages_up_to_date;
}

Result<BasicKV*> LeanStore::CreateBasicKv(const std::string& name, lean_btree_config config) {
  return BasicKV::Create(this, name, std::move(config));
}

void LeanStore::GetBasicKV(const std::string& name, BasicKV** btree) {
  auto* tree = tree_registry_->GetTree(name);
  auto* generic = dynamic_cast<BTreeGeneric*>(tree);
  if (generic == nullptr || generic->tree_type_ != BTreeType::kBasicKV) {
    *btree = nullptr;
    return;
  }
  *btree = static_cast<BasicKV*>(generic);
}

void LeanStore::DropBasicKV(const std::string& name) {
  auto* btree = dynamic_cast<BTreeGeneric*>(tree_registry_->GetTree(name));
  BTreeGeneric::FreeAndReclaim(*btree);
  auto res = tree_registry_->UnregisterTree(name);
  if (!res) {
    Log::Error("Unregister BasicKV failed, error={}", res.error().ToString());
  }
}

Result<TransactionKV*> LeanStore::CreateTransactionKV(const std::string& name,
                                                      lean_btree_config config) {
  static constexpr auto kGraveyardConfig =
      lean_btree_config{.enable_wal_ = false, .use_bulk_insert_ = false};

  // create btree for graveyard
  auto graveyard_name = std::format(kGraveyardNameFormat, name);
  auto graveyard = BasicKV::Create(this, graveyard_name, kGraveyardConfig);
  if (!graveyard) {
    Log::Error("Create graveyard failed, btree_name={}, graveyard_name={}, error={}", name,
               graveyard_name, graveyard.error().ToString());
    return std::move(graveyard.error());
  }

  // create transaction btree
  auto res = TransactionKV::Create(this, name, std::move(config), graveyard.value());
  if (!res) {
    BTreeGeneric::FreeAndReclaim(*static_cast<BTreeGeneric*>(graveyard.value()));
    auto res2 = tree_registry_->UnRegisterTree(graveyard.value()->tree_id_);
    if (!res2) {
      Log::Error("Unregister graveyard failed, btree_name={}, graveyard_name={}, error={}", name,
                 graveyard_name, res2.error().ToString());
    }
  }
  return res;
}

void LeanStore::GetTransactionKV(const std::string& name, TransactionKV** btree) {
  auto* tree = tree_registry_->GetTree(name);
  *btree = dynamic_cast<TransactionKV*>(tree);
}

void LeanStore::DropTransactionKV(const std::string& name) {
  auto* btree = DownCast<BTreeGeneric*>(tree_registry_->GetTree(name));
  BTreeGeneric::FreeAndReclaim(*btree);
  auto res = tree_registry_->UnregisterTree(name);
  if (!res) {
    Log::Error("Unregister TransactionKV failed, error={}", res.error().ToString());
  }

  auto graveyard_name = "_" + name + "_graveyard";
  btree = DownCast<BTreeGeneric*>(tree_registry_->GetTree(graveyard_name));
  LEAN_DCHECK(btree != nullptr, "graveyard not found");
  BTreeGeneric::FreeAndReclaim(*btree);
  res = tree_registry_->UnregisterTree(graveyard_name);
  if (!res) {
    Log::Error("Unregister TransactionKV graveyard failed, error={}", res.error().ToString());
  }
}

Result<Table*> LeanStore::CreateTable(const TableDefinition& definition) {
  const auto& def = definition;
  if (auto res = def.Validate(); !res) {
    return std::move(res.error());
  }

  auto table_res = Table::Create(this, def);

  if (!table_res) {
    return std::move(table_res.error());
  }

  auto table = std::move(table_res.value());
  auto table_name = table->Definition().name_;
  auto register_res = table_registry_->Register(std::move(table));
  if (!register_res) {
    // Undo physical resources that were allocated for the table.
    switch (def.primary_index_type_) {
    case lean_btree_type::LEAN_BTREE_TYPE_ATOMIC:
      DropBasicKV(table_name);
      break;
    case lean_btree_type::LEAN_BTREE_TYPE_MVCC:
      DropTransactionKV(table_name);
      break;
    default:
      break;
    }
    return std::move(register_res.error());
  }
  return std::move(register_res.value());
}

Result<Table*> LeanStore::RegisterTableWithExisting(const TableDefinition& definition) {
  const auto& def = definition;
  if (auto res = def.Validate(); !res) {
    return std::move(res.error());
  }
  auto table_res = Table::WrapExisting(this, def);
  if (!table_res) {
    return std::move(table_res.error());
  }
  auto table = std::move(table_res.value());
  auto register_res = table_registry_->Register(std::move(table));
  if (!register_res) {
    return std::move(register_res.error());
  }
  return std::move(register_res.value());
}

Result<void> LeanStore::DropTable(const std::string& name) {
  auto* table = table_registry_->Get(name);
  if (table == nullptr) {
    return Error::General("table not found: " + name);
  }

  TableDefinition def = table->Definition();
  auto drop_res = table_registry_->Drop(name);
  if (!drop_res) {
    return std::move(drop_res.error());
  }
  auto dropped_table = std::move(drop_res.value());

  switch (def.primary_index_type_) {
  case lean_btree_type::LEAN_BTREE_TYPE_ATOMIC:
    DropBasicKV(def.name_);
    break;
  case lean_btree_type::LEAN_BTREE_TYPE_MVCC:
    DropTransactionKV(def.name_);
    break;
  default:
    break;
  }
  dropped_table.reset();
  return {};
}

Table* LeanStore::GetTable(const std::string& name) {
  return table_registry_->Get(name);
}

} // namespace leanstore
