#include "leanstore/c/leanstore.h"
#include "leanstore/c/types.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <mutex>
#include <numeric>
#include <print>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#ifdef LEAN_ENABLE_PROFILING
#include <gperftools/profiler.h>
#endif

namespace leanstore::benchmarking {

namespace {

constexpr uint32_t kColumnCount = 2;
constexpr uint64_t kDefaultRowCount = 2000000;
constexpr uint64_t kPayloadBytes = 64;
constexpr uint32_t kColumnStoreRowsPerBlock = 65536;
constexpr std::string_view kProfilePhaseEnv = "LEAN_TABLE_BENCH_PROFILE_PHASE";
constexpr std::string_view kProfilePathEnv = "LEAN_TABLE_BENCH_PROFILE_PATH";
constexpr std::string_view kDefaultProfilePath = "/tmp/leanstore/prof/table_bench";
constexpr std::array<std::string_view, 32> kPayloadDictionary = {
    "sku#A",           "sku#B",          "sku#C",           "sku#D",           "region#NA",
    "region#EU",       "region#APAC",    "region#LATAM",    "tier#gold",       "tier#silver",
    "tier#bronze",     "tier#platinum",  "campaign#spring", "campaign#summer", "campaign#fall",
    "campaign#winter", "channel#web",    "channel#mobile",  "channel#store",   "channel#partner",
    "status#active",   "status#paused",  "status#trial",    "status#expired",  "seg#small",
    "seg#mid",         "seg#large",      "seg#enterprise",  "bucket#hot",      "bucket#warm",
    "bucket#cold",     "bucket#archive",
};

std::atomic<uint64_t> g_benchmark_dir_id = 0;

enum class ProfilePhase : uint8_t {
  kDisabled = 0,
  kLoad,
  kBuildColumnStore,
  kPointLookup,
  kFullScan,
};

const char* ToString(ProfilePhase phase) {
  switch (phase) {
  case ProfilePhase::kLoad:
    return "load";
  case ProfilePhase::kBuildColumnStore:
    return "build_column_store";
  case ProfilePhase::kPointLookup:
    return "point_lookup";
  case ProfilePhase::kFullScan:
    return "full_scan";
  case ProfilePhase::kDisabled:
    break;
  }
  return "disabled";
}

[[maybe_unused]] ProfilePhase ParseProfilePhase(const char* phase_env) {
  if (phase_env == nullptr || phase_env[0] == '\0') {
    return ProfilePhase::kDisabled;
  }
  const std::string_view phase{phase_env};
  if (phase == "load") {
    return ProfilePhase::kLoad;
  }
  if (phase == "build" || phase == "build_column_store") {
    return ProfilePhase::kBuildColumnStore;
  }
  if (phase == "point" || phase == "point_lookup") {
    return ProfilePhase::kPointLookup;
  }
  if (phase == "scan" || phase == "full_scan") {
    return ProfilePhase::kFullScan;
  }
  std::println(stderr,
               "[table-bench] unknown profile phase '{}', valid values: load|build|point|scan",
               phase);
  return ProfilePhase::kDisabled;
}

struct ProfileConfig {
  ProfilePhase phase_ = ProfilePhase::kDisabled;
  std::string path_prefix_ = std::string(kDefaultProfilePath);
};

[[maybe_unused]] const ProfileConfig& GetProfileConfig() {
  static const ProfileConfig kConfig = [] {
    ProfileConfig cfg;
    cfg.phase_ = ParseProfilePhase(std::getenv(kProfilePhaseEnv.data()));
    const char* path_env = std::getenv(kProfilePathEnv.data());
    if (path_env != nullptr && path_env[0] != '\0') {
      cfg.path_prefix_ = path_env;
    }
#ifndef LEAN_ENABLE_PROFILING
    if (cfg.phase_ != ProfilePhase::kDisabled) {
      std::println(stderr,
                   "[table-bench] LEAN_ENABLE_PROFILING is OFF in this binary, ignoring profile "
                   "phase '{}'",
                   ToString(cfg.phase_));
    }
#endif
    return cfg;
  }();
  return kConfig;
}

#ifdef LEAN_ENABLE_PROFILING
bool ShouldProfile(ProfilePhase phase) {
  return GetProfileConfig().phase_ == phase && phase != ProfilePhase::kDisabled;
}

struct ProfilerState {
  std::mutex mu;
  bool running = false;
};

ProfilerState& GetProfilerState() {
  static ProfilerState state;
  return state;
}

std::string BuildProfilePath(ProfilePhase phase) {
  const auto& config = GetProfileConfig();
  std::filesystem::path path(config.path_prefix_);
  std::error_code ec;
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path(), ec);
  }
  return std::format("{}_{}.prof", config.path_prefix_, ToString(phase));
}

class ScopedPhaseProfiler {
public:
  explicit ScopedPhaseProfiler(ProfilePhase phase) : phase_(phase) {
#ifdef LEAN_ENABLE_PROFILING
    if (!ShouldProfile(phase_)) {
      return;
    }
    auto& state = GetProfilerState();
    std::lock_guard<std::mutex> guard(state.mu);
    if (state.running) {
      std::println(stderr, "[table-bench] profiler already running, skip phase={}",
                   ToString(phase_));
      return;
    }

    output_path_ = BuildProfilePath(phase_);
    if (ProfilerStart(output_path_.c_str()) == 0) {
      std::println(stderr, "[table-bench] ProfilerStart failed, phase={} path={}", ToString(phase_),
                   output_path_);
      output_path_.clear();
      return;
    }
    state.running = true;
    active_ = true;
    std::println(stderr,
                 "[table-bench] profile_start phase={} path={} (gperftools may append _<pid>)",
                 ToString(phase_), output_path_);
#endif
  }

  ~ScopedPhaseProfiler() {
#ifdef LEAN_ENABLE_PROFILING
    if (!active_) {
      return;
    }
    auto& state = GetProfilerState();
    {
      std::lock_guard<std::mutex> guard(state.mu);
      ProfilerStop();
      ProfilerFlush();
      state.running = false;
    }
    std::println(stderr, "[table-bench] profile_stop phase={} path={}*", ToString(phase_),
                 output_path_);
#endif
  }

private:
  ProfilePhase phase_ = ProfilePhase::kDisabled;
  bool active_ = false;
  std::string output_path_;
};
#else
class ScopedPhaseProfiler {
public:
  explicit ScopedPhaseProfiler(ProfilePhase phase [[maybe_unused]]) {
  }
};
#endif

void PrintPhaseCost(std::string_view phase, uint64_t rows, bool column_store,
                    std::chrono::steady_clock::time_point begin) {
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
  const char* mode = column_store ? "column" : "row";
  std::println(stderr, "[table-bench] phase={} mode={} rows={} elapsed_ms={}", phase, mode, rows,
               elapsed_ms);
}

std::string BuildPayload(std::string_view token) {
  std::string payload(kPayloadBytes, 'v');
  const auto suffix = std::format("|{:s}|", token);
  payload.replace(payload.size() - suffix.size(), suffix.size(), suffix);
  return payload;
}

struct TableBenchEnv {
  lean_store* store_ = nullptr;
  lean_session* session_ = nullptr;
  lean_table* table_ = nullptr;
  std::string store_dir_;
  std::vector<uint64_t> probe_keys_;
  bool column_store_ = false;
  uint64_t row_count_ = 0;

  ~TableBenchEnv() {
    const auto close_begin = std::chrono::steady_clock::now();
    if (table_ != nullptr) {
      table_->close(table_);
      table_ = nullptr;
    }
    if (session_ != nullptr) {
      session_->close(session_);
      session_ = nullptr;
    }
    if (store_ != nullptr) {
      store_->close(store_);
      store_ = nullptr;
    }

    std::error_code ec;
    std::filesystem::remove_all(store_dir_, ec);
    PrintPhaseCost("close", row_count_, column_store_, close_begin);
  }
};

bool InitTableEnv(TableBenchEnv& env, uint64_t row_count, bool build_column_store,
                  std::string& error_msg) {
  const auto dir_id = g_benchmark_dir_id.fetch_add(1, std::memory_order_relaxed);
  const std::string mode = build_column_store ? "column" : "row";
  env.column_store_ = build_column_store;
  env.row_count_ = row_count;
  env.store_dir_ =
      std::format("/tmp/leanstore/bench/table_interface_{}_{}_{}", mode, row_count, dir_id);

  auto* option = lean_store_option_create(env.store_dir_.c_str());
  option->create_from_scratch_ = true;
  option->worker_threads_ = 2;
  option->log_level_ = lean_log_level::LEAN_LOG_LEVEL_WARN;
  option->enable_wal_ = false;
  option->enable_wal_fsync_ = false;

  if (lean_open_store(option, &env.store_) != lean_status::LEAN_STATUS_OK ||
      env.store_ == nullptr) {
    lean_store_option_destroy(option);
    error_msg = "failed to open leanstore";
    return false;
  }

  env.session_ = env.store_->connect(env.store_);
  if (env.session_ == nullptr) {
    error_msg = "failed to open session";
    return false;
  }

  constexpr const char* kTableName = "table_interface_bench";
  lean_table_column_def columns[kColumnCount] = {
      {.name = {.data = "id", .size = 2},
       .type = LEAN_COLUMN_TYPE_INT64,
       .nullable = false,
       .fixed_length = 0},
      {.name = {.data = "payload", .size = 7},
       .type = LEAN_COLUMN_TYPE_BINARY,
       .nullable = false,
       .fixed_length = 0},
  };
  uint32_t pk_columns[1] = {0};
  lean_table_def table_def = {.name = {.data = kTableName, .size = 21},
                              .columns = columns,
                              .num_columns = kColumnCount,
                              .pk_cols = pk_columns,
                              .pk_cols_count = 1,
                              .primary_index_type = lean_btree_type::LEAN_BTREE_TYPE_ATOMIC,
                              .primary_index_config = {
                                  .enable_wal_ = false,
                                  .use_bulk_insert_ = false,
                              }};

  if (env.session_->create_table(env.session_, &table_def) != lean_status::LEAN_STATUS_OK) {
    error_msg = "failed to create benchmark table";
    return false;
  }

  env.table_ = env.session_->get_table(env.session_, kTableName);
  if (env.table_ == nullptr) {
    error_msg = "failed to get benchmark table";
    return false;
  }

  std::vector<std::string> payload_dictionary;
  payload_dictionary.reserve(kPayloadDictionary.size());
  for (const auto token : kPayloadDictionary) {
    payload_dictionary.push_back(BuildPayload(token));
  }
  std::mt19937_64 payload_rng(20260101);
  std::uniform_int_distribution<size_t> payload_dist(0, payload_dictionary.size() - 1);

  const auto load_begin = std::chrono::steady_clock::now();
  {
    ScopedPhaseProfiler load_profiler(ProfilePhase::kLoad);
    for (uint64_t key = 0; key < row_count; ++key) {
      const auto& payload = payload_dictionary[payload_dist(payload_rng)];
      lean_datum columns_data[kColumnCount];
      bool nulls[kColumnCount] = {false, false};
      columns_data[0].i64 = static_cast<int64_t>(key);
      columns_data[1].str = {.data = payload.data(), .size = payload.size()};
      lean_row row{.columns = columns_data, .nulls = nulls, .num_columns = kColumnCount};
      if (env.table_->insert(env.table_, &row) != lean_status::LEAN_STATUS_OK) {
        error_msg = "failed to insert initial rows";
        return false;
      }
    }
  }
  PrintPhaseCost("load", row_count, build_column_store, load_begin);

  if (build_column_store) {
    const auto build_begin = std::chrono::steady_clock::now();
    lean_column_store_options options{
        .max_rows_per_block = kColumnStoreRowsPerBlock, .max_block_bytes = 0, .target_height = 0};
    lean_column_store_stats stats{};
    {
      ScopedPhaseProfiler build_profiler(ProfilePhase::kBuildColumnStore);
      if (env.table_->build_column_store(env.table_, &options, &stats) !=
          lean_status::LEAN_STATUS_OK) {
        error_msg = "failed to convert table to column store";
        return false;
      }
    }
    if (stats.row_count != row_count) {
      error_msg = "column store stats row_count mismatch";
      return false;
    }
    PrintPhaseCost("build_column_store", row_count, build_column_store, build_begin);
  }

  env.probe_keys_.resize(row_count);
  std::iota(env.probe_keys_.begin(), env.probe_keys_.end(), 0);
  std::mt19937_64 rng(42);
  std::shuffle(env.probe_keys_.begin(), env.probe_keys_.end(), rng);
  return true;
}

bool LookupByKey(const TableBenchEnv& env, uint64_t key, uint64_t& value_size) {
  lean_datum key_cols[kColumnCount] = {};
  bool key_nulls[kColumnCount] = {false, true};
  key_cols[0].i64 = static_cast<int64_t>(key);
  key_cols[1].str = {.data = nullptr, .size = 0};
  lean_row key_row{.columns = key_cols, .nulls = key_nulls, .num_columns = kColumnCount};

  lean_datum out_cols[kColumnCount] = {};
  bool out_nulls[kColumnCount] = {false, false};
  lean_row out_row{.columns = out_cols, .nulls = out_nulls, .num_columns = kColumnCount};
  if (env.table_->lookup(env.table_, &key_row, &out_row) != lean_status::LEAN_STATUS_OK) {
    return false;
  }
  value_size = out_cols[1].str.size;
  return true;
}

bool ScanAllRows(const TableBenchEnv& env, uint64_t& row_count, uint64_t& value_bytes) {
  row_count = 0;
  value_bytes = 0;
  auto* cursor = env.table_->open_cursor(env.table_);
  if (cursor == nullptr) {
    return false;
  }

  if (cursor->seek_to_first(cursor)) {
    do {
      lean_datum cols[kColumnCount] = {};
      bool nulls[kColumnCount] = {false, false};
      lean_row row{.columns = cols, .nulls = nulls, .num_columns = kColumnCount};
      cursor->current_row(cursor, &row);
      row_count++;
      value_bytes += cols[1].str.size;
    } while (cursor->next(cursor));
  }
  cursor->close(cursor);
  return true;
}

void BmTablePointLookup(benchmark::State& state, bool build_column_store) {
  const auto row_count = static_cast<uint64_t>(state.range(0));
  if (row_count == 0) {
    state.SkipWithError("row_count must be greater than zero");
    return;
  }

  TableBenchEnv env;
  std::string error_msg;
  if (!InitTableEnv(env, row_count, build_column_store, error_msg)) {
    state.SkipWithError(error_msg.c_str());
    return;
  }

  size_t key_idx = 0;
  uint64_t total_value_bytes = 0;
  {
    ScopedPhaseProfiler lookup_profiler(ProfilePhase::kPointLookup);
    for (auto _ : state) {
      (void)_;
      const auto key = env.probe_keys_[key_idx++ % env.probe_keys_.size()];
      uint64_t value_size = 0;
      if (!LookupByKey(env, key, value_size)) {
        state.SkipWithError("lookup failed during benchmark");
        break;
      }
      total_value_bytes += value_size;
      benchmark::DoNotOptimize(value_size);
    }
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
  state.counters["rows"] = static_cast<double>(row_count);
  state.counters["value_bytes"] = static_cast<double>(total_value_bytes);
}

void BmTableFullScan(benchmark::State& state, bool build_column_store) {
  const auto expected_rows = static_cast<uint64_t>(state.range(0));
  if (expected_rows == 0) {
    state.SkipWithError("row_count must be greater than zero");
    return;
  }

  TableBenchEnv env;
  std::string error_msg;
  if (!InitTableEnv(env, expected_rows, build_column_store, error_msg)) {
    state.SkipWithError(error_msg.c_str());
    return;
  }

  uint64_t total_rows_scanned = 0;
  uint64_t total_value_bytes = 0;
  {
    ScopedPhaseProfiler scan_profiler(ProfilePhase::kFullScan);
    for (auto _ : state) {
      (void)_;
      uint64_t rows = 0;
      uint64_t value_bytes = 0;
      if (!ScanAllRows(env, rows, value_bytes)) {
        state.SkipWithError("scan failed during benchmark");
        break;
      }
      if (rows != expected_rows) {
        state.SkipWithError("scan returned unexpected row count");
        break;
      }
      total_rows_scanned += rows;
      total_value_bytes += value_bytes;
      benchmark::DoNotOptimize(rows);
    }
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * expected_rows));
  state.counters["rows_per_scan"] = static_cast<double>(expected_rows);
  state.counters["total_rows"] = static_cast<double>(total_rows_scanned);
  state.counters["value_bytes"] = static_cast<double>(total_value_bytes);
}

BENCHMARK_CAPTURE(BmTablePointLookup, RowStore, false)->Arg(kDefaultRowCount);
BENCHMARK_CAPTURE(BmTablePointLookup, ColumnStore, true)->Arg(kDefaultRowCount);
BENCHMARK_CAPTURE(BmTableFullScan, RowStore, false)->Arg(kDefaultRowCount);
BENCHMARK_CAPTURE(BmTableFullScan, ColumnStore, true)->Arg(kDefaultRowCount);

} // namespace

} // namespace leanstore::benchmarking

BENCHMARK_MAIN();
