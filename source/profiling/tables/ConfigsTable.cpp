#include "ConfigsTable.hpp"

#include "Config.hpp"

namespace leanstore {
namespace profiling {

std::string ConfigsTable::getName() {
  return "configs";
}

void ConfigsTable::add(std::string name, std::string value) {
  columns.emplace(name, [&, value](Column& col) { col << value; });
}

void ConfigsTable::open() {
  columns.emplace("c_tag", [&](Column& col) { col << FLAGS_tag; });
  columns.emplace("c_worker_threads",
                  [&](Column& col) { col << FLAGS_worker_threads; });
  columns.emplace("c_smt", [&](Column& col) { col << FLAGS_smt; });

  columns.emplace("c_free_pct", [&](Column& col) { col << FLAGS_free_pct; });
  columns.emplace("c_pp_threads",
                  [&](Column& col) { col << FLAGS_pp_threads; });
  columns.emplace("c_partition_bits",
                  [&](Column& col) { col << FLAGS_partition_bits; });
  columns.emplace("c_buffer_pool_size",
                  [&](Column& col) { col << FLAGS_buffer_pool_size; });
  columns.emplace("c_target_gib",
                  [&](Column& col) { col << FLAGS_target_gib; });
  columns.emplace("c_run_for_seconds",
                  [&](Column& col) { col << FLAGS_run_for_seconds; });
  columns.emplace("c_bulk_insert",
                  [&](Column& col) { col << FLAGS_bulk_insert; });

  columns.emplace("c_contention_split",
                  [&](Column& col) { col << FLAGS_contention_split; });
  columns.emplace("c_contention_split_sample_probability", [&](Column& col) {
    col << FLAGS_contention_split_sample_probability;
  });
  columns.emplace("c_cm_period", [&](Column& col) { col << FLAGS_cm_period; });
  columns.emplace("c_contention_split_threshold_pct", [&](Column& col) {
    col << FLAGS_contention_split_threshold_pct;
  });

  columns.emplace("c_xmerge_k", [&](Column& col) { col << FLAGS_xmerge_k; });
  columns.emplace("c_xmerge", [&](Column& col) { col << FLAGS_xmerge; });
  columns.emplace("c_xmerge_target_pct",
                  [&](Column& col) { col << FLAGS_xmerge_target_pct; });
  columns.emplace("c_btree_prefix_compression",
                  [&](Column& col) { col << FLAGS_btree_prefix_compression; });
  columns.emplace("c_btree_heads",
                  [&](Column& col) { col << FLAGS_btree_heads; });
  columns.emplace("c_btree_hints",
                  [&](Column& col) { col << FLAGS_btree_hints; });

  columns.emplace("c_wal", [&](Column& col) { col << FLAGS_wal; });
  columns.emplace("c_wal_io_hack", [&](Column& col) { col << 1; });
  columns.emplace("c_wal_fsync", [&](Column& col) { col << FLAGS_wal_fsync; });
  columns.emplace("c_enable_garbage_collection",
                  [&](Column& col) { col << FLAGS_enable_garbage_collection; });
  columns.emplace("c_vi_fat_tuple",
                  [&](Column& col) { col << FLAGS_enable_fat_tuple; });
  columns.emplace("c_isolation_level",
                  [&](Column& col) { col << FLAGS_isolation_level; });
  columns.emplace("c_enable_long_running_transaction", [&](Column& col) {
    col << FLAGS_enable_long_running_transaction;
  });

  for (auto& c : columns) {
    c.second.generator(c.second);
  }
}

uint64_t ConfigsTable::hash() {
  std::stringstream configConcatenation;
  for (const auto& c : columns) {
    configConcatenation << c.second.values[0];
  }
  return std::hash<std::string>{}(configConcatenation.str());
}

void ConfigsTable::next() {
  // one time is enough
  return;
}

} // namespace profiling
} // namespace leanstore
