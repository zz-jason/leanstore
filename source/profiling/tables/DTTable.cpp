#include "DTTable.hpp"

#include "Config.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "utils/ThreadLocalAggregator.hpp"

using leanstore::utils::threadlocal::Sum;
namespace leanstore {
namespace profiling {

DTTable::DTTable(BufferManager& bm) : bm(bm) {
}

std::string DTTable::getName() {
  return "dt";
}

void DTTable::open() {
  columns.emplace("key", [&](Column& col) { col << mTreeId; });
  columns.emplace("dt_name", [&](Column& col) { col << dt_name; });
  columns.emplace("dt_page_reads", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_page_reads,
               mTreeId);
  });
  columns.emplace("dt_page_writes", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_page_writes,
               mTreeId);
  });
  columns.emplace("dt_restarts_update_same_size", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::dt_restarts_update_same_size, mTreeId);
  });
  columns.emplace("dt_restarts_structural_change", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::dt_restarts_structural_change, mTreeId);
  });
  columns.emplace("dt_restarts_read", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_restarts_read,
               mTreeId);
  });

  columns.emplace("dt_empty_leaf", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_empty_leaf,
               mTreeId);
  });
  columns.emplace("dt_goto_page_exec", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_goto_page_exec,
               mTreeId);
  });
  columns.emplace("dt_goto_page_shared", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_goto_page_shared,
               mTreeId);
  });
  columns.emplace("dt_next_tuple", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_next_tuple,
               mTreeId);
  });
  columns.emplace("dt_next_tuple_opt", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_next_tuple_opt,
               mTreeId);
  });
  columns.emplace("dt_prev_tuple", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_prev_tuple,
               mTreeId);
  });
  columns.emplace("dt_prev_tuple_opt", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_prev_tuple_opt,
               mTreeId);
  });
  columns.emplace("dt_inner_page", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_inner_page,
               mTreeId);
  });
  columns.emplace("dt_scan_asc", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_scan_asc,
               mTreeId);
  });
  columns.emplace("dt_scan_desc", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_scan_desc,
               mTreeId);
  });
  columns.emplace("dt_scan_callback", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_scan_callback,
               mTreeId);
  });

  columns.emplace("dt_append", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_append, mTreeId);
  });
  columns.emplace("dt_append_opt", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_append_opt,
               mTreeId);
  });
  columns.emplace("dt_range_removed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_range_removed,
               mTreeId);
  });

  for (u64 r_i = 0; r_i < WorkerCounters::max_researchy_counter; r_i++) {
    columns.emplace("dt_researchy_" + std::to_string(r_i),
                    [&, r_i](Column& col) {
                      col << Sum(WorkerCounters::sCounters,
                                 &WorkerCounters::dt_researchy, mTreeId, r_i);
                    });
  }

  columns.emplace("contention_split_succ_counter", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::contention_split_succ_counter, mTreeId);
  });
  columns.emplace("contention_split_fail_counter", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::contention_split_fail_counter, mTreeId);
  });
  columns.emplace("dt_split", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_split, mTreeId);
  });
  columns.emplace("dt_merge_succ", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_merge_succ,
               mTreeId);
  });
  columns.emplace("dt_merge_fail", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_merge_fail,
               mTreeId);
  });
  columns.emplace("dt_merge_parent_succ", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_merge_parent_succ,
               mTreeId);
  });
  columns.emplace("dt_merge_parent_fail", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_merge_parent_fail,
               mTreeId);
  });
  columns.emplace("xmerge_partial_counter", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::xmerge_partial_counter, mTreeId);
  });
  columns.emplace("xmerge_full_counter", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::xmerge_full_counter,
               mTreeId);
  });

  columns.emplace("dt_find_parent", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_find_parent,
               mTreeId);
  });
  columns.emplace("dt_find_parent_root", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_find_parent_root,
               mTreeId);
  });
  columns.emplace("dt_find_parent_fast", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_find_parent_fast,
               mTreeId);
  });
  columns.emplace("dt_find_parent_slow", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::dt_find_parent_slow,
               mTreeId);
  });

  columns.emplace("cc_read_versions_visited", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_read_versions_visited, mTreeId);
  });
  columns.emplace("cc_read_versions_visited_not_found", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_read_versions_visited_not_found, mTreeId);
  });
  columns.emplace("cc_read_chains", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_read_chains,
               mTreeId);
  });
  columns.emplace("cc_read_chains_not_found", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_read_chains_not_found, mTreeId);
  });

  columns.emplace("cc_update_versions_visited", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_visited, mTreeId);
  });
  columns.emplace("cc_update_versions_removed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_removed, mTreeId);
  });
  columns.emplace("cc_update_versions_skipped", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_skipped, mTreeId);
  });
  columns.emplace("cc_update_versions_kept", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_kept, mTreeId);
  });
  columns.emplace("cc_update_versions_kept_max", [&](Column& col) {
    col << Max(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_kept_max, mTreeId);
  });
  columns.emplace("cc_update_versions_recycled", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_recycled, mTreeId);
  });
  columns.emplace("cc_update_versions_created", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_versions_created, mTreeId);
  });
  columns.emplace("cc_update_chains", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_update_chains,
               mTreeId);
  });
  columns.emplace("cc_update_chains_hwm", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_update_chains_hwm,
               mTreeId);
  });
  columns.emplace("cc_update_chains_pgc", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_update_chains_pgc,
               mTreeId);
  });
  columns.emplace("cc_update_chains_pgc_skipped", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_chains_pgc_skipped, mTreeId);
  });
  columns.emplace("cc_update_chains_pgc_workers_visited", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_chains_pgc_workers_visited, mTreeId);
  });
  columns.emplace("cc_update_chains_pgc_heavy_removed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_chains_pgc_heavy_removed, mTreeId);
  });
  columns.emplace("cc_update_chains_pgc_heavy", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_chains_pgc_heavy, mTreeId);
  });
  columns.emplace("cc_update_chains_pgc_light_removed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_chains_pgc_light_removed, mTreeId);
  });
  columns.emplace("cc_update_chains_pgc_light", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_update_chains_pgc_light, mTreeId);
  });

  columns.emplace("cc_todo_removed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_todo_removed,
               mTreeId);
  });
  columns.emplace("cc_todo_moved_gy", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_todo_moved_gy,
               mTreeId);
  });
  columns.emplace("cc_todo_oltp_executed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_todo_oltp_executed, mTreeId);
  });
  columns.emplace("cc_todo_olap_executed", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_todo_olap_executed, mTreeId);
  });

  columns.emplace("cc_fat_tuple_triggered", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_fat_tuple_triggered, mTreeId);
  });
  columns.emplace("cc_fat_tuple_convert", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters, &WorkerCounters::cc_fat_tuple_convert,
               mTreeId);
  });
  columns.emplace("cc_fat_tuple_decompose", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_fat_tuple_decompose, mTreeId);
  });

  columns.emplace("cc_versions_space_inserted", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_versions_space_inserted, mTreeId);
  });
  columns.emplace("cc_versions_space_inserted_opt", [&](Column& col) {
    col << Sum(WorkerCounters::sCounters,
               &WorkerCounters::cc_versions_space_inserted_opt, mTreeId);
  });
}

void DTTable::next() {
  clear();
  for (const auto& entry : TreeRegistry::sInstance->mTrees) {
    mTreeId = entry.first;
    dt_name = std::get<1>(entry.second);
    for (auto& c : columns) {
      c.second.generator(c.second);
    }
  }
}

} // namespace profiling
} // namespace leanstore
