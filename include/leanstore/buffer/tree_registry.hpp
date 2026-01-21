#pragma once

#include "leanstore/base/defer.hpp"
#include "leanstore/base/error.hpp"
#include "leanstore/base/log.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/buffer/buffer_frame.hpp"
#include "leanstore/buffer/swip.hpp"
#include "leanstore/c/types.h"
#include "leanstore/c/wal_record.h"
#include "leanstore/coro/lean_mutex.hpp"
#include "leanstore/sync/hybrid_guard.hpp"

#include <cstdlib>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <tuple>
#include <unordered_map>

namespace leanstore {

class ParentSwipHandler {
public:
  /// parent_guard_ is the latch guard to the parent buffer frame. It should
  /// already optimistically latched.
  HybridGuard parent_guard_;

  /// parent_bf_ is the parent buffer frame.
  BufferFrame* parent_bf_;

  /// child_swip_ is the swip reference to the child who generated this
  /// ParentSwipHandler.
  Swip& child_swip_;

  /// pos_in_parent_ is the slot id in the parent buffer frame.
  uint32_t pos_in_parent_ = std::numeric_limits<uint32_t>::max();

  /// is_child_bf_updated_ records whether the child buffer frame is updated
  /// since this ParentSwipHandler was created.
  bool is_child_bf_updated_ = false;
};

enum class SpaceCheckResult : uint8_t { kNothing, kPickAnotherBf, kRestartSameBf };

using ChildSwipCallback = std::function<bool(Swip&)>;

class BufferManagedTree {
public:
  virtual void IterateChildSwips(BufferFrame&, ChildSwipCallback) {
    Log::Fatal("BufferManagedTree::IterateChildSwips is unimplemented");
  }

  virtual ParentSwipHandler FindParent(BufferFrame&) {
    Log::Fatal("BufferManagedTree::FindParent is unimplemented");
    exit(1);
  }

  virtual SpaceCheckResult CheckSpaceUtilization(BufferFrame&) {
    Log::Fatal("BufferManagedTree::CheckSpaceUtilization is unimplemented");
    return SpaceCheckResult::kNothing;
  }

  virtual void Checkpoint(BufferFrame&, void*) {
    Log::Fatal("BufferManagedTree::Checkpoint is unimplemented");
  }

  virtual void Undo(const lean_wal_record*) {
    Log::Fatal("BufferManagedTree::undo is unimplemented");
  }

  virtual void GarbageCollect(const uint8_t*, lean_wid_t, lean_txid_t, bool) {
    Log::Fatal("BufferManagedTree::GarbageCollect is unimplemented");
  }

  virtual void Unlock(const uint8_t*) {
    Log::Fatal("BufferManagedTree::unlock is unimplemented");
  }

  virtual std::unordered_map<std::string, std::string> Serialize() {
    Log::Fatal("BufferManagedTree::Serialize is unimplemented");
    return {};
  }

  virtual void Deserialize(std::unordered_map<std::string, std::string>) {
    Log::Fatal("BufferManagedTree::Deserialize is unimplemented");
  }

  virtual ~BufferManagedTree() = default;
};

using TreeAndName = std::tuple<std::unique_ptr<BufferManagedTree>, std::string>;
using TreeMap = std::unordered_map<lean_treeid_t, TreeAndName>;
using TreeIndexByName = std::unordered_map<std::string, TreeMap::iterator>;

class TreeRegistry {
private:
  /// mutex_ protects concurrent access to trees_, tree_index_by_name_, and the
  /// lifetime of a managed tree object, i.e. the tree should stay valid during
  /// read/write access.
  LeanSharedMutex mutex_;

  /// trees_ records and manages the lifetime of all the trees whose content are
  /// stored the buffer pool, for example BTrees.
  TreeMap trees_;

  /// tree_index_by_name_ is a secondary index for trees_, it allows to find a tree
  /// by its name.
  TreeIndexByName tree_index_by_name_;

  /// Tree ID allocator, tree IDs are global unique. IDs of destoried tree are
  /// not recycled.
  std::atomic<lean_treeid_t> tree_id_allocator_ = 0;

public:
  lean_treeid_t AllocTreeId() {
    auto allocated_tree_id = tree_id_allocator_++;
    return allocated_tree_id;
  }

  void VisitAllTrees(std::function<void(const TreeMap& all_trees)> visitor) {
    LEAN_SHARED_LOCK(mutex_);
    visitor(trees_);
  };

  /// Creates a tree managed by buffer manager.
  std::tuple<BufferManagedTree*, lean_treeid_t> CreateTree(
      const std::string& tree_name, std::function<std::unique_ptr<BufferManagedTree>()> ctor) {
    LEAN_UNIQUE_LOCK(mutex_);

    // check uniqueness
    if (tree_index_by_name_.find(tree_name) != tree_index_by_name_.end()) {
      return std::make_tuple(nullptr, 0);
    }

    // create the tree
    auto tree_id = AllocTreeId();
    auto tree = ctor();

    // register the tree
    auto emplace_result =
        trees_.emplace(std::make_pair(tree_id, std::make_tuple(std::move(tree), tree_name)));
    tree_index_by_name_.emplace(std::make_pair(tree_name, emplace_result.first));

    auto it = emplace_result.first;
    auto* tree_ptr = std::get<0>(it->second).get();

    // return pointer and tree id
    return std::make_tuple(tree_ptr, tree_id);
  }

  bool RegisterTree(lean_treeid_t tree_id, std::unique_ptr<BufferManagedTree> tree,
                    const std::string& tree_name) {
    LEAN_DEFER(if (tree_id > tree_id_allocator_) { tree_id_allocator_ = tree_id; });
    LEAN_UNIQUE_LOCK(mutex_);
    if (tree_index_by_name_.find(tree_name) != tree_index_by_name_.end()) {
      return false;
    }

    auto emplace_result =
        trees_.emplace(std::make_pair(tree_id, std::make_tuple(std::move(tree), tree_name)));
    tree_index_by_name_.emplace(std::make_pair(tree_name, emplace_result.first));
    return true;
  }

  Result<bool> UnregisterTree(const std::string& tree_name) {
    LEAN_UNIQUE_LOCK(mutex_);
    auto it = tree_index_by_name_.find(tree_name);
    if (it != tree_index_by_name_.end()) {
      auto tree_it = it->second;
      tree_index_by_name_.erase(tree_name);
      trees_.erase(tree_it);
      return true;
    }
    return Error::General("TreeId not found");
  }

  Result<bool> UnRegisterTree(lean_treeid_t tree_id) {
    LEAN_UNIQUE_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it != trees_.end()) {
      auto& [tree, tree_name] = it->second;
      tree_index_by_name_.erase(tree_name);
      trees_.erase(it);
      return true;
    }
    return Error::General("TreeId not found");
  }

  BufferManagedTree* GetTree(const std::string& tree_name) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = tree_index_by_name_.find(tree_name);
    if (it != tree_index_by_name_.end()) {
      auto tree_it = it->second;
      return std::get<0>(tree_it->second).get();
    }
    return nullptr;
  }

  void IterateChildSwips(lean_treeid_t tree_id, BufferFrame& bf,
                         std::function<bool(Swip&)> callback) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, tree_id);
    }
    auto& [tree, tree_name] = it->second;
    tree->IterateChildSwips(bf, callback);
  }

  ParentSwipHandler FindParent(lean_treeid_t tree_id, BufferFrame& bf) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, tree_id);
    }
    auto& [tree, tree_name] = it->second;
    return tree->FindParent(bf);
  }

  SpaceCheckResult CheckSpaceUtilization(lean_treeid_t tree_id, BufferFrame& bf) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, tree_id);
    }
    auto& [tree, tree_name] = it->second;
    return tree->CheckSpaceUtilization(bf);
  }

  // Pre: bf is shared/exclusive latched
  void Checkpoint(lean_treeid_t tree_id, BufferFrame& bf, void* dest) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, tree_id);
    }
    auto& [tree, tree_name] = it->second;
    return tree->Checkpoint(bf, dest);
  }

  /// Undo the changes of the given wal record on the target tree.
  void Undo(lean_treeid_t tree_id, const lean_wal_record* record) {
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", tree_id);
    }
    auto& [tree, tree_name] = it->second;
    return tree->Undo(record);
  }

  void GarbageCollect(lean_treeid_t tree_id, const uint8_t* version_data,
                      lean_wid_t version_worker_id, lean_txid_t version_tx_id, bool called_before) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      LEAN_DLOG("Skip GarbageCollect on non-existing tree, it is probably that "
                "the tree is already dropped, treeId={}",
                tree_id);
      return;
    }
    auto& [tree, tree_name] = it->second;
    return tree->GarbageCollect(version_data, version_worker_id, version_tx_id, called_before);
  }

  void Unlock(lean_treeid_t tree_id, const uint8_t* entry) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", tree_id);
    }
    auto& [tree, tree_name] = it->second;
    return tree->Unlock(entry);
  }

  void Deserialize(lean_treeid_t tree_id, std::unordered_map<std::string, std::string> map) {
    LEAN_SHARED_LOCK(mutex_);
    auto it = trees_.find(tree_id);
    if (it == trees_.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", tree_id);
    }
    auto& [tree, tree_name] = it->second;
    return tree->Deserialize(map);
  }
};

} // namespace leanstore
