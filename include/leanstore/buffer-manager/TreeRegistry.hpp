#pragma once

#include "leanstore/Units.hpp"
#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/sync/HybridGuard.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Error.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Result.hpp"

#include <cstdlib>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>

namespace leanstore::storage {

class ParentSwipHandler {
public:
  //! @brief mParentGuard is the latch guard to the parent buffer frame. It
  //! should already optimistically latched.
  HybridGuard mParentGuard;

  //! @brief mParentBf is the parent buffer frame.
  BufferFrame* mParentBf;

  //! @brief mChildSwip is the swip reference to the child who generated this
  //! ParentSwipHandler.
  Swip& mChildSwip;

  //! @brief mPosInParent is the slot id in the parent buffer frame.
  uint32_t mPosInParent = std::numeric_limits<uint32_t>::max();

  //! @brief mIsChildBfUpdated records whether the child buffer frame is updated
  //! since this ParentSwipHandler was created.
  bool mIsChildBfUpdated = false;
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

  virtual void undo(const uint8_t*, const uint64_t) {
    Log::Fatal("BufferManagedTree::undo is unimplemented");
  }

  virtual void GarbageCollect(const uint8_t*, WORKERID, TXID, bool) {
    Log::Fatal("BufferManagedTree::GarbageCollect is unimplemented");
  }

  virtual void unlock(const uint8_t*) {
    Log::Fatal("BufferManagedTree::unlock is unimplemented");
  }

  virtual StringMap Serialize() {
    Log::Fatal("BufferManagedTree::Serialize is unimplemented");
    return {};
  }

  virtual void Deserialize(StringMap) {
    Log::Fatal("BufferManagedTree::Deserialize is unimplemented");
  }

  virtual ~BufferManagedTree() {
  }
};

using TreeAndName = std::tuple<std::unique_ptr<BufferManagedTree>, std::string>;
using TreeMap = std::unordered_map<TREEID, TreeAndName>;
using TreeIndexByName = std::unordered_map<std::string, TreeMap::iterator>;

class TreeRegistry {
public:
  //! mMutex protects concurrent access to mTrees, mTreeIndexByName, and the
  //! lifetime of a managed tree object, i.e. the tree should stay valid during
  //! read/write access.
  std::shared_mutex mMutex;

  //! mTrees records and manages the lifetime of all the trees whose content are
  //! stored the buffer pool, for example BTrees.
  TreeMap mTrees;

  //! mTreeIndexByName is a secondary index for mTrees, it allows to find a tree
  //! by its name.
  TreeIndexByName mTreeIndexByName;

  //! Tree ID allocator, tree IDs are global unique. IDs of destoried tree are
  //! not recycled.
  std::atomic<TREEID> mTreeIdAllocator = 0;

public:
  inline TREEID AllocTreeId() {
    auto allocatedTreeId = mTreeIdAllocator++;
    return allocatedTreeId;
  }

  //! Creates a tree managed by buffer manager.
  inline std::tuple<BufferManagedTree*, TREEID> CreateTree(
      const std::string& treeName, std::function<std::unique_ptr<BufferManagedTree>()> ctor) {
    std::unique_lock uniqueGuard(mMutex);

    // check uniqueness
    if (mTreeIndexByName.find(treeName) != mTreeIndexByName.end()) {
      return std::make_tuple(nullptr, 0);
    }

    // create the tree
    auto treeId = AllocTreeId();
    auto tree = ctor();

    // register the tree
    auto emplaceResult =
        mTrees.emplace(std::make_pair(treeId, std::make_tuple(std::move(tree), treeName)));
    mTreeIndexByName.emplace(std::make_pair(treeName, emplaceResult.first));

    auto it = emplaceResult.first;
    auto* treePtr = std::get<0>(it->second).get();

    // return pointer and tree id
    return std::make_tuple(treePtr, treeId);
  }

  inline bool RegisterTree(TREEID treeId, std::unique_ptr<BufferManagedTree> tree,
                           const std::string& treeName) {
    SCOPED_DEFER(if (treeId > mTreeIdAllocator) { mTreeIdAllocator = treeId; });
    std::unique_lock uniqueGuard(mMutex);
    if (mTreeIndexByName.find(treeName) != mTreeIndexByName.end()) {
      return false;
    }

    auto emplaceResult =
        mTrees.emplace(std::make_pair(treeId, std::make_tuple(std::move(tree), treeName)));
    mTreeIndexByName.emplace(std::make_pair(treeName, emplaceResult.first));
    return true;
  }

  [[nodiscard]] inline Result<bool> UnregisterTree(const std::string& treeName) {
    std::unique_lock uniqueGuard(mMutex);
    auto it = mTreeIndexByName.find(treeName);
    if (it != mTreeIndexByName.end()) {
      auto treeIt = it->second;
      mTreeIndexByName.erase(treeName);
      mTrees.erase(treeIt);
      return true;
    }
    return std::unexpected<utils::Error>(utils::Error::General("TreeId not found"));
  }

  [[nodiscard]] inline Result<bool> UnRegisterTree(TREEID treeId) {
    std::unique_lock uniqueGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it != mTrees.end()) {
      auto& [tree, treeName] = it->second;
      mTreeIndexByName.erase(treeName);
      mTrees.erase(it);
      return true;
    }
    return std::unexpected<utils::Error>(utils::Error::General("TreeId not found"));
  }

  inline BufferManagedTree* GetTree(const std::string& treeName) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTreeIndexByName.find(treeName);
    if (it != mTreeIndexByName.end()) {
      auto treeIt = it->second;
      return std::get<0>(treeIt->second).get();
    }
    return nullptr;
  }

  inline void IterateChildSwips(TREEID treeId, BufferFrame& bf,
                                std::function<bool(Swip&)> callback) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, treeId);
    }
    auto& [tree, treeName] = it->second;
    tree->IterateChildSwips(bf, callback);
  }

  inline ParentSwipHandler FindParent(TREEID treeId, BufferFrame& bf) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->FindParent(bf);
  }

  inline SpaceCheckResult CheckSpaceUtilization(TREEID treeId, BufferFrame& bf) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->CheckSpaceUtilization(bf);
  }

  // Pre: bf is shared/exclusive latched
  inline void Checkpoint(TREEID treeId, BufferFrame& bf, void* dest) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, address={}, treeId={}", (void*)&bf, treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->Checkpoint(bf, dest);
  }

  // Recovery / SI
  inline void undo(TREEID treeId, const uint8_t* walEntry, uint64_t tts) {
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->undo(walEntry, tts);
  }

  inline void GarbageCollect(TREEID treeId, const uint8_t* versionData, WORKERID versionWorkerId,
                             TXID versionTxId, bool calledBefore) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      LS_DLOG("Skip GarbageCollect on non-existing tree, it is probably that "
              "the tree is already dropped, treeId={}",
              treeId);
      return;
    }
    auto& [tree, treeName] = it->second;
    return tree->GarbageCollect(versionData, versionWorkerId, versionTxId, calledBefore);
  }

  inline void unlock(TREEID treeId, const uint8_t* entry) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->unlock(entry);
  }

  // Serialization
  inline StringMap Serialize(TREEID treeId) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->Serialize();
  }

  inline void Deserialize(TREEID treeId, StringMap map) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it == mTrees.end()) {
      Log::Fatal("BufferManagedTree not find, treeId={}", treeId);
    }
    auto& [tree, treeName] = it->second;
    return tree->Deserialize(map);
  }
};

} // namespace leanstore::storage
