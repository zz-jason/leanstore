#pragma once

#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "Units.hpp"
#include "utils/Defer.hpp"
#include "utils/Error.hpp"

#include <glog/logging.h>

#include <expected>
#include <functional>
#include <mutex>
#include <tuple>
#include <unordered_map>

namespace leanstore {
namespace storage {

class ParentSwipHandler {
public:
  /// @brief mParentGuard is the latch guard to the parent buffer frame. It
  /// should already optimistically latched.
  HybridGuard mParentGuard;

  /// @brief mParentBf is the parent buffer frame.
  BufferFrame* mParentBf;

  /// @brief mChildSwip is the swip reference to the child who generated this
  /// ParentSwipHandler.
  Swip<BufferFrame>& mChildSwip;

  /// @brief mPosInParent is the slot id in the parent buffer frame.
  s64 mPosInParent = -2;

  /// @brief mIsChildBfUpdated records whether the child buffer frame is updated
  /// since this ParentSwipHandler was created.
  bool mIsChildBfUpdated = false;

public:
  template <typename T> GuardedBufferFrame<T> GetGuardedParent() {
    return GuardedBufferFrame<T>(std::move(mParentGuard), mParentBf);
  }
};

enum class SpaceCheckResult : u8 { kNothing, kPickAnotherBf, kRestartSameBf };

using ChildSwipCallback = std::function<bool(Swip<BufferFrame>&)>;

class BufferManagedTree {
public:
  virtual void IterateChildSwips(BufferFrame&, ChildSwipCallback) {
    LOG(FATAL) << "BufferManagedTree::IterateChildSwips is unimplemented";
  }

  virtual ParentSwipHandler findParent(BufferFrame&) {
    LOG(FATAL) << "BufferManagedTree::findParent is unimplemented";
  }

  virtual SpaceCheckResult checkSpaceUtilization(BufferFrame&) {
    LOG(FATAL) << "BufferManagedTree::checkSpaceUtilization is unimplemented ";
  }

  virtual void Checkpoint(BufferFrame&, void*) {
    LOG(FATAL) << "BufferManagedTree::Checkpoint is unimplemented";
  }

  virtual void undo(const u8*, const u64) {
    LOG(FATAL) << "BufferManagedTree::undo is unimplemented";
  }

  virtual void todo(const u8*, const u64, const u64, const bool) {
    LOG(FATAL) << "BufferManagedTree::todo is unimplemented";
  }

  virtual void unlock(const u8*) {
    LOG(FATAL) << "BufferManagedTree::unlock is unimplemented";
  }

  virtual StringMap serialize() {
    LOG(FATAL) << "BufferManagedTree::serialize is unimplemented";
    return StringMap();
  }

  virtual void deserialize(StringMap) {
    LOG(FATAL) << "BufferManagedTree::deserialize is unimplemented";
  }

  virtual ~BufferManagedTree() {
  }
};

using TreeAndName = std::tuple<std::unique_ptr<BufferManagedTree>, string>;
using TreeMap = std::unordered_map<TREEID, TreeAndName>;
using TreeIndexByName = std::unordered_map<std::string, TreeMap::iterator>;

class TreeRegistry {
public:
  /**
   * mMutex protects concurrent access to mTrees, mTreeIndexByName, and the
   * lifetime of a managed tree object, i.e. the tree should stay valid during
   * read/write access.
   */
  std::shared_mutex mMutex;

  /**
   * mTrees records and manages the lifetime of all the trees whose content are
   * stored the buffer pool, for example BTrees.
   */
  TreeMap mTrees;

  /**
   *
   */
  TreeIndexByName mTreeIndexByName;

  /**
   * Tree ID allocator, tree IDs are global unique. IDs of destoried tree are
   * not recycled.
   */
  std::atomic<TREEID> mTreeIdAllocator = 0;

public:
  inline TREEID AllocTreeId() {
    return mTreeIdAllocator++;
  }

  /**
   * Creates a tree managed by buffer manager.
   */
  inline std::tuple<BufferManagedTree*, TREEID> CreateTree(
      const std::string& treeName,
      std::function<std::unique_ptr<BufferManagedTree>()> ctor) {
    std::unique_lock uniqueGuard(mMutex);

    // check uniqueness
    if (mTreeIndexByName.find(treeName) != mTreeIndexByName.end()) {
      return std::make_tuple(nullptr, 0);
    }

    // create the tree
    auto treeId = AllocTreeId();
    auto tree = ctor();

    // register the tree
    auto emplaceResult = mTrees.emplace(std::make_pair(
        treeId, std::move(std::make_tuple(std::move(tree), treeName))));
    mTreeIndexByName.emplace(std::make_pair(treeName, emplaceResult.first));

    auto it = emplaceResult.first;
    auto treePtr = std::get<0>(it->second).get();

    // return pointer and tree id
    return std::make_tuple(treePtr, treeId);
  }

  inline bool RegisterTree(TREEID treeId,
                           std::unique_ptr<BufferManagedTree> tree,
                           const std::string& treeName) {
    SCOPED_DEFER(if (treeId > mTreeIdAllocator) { mTreeIdAllocator = treeId; });
    std::unique_lock uniqueGuard(mMutex);
    if (mTreeIndexByName.find(treeName) != mTreeIndexByName.end()) {
      return false;
    }

    auto emplaceResult = mTrees.emplace(std::make_pair(
        treeId, std::move(std::make_tuple(std::move(tree), treeName))));
    mTreeIndexByName.emplace(std::make_pair(treeName, emplaceResult.first));
    return true;
  }

  [[nodiscard]] inline auto UnregisterTree(const std::string& treeName)
      -> std::expected<bool, utils::Error> {
    std::unique_lock uniqueGuard(mMutex);
    auto it = mTreeIndexByName.find(treeName);
    if (it != mTreeIndexByName.end()) {
      auto treeIt = it->second;
      mTreeIndexByName.erase(treeName);
      mTrees.erase(treeIt);
      return true;
    }
    return std::unexpected<utils::Error>(
        utils::Error::General("TreeId not found"));
  }

  [[nodiscard]] inline auto UnRegisterTree(TREEID treeId)
      -> std::expected<bool, utils::Error> {
    std::unique_lock uniqueGuard(mMutex);
    auto it = mTrees.find(treeId);
    if (it != mTrees.end()) {
      auto& [tree, treeName] = it->second;
      mTreeIndexByName.erase(treeName);
      mTrees.erase(it);
      return true;
    }
    return std::unexpected<utils::Error>(
        utils::Error::General("TreeId not found"));
  }

  inline BufferManagedTree* GetTree(const std::string& treeName) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTreeIndexByName.find(treeName);
    if (it != mTreeIndexByName.end()) {
      auto treeIt = it->second;
      return std::get<0>(treeIt->second).get();
    } else {
      return nullptr;
    }
  }

  inline void IterateChildSwips(
      TREEID treeId, BufferFrame& bf,
      std::function<bool(Swip<BufferFrame>&)> callback) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    tree->IterateChildSwips(bf, callback);
  }

  inline ParentSwipHandler findParent(TREEID treeId, BufferFrame& bf) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->findParent(bf);
  }

  inline SpaceCheckResult checkSpaceUtilization(TREEID treeId,
                                                BufferFrame& bf) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->checkSpaceUtilization(bf);
  }

  // Pre: bf is shared/exclusive latched
  inline void Checkpoint(TREEID treeId, BufferFrame& bf, void* dest) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->Checkpoint(bf, dest);
  }

  // Recovery / SI
  inline void undo(TREEID treeId, const u8* walEntry, u64 tts) {
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->undo(walEntry, tts);
  }

  inline void todo(TREEID treeId, const u8* entry, const u64 version_worker_id,
                   u64 version_tx_id, const bool called_before) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->todo(entry, version_worker_id, version_tx_id, called_before);
  }

  inline void unlock(TREEID treeId, const u8* entry) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->unlock(entry);
  }

  // Serialization
  inline StringMap serialize(TREEID treeId) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->serialize();
  }

  inline void deserialize(TREEID treeId, StringMap map) {
    std::shared_lock sharedGuard(mMutex);
    auto it = mTrees.find(treeId);
    DLOG_IF(FATAL, it == mTrees.end())
        << "BufferManagedTree not find, treeId=" << treeId;
    auto& [tree, treeName] = it->second;
    return tree->deserialize(map);
  }

public:
  static std::unique_ptr<TreeRegistry> sInstance;
};

} // namespace storage
} // namespace leanstore
