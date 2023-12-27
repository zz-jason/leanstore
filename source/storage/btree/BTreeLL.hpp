#pragma once

#include "Config.hpp"
#include "KVInterface.hpp"
#include "core/BTreeGeneric.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/RandomGenerator.hpp"

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

class BTreeLL : public KVInterface, public BTreeGeneric {
public:
  struct WALUpdate : WALPayload {
    u16 mKeySize;
    u16 delta_length;
    u8 payload[];
  };

  struct WALRemove : WALPayload {
    u16 mKeySize;
    u16 mValSize;
    u8 payload[];

    WALRemove(Slice key, Slice val)
        : WALPayload(TYPE::WALRemove), mKeySize(key.size()),
          mValSize(val.size()) {
      std::memcpy(payload, key.data(), key.size());
      std::memcpy(payload + key.size(), val.data(), val.size());
    }
  };

public:
  //---------------------------------------------------------------------------
  // Constructor and Destructors
  //---------------------------------------------------------------------------
  BTreeLL() {
    mTreeType = BTREE_TYPE::LL;
  }

public:
  //---------------------------------------------------------------------------
  // KV Interfaces
  //---------------------------------------------------------------------------
  virtual OP_RESULT Lookup(Slice key, ValCallback valCallback) override;

  virtual OP_RESULT insert(Slice key, Slice val) override;

  virtual OP_RESULT updateSameSizeInPlace(Slice key,
                                          MutValCallback updateCallBack,
                                          UpdateDesc& updateDesc) override;

  virtual OP_RESULT remove(Slice key) override;
  virtual OP_RESULT scanAsc(Slice startKey, ScanCallback callback) override;
  virtual OP_RESULT scanDesc(Slice startKey, ScanCallback callback) override;
  virtual OP_RESULT prefixLookup(Slice, PrefixLookupCallback callback) override;
  virtual OP_RESULT prefixLookupForPrev(Slice key,
                                        PrefixLookupCallback callback) override;
  virtual OP_RESULT append(std::function<void(u8*)>, u16,
                           std::function<void(u8*)>, u16,
                           std::unique_ptr<u8[]>&) override;
  virtual OP_RESULT rangeRemove(Slice staryKey, Slice endKey,
                                bool page_used) override;

  virtual u64 countPages() override;
  virtual u64 countEntries() override;
  virtual u64 getHeight() override;

public:
  //---------------------------------------------------------------------------
  // Graveyard Interfaces
  //---------------------------------------------------------------------------
  bool isRangeSurelyEmpty(Slice start_key, Slice end_key);

public:
  static BTreeLL* Create(const std::string& treeName, Config& config) {
    auto [treePtr, treeId] =
        TreeRegistry::sInstance->CreateTree(treeName, [&]() {
          return std::unique_ptr<BufferManagedTree>(
              static_cast<BufferManagedTree*>(new storage::btree::BTreeLL()));
        });
    if (treePtr == nullptr) {
      LOG(ERROR) << "Failed to create BTreeLL, treeName has been taken"
                 << ", treeName=" << treeName;
      return nullptr;
    }
    auto tree = dynamic_cast<storage::btree::BTreeLL*>(treePtr);
    tree->Init(treeId, config);
    return tree;
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
