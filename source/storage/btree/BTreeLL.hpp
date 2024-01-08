#pragma once

#include "Config.hpp"
#include "KVInterface.hpp"
#include "core/BTreeGeneric.hpp"
#include "profiling/counters/WorkerCounters.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/buffer-manager/GuardedBufferFrame.hpp"
#include "utils/Error.hpp"
#include "utils/RandomGenerator.hpp"

#include <expected>

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
  virtual OpCode Lookup(Slice key, ValCallback valCallback) override;

  virtual OpCode insert(Slice key, Slice val) override;

  virtual OpCode updateSameSizeInPlace(Slice key, MutValCallback updateCallBack,
                                       UpdateDesc& updateDesc) override;

  virtual OpCode remove(Slice key) override;
  virtual OpCode scanAsc(Slice startKey, ScanCallback callback) override;
  virtual OpCode scanDesc(Slice startKey, ScanCallback callback) override;
  virtual OpCode prefixLookup(Slice, PrefixLookupCallback callback) override;
  virtual OpCode prefixLookupForPrev(Slice key,
                                     PrefixLookupCallback callback) override;
  virtual OpCode append(std::function<void(u8*)>, u16, std::function<void(u8*)>,
                        u16, std::unique_ptr<u8[]>&) override;
  virtual OpCode rangeRemove(Slice staryKey, Slice endKey,
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
  [[nodiscard]] static auto Create(const std::string& treeName, Config& config)
      -> std::expected<BTreeLL*, utils::Error> {
    auto [treePtr, treeId] =
        TreeRegistry::sInstance->CreateTree(treeName, [&]() {
          return std::unique_ptr<BufferManagedTree>(
              static_cast<BufferManagedTree*>(new storage::btree::BTreeLL()));
        });
    if (treePtr == nullptr) {
      return std::unexpected<utils::Error>(
          utils::Error::General("Tree name has been taken"));
    }
    auto tree = dynamic_cast<storage::btree::BTreeLL*>(treePtr);
    tree->Init(treeId, config);
    return tree;
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
