#pragma once

#include "HistoryTreeInterface.hpp"
#include "Units.hpp"
#include "storage/btree/BTreeLL.hpp"

#include <functional>

namespace leanstore {
namespace cr {

struct __attribute__((packed)) VersionMeta {
  bool called_before = false;
  TREEID mTreeId;
  u8 payload[];

public:
  inline static const VersionMeta* From(const u8* buffer) {
    return reinterpret_cast<const VersionMeta*>(buffer);
  }
};

using BTreeLL = leanstore::storage::btree::BTreeLL;

class HistoryTree : public HistoryTreeInterface {
private:
  struct alignas(64) Session {
    BufferFrame* rightmost_bf;
    BufferFrame* leftmost_bf;
    u64 rightmost_version;
    u64 leftmost_version;
    s64 rightmost_pos = -1;
    TXID last_tx_id;
    bool rightmost_init = false;
    bool leftmost_init = false;
  };
  Session update_sessions[leanstore::cr::STATIC_MAX_WORKERS];
  Session remove_sessions[leanstore::cr::STATIC_MAX_WORKERS];

public:
  std::unique_ptr<BTreeLL*[]> update_btrees;
  std::unique_ptr<BTreeLL*[]> remove_btrees;

  virtual ~HistoryTree() = default;

  virtual void insertVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                             TREEID treeId, bool isRemove, u64 payload_length,
                             std::function<void(u8*)> cb,
                             bool same_thread) override;

  virtual bool retrieveVersion(
      WORKERID workerId, TXID txId, COMMANDID commandId, const bool isRemove,
      std::function<void(const u8*, u64 payload_length)> cb) override;

  virtual void purgeVersions(WORKERID workerId, TXID from_tx_id, TXID to_tx_id,
                             RemoveVersionCallback cb,
                             const u64 limit) override;

  // [from, to]
  virtual void visitRemoveVersions(WORKERID workerId, TXID from_tx_id,
                                   TXID to_tx_id,
                                   RemoveVersionCallback cb) override;
};

} // namespace cr
} // namespace leanstore
