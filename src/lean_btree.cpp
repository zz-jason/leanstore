#ifdef LEAN_ENABLE_CORO

#include "leanstore/lean_btree.hpp"

#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_cursor.hpp"
#include "leanstore/lean_session.hpp"

namespace leanstore {

LeanBTree::LeanBTree(LeanSession* session, std::variant<BasicKV*, TransactionKV*> btree)
    : session_(session),
      btree_(btree) {
}

Result<void> LeanBTree::Insert(Slice key, Slice value) {
  return session_->ExecSync([&]() -> Result<void> {
    OpCode result = std::visit([&](auto* btree) { return btree->Insert(key, value); }, btree_);
    if (result == OpCode::kOK) {
      return {};
    }
    if (result == OpCode::kDuplicated) {
      return Error::General("Duplicate key");
    }
    if (result == OpCode::kAbortTx) {
      return Error::General("Transaction aborted");
    }
    if (result == OpCode::kSpaceNotEnough) {
      return Error::General("Insufficient space");
    }
    return Error::General("Insert failed");
  });
}

Result<void> LeanBTree::Remove(Slice key) {
  return session_->ExecSync([&]() -> Result<void> {
    OpCode result = std::visit([&](auto* btree) { return btree->Remove(key); }, btree_);
    if (result == OpCode::kOK) {
      return {};
    }
    if (result == OpCode::kNotFound) {
      return Error::General("Key not found");
    }
    if (result == OpCode::kAbortTx) {
      return Error::General("Transaction aborted");
    }
    if (result == OpCode::kSpaceNotEnough) {
      return Error::General("Insufficient space");
    }
    return Error::General("Remove failed");
  });
}

Result<void> LeanBTree::Update(Slice key, Slice value) {
  return session_->ExecSync([&]() -> Result<void> {
    OpCode remove_result = std::visit([&](auto* btree) { return btree->Remove(key); }, btree_);
    if (remove_result != OpCode::kOK && remove_result != OpCode::kNotFound) {
      if (remove_result == OpCode::kAbortTx) {
        return Error::General("Transaction aborted");
      }
      if (remove_result == OpCode::kSpaceNotEnough) {
        return Error::General("Insufficient space");
      }
      return Error::General("Update failed during remove");
    }

    OpCode insert_result =
        std::visit([&](auto* btree) { return btree->Insert(key, value); }, btree_);
    if (insert_result == OpCode::kOK) {
      return {};
    }
    if (insert_result == OpCode::kDuplicated) {
      return Error::General("Duplicate key");
    }
    if (insert_result == OpCode::kAbortTx) {
      return Error::General("Transaction aborted");
    }
    if (insert_result == OpCode::kSpaceNotEnough) {
      return Error::General("Insufficient space");
    }
    return Error::General("Update failed during insert");
  });
}

Result<std::vector<uint8_t>> LeanBTree::Lookup(Slice key) {
  return session_->ExecSync([&]() -> Result<std::vector<uint8_t>> {
    std::vector<uint8_t> value;
    OpCode result = std::visit(
        [&](auto* btree) {
          return btree->Lookup(
              key, [&](Slice val) { value.assign(val.data(), val.data() + val.size()); });
        },
        btree_);
    if (result == OpCode::kOK) {
      return value;
    }
    if (result == OpCode::kNotFound) {
      return Error::General("Key not found");
    }
    if (result == OpCode::kAbortTx) {
      return Error::General("Transaction aborted");
    }
    if (result == OpCode::kSpaceNotEnough) {
      return Error::General("Insufficient space");
    }
    return Error::General("Lookup failed");
  });
}

LeanCursor LeanBTree::OpenCursor() {
  std::unique_ptr<BTreeIter> cursor =
      std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_);
  return LeanCursor(this, std::move(cursor));
}

void LeanBTree::Close() {
  // Nothing to do for now
}

} // namespace leanstore

#endif // LEAN_ENABLE_CORO
