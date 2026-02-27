#ifdef LEAN_ENABLE_CORO

#include "leanstore/lean_session.hpp"

#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/tx/transaction_kv.hpp"

namespace leanstore {

LeanSession::LeanSession(LeanStore* store, CoroSession* session)
    : store_(store),
      session_(session) {
}

LeanSession::LeanSession(LeanSession&& other) noexcept
    : store_(other.store_),
      session_(other.session_),
      in_transaction_(other.in_transaction_) {
  other.store_ = nullptr;
  other.session_ = nullptr;
  other.in_transaction_ = false;
}

auto LeanSession::operator=(LeanSession&& other) noexcept -> LeanSession& {
  if (this == &other) {
    return *this;
  }
  Close();
  store_ = other.store_;
  session_ = other.session_;
  in_transaction_ = other.in_transaction_;
  other.store_ = nullptr;
  other.session_ = nullptr;
  other.in_transaction_ = false;
  return *this;
}

LeanSession::~LeanSession() {
  Close();
}

void LeanSession::ExecSyncVoid(std::function<void()> fn) {
  store_->SubmitAndWait(session_, std::move(fn));
}

void LeanSession::StartTx() {
  if (session_ != nullptr) {
    in_transaction_ = true;
    ExecSync([this]() { session_->GetTxMgr()->StartTx(); });
  }
}

void LeanSession::CommitTx() {
  if (session_ != nullptr) {
    ExecSync([this]() { session_->GetTxMgr()->CommitTx(); });
    in_transaction_ = false;
  }
}

void LeanSession::AbortTx() {
  if (session_ != nullptr) {
    ExecSync([this]() { session_->GetTxMgr()->AbortTx(); });
    in_transaction_ = false;
  }
}

Result<LeanBTree> LeanSession::CreateBTree(const std::string& name, lean_btree_type type,
                                           lean_btree_config config) {
  return ExecSync([&]() -> Result<LeanBTree> {
    switch (type) {
    case LEAN_BTREE_TYPE_ATOMIC: {
      auto result = store_->CreateBasicKv(name, config);
      if (!result) {
        return std::move(result.error());
      }
      return LeanBTree(this, result.value());
    }
    case LEAN_BTREE_TYPE_MVCC: {
      auto result = store_->CreateTransactionKV(name, config);
      if (!result) {
        return std::move(result.error());
      }
      return LeanBTree(this, result.value());
    }
    default:
      return Error::General("Unknown B-tree type");
    }
  });
}

void LeanSession::DropBTree(const std::string& name) {
  ExecSync([&]() {
    auto* tree = store_->tree_registry_->GetTree(name);
    if (tree == nullptr) {
      return;
    }
    auto* generic_btree = dynamic_cast<BTreeGeneric*>(tree);
    if (generic_btree == nullptr) {
      return;
    }
    switch (generic_btree->tree_type_) {
    case BTreeType::kBasicKV:
      store_->DropBasicKV(name);
      break;
    case BTreeType::kTransactionKV:
      store_->DropTransactionKV(name);
      break;
    default:
      break;
    }
  });
}

Result<LeanBTree> LeanSession::GetBTree(const std::string& name) {
  return ExecSync([&]() -> Result<LeanBTree> {
    auto* tree = store_->tree_registry_->GetTree(name);
    if (tree == nullptr) {
      return Error::General("B-tree not found: {}", name);
    }
    auto* generic_btree = dynamic_cast<BTreeGeneric*>(tree);
    if (generic_btree == nullptr) {
      return Error::General("Tree '{}' has incompatible type", name);
    }
    switch (generic_btree->tree_type_) {
    case BTreeType::kBasicKV:
      return LeanBTree(this, static_cast<BasicKV*>(tree));
    case BTreeType::kTransactionKV:
      return LeanBTree(this, static_cast<TransactionKV*>(tree));
    default:
      return Error::General("Unsupported B-tree type for '{}': {}", name,
                            static_cast<uint8_t>(generic_btree->tree_type_));
    }
  });
}

void LeanSession::Close() {
  if (session_ != nullptr) {
    store_->ReleaseSession(session_);
    session_ = nullptr;
  }
}

} // namespace leanstore

#endif // LEAN_ENABLE_CORO
