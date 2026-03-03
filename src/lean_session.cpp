#include "leanstore/lean_session.hpp"

#include "leanstore/btree/b_tree_generic.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/tx/transaction_kv.hpp"

#include <format>

namespace leanstore {

LeanSession::LeanSession(LeanStore* store, CoroSession* session)
    : store_(store),
      session_(session) {
}

LeanSession::LeanSession(LeanSession&& other) noexcept
    : store_(other.store_),
      session_(other.session_) {
  other.store_ = nullptr;
  other.session_ = nullptr;
}

auto LeanSession::operator=(LeanSession&& other) noexcept -> LeanSession& {
  if (this == &other) {
    return *this;
  }
  Close();
  store_ = other.store_;
  session_ = other.session_;
  other.store_ = nullptr;
  other.session_ = nullptr;
  return *this;
}

LeanSession::~LeanSession() {
  Close();
}

void LeanSession::ExecSyncVoid(std::function<void()> fn) {
  store_->GetCoroScheduler().Submit(session_, std::move(fn))->Wait();
}

void LeanSession::StartTx() {
  if (session_ != nullptr) {
    ExecSync([this]() { session_->GetTxMgr()->StartTx(); });
  }
}

void LeanSession::CommitTx() {
  if (session_ != nullptr) {
    ExecSync([this]() { session_->GetTxMgr()->CommitTx(); });
  }
}

void LeanSession::AbortTx() {
  if (session_ != nullptr) {
    ExecSync([this]() { session_->GetTxMgr()->AbortTx(); });
  }
}

Result<LeanBTree> LeanSession::CreateBTree(const std::string& name, lean_btree_type type,
                                           lean_btree_config config) {
  return ExecSync([&]() -> Result<LeanBTree> {
    switch (type) {
    case LEAN_BTREE_TYPE_ATOMIC: {
      auto result = BasicKV::Create(store_, name, config);
      if (!result) {
        return std::move(result.error());
      }
      return LeanBTree(this, result.value());
    }
    case LEAN_BTREE_TYPE_MVCC: {
      static constexpr auto kGraveyardConfig =
          lean_btree_config{.enable_wal_ = false, .use_bulk_insert_ = false};
      auto graveyard_name = std::format("_{}_graveyard", name);
      auto graveyard = BasicKV::Create(store_, graveyard_name, kGraveyardConfig);
      if (!graveyard) {
        return std::move(graveyard.error());
      }

      auto result = TransactionKV::Create(store_, name, config, graveyard.value());
      if (!result) {
        BTreeGeneric::FreeAndReclaim(*static_cast<BTreeGeneric*>(graveyard.value()));
        auto unregister_res = store_->tree_registry_->UnRegisterTree(graveyard.value()->tree_id_);
        if (!unregister_res) {
          Log::Error("Unregister graveyard failed, btree_name={}, graveyard_name={}, error={}",
                     name, graveyard_name, unregister_res.error().ToString());
        }
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
      BTreeGeneric::FreeAndReclaim(*generic_btree);
      if (auto unregister_res = store_->tree_registry_->UnregisterTree(name); !unregister_res) {
        Log::Error("Unregister BasicKV failed, error={}", unregister_res.error().ToString());
      }
      break;
    case BTreeType::kTransactionKV:
      BTreeGeneric::FreeAndReclaim(*generic_btree);
      if (auto unregister_res = store_->tree_registry_->UnregisterTree(name); !unregister_res) {
        Log::Error("Unregister TransactionKV failed, error={}", unregister_res.error().ToString());
      }
      {
        auto graveyard_name = std::format("_{}_graveyard", name);
        auto* graveyard = DownCast<BTreeGeneric*>(store_->tree_registry_->GetTree(graveyard_name));
        LEAN_DCHECK(graveyard != nullptr, "graveyard not found");
        BTreeGeneric::FreeAndReclaim(*graveyard);
        if (auto unregister_res = store_->tree_registry_->UnregisterTree(graveyard_name);
            !unregister_res) {
          Log::Error("Unregister TransactionKV graveyard failed, error={}",
                     unregister_res.error().ToString());
        }
      }
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
    if (store_ != nullptr && store_->coro_scheduler_ != nullptr) {
      store_->GetCoroScheduler().ReleaseCoroSession(session_);
    }
    session_ = nullptr;
  }
}

} // namespace leanstore
