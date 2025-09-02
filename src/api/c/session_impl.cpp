#include "api/c/session_impl.hpp"

#include "api/c/btree_impl.hpp"
#include "api/c/btree_mvcc_impl.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/b_tree_generic.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/tree_registry.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "utils/coroutine/coro_env.hpp"

#include <iostream>

namespace leanstore {

lean_status SessionImpl::CreateBTree(const char* btree_name, lean_btree_type btree_type) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  ExecSync([&]() {
    switch (btree_type) {
    case lean_btree_type::LEAN_BTREE_TYPE_ATOMIC: {
      // storage::btree::BasicKV* btree;
      auto res = store_->CreateBasicKv(btree_name);
      if (!res) {
        std::cerr << "CreateBTree failed: " << res.error().ToString() << std::endl;
        status = lean_status::LEAN_ERR_CTEATE_BTREE;
      }
      return;
    }
    case lean_btree_type::LEAN_BTREE_TYPE_MVCC: {
      auto res = store_->CreateTransactionKV(btree_name);
      if (!res) {
        std::cerr << "CreateBTree failed: " << res.error().ToString() << std::endl;
        status = lean_status::LEAN_ERR_CTEATE_BTREE;
      }
      return;
    }
    default: {
      std::cerr << "CreateBTree failed: type unsupported " << (int)(btree_type) << std::endl;
      status = lean_status::LEAN_ERR_CTEATE_BTREE;
      return;
    }
    }
  });
  return status;
}

void SessionImpl::DropBTree(const char* btree_name) {
  ExecSync([&]() {
    auto* tree = store_->tree_registry_->GetTree(btree_name);
    auto* generic_btree = dynamic_cast<leanstore::storage::btree::BTreeGeneric*>(tree);
    if (generic_btree == nullptr) {
      return;
    }

    switch (generic_btree->tree_type_) {
    case leanstore::storage::btree::BTreeType::kGeneric: {
      store_->DropBasicKV(btree_name);
      return;
    }
    case leanstore::storage::btree::BTreeType::kBasicKV: {
      store_->DropTransactionKV(btree_name);
      return;
    }
    default: {
      return;
    }
    }
  });
}

struct lean_btree* SessionImpl::GetBTree(const char* btree_name) {
  struct lean_btree* btree_result = nullptr;
  ExecSync([&]() {
    auto* tree = store_->tree_registry_->GetTree(btree_name);
    auto* generic_btree = dynamic_cast<leanstore::storage::btree::BTreeGeneric*>(tree);
    if (generic_btree == nullptr) {
      btree_result = nullptr;
      return;
    }

    switch (generic_btree->tree_type_) {
    case leanstore::storage::btree::BTreeType::kBasicKV: {
      auto* btree = dynamic_cast<storage::btree::BasicKV*>(generic_btree);
      btree_result = BTreeImpl::Create(btree, this);
      return;
    }
    case leanstore::storage::btree::BTreeType::kTransactionKV: {
      auto* btree_mvcc = dynamic_cast<storage::btree::TransactionKV*>(generic_btree);
      btree_result = BTreeMvccImpl::Create(btree_mvcc, this);
      return;
    }
    default: {
      btree_result = nullptr;
      return;
    }
    }
  });

  return btree_result;
}

} // namespace leanstore