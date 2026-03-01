#pragma once

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_cursor.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/table/table.hpp"
#include "leanstore/tx/transaction_kv.hpp"
#include "leanstore/tx/tx_manager.hpp"

#include <cassert>
#include <utility>

namespace leanstore {

class SessionImpl {
public:
  static struct lean_session* Create(LeanStore* store, LeanSession&& session) {
    auto* impl = new SessionImpl(store, std::move(session));
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_session* session) {
    delete reinterpret_cast<SessionImpl*>(session);
  }

  auto Session() -> LeanSession& {
    return session_;
  }

private:
  SessionImpl(LeanStore* store, LeanSession&& session)
      : store_(store),
        session_(std::move(session)) {
    base_ = {
        .start_tx = &Thunk<&SessionImpl::StartTx>,
        .commit_tx = &Thunk<&SessionImpl::CommitTx>,
        .abort_tx = &Thunk<&SessionImpl::AbortTx>,
        .create_btree =
            &Thunk<&SessionImpl::CreateBTree, lean_status, const char*, lean_btree_type>,
        .drop_btree = &Thunk<&SessionImpl::DropBTree, void, const char*>,
        .get_btree = &Thunk<&SessionImpl::GetBTree, struct lean_btree*, const char*>,
        .create_table =
            &Thunk<&SessionImpl::CreateTable, lean_status, const struct lean_table_def*>,
        .drop_table = &Thunk<&SessionImpl::DropTable, lean_status, const char*>,
        .get_table = &Thunk<&SessionImpl::GetTable, struct lean_table*, const char*>,
        .close = &Destroy,
    };
  }

  ~SessionImpl() = default;

  void StartTx() {
    session_.StartTx();
  }

  void CommitTx() {
    session_.CommitTx();
  }

  void AbortTx() {
    session_.AbortTx();
  }

  lean_status CreateBTree(const char* btree_name, lean_btree_type btree_type);
  void DropBTree(const char* btree_name);
  struct lean_btree* GetBTree(const char* btree_name);
  lean_status CreateTable(const struct lean_table_def* table_def);
  lean_status DropTable(const char* table_name);
  struct lean_table* GetTable(const char* table_name);

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_session* base, Args... args) {
    auto* impl = reinterpret_cast<SessionImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

  lean_session base_;
  LeanStore* store_;
  LeanSession session_;
};

} // namespace leanstore
