#pragma once

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "utils/coroutine/coro_env.hpp"
#include "utils/coroutine/coro_session.hpp"

namespace leanstore {

class SessionImpl {
public:
  static struct lean_session* Create(leanstore::LeanStore* store, leanstore::CoroSession* session) {
    auto* impl = new SessionImpl(store, session);
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_session* session) {
    delete reinterpret_cast<SessionImpl*>(session);
  }

  void ExecSync(std::function<void()>&& job) {
    store_->GetCoroScheduler()->Submit(session_, std::move(job))->Wait();
  }

private:
  SessionImpl(leanstore::LeanStore* store, leanstore::CoroSession* session)
      : store_(store),
        session_(session) {
    base_ = {
        .start_tx = &Thunk<&SessionImpl::StartTx>,
        .commit_tx = &Thunk<&SessionImpl::CommitTx>,
        .abort_tx = &Thunk<&SessionImpl::AbortTx>,
        .create_btree =
            &Thunk<&SessionImpl::CreateBTree, lean_status, const char*, lean_btree_type>,
        .drop_btree = &Thunk<&SessionImpl::DropBTree, void, const char*>,
        .get_btree = &Thunk<&SessionImpl::GetBTree, struct lean_btree*, const char*>,
        .close = &Destroy,
    };
  }

  ~SessionImpl() = default;

  void StartTx() {
    ExecSync([]() { CoroEnv::CurTxMgr().StartTx(); });
  }

  void CommitTx() {
    ExecSync([]() { CoroEnv::CurTxMgr().CommitTx(); });
  }

  void AbortTx() {
    ExecSync([]() { CoroEnv::CurTxMgr().AbortTx(); });
  }

  lean_status CreateBTree(const char* btree_name, lean_btree_type btree_type);
  void DropBTree(const char* btree_name);
  struct lean_btree* GetBTree(const char* btree_name);

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_session* base, Args... args) {
    auto* impl = reinterpret_cast<SessionImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

private:
  lean_session base_;
  leanstore::LeanStore* store_;
  leanstore::CoroSession* session_;
};

} // namespace leanstore