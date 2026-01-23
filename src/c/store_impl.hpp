#pragma once

#include "c/session_impl.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/lean_store.hpp"

#include <memory>

namespace leanstore {

class StoreImpl {
public:
  static struct lean_store* Create(std::unique_ptr<LeanStore> store) {
    auto* impl = new StoreImpl(std::move(store));
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_store* store) {
    delete reinterpret_cast<StoreImpl*>(store);
  }

private:
  explicit StoreImpl(std::unique_ptr<LeanStore> store) : store_(std::move(store)) {
    base_ = {
        .connect = &Thunk<&StoreImpl::Connect>,
        .try_connect = &Thunk<&StoreImpl::TryConnect>,
        .close = &Destroy,
    };
  }

  lean_session* Connect() {
    while (true) {
      auto* session = TryConnect();
      if (session != nullptr) {
        return session;
      }
    }
  }

  lean_session* TryConnect() {
    static std::atomic<uint64_t> runs_on_counter{0};
    auto runs_on = runs_on_counter++ % store_->store_option_->worker_threads_;
    auto* session = store_->TryReserveSession(runs_on);
    if (session == nullptr) {
      return nullptr;
    }
    return SessionImpl::Create(store_.get(), session);
  }

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_store* base, Args... args) {
    auto* impl = reinterpret_cast<StoreImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

  lean_store base_;
  std::unique_ptr<LeanStore> store_;
};

} // namespace leanstore