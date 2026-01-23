#pragma once

#include "c/session_impl.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/tx/transaction_kv.hpp"

#include <cassert>

namespace leanstore {

class BTreeMvccImpl {
public:
  static struct lean_btree* Create(TransactionKV* btree, SessionImpl* session_impl) {
    auto* impl = new BTreeMvccImpl(btree, session_impl);
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_btree* btree) {
    delete reinterpret_cast<BTreeMvccImpl*>(btree);
  }

private:
  BTreeMvccImpl(TransactionKV* btree, SessionImpl* session_impl)
      : btree_(btree),
        session_impl_(session_impl) {
    base_ = {
        .insert = &Thunk<&BTreeMvccImpl::Insert, lean_status, lean_str_view, lean_str_view>,
        .remove = &Thunk<&BTreeMvccImpl::Remove, lean_status, lean_str_view>,
        .lookup = &Thunk<&BTreeMvccImpl::Lookup, lean_status, lean_str_view, lean_str*>,
        .open_cursor = &Thunk<&BTreeMvccImpl::OpenCursor, struct lean_cursor*>,
        .close = Destroy,
    };
  }

  ~BTreeMvccImpl() = default;

  lean_status Insert(lean_str_view key, lean_str_view value);
  lean_status Remove(lean_str_view key);
  lean_status Lookup(lean_str_view key, lean_str* value);
  struct lean_cursor* OpenCursor();

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_btree* btree, Args... args) {
    auto* impl = reinterpret_cast<BTreeMvccImpl*>(btree);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

  lean_btree base_;
  TransactionKV* btree_;
  SessionImpl* session_impl_;
};

} // namespace leanstore