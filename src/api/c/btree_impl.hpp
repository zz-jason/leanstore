#pragma once

#include "api/c/session_impl.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/c/leanstore.h"

#include <cassert>

namespace leanstore {

class BTreeImpl {
public:
  static struct lean_btree* Create(leanstore::storage::btree::BasicKV* btree,
                                   SessionImpl* session_impl) {
    auto* impl = new BTreeImpl(btree, session_impl);
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_btree* btree) {
    delete reinterpret_cast<BTreeImpl*>(btree);
  }

private:
  BTreeImpl(leanstore::storage::btree::BasicKV* btree, SessionImpl* session_impl)
      : btree_(btree),
        session_impl_(session_impl) {
    base_ = {
        .insert = &Thunk<&BTreeImpl::Insert, lean_status, lean_str_view, lean_str_view>,
        .remove = &Thunk<&BTreeImpl::Remove, lean_status, lean_str_view>,
        .lookup = &Thunk<&BTreeImpl::Lookup, lean_status, lean_str_view, lean_str*>,
        .open_cursor = &Thunk<&BTreeImpl::OpenCursor, struct lean_cursor*>,
        .close = &Destroy,
    };
  }

  ~BTreeImpl() = default;

  lean_status Insert(lean_str_view key, lean_str_view value);
  lean_status Remove(lean_str_view key);
  lean_status Lookup(lean_str_view key, lean_str* value);
  struct lean_cursor* OpenCursor();

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_btree* btree, Args... args) {
    auto* impl = reinterpret_cast<BTreeImpl*>(btree);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

private:
  lean_btree base_;
  storage::btree::BasicKV* btree_;
  SessionImpl* session_impl_;
};

} // namespace leanstore