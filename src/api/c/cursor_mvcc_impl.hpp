#pragma once

#include "api/c/session_impl.hpp"
#include "leanstore/btree/core/btree_iter.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/c/leanstore.h"

namespace leanstore {

class CursorMvccImpl {
public:
  static struct lean_cursor* Create(TransactionKV* btree, SessionImpl* session_impl) {
    auto* impl = new CursorMvccImpl(btree, session_impl);
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_cursor* cursor) {
    delete reinterpret_cast<CursorMvccImpl*>(cursor);
  }

private:
  CursorMvccImpl(TransactionKV* btree, SessionImpl* session_impl)
      : btree_(btree),
        session_impl_(session_impl) {
    base_ = {
        .seek_to_first = &Thunk<&CursorMvccImpl::SeekToFirst, bool>,
        .seek_to_first_ge = &Thunk<&CursorMvccImpl::SeekToFirstGe, bool, lean_str_view>,
        .next = &Thunk<&CursorMvccImpl::Next, bool>,
        .seek_to_last = &Thunk<&CursorMvccImpl::SeekToLast, bool>,
        .seek_to_last_le = &Thunk<&CursorMvccImpl::SeekToLastLe, bool, lean_str_view>,
        .prev = &Thunk<&CursorMvccImpl::Prev, bool>,
        .is_valid = &Thunk<&CursorMvccImpl::IsValid, bool>,
        .current_key = &Thunk<&CursorMvccImpl::CurrentKey, void, lean_str*>,
        .current_value = &Thunk<&CursorMvccImpl::CurrentValue, void, lean_str*>,
        .remove_current = &Thunk<&CursorMvccImpl::RemoveCurrent, lean_status>,
        .update_current = &Thunk<&CursorMvccImpl::UpdateCurrent, lean_status, lean_str_view>,
        .close = Destroy,
    };
  }

  ~CursorMvccImpl() = default;

  bool SeekToFirst();
  bool SeekToFirstGe(lean_str_view key);
  bool Next();
  bool SeekToLast();
  bool SeekToLastLe(lean_str_view key);
  bool Prev();
  bool IsValid();
  void CurrentKey(lean_str* key);
  void CurrentValue(lean_str* value);
  lean_status RemoveCurrent();
  lean_status UpdateCurrent(lean_str_view new_value);

  bool SeekToVisibleAsc(BTreeIter* iter);
  bool SeekToVisibleDesc(BTreeIter* iter);

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_cursor* base, Args... args) {
    auto* impl = reinterpret_cast<CursorMvccImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

private:
  lean_cursor base_;
  TransactionKV* btree_;
  std::string current_key_;
  std::string current_value_;
  SessionImpl* session_impl_;
  bool is_valid_ = false;
  bool is_removed_ = false;
};

} // namespace leanstore