#pragma once

#include "api/c/session_impl.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/c/leanstore.h"

namespace leanstore {

class CursorImpl {
public:
  static struct lean_cursor* Create(BasicKV* btree, SessionImpl* session_impl) {
    auto* impl = new CursorImpl(btree, session_impl);
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_cursor* cursor) {
    delete reinterpret_cast<CursorImpl*>(cursor);
  }

private:
  CursorImpl(BasicKV* btree, SessionImpl* session_impl)
      : btree_(btree),
        session_impl_(session_impl) {
    base_ = {
        .seek_to_first = &Thunk<&CursorImpl::SeekToFirst, bool>,
        .seek_to_first_ge = &Thunk<&CursorImpl::SeekToFirstGe, bool, lean_str_view>,
        .next = &Thunk<&CursorImpl::Next, bool>,
        .seek_to_last = &Thunk<&CursorImpl::SeekToLast, bool>,
        .seek_to_last_le = &Thunk<&CursorImpl::SeekToLastLe, bool, lean_str_view>,
        .prev = &Thunk<&CursorImpl::Prev, bool>,
        .is_valid = &Thunk<&CursorImpl::IsValid, bool>,
        .current_key = &Thunk<&CursorImpl::CurrentKey, void, lean_str*>,
        .current_value = &Thunk<&CursorImpl::CurrentValue, void, lean_str*>,
        .remove_current = &Thunk<&CursorImpl::RemoveCurrent, lean_status>,
        .update_current = &Thunk<&CursorImpl::UpdateCurrent, lean_status, lean_str_view>,
        .close = Destroy,
    };
  }

  ~CursorImpl() = default;

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

  void RecordCurrent(BTreeIter* iter);

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_cursor* base, Args... args) {
    auto* impl = reinterpret_cast<CursorImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

  lean_cursor base_;
  BasicKV* btree_;
  std::string current_key_;
  std::string current_value_;
  SessionImpl* session_impl_;
  bool is_valid_ = false;
  bool is_removed_ = false;
};

} // namespace leanstore