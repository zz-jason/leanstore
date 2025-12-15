#pragma once

#include "api/c/session_impl.hpp"
#include "api/c/tx_guard.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/table/table.hpp"

#include <cassert>
#include <memory>
#include <string>

namespace leanstore {

class TableCursorImpl;

class TableImpl {
public:
  static struct lean_table* Create(Table* table, SessionImpl* session_impl) {
    auto* impl = new TableImpl(table, session_impl);
    assert(static_cast<void*>(impl) == static_cast<void*>(&impl->base_));
    return &impl->base_;
  }

  static void Destroy(struct lean_table* table) {
    delete reinterpret_cast<TableImpl*>(table);
  }

private:
  TableImpl(Table* table, SessionImpl* session_impl)
      : base_{.insert = &Thunk<&TableImpl::Insert, lean_status, const struct lean_row*>,
              .remove = &Thunk<&TableImpl::Remove, lean_status, const struct lean_row*>,
              .lookup =
                  &Thunk<&TableImpl::Lookup, lean_status, const struct lean_row*, struct lean_row*>,
              .open_cursor = &Thunk<&TableImpl::OpenCursor, struct lean_table_cursor*>,
              .close = &Destroy},
        table_(table),
        session_impl_(session_impl) {
  }

  ~TableImpl() = default;

  lean_status Insert(const struct lean_row* row);
  lean_status Remove(const struct lean_row* row);
  lean_status Lookup(const struct lean_row* key_row, struct lean_row* out_row);
  struct lean_table_cursor* OpenCursor();

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_table* base, Args... args) {
    auto* impl = reinterpret_cast<TableImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

private:
  lean_table base_;
  Table* table_;
  SessionImpl* session_impl_;
  std::string lookup_buffer_;
};

class TableCursorImpl {
public:
  static struct lean_table_cursor* Create(Table* table, SessionImpl* session_impl);

  static void Destroy(struct lean_table_cursor* cursor) {
    delete reinterpret_cast<TableCursorImpl*>(cursor);
  }

private:
  explicit TableCursorImpl(SessionImpl* session_impl)
      : base_{.seek_to_first = &Thunk<&TableCursorImpl::SeekToFirst, bool>,
              .seek_to_first_ge = &Thunk<&TableCursorImpl::SeekToFirstGe, bool, const lean_row*>,
              .next = &Thunk<&TableCursorImpl::Next, bool>,
              .seek_to_last = &Thunk<&TableCursorImpl::SeekToLast, bool>,
              .seek_to_last_le = &Thunk<&TableCursorImpl::SeekToLastLe, bool, const lean_row*>,
              .prev = &Thunk<&TableCursorImpl::Prev, bool>,
              .is_valid = &Thunk<&TableCursorImpl::IsValid, bool>,
              .current_row = &Thunk<&TableCursorImpl::CurrentRow, void, lean_row*>,
              .remove_current = &Thunk<&TableCursorImpl::RemoveCurrent, lean_status>,
              .update_current =
                  &Thunk<&TableCursorImpl::UpdateCurrent, lean_status, const lean_row*>,
              .close = &Destroy},
        session_impl_(session_impl) {
  }

  ~TableCursorImpl();

  bool SeekToFirst();
  bool SeekToFirstGe(const lean_row* key_row);
  bool Next();
  bool SeekToLast();
  bool SeekToLastLe(const lean_row* key_row);
  bool Prev();
  bool IsValid();
  void CurrentRow(lean_row* row);
  lean_status RemoveCurrent();
  lean_status UpdateCurrent(const lean_row* row);

  template <auto Method, typename Ret, typename... Args>
  static Ret Thunk(struct lean_table_cursor* base, Args... args) {
    auto* impl = reinterpret_cast<TableCursorImpl*>(base);
    return (impl->*Method)(std::forward<Args>(args)...);
  }

private:
  lean_table_cursor base_;
  SessionImpl* session_impl_;
  std::unique_ptr<TableCursor> cursor_;
};

} // namespace leanstore
