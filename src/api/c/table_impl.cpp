#include "api/c/table_impl.hpp"

#include "api/c/tx_guard.hpp"
#include "leanstore/c/leanstore.h"

namespace {
lean_status ToStatus(leanstore::OpCode code) {
  switch (code) {
  case leanstore::OpCode::kOK:
    return lean_status::LEAN_STATUS_OK;
  case leanstore::OpCode::kNotFound:
    return lean_status::LEAN_ERR_NOT_FOUND;
  case leanstore::OpCode::kDuplicated:
    return lean_status::LEAN_ERR_DUPLICATED;
  case leanstore::OpCode::kAbortTx:
    return lean_status::LEAN_ERR_CONFLICT;
  case leanstore::OpCode::kSpaceNotEnough:
    return lean_status::LEAN_ERR_UNSUPPORTED;
  case leanstore::OpCode::kOther:
    return lean_status::LEAN_ERR_UNSUPPORTED;
  }
  return lean_status::LEAN_ERR_UNSUPPORTED;
}
} // namespace

namespace leanstore {
lean_status TableImpl::Insert(const struct lean_row* row) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = ToStatus(table_->Insert(row));
  });
  return status;
}

lean_status TableImpl::Remove(const struct lean_row* row) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = ToStatus(table_->Remove(row));
  });
  return status;
}

lean_status TableImpl::Lookup(const struct lean_row* key_row, struct lean_row* out_row) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = ToStatus(table_->Lookup(key_row, out_row, lookup_buffer_));
  });
  return status;
}

struct lean_table_cursor* TableImpl::OpenCursor() {
  return TableCursorImpl::Create(table_, session_impl_);
}

struct lean_table_cursor* TableCursorImpl::Create(Table* table, SessionImpl* session_impl) {
  auto* impl = new TableCursorImpl(session_impl);
  impl->cursor_ = table->NewCursor();
  assert(impl->cursor_ != nullptr);
  return &impl->base_;
}

TableCursorImpl::~TableCursorImpl() {
  cursor_.reset();
}

bool TableCursorImpl::SeekToFirst() {
  bool result = false;
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    result = cursor_->SeekToFirst();
  });
  return result;
}

bool TableCursorImpl::SeekToFirstGe(const lean_row* key_row) {
  bool result = false;
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    result = cursor_->SeekToFirstGreaterEqual(key_row);
  });
  return result;
}

bool TableCursorImpl::Next() {
  bool result = false;
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    result = cursor_->Next();
  });
  return result;
}

bool TableCursorImpl::SeekToLast() {
  bool result = false;
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    result = cursor_->SeekToLast();
  });
  return result;
}

bool TableCursorImpl::SeekToLastLe(const lean_row* key_row) {
  bool result = false;
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    result = cursor_->SeekToLastLessEqual(key_row);
  });
  return result;
}

bool TableCursorImpl::Prev() {
  bool result = false;
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    result = cursor_->Prev();
  });
  return result;
}

bool TableCursorImpl::IsValid() {
  return cursor_->IsValid();
}

void TableCursorImpl::CurrentRow(lean_row* row) {
  if (!row || !cursor_ || !cursor_->IsValid()) {
    return;
  }
  auto res = cursor_->CurrentRow(row);
  if (!res) {
    return;
  }
}

lean_status TableCursorImpl::RemoveCurrent() {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = ToStatus(cursor_->RemoveCurrent());
  });
  return status;
}

lean_status TableCursorImpl::UpdateCurrent(const lean_row* row) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = ToStatus(cursor_->UpdateCurrent(row));
  });
  return status;
}

} // namespace leanstore
