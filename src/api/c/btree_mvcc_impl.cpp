#include "api/c/btree_mvcc_impl.hpp"

#include "api/c/cursor_mvcc_impl.hpp"
#include "api/c/tx_guard.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/coro/coro_scheduler.hpp"

namespace leanstore {

lean_status BTreeMvccImpl::Insert(lean_str_view key, lean_str_view value) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = (lean_status)btree_->Insert({key.data, key.size}, {value.data, value.size});
  });
  return status;
}

lean_status BTreeMvccImpl::Remove(lean_str_view key) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = (lean_status)btree_->Remove({key.data, key.size});
  });
  return status;
}

lean_status BTreeMvccImpl::Lookup(lean_str_view key, lean_str* value) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->ExecSync([&]() {
    TxGuard tx_guard(status);
    status = (lean_status)btree_->Lookup({key.data, key.size}, [&](Slice val) {
      lean_str_assign(value, (const char*)val.data(), val.size());
    });
  });
  return status;
}

struct lean_cursor* BTreeMvccImpl::OpenCursor() {
  return CursorMvccImpl::Create(btree_, session_impl_);
}

} // namespace leanstore