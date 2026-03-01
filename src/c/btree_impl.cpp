#include "c/btree_impl.hpp"

#include "c/cursor_impl.hpp"
#include "c/tx_guard.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/c/status.h"
#include "leanstore/coro/coro_scheduler.hpp"

namespace leanstore {

lean_status BTreeImpl::Insert(lean_str_view key, lean_str_view value) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->Session().ExecSync([&]() {
    TxGuard tx_guard(status);
    status = (lean_status)btree_->Insert({key.data, key.size}, {value.data, value.size});
  });
  return status;
}

lean_status BTreeImpl::Remove(lean_str_view key) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->Session().ExecSync([&]() {
    TxGuard tx_guard(status);
    status = (lean_status)btree_->Remove({key.data, key.size});
  });
  return status;
}

lean_status BTreeImpl::Lookup(lean_str_view key, lean_str* value) {
  lean_status status = lean_status::LEAN_STATUS_OK;
  session_impl_->Session().ExecSync([&]() {
    TxGuard tx_guard(status);
    status = (lean_status)btree_->Lookup({key.data, key.size}, [&](Slice val) {
      lean_str_assign(value, (const char*)val.data(), val.size());
    });
  });
  return status;
}

struct lean_cursor* BTreeImpl::OpenCursor() {
  return CursorImpl::Create(btree_, session_impl_);
}

} // namespace leanstore
