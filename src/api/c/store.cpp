
#include "api/c/store_impl.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/common/types.h"
#include "leanstore/lean_store.hpp"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <utility>

lean_status lean_open_store(struct lean_store_option* option, struct lean_store** store) {
  auto res = leanstore::LeanStore::Open(option);
  if (!res) {
    std::cerr << "open store failed: " << res.error().ToString() << std::endl;
    return lean_status::LEAN_ERR_OPEN_STORE;
    *store = nullptr;
  }

  *store = leanstore::StoreImpl::Create(std::move(res.value()));
  return lean_status::LEAN_STATUS_OK;
}