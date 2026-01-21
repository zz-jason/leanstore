#include <leanstore/lean_store.hpp>
#include <leanstore/btree/basic_kv.hpp>
#include <leanstore/c/types.h>
#include <leanstore/tx/tx_manager.hpp>

#include <iostream>
#include <memory>

using leanstore::LeanStore;

int main() {
  // create store option
  lean_store_option* option = lean_store_option_create("/tmp/leanstore/examples/BasicKvExample");
  option->create_from_scratch_ = true;
  option->worker_threads_ = 2;
  option->enable_bulk_insert_ = false;
  option->enable_eager_gc_ = true;

  // create store
  auto res = LeanStore::Open(option);
  if (!res) {
    lean_store_option_destroy(option);
    std::cerr << "open store failed: " << res.error().ToString() << std::endl;
    return 1;
  }
  std::unique_ptr<LeanStore> store = std::move(res.value());

  // create btree
  std::string btree_name = "testTree1";
  leanstore::BasicKV* btree = nullptr;
  store->ExecSync(0, [&]() {
    auto res = store->CreateBasicKv(btree_name);
    if (!res) {
      std::cerr << "create btree failed: " << res.error().ToString() << std::endl;
    }
    btree = res.value();
  });

  // insert value
  store->ExecSync(0, [&]() {
    std::string key("hello"), val("world");
    auto ret = btree->Insert(key, val);
    if (ret != leanstore::OpCode::kOK) {
      std::cerr << "insert value failed: " << leanstore::ToString(ret) << std::endl;
    }
  });

  // lookup value
  store->ExecSync(0, [&]() {
    std::string copied_value;
    std::string key("hello");
    auto copy_value_out = [&](leanstore::Slice val) { copied_value = val.ToString(); };
    auto ret = btree->Lookup(key, copy_value_out);
    if (ret != leanstore::OpCode::kOK) {
      std::cerr << "lookup value failed: " << leanstore::ToString(ret) << std::endl;
    }
    std::cout << key << ", " << copied_value << std::endl;
  });

  return 0;
}