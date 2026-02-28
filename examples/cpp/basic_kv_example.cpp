#include <leanstore/c/types.h>
#include <leanstore/lean_store.hpp>

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

  auto session = store->Connect(0);
  auto create_res = session.CreateBTree("testTree1", lean_btree_type::LEAN_BTREE_TYPE_ATOMIC);
  if (!create_res) {
    std::cerr << "create btree failed: " << create_res.error().ToString() << std::endl;
    return 1;
  }
  auto btree = std::move(create_res.value());

  if (auto insert_res = btree.Insert("hello", "world"); !insert_res) {
    std::cerr << "insert value failed: " << insert_res.error().ToString() << std::endl;
    return 1;
  }

  auto lookup_res = btree.Lookup("hello");
  if (!lookup_res) {
    std::cerr << "lookup value failed: " << lookup_res.error().ToString() << std::endl;
    return 1;
  }
  std::string copied_value(reinterpret_cast<const char*>(lookup_res->data()), lookup_res->size());
  std::cout << "hello, " << copied_value << std::endl;

  return 0;
}
