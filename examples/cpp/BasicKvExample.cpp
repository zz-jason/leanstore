#include <leanstore-c/StoreOption.h>
#include <leanstore/LeanStore.hpp>
#include <leanstore/btree/BasicKV.hpp>
#include <leanstore/concurrency/Worker.hpp>

#include <iostream>
#include <memory>

using leanstore::LeanStore;

int main() {
  // create store option
  StoreOption* option = CreateStoreOption("/tmp/leanstore/examples/BasicKvExample");
  option->mCreateFromScratch = true;
  option->mWorkerThreads = 2;
  option->mEnableBulkInsert = false;
  option->mEnableEagerGc = true;

  // create store
  auto res = LeanStore::Open(option);
  if (!res) {
    DestroyStoreOption(option);
    std::cerr << "open store failed: " << res.error().ToString() << std::endl;
    return 1;
  }
  std::unique_ptr<LeanStore> store = std::move(res.value());

  // create btree
  std::string btreeName = "testTree1";
  leanstore::storage::btree::BasicKV* btree = nullptr;
  store->ExecSync(0, [&]() {
    auto res = store->CreateBasicKV(btreeName);
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
    std::string copiedValue;
    std::string key("hello");
    auto copyValueOut = [&](leanstore::Slice val) { copiedValue = val.ToString(); };
    auto ret = btree->Lookup(key, copyValueOut);
    if (ret != leanstore::OpCode::kOK) {
      std::cerr << "lookup value failed: " << leanstore::ToString(ret) << std::endl;
    }
    std::cout << key << ", " << copiedValue << std::endl;
  });

  return 0;
}