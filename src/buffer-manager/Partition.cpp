#include "leanstore/buffer-manager/Partition.hpp"

#include <cstring>

#include <sys/mman.h>

namespace leanstore {
namespace storage {

void* MallocHuge(size_t size) {
  void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  memset(p, 0, size);
  return p;
}

HashTable::Entry::Entry(PID key) : mKey(key) {
}

HashTable::HashTable(uint64_t sizeInBits) {
  uint64_t size = (1ull << sizeInBits);
  mMask = size - 1;
  mEntries = (Entry**)MallocHuge(size * sizeof(Entry*));
}

uint64_t HashTable::HashKey(PID k) {
  // MurmurHash64A
  const uint64_t m = 0xc6a4a7935bd1e995ull;
  const int r = 47;
  uint64_t h = 0x8445d61a4e774912ull ^ (8 * m);
  k *= m;
  k ^= k >> r;
  k *= m;
  h ^= k;
  h *= m;
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

IOFrame& HashTable::Insert(PID key) {
  auto* e = new Entry(key);
  uint64_t pos = HashKey(key) & mMask;
  e->mNext = mEntries[pos];
  mEntries[pos] = e;
  return e->mValue;
}

HashTable::Handler HashTable::Lookup(PID key) {
  uint64_t pos = HashKey(key) & mMask;
  Entry** ePtr = mEntries + pos;
  Entry* e = *ePtr; // e is only here for readability
  while (e) {
    if (e->mKey == key)
      return {ePtr};
    ePtr = &(e->mNext);
    e = e->mNext;
  }
  return {nullptr};
}

void HashTable::Remove(HashTable::Handler& handler) {
  Entry* toDelete = *handler.mHolder;
  *handler.mHolder = (*handler.mHolder)->mNext;
  delete toDelete;
}

void HashTable::Remove(uint64_t key) {
  auto handler = Lookup(key);
  assert(handler);
  Remove(handler);
}

bool HashTable::Has(uint64_t key) {
  uint64_t pos = HashKey(key) & mMask;
  auto* e = mEntries[pos];
  while (e) {
    if (e->mKey == key)
      return true;
    e = e->mNext;
  }
  return false;
}

} // namespace storage
} // namespace leanstore
