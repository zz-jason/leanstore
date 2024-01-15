#include "Partition.hpp"

#include <cstring>

#include <sys/mman.h>

namespace leanstore {
namespace storage {

void* MallocHuge(size_t size) {
  void* p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  memset(p, 0, size);
  return p;
}

HashTable::Entry::Entry(PID key) : key(key) {
}

HashTable::HashTable(u64 sizeInBits) {
  uint64_t size = (1ull << sizeInBits);
  mask = size - 1;
  entries = (Entry**)MallocHuge(size * sizeof(Entry*));
}

u64 HashTable::hashKey(PID k) {
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
  uint64_t pos = hashKey(key) & mask;
  e->next = entries[pos];
  entries[pos] = e;
  return e->value;
}

HashTable::Handler HashTable::Lookup(PID key) {
  uint64_t pos = hashKey(key) & mask;
  Entry** ePtr = entries + pos;
  Entry* e = *ePtr; // e is only here for readability
  while (e) {
    if (e->key == key)
      return {ePtr};
    ePtr = &(e->next);
    e = e->next;
  }
  return {nullptr};
}

void HashTable::Remove(HashTable::Handler& handler) {
  Entry* toDelete = *handler.holder;
  *handler.holder = (*handler.holder)->next;
  delete toDelete;
}

void HashTable::Remove(u64 key) {
  auto handler = Lookup(key);
  assert(handler);
  Remove(handler);
}

bool HashTable::has(u64 key) {
  uint64_t pos = hashKey(key) & mask;
  auto* e = entries[pos];
  while (e) {
    if (e->key == key)
      return true;
    e = e->next;
  }
  return false;
}

} // namespace storage
} // namespace leanstore
