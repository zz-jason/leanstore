#include "leanstore/buffer-manager/partition.hpp"

#include "leanstore/units.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>

#include <fcntl.h>
#include <linux/perf_event.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

namespace leanstore {
namespace storage {

static void* MallocHuge(size_t size) {
  void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  memset(p, 0, size);
  return p;
}

HashTable::Entry::Entry(PID key) : key_(key) {
}

HashTable::HashTable(uint64_t size_in_bits) {
  uint64_t size = (1ull << size_in_bits);
  mask_ = size - 1;
  entries_ = (Entry**)MallocHuge(size * sizeof(Entry*));
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
  uint64_t pos = HashKey(key) & mask_;
  e->next_ = entries_[pos];
  entries_[pos] = e;
  return e->value_;
}

HashTable::Handler HashTable::Lookup(PID key) {
  uint64_t pos = HashKey(key) & mask_;
  Entry** e_ptr = entries_ + pos;
  Entry* e = *e_ptr; // e is only here for readability
  while (e) {
    if (e->key_ == key)
      return {e_ptr};
    e_ptr = &(e->next_);
    e = e->next_;
  }
  return {nullptr};
}

void HashTable::Remove(HashTable::Handler& handler) {
  Entry* to_delete = *handler.holder_;
  *handler.holder_ = (*handler.holder_)->next_;
  delete to_delete;
}

void HashTable::Remove(uint64_t key) {
  auto handler = Lookup(key);
  assert(handler);
  Remove(handler);
}

bool HashTable::Has(uint64_t key) {
  uint64_t pos = HashKey(key) & mask_;
  auto* e = entries_[pos];
  while (e) {
    if (e->key_ == key)
      return true;
    e = e->next_;
  }
  return false;
}

} // namespace storage
} // namespace leanstore
