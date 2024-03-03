#include "shared-headers/Units.hpp"

#include <functional>
#include <list>
#include <memory>

namespace leanstore {
namespace utils {

// Variable-length singel-threaded ring buffer
class RingBufferST {
private:
  struct Entry {
    uint8_t is_cr : 1;
    uint64_t length : 63;
    uint8_t payload[];
  };

  static_assert(sizeof(Entry) == sizeof(uint64_t), "");

  const uint64_t buffer_size;
  std::unique_ptr<uint8_t[]> buffer;
  uint64_t write_cursor = 0;
  uint64_t read_cursor = 0;

  uint64_t continguousFreeSpace() {
    return (read_cursor > write_cursor) ? read_cursor - write_cursor
                                        : buffer_size - write_cursor;
  }

  uint64_t freeSpace() {
    // A , B , C : a - b + c % c
    if (write_cursor == read_cursor) {
      return buffer_size;
    }
    if (read_cursor < write_cursor) {
      return read_cursor + (buffer_size - write_cursor);
    }
    return read_cursor - write_cursor;
  }

  void carriageReturn() {
    reinterpret_cast<Entry*>(buffer.get() + write_cursor)->is_cr = 1;
    write_cursor = 0;
  }

public:
  RingBufferST(uint64_t size) : buffer_size(size) {
    buffer = std::make_unique<uint8_t[]>(buffer_size);
  }

  bool canInsert(uint64_t payload_size) {
    const uint64_t total_size = payload_size + sizeof(Entry);
    const uint64_t contiguous_free_space = continguousFreeSpace();
    if (contiguous_free_space >= total_size) {
      return true;
    } else {
      const uint64_t free_space = freeSpace();
      const uint64_t free_space_beginning = free_space - contiguous_free_space;
      return free_space_beginning >= total_size;
    }
  }

  uint8_t* pushBack(uint64_t payload_length) {
    ENSURE(canInsert(payload_length));
    const uint64_t total_size = payload_length + sizeof(Entry);
    const uint64_t contiguous_free_space = continguousFreeSpace();
    if (contiguous_free_space < total_size) {
      carriageReturn();
    }

    auto& entry = *reinterpret_cast<Entry*>(buffer.get() + write_cursor);
    write_cursor += total_size;
    entry.is_cr = 0;
    entry.length = payload_length;
    return entry.payload;
  }
  void iterateUntilTail(uint8_t* start_payload_ptr,
                        std::function<void(uint8_t* entry)> cb) {
    cb(start_payload_ptr);
    start_payload_ptr -= sizeof(Entry);
    uint64_t cursor = start_payload_ptr - buffer.get();
    while (cursor != write_cursor) {
      auto entry = reinterpret_cast<Entry*>(buffer.get() + cursor);
      if (entry->is_cr) {
        cursor = 0;
        entry = reinterpret_cast<Entry*>(buffer.get());
      } else {
        cb(entry->payload);
        cursor += entry->length + sizeof(Entry);
      }
    }
  }
  bool empty() {
    return write_cursor == read_cursor;
  }
  uint8_t* front() {
    auto entry = reinterpret_cast<Entry*>(buffer.get() + read_cursor);
    if (entry->is_cr) {
      read_cursor = 0;
      entry = reinterpret_cast<Entry*>(buffer.get());
    }
    return entry->payload;
  }
  void popFront() {
    read_cursor +=
        reinterpret_cast<Entry*>(buffer.get() + read_cursor)->length +
        sizeof(Entry);
  }
};
// -------------------------------------------------------------------------------------
class FRingBufferST {
private:
  std::list<std::unique_ptr<uint8_t[]>> entries;
  std::list<std::unique_ptr<uint8_t[]>>::iterator iter;

public:
  FRingBufferST(uint64_t) {
    iter = entries.end();
  }
  bool canInsert(uint64_t) {
    return true;
  }
  uint8_t* pushBack(uint64_t payload_length) {
    entries.emplace_back(std::make_unique<uint8_t[]>(payload_length));
    if (iter == entries.end()) {
      iter = std::prev(entries.end());
    }
    return entries.back().get();
  }
  void iterateUntilTail(uint8_t*, std::function<void(uint8_t* entry)> cb) {
    while (iter != entries.end()) {
      cb((*iter).get());
      iter++;
    }
    iter = entries.end();
  }
  bool empty() {
    return entries.empty();
  }
  uint8_t* front() {
    return entries.front().get();
  }
  void popFront() {
    entries.pop_front();
  }
};
// -------------------------------------------------------------------------------------
} // namespace utils
} // namespace leanstore
