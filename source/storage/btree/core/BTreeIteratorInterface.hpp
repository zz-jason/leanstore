#pragma once

#include "KVInterface.hpp"

#include <functional>

namespace leanstore {
namespace storage {
namespace btree {

class BTreeIteratorInterface {
public:
  virtual bool Seek(Slice key) = 0;
  virtual bool SeekForPrev(Slice key) = 0;
  virtual bool SeekExact(Slice key) = 0;
  virtual bool Next() = 0;
  virtual bool Prev() = 0;
};

class BTreePessimisticIteratorInterface : public BTreeIteratorInterface {
public:
  virtual Slice key() = 0;
  virtual Slice KeyWithoutPrefix() = 0;
  virtual Slice value() = 0;
};

} // namespace btree

} // namespace storage
} // namespace leanstore
