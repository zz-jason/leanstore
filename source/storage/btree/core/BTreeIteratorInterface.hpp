#pragma once

#include "KVInterface.hpp"

#include <functional>

namespace leanstore {
namespace storage {
namespace btree {

class BTreeIteratorInterface {
public:
  virtual OpCode seek(Slice key) = 0;
  virtual OpCode seekForPrev(Slice key) = 0;
  virtual OpCode seekExact(Slice key) = 0;
  virtual OpCode next() = 0;
  virtual OpCode prev() = 0;
  virtual bool isKeyEqualTo(Slice key) = 0;
};

// Can jump
class BTreeOptimisticIteratorInterface : public BTreeIteratorInterface {
public:
  virtual void key(std::function<void(Slice key)> cb) = 0;
  virtual void KeyWithoutPrefix(std::function<void(Slice key)> cb) = 0;
  virtual void keyPrefix(std::function<void(Slice key)> cb) = 0;
  virtual void value(std::function<void(Slice key)> cb) = 0;
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
