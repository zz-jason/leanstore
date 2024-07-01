#pragma once

#include "BTreePessimisticIterator.hpp"

namespace leanstore {
namespace storage {
namespace btree {

class BTreePessimisticSharedIterator : public BTreePessimisticIterator {
public:
  BTreePessimisticSharedIterator(BTreeGeneric& btree)
      : BTreePessimisticIterator(btree, LatchMode::kPessimisticShared) {
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
