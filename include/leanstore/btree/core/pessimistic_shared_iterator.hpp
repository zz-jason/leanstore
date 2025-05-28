#pragma once

#include "pessimistic_iterator.hpp"

namespace leanstore::storage::btree {

class PessimisticSharedIterator : public PessimisticIterator {
public:
  PessimisticSharedIterator(BTreeGeneric& btree)
      : PessimisticIterator(btree, LatchMode::kPessimisticShared) {
  }
};

} // namespace leanstore::storage::btree
