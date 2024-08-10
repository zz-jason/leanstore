#pragma once

#include "PessimisticIterator.hpp"

namespace leanstore::storage::btree {

class PessimisticSharedIterator : public PessimisticIterator {
public:
  PessimisticSharedIterator(BTreeGeneric& btree)
      : PessimisticIterator(btree, LatchMode::kPessimisticShared) {
  }
};

} // namespace leanstore::storage::btree
