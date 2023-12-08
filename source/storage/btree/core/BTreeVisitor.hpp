#pragma once

using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace btree {

class BTreeNode;

class BTreeVisitor {
public:
  virtual void Visit(BTreeNode* node) = 0;
};

} // namespace btree
} // namespace storage
} // namespace leanstore