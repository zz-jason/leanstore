#include "treelize.hpp"

#include <iostream>

namespace treelize {

const std::string TreeNode::sPrefix4Child = "├─";
const std::string TreeNode::sPrefix4LastChild = "└─";
const std::string TreeNode::sTreeBodyForChild = "│ ";
const std::string TreeNode::sTreeBodyForLastChild = "  ";

void TreeNode::ToTree(std::vector<std::string>& result) {
  result.push_back(mContent);
  for (size_t i = 0; i < mChildren.size(); ++i) {
    mChildren[i]->ToTreeInternal(sTreeBodyForLastChild,
                                 i == mChildren.size() - 1, result);
  }
}

void TreeNode::ToTreeInternal(const std::string& prefix, bool isLastChild,
                              std::vector<std::string>& result) {
  std::string curNode;
  std::string treeBody4Child;
  if (isLastChild) {
    curNode = prefix + sPrefix4LastChild + mContent;
    treeBody4Child = prefix + sTreeBodyForLastChild;
  } else {
    curNode = prefix + sPrefix4Child + mContent;
    treeBody4Child = prefix + sTreeBodyForChild;
  }

  result.push_back(curNode);
  for (size_t i = 0; i < mChildren.size(); ++i) {
    mChildren[i]->ToTreeInternal(treeBody4Child, i == mChildren.size() - 1,
                                 result);
  }
}

} // namespace treelize

// int main() {
//   auto root = std::make_unique<treelize::TreeNode>("root");
//
//   auto node1 = std::make_unique<treelize::TreeNode>("node-1");
//   auto node11 = std::make_unique<treelize::TreeNode>("node-1-1");
//   auto node12 = std::make_unique<treelize::TreeNode>("node-1-2");
//   auto node13 = std::make_unique<treelize::TreeNode>("node-1-3");
//   auto node14 = std::make_unique<treelize::TreeNode>("node-1-4");
//
//   node1->AddChild(std::move(node11));
//   node1->AddChild(std::move(node12));
//   node1->AddChild(std::move(node13));
//   node1->AddChild(std::move(node14));
//
//   auto node2 = std::make_unique<treelize::TreeNode>("node-2");
//   auto node21 = std::make_unique<treelize::TreeNode>("node-2-1");
//   auto node22 = std::make_unique<treelize::TreeNode>("node-2-2");
//   auto node23 = std::make_unique<treelize::TreeNode>("node-2-3");
//   auto node24 = std::make_unique<treelize::TreeNode>("node-2-4");
//
//   node2->AddChild(std::move(node21));
//   node2->AddChild(std::move(node22));
//   node2->AddChild(std::move(node23));
//   node2->AddChild(std::move(node24));
//
//   auto node3 = std::make_unique<treelize::TreeNode>("node-3");
//   auto node31 = std::make_unique<treelize::TreeNode>("node-3-1");
//   auto node32 = std::make_unique<treelize::TreeNode>("node-3-2");
//   auto node33 = std::make_unique<treelize::TreeNode>("node-3-3");
//   auto node34 = std::make_unique<treelize::TreeNode>("node-3-4");
//
//   node3->AddChild(std::move(node31));
//   node3->AddChild(std::move(node32));
//   node3->AddChild(std::move(node33));
//   node3->AddChild(std::move(node34));
//
//   root->AddChild(std::move(node1));
//   root->AddChild(std::move(node2));
//   root->AddChild(std::move(node3));
//
//   std::vector<std::string> result;
//   root->ToTree(result);
//
//   for (auto it = result.begin(); it != result.end(); ++it) {
//     std::cout << *it << std::endl;
//   }
//
//   return 0;
// }