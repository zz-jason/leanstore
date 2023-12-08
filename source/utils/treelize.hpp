#pragma once

#include <memory>
#include <string>
#include <vector>

namespace treelize {

class TreeNode {
public:
  static const std::string sPrefix4Child;
  static const std::string sPrefix4LastChild;
  static const std::string sTreeBodyForChild;
  static const std::string sTreeBodyForLastChild;

public:
  TreeNode(const std::string& content) : mContent(content) {
  }
  ~TreeNode() = default;

  void AddChild(std::unique_ptr<TreeNode> child) {
    mChildren.push_back(std::move(child));
  }

  void ToTree(std::vector<std::string>& result);

private:
  void ToTreeInternal(const std::string& prefix, bool isLastChild,
                      std::vector<std::string>& result);

private:
  std::string mContent;
  std::vector<std::unique_ptr<TreeNode>> mChildren;
};

} // namespace treelize