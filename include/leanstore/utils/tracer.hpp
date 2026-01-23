#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <stack>
#include <string>
#include <vector>

namespace leanstore {

class Tracer {
public:
  class TraceNode;
  class TraceCtx;
  class TraceSpan;

  static std::unique_ptr<TraceSpan> Span(std::string_view name);
};

#define LEAN_SCOPED_TRACE_IMPL_MPL(LINE) span_##LINE
#define LEAN_SCOPED_TRACE_IMPL(LINE) LEAN_SCOPED_TRACE_IMPL_MPL(LINE)
#define LEAN_SCOPED_TRACE(name)                                                                    \
  auto LEAN_SCOPED_TRACE_IMPL(__LINE__) = leanstore::Tracer::Span(#name);

class Tracer::TraceNode {
public:
  TraceNode(std::string_view name)
      : name_(name),
        start_(std::chrono::steady_clock::now()),
        elapsed_ns_(0),
        children_() {
  }

  TraceNode(const TraceNode&) = delete;                      // no copy constructor
  TraceNode& operator=(const TraceNode&) = delete;           // no copy assignment
  TraceNode& operator=(TraceNode&& other) noexcept = delete; // no move assignment

  // move constructor
  TraceNode(TraceNode&& other) noexcept
      : name_(std::move(other.name_)),
        start_(other.start_),
        elapsed_ns_(other.elapsed_ns_),
        children_(std::move(other.children_)) {
    other.elapsed_ns_ = 0;
  }

  void AddChild(std::unique_ptr<TraceNode>&& child) {
    children_.emplace_back(std::move(child));
  }

  void End() {
    auto end = std::chrono::steady_clock::now();
    elapsed_ns_ = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count();
  }

private:
  const std::string name_;
  const std::chrono::steady_clock::time_point start_;

  uint64_t elapsed_ns_;
  std::vector<std::unique_ptr<TraceNode>> children_;
};

class Tracer::TraceCtx {
public:
  inline static thread_local std::unique_ptr<TraceNode> s_root = nullptr;
  inline static thread_local std::stack<TraceNode*> s_stack = {};

  static void Push(std::unique_ptr<TraceNode>&& node) {
    auto* node_ptr = node.get();
    if (s_stack.empty()) {
      s_root = std::move(node);
    } else {
      s_stack.top()->AddChild(std::move(node));
    }
    s_stack.push(node_ptr);
  }

  static void Pop() {
    if (!s_stack.empty()) {
      s_stack.pop();
    }
  }
};

class Tracer::TraceSpan {
public:
  TraceSpan(std::string_view name) {
    auto node = std::make_unique<TraceNode>(name);
    node_ = node.get();
    TraceCtx::Push(std::move(node));
  }

  ~TraceSpan() {
    node_->End();
    TraceCtx::Pop();
  }

private:
  TraceNode* node_;
};

inline std::unique_ptr<Tracer::TraceSpan> Span(std::string_view name) {
  LEAN_SCOPED_TRACE(hehe);
  return std::make_unique<Tracer::TraceSpan>(name);
}

} // namespace leanstore