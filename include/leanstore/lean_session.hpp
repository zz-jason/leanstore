#pragma once

#include "leanstore/base/log.hpp"
#include "leanstore/base/optional.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/base/slice.hpp"
#include "leanstore/c/types.h"
#include "leanstore/coro/coro_session.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace leanstore {

// Forward declarations
class LeanStore;
class LeanBTree;
class LeanCursor;

/// LeanSession represents a database session for coroutine mode.
/// It provides transaction management and B-tree operations.
class LeanSession {
public:
  /// Constructor: internal use only, called by LeanStore::Connect.
  LeanSession(LeanStore* store, CoroSession* session);

  LeanSession(const LeanSession&) = delete;
  auto operator=(const LeanSession&) -> LeanSession& = delete;
  LeanSession(LeanSession&& other) noexcept;
  auto operator=(LeanSession&& other) noexcept -> LeanSession&;

  /// Destructor automatically closes the session.
  ~LeanSession();

  /// Start a transaction (only meaningful for TransactionKV mode).
  void StartTx();

  /// Commit the current transaction.
  void CommitTx();

  /// Abort the current transaction.
  void AbortTx();

  /// Create a new B-tree with the given name and type.
  /// @param name Unique name for the B-tree.
  /// @param type B-tree type (basic or MVCC).
  /// @param config Optional configuration parameters.
  /// @return Result containing the created B-tree handle.
  Result<LeanBTree> CreateBTree(const std::string& name, lean_btree_type type,
                                lean_btree_config config = {});

  /// Drop (delete) a B-tree by name.
  void DropBTree(const std::string& name);

  /// Get an existing B-tree by name.
  /// @return Result containing the B-tree handle or error if not found.
  Result<LeanBTree> GetBTree(const std::string& name);

  /// Close the session and release resources.
  void Close();

  template <typename F>
  auto ExecSync(F&& fn) -> std::invoke_result_t<F> {
    using ResultType = std::invoke_result_t<F>;
    LEAN_DCHECK(session_ != nullptr, "Session is closed");
    if constexpr (std::is_void_v<ResultType>) {
      ExecSyncVoid([&]() { fn(); });
    } else {
      Optional<ResultType> result;
      ExecSyncVoid([&]() { result = fn(); });
      LEAN_DCHECK(result.has_value(), "ExecSync result should be set");
      return std::move(result.value());
    }
  }

private:
  friend class LeanBTree;
  friend class LeanCursor;

  void ExecSyncVoid(std::function<void()> fn);

  LeanStore* store_;
  CoroSession* session_;
};

} // namespace leanstore
