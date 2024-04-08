#pragma once

#include "utils/Error.hpp"

#include <expected>

namespace leanstore {

template <typename ResultType>
using Result = std::expected<ResultType, utils::Error>;

// template <typename ResultType>
// class [[nodiscard]] Result : public std::expected<ResultType, utils::Error> {
// public:
//   //  Result(const ResultType result)
//   //      : std::expected<ResultType, utils::Error>(std::move(result)) {
//   //  }
//
//   //  Result(const ResultType& result)
//   //      : std::expected<ResultType, utils::Error>(std::move(result)) {
//   //  }
//
//   Result(ResultType&& result)
//       : std::expected<ResultType, utils::Error>(std::move(result)) {
//   }
//
//   Result(utils::Error&& error)
//       : std::expected<ResultType, utils::Error>(
//             std::unexpected<utils::Error>(std::move(error))) {
//   }
//
//   Result(std::expected<ResultType, utils::Error>&& expected)
//       : std::expected<ResultType, utils::Error>(expected) {
//   }
//
//   Result(std::unexpected<utils::Error>&& unexpected)
//       : std::expected<ResultType, utils::Error>(unexpected) {
//   }
// };

} // namespace leanstore