#pragma once

#include "utils/Error.hpp"

#include <expected>

namespace leanstore {

template <typename ResultType>
using Result = std::expected<ResultType, utils::Error>;

} // namespace leanstore