#pragma once

#include "leanstore/utils/Error.hpp"

#include <expected>

namespace leanstore {

template <typename ResultType>
using Result = std::expected<ResultType, utils::Error>;

} // namespace leanstore