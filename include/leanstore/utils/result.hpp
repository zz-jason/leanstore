#pragma once

#include "leanstore/utils/error.hpp"

#include <expected>

namespace leanstore {

template <typename ResultType>
using Result = std::expected<ResultType, utils::Error>;

} // namespace leanstore