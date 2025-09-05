#pragma once

#include "leanstore/common/types.h"

#include <limits>
#include <string>
#include <unordered_map>

constexpr lean_cmdid_t kRemoveCommandMark = 1u << 31;
constexpr lean_cmdid_t kInvalidCommandid = std::numeric_limits<lean_cmdid_t>::max();

using StringMap = std::unordered_map<std::string, std::string>;
