#pragma once

#include "leanstore/common/types.h"

#include <limits>

constexpr lean_cmdid_t kCmdRemoveMark = 1u << 31;
constexpr lean_cmdid_t kCmdInvalid = std::numeric_limits<lean_cmdid_t>::max();
