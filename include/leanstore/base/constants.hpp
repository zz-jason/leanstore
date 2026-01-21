#pragma once

#include "leanstore/c/types.h"

#include <limits>

constexpr lean_cmdid_t kCmdRemoveMark = 1U << 31;
constexpr lean_cmdid_t kCmdInvalid = std::numeric_limits<lean_cmdid_t>::max();
