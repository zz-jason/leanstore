#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

using TREEID = uint64_t; // data structure ID
using PID = uint64_t;    // page ID
using LID = uint64_t;    // log ID
using TXID = uint64_t;

using COMMANDID = uint32_t;
using WORKERID = uint16_t;

const COMMANDID kRemoveCommandMark = 1u << 31;

using StringMap = std::unordered_map<std::string, std::string>;

namespace leanstore {} // namespace leanstore
