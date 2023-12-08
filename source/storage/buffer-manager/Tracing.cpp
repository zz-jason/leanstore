#include "Tracing.hpp"

namespace leanstore {
namespace storage {

std::mutex Tracing::mutex;
std::unordered_map<PID, std::tuple<TREEID, u64>> Tracing::ht;

} // namespace storage
} // namespace leanstore
