#include "DebugFlags.hpp"

namespace leanstore {
namespace utils {

std::unique_ptr<DebugFlagsRegistry> DebugFlagsRegistry::sInstance =
    std::make_unique<DebugFlagsRegistry>();

}
} // namespace leanstore