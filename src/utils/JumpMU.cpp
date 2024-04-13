#include "JumpMU.hpp"

#include "utils/Log.hpp"

namespace jumpmu {

__thread int tlsNumJumpPoints = 0;
__thread jmp_buf tlsJumpPoints[JUMPMU_STACK_SIZE];
__thread int tlsJumpPointNumStackObjs[JUMPMU_STACK_SIZE];

__thread int tlsNumStackObjs = 0;
__thread void* tlsObjs[JUMPMU_STACK_OBJECTS_LIMIT];
__thread void (*tlsObjDtors[JUMPMU_STACK_OBJECTS_LIMIT])(void*);

void Jump() {
  leanstore::Log::DebugCheck(tlsNumJumpPoints > 0, "tlsNumJumpPoints={}",
                             tlsNumJumpPoints);
  leanstore::Log::DebugCheck(tlsNumStackObjs >= 0, "tlsNumStackObjs={}",
                             tlsNumStackObjs);
  auto numJumpStackObjs = tlsJumpPointNumStackObjs[tlsNumJumpPoints - 1];

  // Release resource hold by stack objects in reverse (FILO) order.
  if (numJumpStackObjs < tlsNumStackObjs) {
    int first = numJumpStackObjs;
    int last = tlsNumStackObjs - 1;
    for (int i = last; i >= first; i--) {
      tlsObjDtors[i](tlsObjs[i]);
    }
  }

  // Jump to the preset jump point
  auto& jumpPoint = jumpmu::tlsJumpPoints[jumpmu::tlsNumJumpPoints - 1];
  leanstore::Log::Debug(
      "Jump to jump point {} ({} stack objects, {} jump stack objects)",
      jumpmu::tlsNumJumpPoints - 1, jumpmu::tlsNumStackObjs,
      jumpmu::tlsJumpPointNumStackObjs[jumpmu::tlsNumJumpPoints - 1]);
  tlsNumJumpPoints--;
  longjmp(jumpPoint, 1);
}

} // namespace jumpmu
