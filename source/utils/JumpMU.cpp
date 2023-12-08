#include "JumpMU.hpp"

#include <signal.h>

namespace jumpmu {

__thread int tlsNumJumpPoints = 0;
__thread jmp_buf tlsJumpPoints[JUMPMU_STACK_SIZE];
__thread int tlsJumpPointNumStackObjs[JUMPMU_STACK_SIZE];

__thread int tlsNumStackObjs = 0;
__thread void* tlsObjs[JUMPMU_STACK_OBJECTS_LIMIT];
__thread void (*tlsObjDtors[JUMPMU_STACK_OBJECTS_LIMIT])(void*);

void jump() {
  assert(tlsNumJumpPoints > 0);
  assert(tlsNumStackObjs >= 0);

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
  tlsNumJumpPoints--;
  longjmp(jumpPoint, 1);
}

} // namespace jumpmu
