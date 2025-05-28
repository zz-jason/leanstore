#include "leanstore/utils/jump_mu.hpp"

#include "leanstore/utils/log.hpp"

namespace jumpmu {

__thread int tls_num_jump_points = 0;
__thread jmp_buf tls_jump_points[JUMPMU_STACK_SIZE];
__thread int tls_jump_point_num_stack_objs[JUMPMU_STACK_SIZE];

__thread int tls_num_stack_objs = 0;
__thread void* tls_objs[JUMPMU_STACK_OBJECTS_LIMIT];
__thread void (*tls_obj_dtors[JUMPMU_STACK_OBJECTS_LIMIT])(void*);

void Jump() {
  LS_DCHECK(tls_num_jump_points > 0, "tlsNumJumpPoints={}", tls_num_jump_points);
  LS_DCHECK(tls_num_stack_objs >= 0, "tlsNumStackObjs={}", tls_num_stack_objs);
  auto num_jump_stack_objs = tls_jump_point_num_stack_objs[tls_num_jump_points - 1];

  // Release resource hold by stack objects in reverse (FILO) order.
  if (num_jump_stack_objs < tls_num_stack_objs) {
    int first = num_jump_stack_objs;
    int last = tls_num_stack_objs - 1;
    for (int i = last; i >= first; i--) {
      tls_obj_dtors[i](tls_objs[i]);
    }
  }

  // Jump to the preset jump point
  auto& jump_point = jumpmu::tls_jump_points[jumpmu::tls_num_jump_points - 1];
  LS_DLOG("Jump to jump point {} ({} stack objects, {} jump stack objects)",
          jumpmu::tls_num_jump_points - 1, jumpmu::tls_num_stack_objs,
          jumpmu::tls_jump_point_num_stack_objs[jumpmu::tls_num_jump_points - 1]);
  tls_num_jump_points--;
  longjmp(jump_point, 1);
}

} // namespace jumpmu
