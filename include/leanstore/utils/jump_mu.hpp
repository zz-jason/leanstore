#pragma once

#include <cassert>
#include <utility>

#include <setjmp.h>

#define JUMPMU_STACK_SIZE 100
#define JUMPMU_STACK_OBJECTS_LIMIT 100

namespace jumpmu {

extern __thread int tls_num_jump_points;
extern __thread jmp_buf tls_jump_points[JUMPMU_STACK_SIZE];
extern __thread int tls_jump_point_num_stack_objs[JUMPMU_STACK_SIZE];

extern __thread int tls_num_stack_objs;
extern __thread void* tls_objs[JUMPMU_STACK_OBJECTS_LIMIT];
extern __thread void (*tls_obj_dtors[JUMPMU_STACK_OBJECTS_LIMIT])(void*);

void Jump();

inline void PopBackDestructor() {
  assert(tls_num_stack_objs > 0);

  tls_objs[tls_num_stack_objs - 1] = nullptr;
  tls_obj_dtors[tls_num_stack_objs - 1] = nullptr;
  tls_num_stack_objs--;
}

inline void PushBackDesctructor(void* obj, void (*dtor)(void*)) {
  assert(tls_num_stack_objs < JUMPMU_STACK_SIZE);
  assert(obj != nullptr);
  assert(dtor != nullptr);

  tls_obj_dtors[tls_num_stack_objs] = dtor;
  tls_objs[tls_num_stack_objs] = obj;
  tls_num_stack_objs++;
}

} // namespace jumpmu

template <typename T>
class JumpScoped {
public:
  T obj_;

  template <typename... Args>
  JumpScoped(Args&&... args) : obj_(std::forward<Args>(args)...) {
    jumpmu::PushBackDesctructor(this, &DestructBeforeJump);
  }

  ~JumpScoped() {
    jumpmu::PopBackDestructor();
  }

  T* operator->() {
    return reinterpret_cast<T*>(&obj_);
  }

  /// Destructs the object, releases all the resources before longjump.
  static void DestructBeforeJump(void* jmuw_obj) {
    reinterpret_cast<JumpScoped<T>*>(jmuw_obj)->~JumpScoped<T>();
  }
};

/// Set a jump point, save the execution context before jump
#define JUMPMU_TRY()                                                                               \
  jumpmu::tls_jump_point_num_stack_objs[jumpmu::tls_num_jump_points] = jumpmu::tls_num_stack_objs; \
  if (setjmp(jumpmu::tls_jump_points[jumpmu::tls_num_jump_points++]) == 0) {

/// Remove the last jump point, add the execution path once jump happens
#define JUMPMU_CATCH()                                                                             \
  jumpmu::tls_num_jump_points--;                                                                   \
  }                                                                                                \
  else

/// Remove the last jump point, finish the function execution
#define JUMPMU_RETURN                                                                              \
  jumpmu::tls_num_jump_points--;                                                                   \
  return

/// Remove the last jump point, break the current loop
#define JUMPMU_BREAK                                                                               \
  jumpmu::tls_num_jump_points--;                                                                   \
  break

/// Remove the last jump point, continue the current loop
#define JUMPMU_CONTINUE                                                                            \
  jumpmu::tls_num_jump_points--;                                                                   \
  continue

/// Define a class function to destruct the object. Usually used together with
/// JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP() and JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
#define JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(T)                                                    \
  static void DestructBeforeJump(void* obj) {                                                      \
    reinterpret_cast<T*>(obj)->~T();                                                               \
  }

/// Pushe the desctructor to be called before jump to the thread local stack. Should be put in
/// every constructor.
#define JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP()                                                  \
  jumpmu::PushBackDesctructor(this, &DestructBeforeJump);

/// Pop the desctructor to be called before jump from the thread local stack. Should be put in the
/// desctructor.
#define JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP() jumpmu::PopBackDestructor();