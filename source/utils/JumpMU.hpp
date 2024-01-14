#pragma once

#include <cassert>
#include <utility>

#include <setjmp.h>

#define JUMPMU_STACK_SIZE 100
#define JUMPMU_STACK_OBJECTS_LIMIT 100

namespace jumpmu {

extern __thread int tlsNumJumpPoints;
extern __thread jmp_buf tlsJumpPoints[JUMPMU_STACK_SIZE];
extern __thread int tlsJumpPointNumStackObjs[JUMPMU_STACK_SIZE];

extern __thread int tlsNumStackObjs;
extern __thread void* tlsObjs[JUMPMU_STACK_OBJECTS_LIMIT];
extern __thread void (*tlsObjDtors[JUMPMU_STACK_OBJECTS_LIMIT])(void*);

void jump();

inline void PopBackDestructor() {
  assert(tlsNumStackObjs > 0);

  tlsObjs[tlsNumStackObjs - 1] = nullptr;
  tlsObjDtors[tlsNumStackObjs - 1] = nullptr;
  tlsNumStackObjs--;
}

inline void PushBackDesctructor(void* obj, void (*dtor)(void*)) {
  assert(tlsNumStackObjs < JUMPMU_STACK_SIZE);
  assert(obj != nullptr);
  assert(dtor != nullptr);

  tlsObjDtors[tlsNumStackObjs] = dtor;
  tlsObjs[tlsNumStackObjs] = obj;
  tlsNumStackObjs++;
}

} // namespace jumpmu

template <typename T> class JumpScoped {
public:
  T obj;

  template <typename... Args>
  JumpScoped(Args&&... args) : obj(std::forward<Args>(args)...) {
    jumpmu::PushBackDesctructor(this, &DestructBeforeJump);
  }

  ~JumpScoped() {
    jumpmu::PopBackDestructor();
  }

  T* operator->() {
    return reinterpret_cast<T*>(&obj);
  }

  /// @brief DestructBeforeJump destructs the object, releases all the resources
  /// hold by it before longjump.
  /// @param jmuwObj
  static void DestructBeforeJump(void* jmuwObj) {
    reinterpret_cast<JumpScoped<T>*>(jmuwObj)->~JumpScoped<T>();
  }
};

/// JUMPMU_TRY sets a jump point and saves the execution context before jump
/// @note !!! DO NOT DO ANYTHING BETWEEN setjmp and if !!!
#define JUMPMU_TRY()                                                           \
  jumpmu::tlsJumpPointNumStackObjs[jumpmu::tlsNumJumpPoints] =                 \
      jumpmu::tlsNumStackObjs;                                                 \
  if (setjmp(jumpmu::tlsJumpPoints[jumpmu::tlsNumJumpPoints++]) == 0) {

/// JUMPMU_CATCH supplements normal execution path which is to remove the last
/// jump point set by JUMPMU_TRY, adds the execution path once jump happens
#define JUMPMU_CATCH()                                                         \
  jumpmu::tlsNumJumpPoints--;                                                  \
  }                                                                            \
  else

/// JUMPMU_RETURN removes the last jump point and finish the function execution
#define JUMPMU_RETURN                                                          \
  jumpmu::tlsNumJumpPoints--;                                                  \
  return

/// JUMPMU_BREAK removes the last jump point and breaks the current loop
#define JUMPMU_BREAK                                                           \
  jumpmu::tlsNumJumpPoints--;                                                  \
  break

/// JUMPMU_CONTINUE removes the last jump point and continues the current loop
#define JUMPMU_CONTINUE                                                        \
  jumpmu::tlsNumJumpPoints--;                                                  \
  continue

/// JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP defines a static function to destruct
/// the object. It's usually used together with
/// JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP() and
/// JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
#define JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(T)                                \
  static void DestructBeforeJump(void* obj) { reinterpret_cast<T*>(obj)->~T(); }

/// JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP pushes the desctructor to be called
/// before jump to the thread local stack.
///
/// JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP should be put in every constructor.
#define JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP()                              \
  jumpmu::PushBackDesctructor(this, &DestructBeforeJump);

/// JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP pops the desctructor to be called
/// before jump from the thread local stack.
///
/// JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP should be put in the desctructor.
#define JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP() jumpmu::PopBackDestructor();