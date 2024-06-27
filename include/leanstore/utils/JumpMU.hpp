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

void Jump();

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

template <typename T>
class JumpScoped {
public:
  T mObj;

  template <typename... Args>
  JumpScoped(Args&&... args) : mObj(std::forward<Args>(args)...) {
    jumpmu::PushBackDesctructor(this, &DestructBeforeJump);
  }

  ~JumpScoped() {
    jumpmu::PopBackDestructor();
  }

  T* operator->() {
    return reinterpret_cast<T*>(&mObj);
  }

  //! Destructs the object, releases all the resources before longjump.
  static void DestructBeforeJump(void* jmuwObj) {
    reinterpret_cast<JumpScoped<T>*>(jmuwObj)->~JumpScoped<T>();
  }
};

//! Set a jump point, save the execution context before jump
#define JUMPMU_TRY()                                                                               \
  jumpmu::tlsJumpPointNumStackObjs[jumpmu::tlsNumJumpPoints] = jumpmu::tlsNumStackObjs;            \
  if (setjmp(jumpmu::tlsJumpPoints[jumpmu::tlsNumJumpPoints++]) == 0) {

//! Remove the last jump point, add the execution path once jump happens
#define JUMPMU_CATCH()                                                                             \
  jumpmu::tlsNumJumpPoints--;                                                                      \
  }                                                                                                \
  else

//! Remove the last jump point, finish the function execution
#define JUMPMU_RETURN                                                                              \
  jumpmu::tlsNumJumpPoints--;                                                                      \
  return

//! Remove the last jump point, break the current loop
#define JUMPMU_BREAK                                                                               \
  jumpmu::tlsNumJumpPoints--;                                                                      \
  break

//! Remove the last jump point, continue the current loop
#define JUMPMU_CONTINUE                                                                            \
  jumpmu::tlsNumJumpPoints--;                                                                      \
  continue

//! Define a class function to destruct the object. Usually used together with
//! JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP() and JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP()
#define JUMPMU_DEFINE_DESTRUCTOR_BEFORE_JUMP(T)                                                    \
  static void DestructBeforeJump(void* obj) {                                                      \
    reinterpret_cast<T*>(obj)->~T();                                                               \
  }

//! Pushe the desctructor to be called before jump to the thread local stack. Should be put in
//! every constructor.
#define JUMPMU_PUSH_BACK_DESTRUCTOR_BEFORE_JUMP()                                                  \
  jumpmu::PushBackDesctructor(this, &DestructBeforeJump);

//! Pop the desctructor to be called before jump from the thread local stack. Should be put in the
//! desctructor.
#define JUMPMU_POP_BACK_DESTRUCTOR_BEFORE_JUMP() jumpmu::PopBackDestructor();