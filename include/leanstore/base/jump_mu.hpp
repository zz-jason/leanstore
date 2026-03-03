#pragma once

#include "leanstore/base/log.hpp"

#include <cassert>
#include <cstdint>
#include <utility>

#include <setjmp.h>
#include <sys/types.h>

namespace leanstore {

/// Context for the setjmp/longjmp mechanism. It contains the jump envs and the
/// stack of objects to destruct before jump.
///
/// longjmp() will directly jump to the last setjmp() call without executing the
/// destructors of the objects in the stack, breaking the RAII principle. This
/// is a problem when the objects hold resources (locks, memory, etc.) that need
/// to be released before the jump happens. To solve this problem, we maintain
/// a stack of destructors that will be called before longjmp().
class JumpContext {
public:
  static thread_local JumpContext* s_current_context;

  enum class JumpReason : uint8_t {
    kNormal = 0,
    kWaitingLock,
    kWaitingBufferframe,
  };

  static JumpContext* Current() {
    return s_current_context;
  }

  static void SetCurrent(JumpContext* context) {
    s_current_context = context;
  }

  static void Jump(JumpContext::JumpReason reason = JumpContext::JumpReason::kNormal) {
    assert(JumpContext::Current() != nullptr && "JumpContext should be set before longjump");
    JumpContext::Current()->SafeJump(reason);
  }

  ~JumpContext() {
    assert(jump_env_counter_ == 0 && "JumpContext should not be destroyed with active jump envs");
    assert(stack_obj_counter_ == 0 &&
           "JumpContext should not be destroyed with registered stack objects");
  }

  jmp_buf& AllocJumpEnv() {
    LEAN_DCHECK(jump_env_counter_ < kJumpEnvsLimit,
                "Jump envs limit exceeded, jump_env_counter_={}, kJumpEnvsLimit={}",
                jump_env_counter_, kJumpEnvsLimit);

    stack_obj_counter_snapshot_[jump_env_counter_] = stack_obj_counter_;
    return jump_envs_[jump_env_counter_++];
  }

  void DeallocLastJumpEnv() {
    assert(jump_env_counter_ > 0);
    jump_env_counter_--;
  }

  /// Register a stack object, should be called in the object constructor. The
  /// object is freed in 2 cases:
  ///
  /// 1. longjmp() is required, in which case the destructor is called by the
  ///    ReleaseObjectsBeforeJump() method.
  /// 2. longjmp() is not required, the routine returns normally, in which case
  ///    the destructor is called automatically when the object goes out of scope.
  ///
  /// In either case, UnregisterObject() is called in the object destructor
  /// to remove the object record from the JumpContext.
  void RegisterObject(void* obj, void (*dtor)(void*)) {
    assert(stack_obj_counter_ < kStackObjectsLimit);
    stack_objs_[stack_obj_counter_] = obj;
    stack_obj_dtors_[stack_obj_counter_] = dtor;
    stack_obj_counter_++;
  }

  /// Removes the record of destoried stack object. Should be called in the
  /// object destructor
  void UnregisterObject(void* obj [[maybe_unused]]) {
    assert(stack_obj_counter_ > 0);
    LEAN_DCHECK(stack_objs_[stack_obj_counter_ - 1] == obj &&
                "Unregistering an object that is not the last registered one");

    stack_obj_counter_--;
    stack_objs_[stack_obj_counter_] = nullptr;
    stack_obj_dtors_[stack_obj_counter_] = nullptr;
  }

private:
  void SafeJump(JumpReason reason) {
    assert(jump_env_counter_ > 0 && "At least 1 jump env should be there");
    ReleaseObjectsBeforeJump();
    jump_reason_ = reason;
    longjmp(jump_envs_[--jump_env_counter_], 1);
  }

  void ReleaseObjectsBeforeJump() {
    assert(jump_env_counter_ > 0 && "At least 1 jump env should be there");

    auto stack_obj_counter_after_jump = stack_obj_counter_snapshot_[jump_env_counter_ - 1];
    if (stack_obj_counter_after_jump < stack_obj_counter_) {
      // Release resources held by stack objects in reverse (FILO) order.
      auto first = stack_obj_counter_after_jump;
      auto last = stack_obj_counter_ - 1;
      for (int i = last; i >= first; i--) {
        assert(stack_objs_[i] != nullptr && "Stack object should not be null");
        assert(stack_obj_dtors_[i] != nullptr && "Stack object destructor should not be null");
        stack_obj_dtors_[i](stack_objs_[i]);
        assert(stack_obj_counter_ == i && "Stack object counter should -1 after destruction");

        stack_objs_[i] = nullptr;
        stack_obj_dtors_[i] = nullptr;
      }
    }
    assert(stack_obj_counter_ == stack_obj_counter_after_jump);
  }

  static constexpr int kJumpEnvsLimit = 20;
  static constexpr int kStackObjectsLimit = 20;

  int stack_obj_counter_ = 0;
  void* stack_objs_[kStackObjectsLimit];
  void (*stack_obj_dtors_[kStackObjectsLimit])(void*);

  int jump_env_counter_ = 0;
  jmp_buf jump_envs_[kJumpEnvsLimit];
  int stack_obj_counter_snapshot_[kJumpEnvsLimit];

  JumpReason jump_reason_ = JumpReason::kNormal;
};

#define JUMPMU_TRY() if (setjmp(::leanstore::JumpContext::Current()->AllocJumpEnv()) == 0) {

#define JUMPMU_CATCH()                                                                             \
  ::leanstore::JumpContext::Current()->DeallocLastJumpEnv();                                       \
  }                                                                                                \
  else

#define JUMPMU_RETURN                                                                              \
  ::leanstore::JumpContext::Current()->DeallocLastJumpEnv();                                       \
  return

#define JUMPMU_BREAK                                                                               \
  ::leanstore::JumpContext::Current()->DeallocLastJumpEnv();                                       \
  break

#define JUMPMU_CONTINUE                                                                            \
  ::leanstore::JumpContext::Current()->DeallocLastJumpEnv();                                       \
  continue

#define JUMPMU_DEFINE_DESTRUCTOR(T)                                                                \
  static void DestructBeforeJump(void* obj) {                                                      \
    reinterpret_cast<T*>(obj)->~T();                                                               \
  }

#define JUMPMU_REGISTER_STACK_OBJECT(obj_ptr)                                                      \
  ::leanstore::JumpContext::Current()->RegisterObject(obj_ptr, &DestructBeforeJump);

#define JUMPMU_UNREGISTER_STACK_OBJECT(obj_ptr)                                                    \
  ::leanstore::JumpContext::Current()->UnregisterObject(obj_ptr);

template <typename T>
class JumpScoped {
public:
  T obj_;

  template <typename... Args>
  explicit JumpScoped(Args&&... args) : obj_(std::forward<Args>(args)...) {
    JUMPMU_REGISTER_STACK_OBJECT(this);
  }

  ~JumpScoped() {
    JUMPMU_UNREGISTER_STACK_OBJECT(this);
  }

  T* operator->() {
    return reinterpret_cast<T*>(&obj_);
  }

  JUMPMU_DEFINE_DESTRUCTOR(JumpScoped<T>);
};

} // namespace leanstore
