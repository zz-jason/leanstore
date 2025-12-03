#include "leanstore/cpp/base/jump_mu.hpp"

namespace leanstore {

namespace {
thread_local leanstore::JumpContext tls_jump_context;
} // namespace

thread_local JumpContext* JumpContext::s_current_context = &tls_jump_context;

} // namespace leanstore
