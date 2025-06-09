#include "utils/coroutine/coro_io.hpp"

#include "utils/coroutine/coroutine.hpp"
#include "utils/coroutine/thread.hpp"

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <expected>
#include <vector>

#include <fcntl.h>
#include <libaio.h>

namespace leanstore {

void CoroIo::Read(int32_t fd, void* buf, size_t count, uint64_t offset) {
  Coroutine* self = Thread::CurrentCoro();
  assert(self != nullptr && "CoroIo::Read() should be called from a coroutine");
  assert((reinterpret_cast<uint64_t>(buf) & (512 - 1)) == 0 &&
         "Buffer must be aligned to 512 bytes for O_DIRECT");
  assert((count & (512 - 1)) == 0 && "Count must be a multiple of 512 bytes for O_DIRECT");
  assert((offset & (512 - 1)) == 0 && "Offset must be a multiple of 512 bytes for O_DIRECT");

  // prepare the iocb for asynchronous read
  struct iocb* cb = new iocb();
  io_prep_pread(cb, fd, buf, count, offset);
  cb->data = self;

  // submit the iocb to the aio context
  int ret = io_submit(aio_ctx_, 1, &cb);
  if (ret < 0) {
    Log::Fatal("Failed to submit aio request, error={}", strerror(errno));
    delete cb;
    throw std::runtime_error("aio_submit failed");
  }
  num_reqs_++;

  // yield the coroutine, execution will resume when the IO is complete
  self->IncWaitingIoReqs();
  self->Yield(CoroState::kWaitingIo);

  // clean up after the coroutine resumes
  delete cb;
}

void CoroIo::Write(int32_t fd, const void* buf, size_t count, uint64_t offset) {
  Coroutine* self = Thread::CurrentCoro();
  assert(self != nullptr && "CoroIo::Write() should be called from a coroutine");
  assert((reinterpret_cast<uint64_t>(buf) & (512 - 1)) == 0 &&
         "Buffer must be aligned to 512 bytes for O_DIRECT");
  assert((count & (512 - 1)) == 0 && "Count must be a multiple of 512 bytes for O_DIRECT");
  assert((offset & (512 - 1)) == 0 && "Offset must be a multiple of 512 bytes for O_DIRECT");

  // prepare the iocb for asynchronous write
  struct iocb* cb = new iocb();
  io_prep_pwrite(cb, fd, const_cast<void*>(buf), count, offset);
  cb->data = self;

  // submit the iocb to the aio context
  int ret = io_submit(aio_ctx_, 1, &cb);
  if (ret < 0) {
    Log::Fatal("Failed to submit aio request, error={}", strerror(errno));
    delete cb;
    throw std::runtime_error("aio_submit failed");
  }
  num_reqs_++;

  // yield the coroutine, execution will resume when the IO is complete
  self->IncWaitingIoReqs();
  self->Yield(CoroState::kWaitingIo);

  // clean up after the coroutine resumes
  delete cb;
}

void CoroIo::Fsync(int32_t fd) {
  Coroutine* self = Thread::CurrentCoro();
  assert(self != nullptr && "CoroIo::Fsync() should be called from a coroutine");

  // prepare the iocb for fsync
  struct iocb* cb = new iocb();
  io_prep_fsync(cb, fd);
  cb->data = self;

  // submit the iocb to the aio context
  int ret = io_submit(aio_ctx_, 1, &cb);
  if (ret < 0) {
    Log::Fatal("Failed to submit aio request, error={}", strerror(errno));
    delete cb;
    throw std::runtime_error("aio_submit failed");
  }
  num_reqs_++;

  // yield the coroutine, execution will resume when the fsync is complete
  self->IncWaitingIoReqs();
  self->Yield(CoroState::kWaitingIo);

  // clean up after the coroutine resumes
  delete cb;
}

void CoroIo::Poll() {
  assert(Thread::CurrentCoro() != nullptr && "CoroIo::Poll() should be called from a coroutine");

  // skip if there are no pending requests
  if (num_reqs_ == 0) {
    return;
  }

  // poll for completed IO requests
  timespec timeout{0, 0};
  int completed_reqs = io_getevents(aio_ctx_, 1, num_reqs_, &io_events_[0], &timeout);

  // change coroutine state to running for each completed request
  for (int i = 0; i < completed_reqs; i++) {
    Coroutine* coro = static_cast<Coroutine*>(io_events_[i].data);
    assert(coro != nullptr && "CoroIo: Completed IO event has no associated coroutine");
    coro->DecWaitingIoReqs();
  }

  // reduce the number of pending requests
  num_reqs_ -= completed_reqs;
}

void CoroRead(int32_t fd, void* buf, size_t count, uint64_t offset) {
  Thread::CurrentCoroIo()->Read(fd, buf, count, offset);
}

void CoroWrite(int32_t fd, const void* buf, size_t count, uint64_t offset) {
  Thread::CurrentCoroIo()->Write(fd, buf, count, offset);
}

void CoroFsync(int32_t fd) {
  Thread::CurrentCoroIo()->Fsync(fd);
}

} // namespace leanstore