#include "Files.hpp"

#include "Exceptions.hpp"
#include "Units.hpp"

#include <glog/logging.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cmath>
#include <sstream>
#include <thread>

using namespace std;

namespace leanstore {
namespace utils {

// WARNING: this only works with 4 byte types
template <class T>
bool CreateTestFileImpl(const string& fileName, uint64_t count,
                        function<T(int)> factory) {
  // Open file
  ofstream of(fileName, ios::binary);
  if (!of.is_open() || !of.good())
    return false;

  // Write file in buffered fashion
  const uint32_t kMaxBufferSize = 1 << 22;
  vector<T> buffer(kMaxBufferSize / sizeof(uint32_t));
  for (uint64_t i = 0; i < count;) {
    // Fill buffer and write
    uint64_t limit = i + buffer.size();
    for (; i < count && i < limit; i++)
      buffer[i % buffer.size()] = factory(i);
    of.write(reinterpret_cast<char*>(buffer.data()),
             (buffer.size() - (limit - i)) * sizeof(uint32_t));
  }

  // Finish up
  of.flush();
  of.close();
  return of.good();
}

template <class T>
bool ForeachInFileImpl(const string& fileName, function<void(T)> callback) {
  // Open file
  ifstream in(fileName, ios::binary);
  if (!in.is_open() || !in.good())
    return false;

  // Loop over each entry
  T entry;
  while (true) {
    in.read(reinterpret_cast<char*>(&entry), sizeof(uint32_t));
    if (!in.good())
      break;
    callback(entry);
  }
  return true;
}

bool CreateTestFile(const string& fileName, uint64_t count,
                    function<int32_t(int32_t)> factory) {
  return CreateTestFileImpl<int32_t>(fileName, count, factory);
}

bool ForeachInFile(const string& fileName, function<void(uint32_t)> callback) {
  return ForeachInFileImpl<uint32_t>(fileName, callback);
}

bool CreateDirectory(const string& dirName) {
  return mkdir(dirName.c_str(), 0666) == 0;
}

bool CreateFile(const string& fileName, const uint64_t bytes) {
  int fileFd = open(fileName.c_str(), O_CREAT | O_WRONLY, 0666);
  if (fileFd < 0) {
    return false; // Use strerror(errno) to find error
  }

  if (ftruncate(fileFd, bytes) != 0) {
    return false; // Use strerror(errno) to find error
  }

  if (close(fileFd) != 0) {
    return false; // Use strerror(errno) to find error
  }

  return true;
}

bool CreateFile(const string& fileName, const string& content) {
  int fileFd = open(fileName.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fileFd < 0) {
    return false; // Use strerror(errno) to find error
  }

  size_t writtenBytes = write(fileFd, content.data(), content.size());
  return writtenBytes == content.size();
}

void DeleteFile(const std::string& fileName) {
  remove(fileName.c_str());
}

uint64_t GetFileLength(const string& fileName) {
  int fileFD = open(fileName.c_str(), O_RDWR);
  if (fileFD < 0) {
    LOG(ERROR) << "Unable to open file: " << fileName;
    return 0;
  }

  if (fcntl(fileFD, F_GETFL) == -1) {
    LOG(ERROR) << "Unable to call fcntl on file: " << fileName;
    return 0;
  }

  struct stat st;
  fstat(fileFD, &st);
  close(fileFD);
  return st.st_size;
}

bool FileExists(const string& fileName) {
  struct stat buffer;
  bool exists = (stat(fileName.c_str(), &buffer) == 0);
  return exists && (buffer.st_mode & S_IFREG);
}

bool DirectoryExists(const string& fileName) {
  struct stat buffer;
  bool exists = (stat(fileName.c_str(), &buffer) == 0);
  return exists && (buffer.st_mode & S_IFDIR);
}

bool PathExists(const string& fileName) {
  struct stat buffer;
  bool exists = (stat(fileName.c_str(), &buffer) == 0);
  return exists;
}

string LoadFileToMemory(const string& fileName) {
  uint64_t length = GetFileLength(fileName);
  string data(length, 'a');
  ifstream in(fileName);
  in.read(&data[0], length);
  return data;
}

namespace {

uint64_t ApplyPrecision(uint64_t input, uint32_t precision) {
  uint32_t digits = log10(input) + 1;
  if (digits <= precision)
    return input;
  uint32_t invalidDigits = pow(10, digits - precision);
  return (uint64_t)((double)input / invalidDigits + .5f) * invalidDigits;
}

} // namespace

string FormatTime(chrono::nanoseconds ns, uint32_t precision) {
  ostringstream os;

  uint64_t timeSpan = ApplyPrecision(ns.count(), precision);

  // Convert to right unit
  if (timeSpan < 1000ll)
    os << timeSpan << "ns";
  else if (timeSpan < 1000ll * 1000ll)
    os << timeSpan / 1000.0f << "us";
  else if (timeSpan < 1000ll * 1000ll * 1000ll)
    os << timeSpan / 1000.0f / 1000.0f << "ms";
  else if (timeSpan < 60l * 1000ll * 1000ll * 1000ll)
    os << timeSpan / 1000.0f / 1000.0f / 1000.0f << "s";
  else if (timeSpan < 60l * 60l * 1000ll * 1000ll * 1000ll)
    os << timeSpan / 1000.0f / 1000.0f / 1000.0f / 60.0f << "m";
  else
    os << timeSpan / 1000.0f / 1000.0f / 1000.0f / 60.0f / 60.0f << "h";

  return os.str();
}

void PinThread(int socket) {
#ifdef __linux__
  // Doesn't work on OS X right now
  cpu_set_t cpuset;
  pthread_t thread;
  thread = pthread_self();
  CPU_ZERO(&cpuset);
  CPU_SET(socket, &cpuset);
  pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  int s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (s != 0) {
    fprintf(stderr, "error: pthread_getaffinity_np");
    exit(-1);
  }
#endif
  (void)socket;
}

void RunMultithreaded(uint32_t threadCount, function<void(uint32_t)> foo) {
  atomic<bool> start(false);
  vector<unique_ptr<thread>> threads(threadCount);
  for (uint32_t i = 0; i < threadCount; i++) {
    threads[i] = make_unique<thread>([i, &foo, &start]() {
      while (!start)
        ;
      foo(i);
    });
  }
  start = true;
  for (auto& iter : threads) {
    iter->join();
  }
}

uint8_t* AlignedAlloc(uint64_t alignment, uint64_t size) {
  void* result = nullptr;
  int error = posix_memalign(&result, alignment, size);
  if (error) {
    throw ex::GenericException("posix_memalign failed in utility");
  }
  return reinterpret_cast<uint8_t*>(result);
}

namespace {

array<char, 16> numToHex{{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
                          'b', 'c', 'd', 'e', 'f'}};
uint8_t HexToNum(char c) {
  if ('a' <= c && c <= 'f') {
    return 10 + (c - 'a');
  }
  if ('0' <= c && c <= '9') {
    return (c - '0');
  }
  UNREACHABLE();
}

} // namespace

const string DataToHex(const uint8_t* data, uint32_t len, bool spaces) {
  string result;
  for (uint32_t i = 0; i < len; i++) {
    result += numToHex[*(data + i) >> 4];
    result += numToHex[*(data + i) & 0x0f];
    if (spaces && i != len - 1)
      result += ' ';
  }
  return result;
}

const string StringToHex(const string& str, bool spaces) {
  return DataToHex((const uint8_t*)str.data(), str.size(), spaces);
}

const vector<uint8_t> HexToData(const string& str, bool spaces) {
  assert(spaces || str.size() % 2 == 0);

  uint32_t resultSize = spaces ? ((str.size() + 1) / 3) : (str.size() / 2);
  vector<uint8_t> result(resultSize);
  for (uint32_t i = 0, out = 0; i < str.size(); i += 2, out++) {
    result[out] = (HexToNum(str[i]) << 4) | HexToNum(str[i + 1]);
    i += spaces ? 1 : 0;
  }

  return result;
}

const string HexToString(const string& str, bool spaces) {
  assert(spaces || str.size() % 2 == 0);

  uint32_t resultSize = spaces ? ((str.size() + 1) / 3) : (str.size() / 2);
  string result(resultSize, 'x');
  for (uint32_t i = 0, out = 0; i < str.size(); i += 2, out++) {
    result[out] = (char)((HexToNum(str[i]) << 4) | HexToNum(str[i + 1]));
    i += spaces ? 1 : 0;
  }

  return result;
}

} // namespace utils
} // namespace leanstore
