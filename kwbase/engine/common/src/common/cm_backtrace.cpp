// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.


#include <sys/syscall.h>
#include <sys/stat.h>
#include <cxxabi.h>
#include <fcntl.h>
#include <unistd.h>
#include <dlfcn.h>
#include <signal.h>
#include <execinfo.h>
#include <dirent.h>
#include <stdio.h>
#include <memory>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cstdint>
#include "cm_backtrace.h"
#include "cm_kwdb_context.h"
#include "kwdb_compatible.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "lt_rw_latch.h"
#include "lt_cond.h"

namespace kwdbts {
const char* kThreadBtName = "thread_backtrace";
k_char kThreadBtPath[FULL_FILE_NAME_MAX_LEN];
struct sigaction oldsa;
static constexpr size_t kDemangleBufferSize = 8 * 1024;

void DumpLatchInfo();

// Generate thread stack backtrace file absolute path.
void GetThreadBtFilePath(char *threadBtPath, char* folder, char* nowTimeStamp) {
  if ((std::strlen(folder) + std::strlen(kThreadBtName) + std::strlen(nowTimeStamp) + 6)
       < FULL_FILE_NAME_MAX_LEN) {
    snprintf(threadBtPath, FULL_FILE_NAME_MAX_LEN, "%s/%s.%s.txt", folder,
             kThreadBtName, nowTimeStamp);
  } else {
    snprintf(threadBtPath, FULL_FILE_NAME_MAX_LEN, "./%s",
             kThreadBtName);
  }
}

// Signal SIGUSR2 to thread by syscall.
int SignalThreadDump(pid_t pid, uid_t uid, pid_t tid) {
  // Similar to pthread_sigqueue(), but usable with a tid since we
  // don't have a pthread_t.
  siginfo_t info;
  sigval nullVal;
  memset(&info, 0, sizeof(info));
  info.si_signo = SIGUSR2;
  info.si_code = SI_QUEUE;
  info.si_pid = pid;
  info.si_uid = uid;
  info.si_value = nullVal;
  return syscall(SYS_rt_tgsigqueueinfo, pid, tid, SIGUSR2, &info);
}

void SetThreadBtPath(char* folder, char* nowTimeStamp) {
  memset(kThreadBtPath, 0, sizeof(kThreadBtPath));
  GetThreadBtFilePath(kThreadBtPath, folder, nowTimeStamp);
}

// Send signal SIGUSR2 to all threads in /proc/<PID>/task.
bool DumpAllThreadBacktrace(char* folder, char* nowTimeStamp) {
  DIR *dir;
  struct dirent *entry;
  // dump latch info
  DumpLatchInfo();
  // set file path
  SetThreadBtPath(folder, nowTimeStamp);

  // Get all thread tids.
  char task_dir[] = "/proc/self/task/";
  dir = opendir(task_dir);
  if (dir == NULL) {
    return false;
  }
  // dump all threads info
  while ((entry = readdir(dir)) != NULL) {
    std::string full_path = task_dir;
    full_path += entry->d_name;
    struct stat file_stat{};
    if (stat(full_path.c_str(), &file_stat) != 0) {
      LOG_ERROR("stat[%s] failed", full_path.c_str());
      closedir(dir);
      return false;
    }
    if (S_ISDIR(file_stat.st_mode)) {
      if (entry->d_name[0] >= '0' && entry->d_name[0] <= '9') {
        usleep(20000);
        SignalThreadDump(getpid(), getuid(), strtoll(entry->d_name, nullptr, 10));
      }
    }
  }

  closedir(dir);
  return true;
}

void demangle(const char* symbol, char* buffer, size_t buffer_size) {
  if (!symbol || !buffer || buffer_size == 0) {
    return;
  }
  // If the symbol is too long, maybe buffer not enough. so we truncate it.
  if (strlen(symbol) > buffer_size / 8) {
      strncpy(buffer, symbol, buffer_size - 1);
      buffer[buffer_size - 1] = '\0';
      return;
  }
  // extract symbol from - _ZN6kwdbts26DumpThreadBacktraceForTestEPcS0_iP9siginfo_tPv
  if (symbol[0] == '_' && symbol[1] == 'Z') {
    int status = 0;
    size_t len = buffer_size;
    // Use the provided stack buffer. __cxa_demangle will use it if it's large enough.
    char* ret = abi::__cxa_demangle(symbol, buffer, &len, &status);
    if (status != 0 || ret == nullptr) {
      // If demangling fails, fallback to original symbol or mark as unknown
      strncpy(buffer, symbol, buffer_size - 1);
      buffer[buffer_size - 1] = '\0';
    } else {
      // If ret != buffer, it means realloc happened (which we want to avoid ideally,
      // but if buffer was too small, __cxa_demangle might realloc.
      // However, passing a non-null buffer usually prevents malloc if it fits).
      // To strictly avoid malloc, we ensure buffer is large enough.
      // If ret != buffer, we should copy back if possible,
      // but typically with a large enough static buffer, ret == buffer.
      if (ret != buffer) {
        strncpy(buffer, ret, buffer_size - 1);
        buffer[buffer_size - 1] = '\0';
        free(ret);  // Free if it did allocate
      }
    }
  } else {
    strncpy(buffer, symbol, buffer_size - 1);
    buffer[buffer_size - 1] = '\0';
  }
}
char* AppendLiteral(char* cursor, char* end, const char* text) {
  while (*text != '\0' && cursor < end) {
    *cursor++ = *text++;
  }
  return cursor;
}

char* AppendUInt64(char* cursor, char* end, uint64_t value) {
  char digits[32];
  size_t len = 0;
  do {
    digits[len++] = static_cast<char>('0' + (value % 10));
    value /= 10;
  } while (value != 0 && len < sizeof(digits));
  while (len > 0 && cursor < end) {
    *cursor++ = digits[--len];
  }
  return cursor;
}

char* AppendInt32(char* cursor, char* end, int32_t value) {
  bool is_negative = false;
  uint64_t num;
  if (value < 0) {
    *cursor++ = '-';
    num = static_cast<unsigned int>(-(value + 1)) + 1;
  } else {
    num = static_cast<unsigned int>(value);
  }
  return AppendUInt64(cursor, end, num);
}
char* AppendPtr(char* cursor, char* end, uintptr_t ptr_value) {
  const char hex_digits[] = "0123456789abcdef";
  const int ptr_bytes = sizeof(ptr_value);
  int cur_offset = 0;
  if (end - cursor <= ptr_bytes * 2) {
    // no enough buffer to store result.
    return cursor;
  }
  bool header = true;
  for (int i = 0; i < sizeof(ptr_value); i++) {
    uint8_t byte = (ptr_value >> ((ptr_bytes - 1 - i) * 8)) & 0xFF;
    if (byte == 0 && header) {
      continue;
    }
    header = false;
    *cursor++ = hex_digits[(byte >> 4) & 0x0F];  // high 4 bits.
    *cursor++ = hex_digits[byte & 0x0F];         // low  4 bits.
  }
  return cursor;
}
void GenThreadHeader(char* header, size_t* head_size, size_t frame_size) {
  char* cursor = header;
  char* end = header + *head_size;
  cursor = AppendLiteral(cursor, end, "\nThread 0x");
  cursor = AppendPtr(cursor, end, pthread_self());
  cursor = AppendLiteral(cursor, end, " pid=");
  cursor = AppendUInt64(cursor, end, static_cast<uint64_t>(getpid()));
  cursor = AppendLiteral(cursor, end, " tid=");
  cursor = AppendUInt64(cursor, end, static_cast<uint64_t>(gettid()));
  cursor = AppendLiteral(cursor, end, "\nbacktrace: size:");
  cursor = AppendUInt64(cursor, end, static_cast<uint64_t>(frame_size));
  cursor = AppendLiteral(cursor, end, "\n");
  *head_size = static_cast<size_t>(cursor - header);
}

void GenTraceLine(char* header, size_t* head_size,
  size_t i, const char* lib_name, const char* display_name, uintptr_t offset, void* array_i) {
  char* cursor = header;
  char* end = header + *head_size;
  //  "#%zu %s(%s+0x%lx) [%p]
  cursor = AppendLiteral(cursor, end, "#");
  cursor = AppendUInt64(cursor, end, static_cast<uint64_t>(i));
  cursor = AppendLiteral(cursor, end, " ");
  cursor = AppendLiteral(cursor, end, lib_name);
  cursor = AppendLiteral(cursor, end, "(");
  cursor = AppendLiteral(cursor, end, display_name);
  cursor = AppendLiteral(cursor, end, "+0x");
  cursor = AppendPtr(cursor, end, offset);
  cursor = AppendLiteral(cursor, end, ") [0x");
  cursor = AppendPtr(cursor, end, reinterpret_cast<uintptr_t>(array_i));
  cursor = AppendLiteral(cursor, end, "]\n");
  *head_size = static_cast<size_t>(cursor - header);
}

void DumpThreadBacktraceToFile(int fd) {
  if (fd < 0) {
    return;
  }
  const k_int32 MAX_FRAME_LEVEL = 128;
  void *array[MAX_FRAME_LEVEL];
  size_t size = backtrace(array, MAX_FRAME_LEVEL);
  char thread_info[512];
  size_t head_size = sizeof(thread_info);
  GenThreadHeader(thread_info, &head_size, size);
  if (-1 ==write(fd, thread_info, head_size)) {
    return;
  }
  // Use a stack-allocated buffer for demangling to avoid malloc
  char demangle_buf[kDemangleBufferSize];
  for (size_t i = 0; i < size; i++) {
    size_t line_len = sizeof(thread_info);
    Dl_info dl_info;
    if (dladdr(array[i], &dl_info)) {
      std::string symname("??");
      const char* display_name = "??";
      if (dl_info.dli_sname) {
        demangle(dl_info.dli_sname, demangle_buf, sizeof(demangle_buf));
        display_name = demangle_buf;
      }
      uintptr_t offset = (uintptr_t)array[i] - (uintptr_t)dl_info.dli_saddr;
      if (dl_info.dli_fname) {
        const char* cur_char = dl_info.dli_fname;
        const char* lib_name = dl_info.dli_fname;
        while (*cur_char != 0) {
          if (*cur_char == '/') {
            lib_name = cur_char + 1;
          }
          cur_char++;
        }
        GenTraceLine(thread_info, &line_len, i, lib_name, display_name, offset, array[i]);
      } else {
        GenTraceLine(thread_info, &line_len, i, "??", display_name, offset, array[i]);
      }
    } else {
      GenTraceLine(thread_info, &line_len, i, "??", "??", 1, array[i]);
    }
    if (-1 == write(fd, thread_info, line_len)) {
      return;
    }
  }
  if (-1 == write(fd, "\n", 1)) {
    return;
  }
}

std::mutex lock_4_backtrace;
// the callback function of SIGUSR2 for dump thread backtrace.
static void DumpThreadBacktrace(int signr, siginfo_t *info, void *secret) {
  if (kThreadBtPath[0] == '\0') {
    return;
  }
  lock_4_backtrace.lock();
  int fd = open(kThreadBtPath, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) {
    lock_4_backtrace.unlock();
    return;
  }
  DumpThreadBacktraceToFile(fd);
  close(fd);
  lock_4_backtrace.unlock();
}

// Register signal SIGUSR2 action function.
void RegisterBacktraceSignalHandler() {
  // Register SIGUSR2 for dump thread backtrace
  struct sigaction sa;
  sigfillset(&sa.sa_mask);
  sa.sa_flags = SA_ONSTACK | SA_RESTART | SA_SIGINFO;
  sa.sa_sigaction = DumpThreadBacktrace;
  sigaction(SIGUSR2, &sa, &oldsa);

  // load library, so sigaction no need call malloc.
  const k_int32 MAX_FRAME_LEVEL = 2;
  void *array[MAX_FRAME_LEVEL];
  backtrace(array, MAX_FRAME_LEVEL);
}

// dump latchs stats
FILE* openNewFile(const char* file_prefix) {
  std::string dump_file_path = Logger::GetInstance().LogRealPath();
  if (dump_file_path.empty()) {
    dump_file_path = "./";
  }
  if (dump_file_path.back() != '/') {
    dump_file_path += "/";
  }

  // file ext name
  time_t now = time(0);
  tm* localTime = localtime(&now);
  char timestamp[20] = {0};
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H_%M_%S", localTime);
  std::string ext_name(timestamp);
  std::string file_name = dump_file_path + file_prefix + "." + ext_name + ".dmp";
  // open file
  FILE* fp = fopen(file_name.c_str(), "a+");
  if (fp == nullptr) {
    LOG_ERROR("open file: %s failed,error: %s", file_name.c_str(), strerror(errno));
    return nullptr;
  }
  return fp;
}
void DumpLatchInfo() {
  // 1. dump latch info
  FILE* fp = openNewFile("latch");
  if (fp == nullptr) {
    return;
  }
  debug_latch_print(fp);
  fclose(fp);

  // 2. dump rwlatch info
  FILE* fp1 = openNewFile("rw_latch");
  if (fp1 == nullptr) {
    return;
  }
  debug_rwlock_print(fp1);
  fclose(fp1);

  // 3. dump cond wait info
  FILE* fp2 = openNewFile("cond_wait");
  if (fp2 == nullptr) {
    return;
  }
  debug_condwait_print(fp2);
  fclose(fp2);
}

void DumpThreadBacktraceForTest(char* folder, char* nowTimeStamp, int signr, siginfo_t *info, void *secret) {
  SetThreadBtPath(folder, nowTimeStamp);
  DumpThreadBacktrace(signr, info, secret);
}

}  // namespace kwdbts
