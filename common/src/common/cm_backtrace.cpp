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


#include <signal.h>
#include <execinfo.h>
#include <dirent.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/syscall.h>
#include <sys/stat.h>
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

extern KString demangle(const char* symbol);

void DumpLatchInfo();

// send thread backtrace to ostream.
void PrintThreadBacktrace(std::ostream& os) {
  const k_int32 MAX_FRAME_LEVEL = 128;
  void *array[MAX_FRAME_LEVEL];
  size_t size = backtrace(array, MAX_FRAME_LEVEL);
  char **symbols = backtrace_symbols(array, size);
  os << "\nThread 0x"<< std::hex << pthread_self() << std::dec << " pid=" << getpid() << 
     " tid=" << gettid() << std::endl;
  os << "backtrace: size:" << size << std::endl;
  for (size_t i = 0; i < size; i++) {
    os << "#" << i << " " << demangle(symbols[i]) << std::endl;
  }
  free(symbols);
}

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

// Send signal SIGUSR2 to all threads in /proc/<PID>/task.
bool DumpAllThreadBacktrace(char* folder, char* nowTimeStamp) {
  DIR *dir;
  struct dirent *entry;
  // dump latch info
  DumpLatchInfo();

  // dump all threads info 
  memset(kThreadBtPath, 0, sizeof(kThreadBtPath));
  GetThreadBtFilePath(kThreadBtPath, folder, nowTimeStamp);

  // Get all thread tids.
  char task_dir[] = "/proc/self/task/";
  dir = opendir(task_dir);
  if (dir == NULL) {
    return false;
  }

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

// the callback function of SIGUSR2 for dump thread backtrace.
static void DumpThreadBacktrace(int signr, siginfo_t *info, void *secret) {
  std::ostringstream oss;
  PrintThreadBacktrace(oss);

  std::ofstream logfile;
  logfile.open(kThreadBtPath, std::ios_base::app);
  logfile << oss.str();
  logfile.flush();
  logfile.close();
}

// Register signal SIGUSR2 action function.
void RegisterBacktraceSignalHandler() {
  // Register SIGUSR2 for dump thread backtrace
  struct sigaction sa;
	sigfillset(&sa.sa_mask);
	sa.sa_flags = SA_ONSTACK | SA_RESTART | SA_SIGINFO;
	sa.sa_sigaction = DumpThreadBacktrace;
	sigaction(SIGUSR2, &sa, &oldsa);
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


}  // namespace kwdbts
