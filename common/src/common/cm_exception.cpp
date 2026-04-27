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

#include <cstdio> 
#include <signal.h>
#include <execinfo.h>
#include <cxxabi.h>
#include <unistd.h>
#include <fcntl.h>
#include <atomic>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

#include "cm_exception.h"
#include "cm_kwdb_context.h"
#include "kwdb_type.h"
#include "kwdb_compatible.h"


namespace kwdbts {

const char* kErrlogName = "errlog.log";
k_char kErrlogPath[FULL_FILE_NAME_MAX_LEN];
char kEmergencyBuf[512];
PostExceptionCb kPostExceptionCb = nullptr;

#define EXCEPTION_SIGNAL_CNT (6)
static k_int32 kExceptionSignals[EXCEPTION_SIGNAL_CNT] = {
  SIGSEGV, SIGABRT, SIGBUS, SIGSYS, SIGFPE, SIGILL
};
static struct sigaction kOldSigactions[EXCEPTION_SIGNAL_CNT];

extern void DumpThreadBacktraceToFile(int fd);
extern char* AppendLiteral(char* cursor, char* end, const char* text);
extern char* AppendUInt64(char* cursor, char* end, uint64_t value);
extern char* AppendPtr(char* cursor, char* end, uintptr_t ptr_value);
extern char* AppendInt32(char* cursor, char* end, int32_t value);

static int FastSecondToDate(const time_t& unix_sec, struct tm* tm, int time_zone) {
    static const int kHoursInDay = 24;
    static const int kMinutesInHour = 60;
    static const int kDaysFromUnixTime = 2472632;
    static const int kDaysFromYear = 153;
    static const int kMagicUnkonwnFirst = 146097;
    static const int kMagicUnkonwnSec = 1461;
    tm->tm_sec  =  unix_sec % kMinutesInHour;
    int i      = (unix_sec/kMinutesInHour);
    tm->tm_min  = i % kMinutesInHour;  // nn
    i /= kMinutesInHour;
    tm->tm_hour = (i + time_zone) % kHoursInDay;  // hh
    tm->tm_mday = (i + time_zone) / kHoursInDay;
    int a = tm->tm_mday + kDaysFromUnixTime;
    int b = (a*4  + 3)/kMagicUnkonwnFirst;
    int c = (-b*kMagicUnkonwnFirst)/4 + a;
    int d =((c*4 + 3) / kMagicUnkonwnSec);
    int e = -d * kMagicUnkonwnSec;
    e = e/4 + c;
    int m = (5*e + 2)/kDaysFromYear;
    tm->tm_mday = -(kDaysFromYear * m + 2)/5 + e + 1;
    tm->tm_mon = (-m/10)*12 + m + 2;
    tm->tm_year = b*100 + d  - 6700 + (m/10);
    return 0;
}

void GenExceptionHeader(char* buff, size_t* length, const char* sigstr, int sig, int si_code, uintptr_t ptr) {
  //  "Exception time(UTC):%s\nsignal:%s(%d)\npid=%d tid=%d si_code=%d si_addr=%p\n",
  char* cursor = buff;
  char* end = buff + *length;
  cursor = AppendLiteral(cursor, end, "Exception time(UTC):");
  time_t curr_time = 0;
  char time_buffer[32];
  struct tm curr_time_info;
  curr_time = time(NULL);
  FastSecondToDate(curr_time, &curr_time_info, 8);
  strftime(time_buffer, 32, "%Y-%m-%d %H:%M:%S", &curr_time_info);
  cursor = AppendLiteral(cursor, end, time_buffer);
  cursor = AppendLiteral(cursor, end, "\nsignal:");
  cursor = AppendLiteral(cursor, end, sigstr);
  cursor = AppendLiteral(cursor, end, "(");
  cursor = AppendInt32(cursor, end, sig);
  cursor = AppendLiteral(cursor, end, ")\npid=");
  cursor = AppendUInt64(cursor, end, static_cast<uint64_t>(getpid()));
  cursor = AppendLiteral(cursor, end, " tid=");
  cursor = AppendUInt64(cursor, end, static_cast<uint64_t>(gettid()));
  cursor = AppendLiteral(cursor, end, " si_code=");
  cursor = AppendInt32(cursor, end, si_code);
  cursor = AppendLiteral(cursor, end, " si_addr=0x");
  cursor = AppendPtr(cursor, end, ptr);
  cursor = AppendLiteral(cursor, end, "\n");
  *length = static_cast<size_t>(cursor - buff);
}

void StoreExceptionStackToFile(const int sig, siginfo_t* const info) {
  // Replace strsignal with a async-signal-safe lookup
  const char* sigstr = "Unknown";
  switch (sig) {
    case SIGSEGV: sigstr = "SIGSEGV"; break;
    case SIGABRT: sigstr = "SIGABRT"; break;
    case SIGBUS:  sigstr = "SIGBUS"; break;
    case SIGSYS:  sigstr = "SIGSYS"; break;
    case SIGFPE:  sigstr = "SIGFPE"; break;
    case SIGILL:  sigstr = "SIGILL"; break;
    default:      sigstr = "Unknown"; break;
  }
  size_t buf_size = sizeof(kEmergencyBuf);
  GenExceptionHeader(kEmergencyBuf, &buf_size, sigstr, sig, info->si_code, reinterpret_cast<uintptr_t>(info->si_addr));
  if (-1 == write(STDOUT_FILENO, kEmergencyBuf, buf_size)) {
    return;
  }
  DumpThreadBacktraceToFile(STDOUT_FILENO);

  if (kErrlogPath[0] == '\0') {
    return;
  }
  int fd = open(kErrlogPath, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) {
    return;
  }
  if (-1 != write(fd, kEmergencyBuf, buf_size)) {
    DumpThreadBacktraceToFile(fd);
  }
  close(fd);
}
void ExceptionHandler(const int sig, siginfo_t* const info, void*) {
  static std::atomic_bool handlered{false};
  if (handlered.exchange(true)) {
    // avoid recursive call
    signal(sig, SIG_DFL);
    return;
  }
  StoreExceptionStackToFile(sig, info);
  // https://pkg.go.dev/os/signal#hdr-Go_programs_that_use_cgo_or_SWIG
  // pass to GO or default
  for (k_int32 i = 0; i < EXCEPTION_SIGNAL_CNT; i++) {
    if (sig == kExceptionSignals[i]) {
      sigaction(sig, &kOldSigactions[i], NULL);
    }
  }
}

int32_t RegisterExceptionHandler(char *dir, PostExceptionCb cb) {
  const char* kwdb_data_root;
  if ( (kwdb_data_root = std::getenv("KWDB_DATA_ROOT")) &&
    ((std::strlen(kwdb_data_root) + 1 + std::strlen(kErrlogName)) < FULL_FILE_NAME_MAX_LEN) ) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(kErrlogPath, FULL_FILE_NAME_MAX_LEN, "%s/%s", kwdb_data_root,
             kErrlogName);
#pragma GCC diagnostic pop
  } else {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(kErrlogPath, FULL_FILE_NAME_MAX_LEN, "%s/%s", dir, kErrlogName);
#pragma GCC diagnostic pop
  }

  kPostExceptionCb = cb;
  for (k_int32 i = 0; i < EXCEPTION_SIGNAL_CNT; i++) {
    struct sigaction sa;
    sa.sa_sigaction = ExceptionHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO;
    // Golang will register the exception handler at the very begin.
    // so save it to kOldSigactions for reraise. Other program(AE/T_ME/KSQL) will
    // save a DFL handler, it's fine.
    if (sigaction(kExceptionSignals[i], &sa, &kOldSigactions[i]) == -1) {
      return -1;
    }
  }
  return 0;
}

void ExceptionHandlerForTest(int signr, siginfo_t *info, void *secret) {
  ExceptionHandler(signr, info, secret);
}

}  // namespace kwdbts
