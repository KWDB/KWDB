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
#include <cxxabi.h>
#include <unistd.h>

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

KString demangle(const char* symbol) {
  // extract symbol from - me.so(_ZN6kwdbts14PrintBacktraceERSo+0x34) [0xffff974b3530]
  static constexpr k_char OPEN = '(';
  const k_char* begin = nullptr;
  const k_char* end = nullptr;
  for (const k_char *j = symbol; *j; ++j) {
    if (*j == OPEN) {
      begin = j + 1;
    } else if (*j == '+') {
      end = j;
    }
  }
  if (begin && end && begin < end) {
    KString mangled(begin, end);
    if (mangled.compare(0, 2, "_Z") == 0) {
      char* demangled = abi::__cxa_demangle(mangled.c_str(), nullptr, nullptr, nullptr);
      if (demangled) {
        KString full_name(symbol, begin);
        full_name += demangled;
        full_name += end;
        free(demangled);
        return full_name;
      }
    }
    // C function
    return symbol;
  } else {
    return symbol;
  }
}

void PrintBacktrace(std::ostream& os) {
  const k_int32 max_frame_level = 32;
  void *array[max_frame_level];
  size_t size = backtrace(array, max_frame_level);
  char **symbols = backtrace_symbols(array, size);
  os << "backtrace: size:" << size << std::endl;
  for (size_t i = 0; i < size; i++) {
    os << "#" << i << " " << demangle(symbols[i]) << std::endl;
  }
  free(symbols);
}

void Out2Console(const KString &str) {
  std::cout << str;
  std::cout.flush();
}

void Out2Console(const char* str) {
  std::cout << str;
  std::cout.flush();
}

void ExceptionHandler(const int sig, siginfo_t* const info, void*) {
  static std::atomic_bool handlered{false};
  if (handlered.exchange(true)) {
    // avoid recursive call
    signal(sig, SIG_DFL);
    return;
  }

  time_t curr_time;
  char time_buffer[32];
  struct tm curr_time_info;
  curr_time = time(NULL);
  localtime_r(&curr_time, &curr_time_info);
  strftime(time_buffer, 32, "%Y-%m-%d %H:%M:%S", &curr_time_info);
  const char* sigstr = strsignal(sig);
  // https://man7.org/linux/man-pages/man2/sigaction.2.html
  snprintf(kEmergencyBuf, sizeof(kEmergencyBuf),
#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
    "Exception time(UTC):%s\nsignal:%s(%d)\npid=%d tid=%ld si_code=%d si_addr=%p\n",
#else
    "Exception time(UTC):%s\nsignal:%s(%d)\npid=%d tid=%d si_code=%d si_addr=%p\n",
#endif
    time_buffer, sigstr, sig, getpid(), gettid(), info->si_code, info->si_addr);
  Out2Console(kEmergencyBuf);

  // TODO(jinmou): Won't print backtrace if can't malloc.
  std::ostringstream oss;
  PrintBacktrace(oss);

  std::string msg = oss.str();
  Out2Console(msg);

  std::ofstream logfile;
  logfile.open(kErrlogPath, std::ios_base::app);
  logfile << kEmergencyBuf;
  logfile << msg;
  logfile.flush();

  if (kPostExceptionCb) {
    oss.str("");
    kPostExceptionCb(sig, oss);
    msg = oss.str();
    Out2Console(msg);
    logfile << msg;
    logfile.flush();
  }
  logfile.close();

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
    snprintf(kErrlogPath, FULL_FILE_NAME_MAX_LEN, "%s/%s", dir,
             kErrlogName);
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

}  // namespace kwdbts
