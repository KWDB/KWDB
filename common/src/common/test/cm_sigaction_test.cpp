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

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <execinfo.h>
#include "gtest/gtest.h"
#include "cm_backtrace.h"
#include "cm_exception.h"

bool cust_malloc_called = false;
static int use_custom = 0;  // 1: 使用自定义, 0: 使用原始
void revert_to_original_malloc() {
  use_custom = 0;
  cust_malloc_called = false;
}
void revert_to_custom_malloc() {
  use_custom = 1;
  cust_malloc_called = false;
}

extern "C" {
  void* malloc(size_t size) {
    static void* (*original_malloc)(size_t) = NULL;
    if (original_malloc == NULL) {
      original_malloc =  (void*(*)(size_t))dlsym(RTLD_NEXT, "malloc");
    }
    if (use_custom) {
      // printf("Custom malloc: %zu bytes\n", size);
      cust_malloc_called = true;
      return original_malloc(size);
    } else {
      return original_malloc(size);
    }
  }
}

namespace kwdbts {

class TestSignalCallBackFunc: public ::testing::Test {
 public:
  std::string folder_ = "./";

  TestSignalCallBackFunc() {
    RegisterBacktraceSignalHandler();
  }
  std::string GetCurTS() {
    char buf[32];
    time_t now = time(0);
    snprintf(buf, sizeof(buf), "%lu", now);
    return std::string(buf);
  }
};

// Test normal
TEST_F(TestSignalCallBackFunc, empty) {
  revert_to_original_malloc();
  auto tmp = malloc(123);
  free(tmp);
  ASSERT_TRUE(!cust_malloc_called);
  revert_to_custom_malloc();
  tmp = malloc(123);
  free(tmp);
  ASSERT_TRUE(cust_malloc_called);
  revert_to_original_malloc();
}

TEST_F(TestSignalCallBackFunc, DumpThreadBacktraceTest) {
  revert_to_custom_malloc();
  auto ts = GetCurTS();
  DumpThreadBacktraceForTest(const_cast<char*>(folder_.c_str()), const_cast<char*>(ts.c_str()), 1, nullptr, nullptr);
  ASSERT_TRUE(!cust_malloc_called);
  revert_to_original_malloc();
}

TEST_F(TestSignalCallBackFunc, ExceptionHandlerTest) {
  revert_to_custom_malloc();
  auto ts = GetCurTS();
  siginfo_t mock_sig;
  mock_sig.si_code = 1234;
  mock_sig.si_errno = 12;
  mock_sig.si_signo = 6;
  mock_sig.si_addr = nullptr;
  mock_sig.si_addr_lsb = 12;
  ExceptionHandlerForTest(6, &mock_sig, nullptr);
  ASSERT_TRUE(!cust_malloc_called);
  revert_to_original_malloc();
}

}  //  namespace kwdbts
