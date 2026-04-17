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

#include <gtest/gtest.h>
#include "ts_object_error.h"

TEST(ErrorInfoFuncTest, ConstructorClear) {
  ErrorInfo err;
  err.setError(KWENOMEM, "test");
  err.clear();
}

TEST(ErrorInfoFuncTest, SetErrorString) {
  ErrorInfo err;
  err.setError(KWEPERM, "test");
}

TEST(ErrorInfoFuncTest, ErrorCodeString) {
  ErrorInfo err;
  err.errorCodeString(KWEPERM);
  err.errorCodeString(9999);
}

TEST(ErrorInfoFuncTest, SetErrorFromOther) {
  ErrorInfo e1;
  ErrorInfo e2;
  e1.setError(KWECORR, "test");
  e2.setError(e1);
}

TEST(ErrorInfoFuncTest, DummyErrorInfo) {
  getDummyErrorInfo();
}

TEST(ErrorInfoFuncTest, ErrnoConvert) {
  errno = EEXIST;
  errnoToErrorCode();

  errno = ENOENT;
  errnoToErrorCode();

  errno = 0;
  errnoToErrorCode();
}

TEST(ErrorInfoFuncTest, MutexMode) {
  ErrorInfo e1(false);
  ErrorInfo e2(true);
  e1.setError(KWERLOCK, "test");
  e1.clear();
}