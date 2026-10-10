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

#ifndef COMMON_SRC_H_CM_ASSERT_H_
#define COMMON_SRC_H_CM_ASSERT_H_

#include <assert.h>
#include <shared_mutex>
#include "cm_kwdb_context.h"
#include "cm_func.h"

namespace kwdbts {
#define MAX_STACK_TRACE_SIZE 32
#define ERR_ASSERT 1

extern void dumpAssertContext(const char* assert_file, const char* fun, int line, const char* x);

#ifdef Assert
#undef Assert
#endif

#ifdef K_DEBUG
#define Assert(x)  assert(x)
#else
#ifdef KMALLOC_PERFORMANCE_OPTIMIZATION
#define Assert(x)
#else
#define Assert(x) { \
if(!(x)) { \
  kwdbts::dumpAssertContext(__FILE__, __FUNCTION__, __LINE__, #x); \
  } \
}
#endif
#endif

#ifdef K_DEBUG
#define AssertWithReturnValue(x, r)  assert(x)
#else
#define AssertWithReturnValue(x, r) { \
if(!(x)) { \
  kwdbts::dumpAssertContext(__FILE__, __FUNCTION__, __LINE__, #x); \
  return(KStatus::FAIL);  \
  } \
}
#endif

// define normal macro, assert point is not null, else return errCode 1 (point is nullptr).
// TODO(liangbo01): correct error code
#define AssertNotNull(ptr) AssertWithReturnValue(nullptr != ptr, 1)
#define AssertNotNullNoRet(ptr) Assert(nullptr != ptr, 1)

}  // namespace kwdbts

#endif  // COMMON_SRC_H_CM_ASSERT_H_
