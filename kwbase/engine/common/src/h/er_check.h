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

#ifndef COMMON_SRC_H_ER_CHECK_H_
#define COMMON_SRC_H_ER_CHECK_H_

#include "cm_assert.h"

namespace kwdbts {

#ifdef K_DEBUG
/**
 * @brief This function is used to verify that the passed arguments are valid at compile time.
 * @param format err_msg Predefined error messages.
 * @param ... Parameters used to concatenate error messages.
 */
void CompileCheckParam(const char *format, ...) __attribute__((format(printf, 1, 2)));

/**
 * @brief This macro encapsulates the function CompileCheckParam, which verifies the validity of
 * passed arguments at compile time.
 * @param ...  err_fmt Parameters used to concatenate error messages.
 */
#define COMPILE_CHECK_PARAM(...) \
  if (0) CompileCheckParam(__VA_ARGS__);

#endif
}  // namespace kwdbts
#endif  // COMMON_SRC_H_ER_CHECK_H_
