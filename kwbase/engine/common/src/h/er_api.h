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

#ifndef COMMON_SRC_H_ER_API_H_
#define COMMON_SRC_H_ER_API_H_

#include <memory>
#include "er_check.h"   // for COMPILE_CHECK_PARAM
#include "er_stack.h"

namespace kwdbts {

KStatus PushError(kwdbContext_p ctx, const char *file, k_uint32 line, KwdbErrCode code, ...);
KwdbErrorStack* GetErrStack(kwdbContext_p ctx);

/**
 * @brief This macro is defined to get the KwdbErrorStack in ctx
 */
#define ERR_STACK() kwdbts::GetErrStack(ctx)

/**
 * @brief This macro is defined to build KwdbError and place this KwdbError into the KwdbErrorStack in ctx.
 * Note that you do not need to create a new KwdbError to use this macro.
 * @param[in] stack  A pointer to the KwdbErrorStack.
 * @param[in]  code  Error code corresponding to the error.
 * @param[in]  ...   Parameters used to concatenate error messages.
 * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
 */
#define PUSH_ERR(code, ...) kwdbts::PushError(ctx, __FILE__, __LINE__, code, ##__VA_ARGS__);

/**
 * @brief This macro is defined to build an error message immutable KwdbError and place this KwdbError
 * into the KwdbErrorStack in ctx. Note that you do not need to create a new KwdbError to use this macro.
 * @param[in] stack  A pointer to the KwdbErrorStack.
 * @param[in]  code  Error code corresponding to the error.
 * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
 */
#ifdef K_DEBUG
#define PUSH_ERR_0(code) \
                         COMPILE_CHECK_PARAM(_CHECKER_##code) \
                         PUSH_ERR(code)
#else
#define PUSH_ERR_0(code) PUSH_ERR(code)
#endif

/**
 * @brief This macro is defined to build a KwdbError whose error message contains only one variable argument,
 * and place this KwdbError into the KwdbErrorStack in ctx. Note that you do not need to create a new KwdbError
 * to use this macro.
 * @param[in] stack  A pointer to the KwdbErrorStack.
 * @param[in]  code  Error code corresponding to the error.
 * @param[in]  arg1  The parameter used to construct the error message, which can be of any type.
 * Note that the parameter type must be the same as err_msg_ in KWDBErrorInfo predefined according to the error code
 * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
 */
#ifdef K_DEBUG
#define PUSH_ERR_1(code, arg1) \
                         COMPILE_CHECK_PARAM(_CHECKER_##code, arg1) \
                         PUSH_ERR(code, arg1)
#else
#define PUSH_ERR_1(code, arg1) PUSH_ERR(code, arg1)
#endif

/**
 * @brief This macro is defined to build a KwdbError whose error message contains only two variable arguments,
 * and place this KwdbError into the KwdbErrorStack in ctx. Note that you do not need to create a new KwdbError
 * to use this macro.
 * @param[in] stack  A pointer to the KwdbErrorStack.
 * @param[in]  code  Error code corresponding to the error.
 * @param[in]  arg1  The parameter used to construct the error message, which can be of any type.
 * Note that the parameter type must be the same as err_msg_ in KWDBErrorInfo predefined according to the error code
 * @param[in]  arg2
 * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
 */
#ifdef K_DEBUG
#define PUSH_ERR_2(code, arg1, arg2) \
                         COMPILE_CHECK_PARAM(_CHECKER_##code, arg1, arg2) \
                         PUSH_ERR(code, arg1, arg2)
#else
#define PUSH_ERR_2(code, arg1, arg2) PUSH_ERR(code, arg1, arg2)
#endif

/**
 * @brief This macro is defined to build a KwdbError whose error message contains only three variable arguments,
 * and place this KwdbError into the KwdbErrorStack in ctx. Note that you do not need to create a new KwdbError
 * to use this macro.
 * @param[in] stack  A pointer to the KwdbErrorStack.
 * @param[in]  code  Error code corresponding to the error.
 * @param[in]  arg1  The parameter used to construct the error message, which can be of any type.
 * Note that the parameter type must be the same as err_msg_ in KWDBErrorInfo predefined according to the error code
 * @param[in]  arg2
 * @param[in]  arg3
 * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
 */
#ifdef K_DEBUG
#define PUSH_ERR_3(code, arg1, arg2, arg3) \
                         COMPILE_CHECK_PARAM(_CHECKER_##code, arg1, arg2, arg3) \
                         PUSH_ERR(code, arg1, arg2, arg3)
#else
#define PUSH_ERR_3(code, arg1, arg2, arg3) PUSH_ERR(code, arg1, arg2, arg3)
#endif

}  // namespace kwdbts

#endif  // COMMON_SRC_H_ER_API_H_
