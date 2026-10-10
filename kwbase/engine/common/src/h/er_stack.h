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

#ifndef COMMON_SRC_H_ER_STACK_H_
#define COMMON_SRC_H_ER_STACK_H_

#include <vector>
#include "er_error.h"

namespace kwdbts {

constexpr k_uint32 ERR_NUM_MAX = 8;

/**
  @class KwdbErrorStack
*/
class KwdbErrorStack {
 private:
  KwdbError kwdb_errors_[ERR_NUM_MAX];
  k_uint8 size_ = 0;

 public:
  KwdbErrorStack() = default;
  explicit KwdbErrorStack(KwdbErrorStack *perrStack) {}
  ~KwdbErrorStack() = default;
  
  /**
   * @brief Push an error into the error stack in ctx.
   * @param[in] file The name of the file where the error occurred.
   * @param[in] line  The number of rows where the error occurred.
   * @param[in]  code  Error code corresponding to the error.
   * @param[in]  ...   Parameters used to concatenate error messages.
   * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
   */
  KStatus PushError(const char *file, k_uint32 line, KwdbErrCode code,  va_list vlist);

  inline k_uint8 Size() const { return size_; }
  inline k_bool IsEmpty() const { return size_ == 0; }
  inline k_bool IsFull() const { return ERR_NUM_MAX == size_; }

  /**
   * @brief This function is used to get the KwdbError that is last stored in the KwdbErrorStack.
   * This function does not change the contents of the KwdbErrorStack.
   * @param[out] error A KwdbError that finally occurs and is stored in the KwdbErrorStack.
   * @return Returns a KStatus that checks whether KwdbError was successfully found.
   */
  KStatus GetLastError(KwdbError **error);

  /**
   * @brief Reset the KwdbErrorStack by cleaning the KwdbError in the KwdbErrorStack.
   * It is recommended to use RESET_ERR_STACK() to perform the reset.
   * @return Returns a KStatus that checks whether KwdbErrorStack was successfully reset.
   */
  KStatus Reset();

  /**
   * @brief Output KwdbError information stored in the KwdbErrorStack in fixed json format.
   * See the following example for the specific json format: {"kwdbErrors":[{"Code":"xxx",
   * "Message":"xxx % s. % sxxx ","num_tokens":2,"Action":"xxx","Reason":"xxx","Version":1.0,
   * "Internal_actions_taken":"xxx%s,%s"}]}.
   * @param[out] buf Holds the output buf of the formatted KwdbError information string.
   * @param[in] size The maximum capacity of the output buf.
   * @return Returns a KStatus that checks whether the KwdbError stored in the KwdbErrorStack was successfully dumped.
   */
  KStatus DumpToJson(char *buf, k_uint32 size);

  /**
   * @brief Push an error into the error stack in ctx.
   * @param[in] code The error code defined in KwdbErrCode.
   * @param[in] msg The error message defined in KwdbErrMsg
   * @return Returns a KStatus that checks whether KwdbError was pushed successfully.
   */
  KStatus PushError(KwdbErrCode code, const char *msg);
};

}  // namespace kwdbts

#endif  // COMMON_SRC_H_ER_STACK_H_
