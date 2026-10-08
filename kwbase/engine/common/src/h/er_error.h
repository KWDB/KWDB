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

#ifndef COMMON_SRC_H_ER_ERROR_H_
#define COMMON_SRC_H_ER_ERROR_H_

#include <cstdarg>
#include <cstring>
#include <list>

#include "er_cfg.h"
#include "kwdb_type.h"

namespace kwdbts {

#define ERR_MSG_MAX_LEN 512
#define PG_CODE_MAX_LEN 16
/**
  @class KwdbError
*/
class KwdbError {
 private:
  KwdbErrCode code_;
  char err_msg_[ERR_MSG_MAX_LEN] = {0};

 public:
  KwdbError() : code_(KWDB_ERROR_LAST) {}
  ~KwdbError() = default;

  /**
   * @brief This function assigns the error code and error message to KwdbError.
   * @param[in]  file    The name of the file where the error occurred.
   * @param[in]  line    The number of rows where the error occurred.
   * @param[in]  code    Error code corresponding to the error.
   * @param[in]  ...     Parameters used to concatenate error messages.
   * @return Returns a KStatus that checks whether the error code and error message was assigned successfully.
   */
  KStatus KwdbErrorFill(const char *file, k_uint32 line, KwdbErrCode code,
                       const char *fmt, ...);

  KStatus KwdbErrorFill(const char *file, k_uint32 line, KwdbErrCode code,
                       const char *fmt, va_list vlist);
  /**
   * @brief This function assigns the error code and formatted error message to KwdbError.
   * @param[in]  code    Error code corresponding to the error.
   * @param[in]  msg     Format processed error messages.
   * @return  Returns a KStatus that checks whether the error code and error message was assigned successfully.
   */
  KStatus KwdbErrorFill(KwdbErrCode code, const char *msg);

  /**
   * @brief This function gets the error code for KwdbError.
   * @return Return the error code corresponding to KwdbError.
   */
  inline KwdbErrCode GetCode() const { return code_; }
  inline const char *GetErrMsg() { return err_msg_; }

  /**
   * @brief Converts the value of a KwdbError object member variable to a fixed-format json string.
   * @param[out] buf Holds the output buf of the formatted KwdbError information string.
   * @param[in] size The maximum capacity of the output buf.
   * @param[in] err_info Predefined formatting error message templates.
   * @return Return the actual length of the json string.
   */
  k_uint32 ToString(char *buf, k_int32 size, const KWDBErrorInfo_t &err_info);
};

}   // namespace kwdbts
#endif   // COMMON_SRC_H_ER_ERROR_H_
