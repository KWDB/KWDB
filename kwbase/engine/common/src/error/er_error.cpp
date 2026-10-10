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

#include "er_error.h"

#include <cstdarg>
#include <sstream>
#include <string>

namespace kwdbts {

KStatus KwdbError::KwdbErrorFill(const char *file, k_uint32 line, KwdbErrCode code,
                              const char *fmt, va_list vlist) {
  if (nullptr == file) {
    return FAIL;
  }

  code_ = code;
  k_int32 len = 0;
  len += snprintf(nullptr, 0, "%s:%d ", file, line);
  va_list tmp_args;
  va_copy(tmp_args, vlist);
  kwdbts::k_int32 temp = vsnprintf(nullptr, 0, fmt, tmp_args);
  if ( temp < 0 ) {
    va_end(tmp_args);
    return FAIL;
  }
  len += temp + 1;
  va_end(tmp_args);
  if (len > sizeof(err_msg_)) {
    return FAIL;
  }
#ifdef K_DEBUG_FILE_INFO
  // add [file] [line]
  len = snprintf(err_msg_, sizeof(err_msg_), "%s:%d ", file, line);
  // Add custom information
  vsnprintf(err_msg_ + len, sizeof(err_msg_) - len, fmt, vlist);
#else
  // Add custom information
  vsnprintf(err_msg_, sizeof(err_msg_), fmt, vlist);
#endif
  return SUCCESS;
}

KStatus KwdbError::KwdbErrorFill(const char *file, k_uint32 line, KwdbErrCode code,
                              const char *fmt, ...) {
  va_list arg;
  va_start(arg, fmt);
  KStatus result = KwdbErrorFill(file, line, code, fmt, arg);
  va_end(arg);

  return result;
}

KStatus KwdbError::KwdbErrorFill(KwdbErrCode code, const char *msg) {
  if (nullptr == msg) {
    return FAIL;
  }
  // copy [code_]
  code_ = code;
  // copy [err_msg_]
  snprintf(err_msg_, sizeof(err_msg_), "%s", msg);
  return SUCCESS;
}


k_uint32 KwdbError::ToString(char *buf, k_int32 size,
                            const KWDBErrorInfo_t &err_info) {
  if (nullptr == buf || size <= 0) {
    return 0;
  }
  char pg_code_buf[PG_CODE_MAX_LEN] = {0};
  if (!ErrCode2PgCode(code_, pg_code_buf, sizeof(pg_code_buf))) {
    return 0;
  }

  std::ostringstream ss;

  ss << "{\"Code\":"
     << "\"" << pg_code_buf << "\""
     << ", \"Message\":"
     << "\"" << err_msg_ << "\""
     << ", \"Action\":"
     << "\"" << err_info.action << "\""
     << ", \"Reason\":"
     << "\"" << err_info.reason << "\""
     << ", \"Internal_actions_taken\":"
     << "\"" << err_info.internal_actions_taken << "\""
     << "},";

  return snprintf(buf, size, "%s", ss.str().c_str());
}
}  // namespace kwdbts
