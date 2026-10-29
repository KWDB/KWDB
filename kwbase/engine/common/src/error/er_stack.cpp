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

#include "er_stack.h"

#include <cstdio>
#include <vector>

#include "cm_assert.h"
#include "cm_config.h"

namespace kwdbts {

KStatus PushError(kwdbContext_p ctx, const char *file, k_uint32 line, KwdbErrCode code, ...) {
  va_list arg;
  va_start(arg, code);
  (&ctx->err_stack)->PushError(file, line, code, arg);
  va_end(arg);
  return SUCCESS;
}

KwdbErrorStack* GetErrStack(kwdbContext_p ctx) {
  return &(ctx->err_stack);
}

KStatus KwdbErrorStack::DumpToJson(char *buf, k_uint32 size) {
  if (nullptr == buf || 0 == size) {
    return FAIL;
  }
  k_uint32 len = 0;

  len = snprintf(buf, size, "\"kwdbErrors\": [");
  if (len > size) {
    return FAIL;
  }

  for (k_uint32 i = 0; i < size_; i++) {
    const KWDBErrorInfo_t &errorInfo = GetErrInfoByErrCode(kwdb_errors_[i].GetCode());
    len += kwdb_errors_[i].ToString(buf + len, size - len, errorInfo);
  }
  // Remove the last superfluous ","
  if (',' == buf[len - 1]) {
    len--;
  }
  if (len >= size) {
    return FAIL;
  }
  snprintf(buf + len, size - len, "]");
  return SUCCESS;
}

KStatus KwdbErrorStack::PushError(const char *file, k_uint32 line, KwdbErrCode code,  va_list vlist) {
  if (nullptr == file || IsFull()) {
    return FAIL;
  }
  kwdb_errors_[size_].KwdbErrorFill(GetFileName(file), line, code, GetFmtStrByErrCode(code), vlist);
  size_++;
  return SUCCESS;
}

KStatus KwdbErrorStack::GetLastError(KwdbError **error) {
  if (nullptr == error || IsEmpty()) {
    return FAIL;
  }
  *error = &kwdb_errors_[size_ - 1];
  return SUCCESS;
}

KStatus KwdbErrorStack::Reset() {
  size_ = 0;
  return SUCCESS;
}

KStatus KwdbErrorStack::PushError(KwdbErrCode code, const char *msg) {
  if (nullptr == msg || IsFull()) {
    return FAIL;
  }
  kwdb_errors_[size_].KwdbErrorFill(code, msg);
  size_++;
  return SUCCESS;
}

}  // namespace kwdbts
