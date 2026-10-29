// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
#include "ee_global.h"

namespace kwdbts {

thread_local EEPgErrorInfo g_pg_error_info;

void EEPgErrorInfo::ResetPgErrorInfo() {
  g_pg_error_info.code = 0;
  g_pg_error_info.msg[0] = '\0';
}

void EEPgErrorInfo::SetPgErrorInfo(k_int32 code, const char* msg) {
  if (g_pg_error_info.code != 0) {
    return;
  }

  g_pg_error_info.code = code;
  if (!msg) {
    return;
  }
  k_int32 len = strlen(msg);
  if (len >= MAX_PG_ERROR_MSG_LEN) {
    len = MAX_PG_ERROR_MSG_LEN - 1;
  }
  memcpy(g_pg_error_info.msg, msg, len);
  g_pg_error_info.msg[len] = '\0';
}

bool EEPgErrorInfo::IsError() { return g_pg_error_info.code > 0; }

EEPgErrorInfo& EEPgErrorInfo::GetPgErrorInfo() { return g_pg_error_info; }

}  // namespace kwdbts
