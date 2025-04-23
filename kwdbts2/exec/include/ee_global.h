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
// Created by liguoliang on 2022/07/18.
#pragma once

#include <cstring>
#include <memory>
#include <string>

#include "tr_api.h"
#include "kwdb_type.h"
#include "pgcode.h"

namespace kwdbts {

#define MILLISECOND_PER_SECOND 1000
#define MILLISECOND_PER_MINUTE 60000
#define MILLISECOND_PER_HOUR   3600000
#define MILLISECOND_PER_DAY    86400000
#define MILLISECOND_PER_WEEK   604800000

#define TIME_WINDOW_MIN_DURATION_MS 10

#define EE_TRACE_INFO(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 3, __VA_ARGS__)
#define EE_TRACE_WARN(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 2, __VA_ARGS__)
#define EE_TRACE_ERROR(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 1, __VA_ARGS__)

#define SafeDeletePointer(p)      \
  if (nullptr != (p)) delete (p); \
  (p) = nullptr;
#define SafeFreePointer(p)      \
  if (nullptr != (p)) free (p); \
  (p) = nullptr;

#define SafeDeleteArray(p)         \
  if (nullptr != (p)) delete[](p); \
  (p) = nullptr;

enum EEIteratorErrCode {
  EE_OK = 0,
  EE_ERROR,
  EE_DATA_ERROR,
  EE_END_OF_RECORD,
  EE_KILLED,
  EE_QUIT,
  EE_Sample,
  EE_TIMESLICE_OUT,
  EE_ERROR_ZERO,
  EE_PTAG_COUNT_NOT_MATCHED
};
/*
 * exclude info
 */
typedef enum {
  EE_BOTH = 0,
  EE_FROM,
  EE_TO,
  EE_NONE,
} exclude_enum;

#define CHECK_VALID_TINYINT(n)   ((n) >= INT8_MIN && (n) <= INT8_MAX)
#define CHECK_VALID_SMALLINT(n)  ((n) >= INT16_MIN && (n) <= INT16_MAX)
#define CHECK_VALID_INT(n)       ((n) >= INT32_MIN && (n) <= INT32_MAX)
#define CHECK_VALID_UTINYINT(n)  ((n) >= 0 && (n) <= UINT8_MAX)
#define CHECK_VALID_USMALLINT(n) ((n) >= 0 && (n) <= UINT16_MAX)
#define CHECK_VALID_UINT(n)      ((n) >= 0 && (n) <= UINT32_MAX)
#define CHECK_VALID_FLOAT(n)     ((n) >= -FLT_MAX && (n) <= FLT_MAX)
#define CHECK_VALID_DOUBLE(n)    ((n) >= -DBL_MAX && (n) <= DBL_MAX)

#define I64_SAFE_ADD_CHECK(a, b) (((a) >= 0 && (b) <= INT64_MAX - (a)) || ((a) < 0 && (b) >= INT64_MIN - (a)))
#define I64_SAFE_SUB_CHECK(a, b) (((a) >= 0 && (b) <= INT64_MIN + (a)) || ((a) < 0 && (b) >= INT64_MAX + (a)))
#define I64_SAFE_MUL_CHECK(a, b) \
    (((a) == 0 || (b) == 0) || \
     (((a) > 0 && (b) > 0 && (INT64_MAX / (a)) >= (b)) || \
      ((a) > 0 && (b) < 0 && (INT64_MIN / (a)) <= (b)) || \
      ((a) < 0 && (b) > 0 && (INT64_MIN / (b)) <= (a)) || \
      ((a) < 0 && (b) < 0 && (INT64_MAX / (b)) >= (a))))
#define MAX_PG_ERROR_MSG_LEN 128
struct EEPgErrorInfo {
  // error code
  k_int32 code{0};
  // error message
  char msg[MAX_PG_ERROR_MSG_LEN]{0};
  static void ResetPgErrorInfo();
  static void SetPgErrorInfo(k_int32 code, const char *msg = nullptr);
  static bool IsError();
  static EEPgErrorInfo &GetPgErrorInfo();
};

extern thread_local EEPgErrorInfo g_pg_error_info;

enum TsNextRetState {  DML_NEXT, DML_VECTORIZE_NEXT, DML_PG_RESULT };

#define OPERATOR_DIRECT_ENCODING(ctx, output_encoding, thd, chunk)                                       \
  if (output_encoding) {                                                \
    KStatus ret =                                                        \
        chunk->Encoding(ctx, thd->GetPgEncode(), thd->GetCommandLimit(), \
                        thd->GetCountForLimit());                        \
    if (ret != SUCCESS) {                                                \
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,               \
                                    "Insufficient memory");              \
      Return(EEIteratorErrCode::EE_ERROR);                               \
    }                                                                    \
  }

#define CALCULATE_TIME_BUCKET_VALUE(original_timestamp, time_diff, interval_seconds) \
({ \
    KTimestampTz bucket_start = (original_timestamp + time_diff) / interval_seconds * interval_seconds; \
    if (original_timestamp + time_diff < 0 && (original_timestamp + time_diff) % interval_seconds!= 0) { \
        bucket_start -= interval_seconds; \
    } \
    KTimestampTz last_time_bucket_value = bucket_start - time_diff; \
    last_time_bucket_value; \
})

enum WindowGroupType {
  EE_WGT_UNKNOWN,
  EE_WGT_STATE,
  EE_WGT_EVENT,
  EE_WGT_SESSION,
  EE_WGT_COUNT,
  EE_WGT_COUNT_SLIDING,
  EE_WGT_TIME,
  EE_WGT_TIME_SLIDING
};

enum SlidingWindowStep {
  SWS_NEXT_WINDOW,
  SWS_READ_BATCH,
  SWS_NEXT_BATCH,
  SWS_RT_CHUNK,
  SWS_NEXT_ENTITY,
};

#define IS_LEAP_YEAR(year) (((year) % 4 == 0 && (year) % 100!= 0) || (year) % 400 == 0)

}  // namespace kwdbts

