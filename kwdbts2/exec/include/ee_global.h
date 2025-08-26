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

#include "ee_comm_def.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {


extern std::string EEIteratorErrCodeToString(EEIteratorErrCode code);

extern std::string KWDBTypeFamilyToString(KWDBTypeFamily type);

#define OPERATOR_DIRECT_ENCODING(ctx, output_encoding, use_query_short_circuit, thd, chunk)       \
  if (output_encoding) {                                                 \
    KStatus ret =                                                        \
        chunk->Encoding(ctx, thd->GetPgEncode(), use_query_short_circuit, thd->GetCommandLimit(), \
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
}  // namespace kwdbts
