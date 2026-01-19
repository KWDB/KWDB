// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_time_window_helper.h"

#include "ee_common.h"
#include "ee_kwthd_context.h"
#include "ee_scan_op.h"
#include "ee_storage_handler.h"

namespace kwdbts {

void TimeWindowHelper::CalSkipTsSpan(kwdbts::KTimestampTz &ts) {
  if (d_var_) {
    KTimestampTz start = ts / MILLISECOND_PER_SECOND;
    struct tm tm, etm;
    time_t tt = (time_t)start;
    ToGMT(tt, tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;
    etm = tm;
    if (d_year_) {
      tm.tm_mon = 0;
      tm.tm_year = (k_int32)(tm.tm_year / duration_ * duration_);
      etm.tm_mon = 0;
      etm.tm_year = (k_int32)(tm.tm_year + duration_);
    } else {
      k_int32 mon = tm.tm_year * 12 + tm.tm_mon;
      mon = (k_int32)(mon / duration_ * duration_);
      tm.tm_year = mon / 12;
      tm.tm_mon = mon % 12;
      mon = mon + duration_;
      etm.tm_year = mon / 12;
      etm.tm_mon = mon % 12;
    }
    tm.tm_hour -= time_zone_;
    etm.tm_hour -= time_zone_;
    first_ts_ = (k_int64)(timegm(&tm) * MILLISECOND_PER_SECOND);
    last_ts_ = (k_int64)(timegm(&etm) * MILLISECOND_PER_SECOND);
    src_first_ts_ = first_ts_;
    src_last_ts_ = last_ts_;
    last_tm_ = etm;
  } else {
    first_ts_ = CALCULATE_TIME_BUCKET_VALUE(ts, time_diff_, duration_);
    last_ts_ = first_ts_ + duration_;
    src_first_ts_ = CALCULATE_TIME_BUCKET_VALUE(ts, 0, duration_);
    src_last_ts_ = src_first_ts_ + duration_;
  }
}

void TimeWindowHelper::NextSkipTsSpan(KTimestampTz &ts) {
  if (d_var_) {
    if (d_year_) {
      last_tm_.tm_mon = 0;
      last_tm_.tm_year = (k_int32)(last_tm_.tm_year + duration_);
    } else {
      k_int32 mon = last_tm_.tm_year * 12 + last_tm_.tm_mon;
      mon = mon + duration_;
      last_tm_.tm_year = mon / 12;
      last_tm_.tm_mon = mon % 12;
    }
    first_ts_ = last_ts_;
    last_ts_ = (k_int64)(timegm(&last_tm_) * MILLISECOND_PER_SECOND);
    src_first_ts_ = src_last_ts_;
    src_last_ts_ = last_ts_;
  } else {
    last_ts_ = first_ts_ + (ts + duration_ - first_ts_) / duration_ * duration_;
    first_ts_ = last_ts_ - duration_;
    src_last_ts_ = src_first_ts_ + (ts + duration_ - src_first_ts_) / duration_ * duration_;
    src_first_ts_ = src_last_ts_ - duration_;
  }
}

bool TimeWindowHelper::MaterializeWindowField(KWThdContext *thd, Field *field,
                                              DataChunk *chunk, k_int32 row,
                                              k_int32 col) {
  if (field == thd->window_field_) {
    chunk->InsertData(row, col, reinterpret_cast<char *>(&group_id_),
                      field->get_storage_length(), false);
    return true;
  }
  if (field == thd->window_start_field_) {
    chunk->InsertData(row, col, reinterpret_cast<char *>(&first_ts_),
                      field->get_storage_length(), false);
    return true;
  }
  if (field == thd->window_end_field_) {
    chunk->InsertData(row, col, reinterpret_cast<char *>(&last_ts_),
                      field->get_storage_length(), false);
    return true;
  }
  return false;
}

EEIteratorErrCode TimeWindowHelper::Materialize(kwdbContext_p ctx,
                                                ScanRowBatch *rowbatch,
                                                DataChunkPtr &chunk) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  DataChunk *pchunk = chunk.get();
  k_int32 count = pchunk->Count();
  if (first_span_) {
    current_entity_index_ = rowbatch->res_.entity_index;
  } else if (!current_entity_index_.equalsWithoutMem(
                 rowbatch->res_.entity_index)) {
    first_span_ = true;
    current_entity_index_ = rowbatch->res_.entity_index;
  }

  thd->SetRowBatch(rowbatch);
  rowbatch->ResetLine();
  for (k_int32 i = 0; i < rowbatch->Count(); i++) {
    KTimestampTz ts = *static_cast<
        KTimestampTz *>(static_cast<void *>(rowbatch->GetData(
        0, sizeof(KTimestampTz),
        roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA,
        roachpb::DataType::TIMESTAMP)));
    ts /= type_scale_;
    if (first_span_) {
      CalSkipTsSpan(ts);
      first_span_ = false;
      fill_first_ts_ = false;
      group_id_++;
    }
    if (ts >= src_last_ts_) {
      group_id_++;
      do {
        NextSkipTsSpan(ts);
        fill_first_ts_ = false;
      } while (ts >= src_last_ts_);
    }
    if (ts < src_first_ts_) {
      CalSkipTsSpan(ts);
      fill_first_ts_ = false;
      group_id_++;
    }
    MaterializeRow(thd, pchunk, count);
    count++;
    rowbatch->NextLine();
  }
  return code;
}

void TimeWindowHelper::CalSlidingTsSpan(KTimestampTz &ts) {
  if (d_var_) {
    KTimestampTz sts = CALCULATE_TIME_BUCKET_VALUE(ts, time_diff_, sliding_);
    KTimestampTz ets = TimeAddDuration(sts, duration_, d_var_, d_year_);
    first_ts_ = sts;
    if (ets < ts) {
      while (ets < ts && sts < ts) {
        sts += sliding_;
        ets = TimeAddDuration(sts, duration_, d_var_, d_year_);
      }
      first_ts_ = sts;
      last_ts_ = ets;
    } else {
      while (ets >= ts) {
        first_ts_ = sts;
        last_ts_ = ets;
        sts -= sliding_;
        ets = TimeAddDuration(sts, duration_, d_var_, d_year_);
      }
    }
    src_first_ts_ = first_ts_;
    src_last_ts_ = last_ts_;
  } else {
    last_ts_ = CALCULATE_TIME_BUCKET_VALUE(ts, time_diff_, sliding_) + sliding_;
    first_ts_ = last_ts_ - duration_;
    src_last_ts_ = CALCULATE_TIME_BUCKET_VALUE(ts, 0, sliding_) + sliding_;
    src_first_ts_ = src_last_ts_ - duration_;
  }
}

void TimeWindowHelper::NextSlidingTsSpan() {
  if (d_var_) {
    first_ts_ = first_ts_ + sliding_;
    last_ts_ = TimeAddDuration(first_ts_, duration_, d_var_, d_year_);
    src_first_ts_ = src_first_ts_ + sliding_;
    src_last_ts_ = TimeAddDuration(src_first_ts_, duration_, d_var_, d_year_);
  } else {
    last_ts_ = last_ts_ + sliding_;
    first_ts_ = first_ts_ + sliding_;
    src_last_ts_ = src_last_ts_ + sliding_;
    src_first_ts_ = src_first_ts_ + sliding_;
  }
}

EEIteratorErrCode TimeWindowHelper::NextChunk(kwdbContext_p ctx,
                                              DataChunkPtr &chunk) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  if (skip_) {
    code = ScanHelper::NextChunk(ctx, chunk);
  } else {
    code = SlidingCase(ctx, chunk);
  }

  return code;
}
}  // namespace kwdbts
