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
#pragma once

#include <vector>

#include "ee_field_window.h"
#include "ee_kwthd_context.h"
#include "ee_window_helper.h"
#include "ee_scan_row_batch.h"
#include "kwdb_type.h"

namespace kwdbts {
class TimeWindowHelper : public WindowHelper {
 private:
  k_int32 start_line_{0};
  k_int32 last_line_{0};
  KTimestampTz first_ts_{INT64_MIN};
  KTimestampTz last_ts_{INT64_MAX};
  KTimestampTz src_first_ts_{INT64_MIN};
  KTimestampTz src_last_ts_{INT64_MAX};
  k_int64 duration_{0};
  bool d_year_{false};
  bool d_var_{false};
  k_int64 sliding_{0};
  bool s_year_{false};
  bool s_var_{false};
  bool skip_{true};
  k_int64 time_diff_{0};
  k_int32 group_id_{-1};
  bool fill_first_ts_{false};
  k_int64 type_scale_{1};

  struct tm first_tm_ {
  }, last_tm_{};

 public:
  explicit TimeWindowHelper(TableScanOperator *op) : WindowHelper(op) {}
  ~TimeWindowHelper() {}
  EEIteratorErrCode NextChunk(kwdbContext_p ctx, DataChunkPtr &chunk) override;
  EEIteratorErrCode Init(kwdbContext_p ctx) override {
    KWThdContext *thd = current_thd;
    FieldFuncTimeWindow *field =
        static_cast<FieldFuncTimeWindow *>(thd->window_field_);
    bool is_sliding = false;
    field->GetParams(&duration_, &d_var_, &d_year_, &sliding_, &s_var_,
                     &s_year_, &is_sliding, &type_scale_);
    if (is_sliding) {
      skip_ = false;
    }
    for (auto i : field->table_->scan_cols_) {
      Field *f = field->table_->GetFieldWithColNum(i);
      row_size_ += f->get_storage_length();
    }
    for (auto i : field->table_->scan_tags_) {
      Field *f =
          field->table_->GetFieldWithColNum(i + field->table_->min_tag_id_);
      row_size_ += f->get_storage_length();
    }
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode BeginMaterialize(kwdbContext_p ctx,
                                     ScanRowBatch *rowbatch) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Materialize(kwdbContext_p ctx, ScanRowBatch *rowbatch,
                                DataChunkPtr &chunk) override;
  bool MaterializeWindowField(KWThdContext *thd, Field *field, DataChunk *chunk,
                              k_int32 row, k_int32 col);
  bool ProcessWindow(ScanRowBatch *rowbatch, bool &next_window) override {
    KTimestampTz ts = *static_cast<
        KTimestampTz *>(static_cast<void *>(rowbatch->GetData(
        0, sizeof(KTimestampTz),
        roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA,
        roachpb::DataType::TIMESTAMP)));
    ts /= type_scale_;
    if (first_span_) {
      CalSlidingTsSpan(ts);
      first_span_ = false;
      group_id_++;
    }
    if (ts >= src_last_ts_) {
      next_window = true;
      return false;
    }
    if (ts < src_first_ts_) {
      return true;
    }
    return false;
  }
  void NextSlidingWindow() override {
    group_id_++;
    NextSlidingTsSpan();
    fill_first_ts_ = false;
  }
  void NextEntity() override { fill_first_ts_ = false; }

 private:
  inline void CalSkipTsSpan(KTimestampTz &ts);
  inline void NextSkipTsSpan(KTimestampTz &ts);
  void CalSlidingTsSpan(KTimestampTz &ts);
  void NextSlidingTsSpan();

  EEIteratorErrCode SkipCase(kwdbContext_p ctx, DataChunkPtr &chunk);
};
};  // namespace kwdbts
