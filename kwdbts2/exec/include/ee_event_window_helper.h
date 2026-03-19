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
#include "ee_scan_row_batch.h"
#include "ee_window_helper.h"
#include "kwdb_type.h"

namespace kwdbts {
class EventWindowHelper : public WindowHelper {
 private:
  k_int32 start_line_{-1};
  k_int32 end_line_{-1};
  bool is_begin_{false};
  FieldFuncEventWindow* field_{nullptr};
  k_int32 group_id_{0};
  bool is_first_batch_{true};
  KWThdContext* thd_{nullptr};
  k_int32 start_row_{0};

 public:
  explicit EventWindowHelper(TableScanOperator* op) : WindowHelper(op) {
    sliding_status_ = SWS_NEXT_BATCH;
  }
  ~EventWindowHelper() {
    SafeDeletePointer(rowbatch_);
  }
  EEIteratorErrCode NextChunk(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  EEIteratorErrCode Init(kwdbContext_p ctx) override {
    KWThdContext* thd = current_thd;
    field_ = static_cast<FieldFuncEventWindow*>(thd->window_field_);
    for (auto i : field_->table_->scan_cols_) {
      Field* f = field_->table_->GetFieldWithColNum(i);
      row_size_ += f->get_storage_length();
    }
    for (auto i : field_->table_->scan_tags_) {
      Field* f = field_->table_->GetFieldWithColNum(i + field_->table_->min_tag_id_);
      row_size_ += f->get_storage_length();
    }
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode BeginMaterialize(kwdbContext_p ctx, ScanRowBatch* rowbatch) override {
    return EEIteratorErrCode::EE_OK;
  }
  bool MaterializeWindowField(KWThdContext* thd, Field* field, DataChunk* chunk, k_int32 row, k_int32 col);

 private:
  EEIteratorErrCode ReadRowBatch(kwdbContext_p ctx, DataChunk* chunk, bool continue_last_rowbatch);
};
};  // namespace kwdbts
