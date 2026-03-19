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
#include "ee_scan_helper.h"
#include "ee_scan_row_batch.h"
#include "kwdb_type.h"

namespace kwdbts {

class WindowHelper : public ScanHelper {
 public:
  bool first_span_{true};
  k_int32 row_size_{0};
  k_int32 current_line_{0};
  EntityResultIndex current_entity_index_{};

 public:
  std::vector<ScanRowBatch *> rowbatchs_;
  ScanRowBatch *rowbatch_{nullptr};
  SlidingWindowStep sliding_status_{SWS_NEXT_WINDOW};
  bool no_more_data_{false};
  bool continue_last_rowbatch_{false};

 private:
  k_int32 mem_size_{0};
  k_int32 current_rowbatch_index_{-1};
  k_int32 last_rowbatch_line_{0};

 public:
  explicit WindowHelper(TableScanOperator *op) : ScanHelper(op) {}
  virtual ~WindowHelper() {
    for (auto batch : rowbatchs_) {
      SafeDeletePointer(batch);
    }
    rowbatchs_.clear();
  }

 private:
  EEIteratorErrCode ReadRowBatch(kwdbContext_p ctx, ScanRowBatch *rowbatch,
                                 DataChunk *chunk,
                                 bool continue_last_rowbatch = false);

 public:
  EEIteratorErrCode NextBatch(kwdbContext_p ctx, ScanRowBatch *&rowbatch);
  KStatus PushBatch(ScanRowBatch *rowbatch) {
    mem_size_ = mem_size_ + rowbatch->Count() * row_size_;
    if (mem_size_ > BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      return FAIL;
    }
    rowbatchs_.push_back(rowbatch);
    return SUCCESS;
  }
  void PopBatch() {
    auto rowbatch = rowbatchs_[0];
    mem_size_ = mem_size_ - rowbatch->Count() * row_size_;
    rowbatchs_.erase(rowbatchs_.begin());
    SafeDeletePointer(rowbatch);
  }

 public:
  EEIteratorErrCode SlidingCase(kwdbContext_p ctx, DataChunkPtr &chunk);
  void MaterializeRow(KWThdContext *thd, DataChunk *chunk, k_int32 count);
  virtual bool MaterializeWindowField(KWThdContext *thd, Field *field,
                                      DataChunk *chunk, k_int32 row,
                                      k_int32 col) {
    return false;
  }

  virtual bool ProcessWindow(ScanRowBatch *rowbatch, bool &next_window) {
    return false;
  }
  virtual k_int32 ResetCurrentLine(k_int32 current_line,
                                   k_int32 rowbatch_count) {
    return 0;
  }
  virtual void NextSlidingWindow() {}
  virtual void NextEntity() {}
};
class CountWindowHelper : public WindowHelper {
 private:
  k_int32 sliding_{0};
  k_int32 count_val_{0};
  k_int32 current_count_{0};
  k_int32 group_id_{0};

 public:
  explicit CountWindowHelper(TableScanOperator *op) : WindowHelper(op) {}
  EEIteratorErrCode NextChunk(kwdbContext_p ctx, DataChunkPtr &chunk) override {
    EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
    code = SlidingCase(ctx, chunk);
    return code;
  }
  EEIteratorErrCode Init(kwdbContext_p ctx) override {
    EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
    KWThdContext *thd = current_thd;
    FieldFuncCountWindow *field =
        static_cast<FieldFuncCountWindow *>(thd->window_field_);
    bool is_sliding = false;
    field->GetParams(&count_val_, &sliding_);
    for (auto i : field->table_->scan_cols_) {
      Field *f = field->table_->GetFieldWithColNum(i);
      row_size_ += f->get_storage_length();
    }
    for (auto i : field->table_->scan_tags_) {
      Field *f =
          field->table_->GetFieldWithColNum(i + field->table_->min_tag_id_);
      row_size_ += f->get_storage_length();
    }
    return code;
  }
  bool MaterializeWindowField(KWThdContext *thd, Field *field, DataChunk *chunk,
                              k_int32 row, k_int32 col) override {
    if (field == thd->window_field_) {
      chunk->InsertData(row, col, reinterpret_cast<char *>(&group_id_),
                        field->get_storage_length());
      return true;
    }
    return false;
  }

  bool ProcessWindow(ScanRowBatch *rowbatch, bool &next_window) override {
    if (current_count_ == count_val_) {
      next_window = true;
    }
    first_span_ = false;
    current_count_++;
    return false;
  }
  k_int32 ResetCurrentLine(k_int32 current_line,
                           k_int32 rowbatch_count) override {
    return current_line - rowbatch_count;
  }

  void NextSlidingWindow() override {
    group_id_++;
    current_count_ = 0;
    current_line_ = current_line_ + sliding_;
  }
  void NextEntity() override {
    group_id_++;
    current_count_ = 0;
    current_line_ = 0;
  }
};
};  // namespace kwdbts
