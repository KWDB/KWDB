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
#pragma once
#include <map>
#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_row_batch.h"
#include "ee_table.h"
#include "ee_tag_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"

namespace kwdbts {

struct SelectionItem {
  k_uint32 batch_;
  k_uint32 line_;
};

class ScanRowBatch;
typedef std::vector<SelectionItem> Selection;

class ScanRowBatch : public RowBatch {
 protected:
  Selection selection_;
  k_uint32 current_batch_line_{0};
  k_uint32 current_batch_no_{0};
  k_int32 current_line_{0};
  k_uint32 effect_count_{0};
  k_uint32 tag_col_offset_{0};
  TABLE *table_{nullptr};
  k_int32 current_batch_count_{-1};

 public:
  TagData tagdata_;
  void *tag_bitmap_{nullptr};
  ResultSet res_;
  k_uint32 count_{0};
  TagRowBatchPtr tag_rowbatch_{nullptr};
  bool is_filter_{false};
  uint64_t ts_{INT64_MAX};

 public:
  explicit ScanRowBatch(TABLE *table) {
    typ_ = RowBatchType::RowBatchTypeScan;
    stage_ = Stage::STAGE_SCAN;
    table_ = table;
    res_.setColumnNum(table_->scan_cols_.size());
  }
  virtual ~ScanRowBatch() { res_.clear(); }
  explicit ScanRowBatch(ScanRowBatch *handle) {
    typ_ = RowBatchType::RowBatchTypeScan;
    stage_ = Stage::STAGE_SCAN;
    tagdata_ = handle->tagdata_;
    tag_bitmap_ = handle->tag_bitmap_;
    tag_col_offset_ = handle->tag_col_offset_;
    table_ = handle->table_;
    tag_rowbatch_ = handle->tag_rowbatch_;
    res_.setColumnNum(table_->scan_cols_.size());
  }

  void Copy(ScanRowBatch *handle) {
    typ_ = RowBatchType::RowBatchTypeScan;
    stage_ = Stage::STAGE_SCAN;
    tagdata_ = handle->tagdata_;
    tag_bitmap_ = handle->tag_bitmap_;
    tag_col_offset_ = handle->tag_col_offset_;
    table_ = handle->table_;
    tag_rowbatch_ = handle->tag_rowbatch_;
    res_.setColumnNum(table_->scan_cols_.size());
  }

  char *GetData(k_uint32 col, k_uint32 offset,
                roachpb::KWDBKTSColumn::ColumnType ctype,
                roachpb::DataType dt) override;
  k_uint16 GetDataLen(k_uint32 col, k_uint32 offset,
                      roachpb::KWDBKTSColumn::ColumnType ctype) override;
  k_bool IsOverflow(k_uint32 col,
                    roachpb::KWDBKTSColumn::ColumnType ctype) override;

  void Reset();

  /**
   * data count
   */
  k_uint32 Count() override;
  /**
   *  Move the cursor to the next line, default 0
   */
  virtual k_int32 NextLine();
  /**
   *  Move the cursor to the first line
   */
  virtual void ResetLine();

  bool IsNull(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype) override;

  ResultSet *GetResultSet() { return &res_; }
  k_uint32 *GetCount() { return &count_; }
  k_uint32 GetEffectCount() { return effect_count_; }
  void AddSelection() override {
    if (effect_count_ == 0) {
      selection_.reserve(count_);
    }
    selection_[effect_count_] = {current_batch_no_, current_batch_line_};
    ++effect_count_;
  }
  Selection *GetSelection() { return &selection_; }
  KStatus Sort(Field **renders, const std::vector<k_uint32> &cols,
               const std::vector<k_int32> &order_type) override {
    return FAIL;
  }
  void SetLimitOffset(k_uint32 limit, k_uint32 offset) override {}
  void SetTagToColOffset(k_uint32 offset) { tag_col_offset_ = offset; }

  [[nodiscard]] bool HasFilter() override {
    return effect_count_ > 0;
  }

  virtual void CopyColumnData(k_uint32 col_idx, char* dest, k_uint32 data_len,
                      roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt);
  virtual bool SetCurrentLine(k_int32 line);
  void SetCount(k_uint32 count) {
    if (is_filter_) {
      effect_count_ = count;
    } else {
      count_ = count;
    }
  }
};

class ReverseScanRowBatch : public ScanRowBatch {
 public:
  using ScanRowBatch::ScanRowBatch;
  k_int32 NextLine() override;
  void ResetLine() override;
  void CopyColumnData(k_uint32 col_idx, char* dest, k_uint32 data_len,
                      roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) override;
  bool SetCurrentLine(k_int32 line) override;
};

};  // namespace kwdbts
