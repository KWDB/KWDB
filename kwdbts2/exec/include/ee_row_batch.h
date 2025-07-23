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

#include <memory>
#include <vector>

#include "ee_data_container.h"
#include "ee_table.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_string.h"

namespace kwdbts {

class Field;
class RowBatch;

typedef std::shared_ptr<RowBatch> RowBatchPtr;

enum RowBatchType {
  RowBatchTypeUnknow = -1,

  RowBatchTypeTag = 0,
  RowBatchTypeScan,
  RowBatchTypeHashAgg,
  RowBatchTypeDistinct,
  RowBatchTypeSortAgg,
  RowBatchTypeSortDistinct,
  RowBatchTypeTempTable,
  RowBatchTypeSample
};

struct TagRawData {
  char *tag_data{nullptr};
  k_uint16 size{0};
  k_bool is_null{false};
};

class RowBatch{
 public:
  enum Stage {
    STAGE_UNKNOW = -1,

    STAGE_SCAN,
    STAGE_HASH_AGG,
    STAGE_SORT_AGG,
    STAGE_SORT,
    STAGE_JOIN,
    STAGE_DISTINCT,
  };

  RowBatchType typ_;
  Stage stage_{
      Stage::STAGE_SCAN};  // It is used to distinguish where to read the data
  virtual ~RowBatch() = default;
  virtual char *GetData(k_uint32 col, k_uint32 offset,
                        roachpb::KWDBKTSColumn::ColumnType ctype,
                        roachpb::DataType dt) = 0;
  virtual k_uint16 GetDataLen(k_uint32 col, k_uint32 offset,
                              roachpb::KWDBKTSColumn::ColumnType ctype) {
    return 0;
  }
  virtual bool IsNull(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype) = 0;
  virtual k_bool IsOverflow(k_uint32 col,
                            roachpb::KWDBKTSColumn::ColumnType ctype) {
    return false;
  }
  /**
   * data count
   */
  virtual k_uint32 Count() = 0;
  /**
   *  Move the cursor to the next line, default 0
   */
  virtual k_int32 NextLine() = 0;
  /**
   *  Set the cursor to the first line
   */
  virtual void ResetLine() = 0;
  virtual KStatus Sort(Field **renders, const std::vector<k_uint32> &cols,
                       const std::vector<k_int32> &order_type) = 0;
  virtual void SetLimitOffset(k_uint32 limit, k_uint32 offset) = 0;
  virtual EEIteratorErrCode Distinct(Field **order_fields,
                                     k_uint32 order_fields_num,
                                     Field **not_order_fields,
                                     k_uint32 not_order_fields_num) {
    return EEIteratorErrCode::EE_ERROR;
  }

  virtual void AddSelection() {}
  virtual void EndSelection() {}
  virtual void CopyColumnData(k_uint32 col_idx, char* dest, k_uint32 data_len,
                              roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) {}
  virtual void SetCount(k_uint32 count) {}
  virtual bool HasFilter() { return false; }
};

};  // namespace kwdbts
