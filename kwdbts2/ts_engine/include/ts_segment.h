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

#include "libkwdbts2.h"
#include "ts_common.h"
#include "data_type.h"
#include "ts_bitmap.h"

namespace kwdbts {

class TsSegmentBase;
// conditions used for flitering blockitem data.
struct TsBlockITemFilterParams {
  uint32_t db_id;
  TSTableID table_id;
  TSEntityID entity_id;
  std::vector<KwTsSpan>& ts_spans_;
  std::shared_ptr<TsSegmentBase> segment_;  // store shared_ptr to prevent deleted.
};

class TsSegmentBlockSpan {
 public:
  virtual TSEntityID GetEntityId() = 0;
  virtual TSTableID GetTableId() = 0;
  virtual uint32_t GetTableVersion() = 0;
  virtual void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) = 0;
  virtual size_t GetRowNum() = 0;
  // if has three rows, this return three value for certain column using col-based storege struct.
  virtual KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value, TsBitmap& bitmap) = 0;
  virtual KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) = 0;
  virtual inline bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) = 0;
  // if just get timestamp , this function return fast.
  virtual timestamp64 GetTS(int row_num, const std::vector<AttributeInfo>& schema) = 0;
};

// base class for data segment
class TsSegmentBase : public std::enable_shared_from_this<TsSegmentBase> {
 public:
  // filter blockspans that satisfied condition.
  virtual KStatus GetBlockSpans(const TsBlockITemFilterParams& filter,
                                std::list<std::shared_ptr<TsSegmentBlockSpan>>* blocks) = 0;
};

}  // namespace kwdbts
