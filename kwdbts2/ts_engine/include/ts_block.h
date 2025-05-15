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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_blkspan_type_convert.h"

namespace kwdbts {
class TsBlock {
 public:
  virtual ~TsBlock() {}
  virtual TSTableID GetTableId() = 0;
  virtual uint32_t GetTableVersion() = 0;
  virtual size_t GetRowNum() = 0;
  // if has three rows, this return three value for certain column using col-based storege struct.
  virtual KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                             char** value) = 0;
  virtual KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                               TsBitmap& bitmap) = 0;
  virtual KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                                TSSlice& value) = 0;
  virtual bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) = 0;
  // if just get timestamp , this function return fast.
  virtual timestamp64 GetTS(int row_num) = 0;

  virtual uint64_t* GetLSNAddr(int row_num) = 0;

  virtual KStatus GetAggResult(uint32_t begin_row_idx, uint32_t row_num, uint32_t blk_col_idx,
                               const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type,
                               const std::vector<Sumfunctype>& agg_types, uint64_t* count_ptr,
                               void* max_addr, void* min_addr, void* sum_addr);

  virtual KStatus GetLastInfo(uint32_t begin_row_idx, uint32_t row_num, uint32_t col_id,
    const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type,
    int64_t* out_ts, int* out_row_idx);
};

struct TsBlockSpan {
 private:
  std::shared_ptr<TsBlock> block_ = nullptr;
  TSEntityID entity_id_ = 0;
  int start_row_ = 0, nrow_ = 0;
  TSBlkDataTypeConvert convert_;

  friend TSBlkDataTypeConvert;

 public:
  TsBlockSpan() = default;

  TsBlockSpan(TSTableID table_id, uint32_t table_version, TSEntityID entity_id,
              std::shared_ptr<TsBlock> block, int start, int nrow);

  bool operator<(const TsBlockSpan& other) const;

  TSEntityID GetEntityID() const;
  int GetRowNum() const;
  int GetStartRow() const;
  std::shared_ptr<TsBlock> GetTsBlock() const;
  TSTableID GetTableID() const;
  uint32_t GetTableVersion() const;
  timestamp64 GetTS(uint32_t row_idx) const;
  uint64_t* GetLSNAddr(int row_idx) const;

  // if just get timestamp, these function return fast.
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts);

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type,
                             char** value, TsBitmap& bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
    const AttributeInfo& dest_type, DataFlags& flag, TSSlice& data);

  KStatus GetAggResult(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
    const AttributeInfo& dest_type, std::vector<Sumfunctype> agg_types, uint64_t* count_ptr,
    void* max_addr, void* min_addr, void* sum_addr);

  KStatus GetLastInfo(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
    const AttributeInfo& dest_type, int64_t* out_ts, int* out_row_idx);

  void SplitFront(int row_num, TsBlockSpan* front_span);

  void SplitBack(int row_num, TsBlockSpan* back_span);

  void Truncate(int row_num);

  void Clear();
};
}  // namespace kwdbts
