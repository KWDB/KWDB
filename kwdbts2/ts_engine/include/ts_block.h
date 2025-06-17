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

class TsBlockSpan;

struct AggCandidate {
  int64_t ts;
  int row_idx;
  shared_ptr<TsBlockSpan> blk_span{nullptr};
};

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

  /*
  * Pre agg includes count/min/max/sum, it doesn't have pre-agg by default
  */
  virtual bool HasPreAgg(uint32_t begin_row_idx, uint32_t row_num);
  virtual KStatus GetPreCount(uint32_t blk_col_idx, uint16_t& count);
  virtual KStatus GetPreSum(uint32_t blk_col_idx, int32_t size, void* &pre_sum, bool& is_overflow);
  virtual KStatus GetPreMax(uint32_t blk_col_idx, void* &pre_max);
  virtual KStatus GetPreMin(uint32_t blk_col_idx, int32_t size, void* &pre_min);
  virtual KStatus GetVarPreMax(uint32_t blk_col_idx, TSSlice& pre_max);
  virtual KStatus GetVarPreMin(uint32_t blk_col_idx, TSSlice& pre_min);
  KStatus UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates);
};

struct TsBlockSpan {
 private:
  std::shared_ptr<TsBlock> block_ = nullptr;
  TSEntityID entity_id_ = 0;
  int start_row_ = 0, nrow_ = 0;
  bool has_pre_agg_{false};
  std::shared_ptr<TSBlkDataTypeConvert> convert_;

  friend TSBlkDataTypeConvert;

 public:
  TsBlockSpan() = default;

  TsBlockSpan(TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
              std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr = nullptr,
              uint32_t scan_version = 0,
              const std::vector<uint32_t>& ts_scan_cols = {});

  bool operator<(const TsBlockSpan& other) const;

  KStatus SetConvertVersion(std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version = 0,
                            std::vector<uint32_t> ts_scan_cols = {});

  TSEntityID GetEntityID() const;
  int GetRowNum() const;
  int GetStartRow() const;
  std::shared_ptr<TsBlock> GetTsBlock() const;
  TSTableID GetTableID() const;
  uint32_t GetTableVersion() const;
  timestamp64 GetTS(uint32_t row_idx) const;
  timestamp64 GetFirstTS() const;
  timestamp64 GetLastTS() const;
  uint64_t* GetLSNAddr(int row_idx) const;

  // if just get timestamp, these function return fast.
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts);

  bool IsColExist(uint32_t scan_idx);
  bool IsColNotNull(uint32_t scan_idx);
  bool IsSameType(uint32_t scan_idx);
  KStatus GetColBitmap(uint32_t scan_idx, TsBitmap& bitmap);

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t scan_idx, char** value, TsBitmap& bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx, DataFlags& flag, TSSlice& data);

  KStatus GetCount(uint32_t scan_idx, uint32_t& count);
  KStatus GetSum(uint32_t scan_idx, void* &pre_sum, bool& is_overflow);
  KStatus GetMax(uint32_t scan_idx, void* &pre_max);
  KStatus GetMin(uint32_t scan_idx, void* &pre_min);
  KStatus GetVarMax(uint32_t scan_idx, TSSlice& pre_max);
  KStatus GetVarMin(uint32_t scan_idx, TSSlice& pre_min);

  bool HasPreAgg();
  KStatus GetPreCount(uint32_t scan_idx, uint16_t& count);
  KStatus GetPreSum(uint32_t scan_idx, void* &pre_sum, bool& is_overflow);
  KStatus GetPreMax(uint32_t scan_idx, void* &pre_max);
  KStatus GetPreMin(uint32_t scan_idx, void* &pre_min);
  KStatus GetVarPreMax(uint32_t scan_idx, TSSlice& pre_max);
  KStatus GetVarPreMin(uint32_t scan_idx, TSSlice& pre_min);

  KStatus UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates);

  void SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span);

  void SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span);

  void Truncate(int row_num);

  void Clear();
};
}  // namespace kwdbts
