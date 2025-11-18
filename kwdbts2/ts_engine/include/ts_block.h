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
#include <string>
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
  virtual uint32_t GetBlockVersion() const = 0;
  virtual TSTableID GetTableId() = 0;
  virtual uint32_t GetTableVersion() = 0;
  virtual size_t GetRowNum() = 0;
  // if has three rows, this return three value for certain column using col-based storege struct.
  virtual KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                             char** value) = 0;
  virtual KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                               std::unique_ptr<TsBitmapBase>* bitmap) = 0;
  virtual KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                                TSSlice& value) = 0;
  virtual bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema) = 0;
  // if just get timestamp , this function return fast.
  virtual timestamp64 GetTS(int row_num) = 0;

  virtual timestamp64 GetFirstTS() = 0;

  virtual timestamp64 GetLastTS() = 0;

  virtual void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) = 0;

  virtual uint64_t GetFirstOSN() = 0;

  virtual uint64_t GetLastOSN() = 0;

  virtual const uint64_t* GetOSNAddr(int row_num) = 0;

  virtual KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, std::string& data) = 0;

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
                                                const std::vector<AttributeInfo>* schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates);
};

class TsBlockSpan {
 private:
  std::shared_ptr<TsBlock> block_ = nullptr;
  uint32_t vgroup_id_ = 0;
  TSEntityID entity_id_ = 0;
  int start_row_ = 0, nrow_ = 0;
  bool has_pre_agg_{false};
  const std::vector<AttributeInfo>* scan_attrs_;  // used only if block version equals scan version.

 public:
  std::shared_ptr<TSBlkDataTypeConvert> convert_;

  friend TSBlkDataTypeConvert;

 public:
  TsBlockSpan() = default;

  TsBlockSpan(uint32_t vgroup_id, TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
              std::shared_ptr<TSBlkDataTypeConvert>& convert,
              uint32_t scan_version, const std::vector<AttributeInfo>* scan_attrs);

  TsBlockSpan(const TsBlockSpan& src, std::shared_ptr<TsBlock> block, int start, int nrow, TSEntityID entity_id = 0);

  static KStatus GenDataConvert(uint32_t blk_version, uint32_t scan_version,
    const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr, std::shared_ptr<TSBlkDataTypeConvert>& ret);

  static KStatus MakeNewBlockSpan(TsBlockSpan* src_blk_span, uint32_t vgroup_id,
    TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
    uint32_t scan_version, const std::vector<AttributeInfo>* scan_attrs,
    const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr, std::shared_ptr<TsBlockSpan>& ret);
  bool operator<(const TsBlockSpan& other) const;
  void operator=(TsBlockSpan& other) = delete;

  void Clear() {
    assert(block_ != nullptr);
    block_ = nullptr;
    entity_id_ = 0;
    start_row_ = 0;
    nrow_ = 0;
    convert_ = nullptr;
  }

  uint32_t GetBlockVersion() const { return block_->GetBlockVersion(); }
  uint32_t GetVGroupID() const { return vgroup_id_; }
  TSEntityID GetEntityID() const { return entity_id_; }
  int GetRowNum() const { return nrow_; }
  int GetStartRow() const { return start_row_; }
  int GetColCount() const { return scan_attrs_->size(); }
  std::shared_ptr<TsBlock> GetTsBlock() const { return block_; }
  TSTableID GetTableID() const { return block_->GetTableId(); }
  uint32_t GetTableVersion() const { return block_->GetTableVersion(); }
  timestamp64 GetTS(uint32_t row_idx) const { return block_->GetTS(start_row_ + row_idx); }
  timestamp64 GetFirstTS() const {
    if (start_row_ == 0) {
      return block_->GetFirstTS();
    } else {
      return block_->GetTS(start_row_);
    }
  }
  timestamp64 GetLastTS() const {
    if (start_row_ + nrow_ == block_->GetRowNum()) {
      return block_->GetLastTS();
    } else {
      return block_->GetTS(start_row_ + nrow_ - 1);
    }
  }
  void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) const {
    if (nrow_ == block_->GetRowNum()) {
      block_->GetMinAndMaxOSN(min_osn, max_osn);
    } else {
      min_osn = UINT64_MAX;
      max_osn = 0;
      for (int i = start_row_; i < start_row_ + nrow_; i++) {
        uint64_t cur_osn = *block_->GetOSNAddr(i);
        if (cur_osn < min_osn) {
          min_osn = cur_osn;
        }
        if (cur_osn > max_osn) {
          max_osn = cur_osn;
        }
      }
    }
  }
  uint64_t GetFirstOSN() const {
    if (start_row_ == 0) {
      return block_->GetFirstOSN();
    } else {
      return *block_->GetOSNAddr(start_row_);
    }
  }
  uint64_t GetLastOSN() const {
    if (start_row_ + nrow_ == block_->GetRowNum()) {
      return block_->GetLastOSN();
    } else {
      return *block_->GetOSNAddr(start_row_ + nrow_ - 1);
    }
  }
  const uint64_t* GetOSNAddr(int row_idx) const { return block_->GetOSNAddr(start_row_ + row_idx); }

  // convert value to compressed entity block data
  KStatus BuildCompressedData(std::string& data);
  KStatus GetCompressData(std::string& data);

  // if just get timestamp, these function return fast.
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts);

  bool IsColExist(uint32_t scan_idx) {
    if (!convert_) {
      return scan_idx <= scan_attrs_->size() - 1;
    }
    return convert_->IsColExist(scan_idx);
  }
  bool IsColNotNull(uint32_t scan_idx) {
    if (!convert_) {
      return (*scan_attrs_)[scan_idx].isFlag(AINFO_NOT_NULL);
    }
    return convert_->IsColNotNull(scan_idx);
  }
  bool IsSameType(uint32_t scan_idx) {
    if (!convert_) {
      return true;
    }
    return convert_->IsSameType(scan_idx);
  }
  bool IsVarLenType(uint32_t scan_idx) {
    if (!convert_) {
      return isVarLenType((*scan_attrs_)[scan_idx].type);
    }
    return convert_->IsVarLenType(scan_idx);
  }
  int32_t GetColSize(uint32_t scan_idx) {
    if (!convert_) {
      return (*scan_attrs_)[scan_idx].size;
    }
    return convert_->GetColSize(scan_idx);
  }
  int32_t GetColType(uint32_t scan_idx) {
    if (!convert_) {
      return (*scan_attrs_)[scan_idx].type;
    }
    return convert_->GetColType(scan_idx);
  }

  KStatus GetColBitmap(uint32_t scan_idx, std::unique_ptr<TsBitmapBase>* bitmap);
  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t scan_idx, char** value, std::unique_ptr<TsBitmapBase>* bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx, DataFlags& flag, TSSlice& data);
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx, TSSlice& data);

  KStatus GetCount(uint32_t scan_idx, uint32_t& count);

  bool HasPreAgg() {
    return has_pre_agg_;
  }
  KStatus GetPreCount(uint32_t scan_idx, uint16_t& count) {
    if (!convert_) {
      return block_->GetPreCount(scan_idx, count);
    }
    return convert_->GetPreCount(this, scan_idx, count);
  }
  KStatus GetPreSum(uint32_t scan_idx, void* &pre_sum, bool& is_overflow) {
    if (!convert_) {
      int32_t size = (*scan_attrs_)[scan_idx].size;
      return block_->GetPreSum(scan_idx, size, pre_sum, is_overflow);
    }
    int32_t size = (*convert_->version_conv_->blk_attrs_)[scan_idx].size;
    return convert_->GetPreSum(this, scan_idx, size, pre_sum, is_overflow);
  }
  KStatus GetPreMax(uint32_t scan_idx, void* &pre_max) {
    if (!convert_) {
      return block_->GetPreMax(scan_idx, pre_max);
    }
    return convert_->GetPreMax(this, scan_idx, pre_max);
  }
  KStatus GetPreMin(uint32_t scan_idx, void* &pre_min) {
    if (!convert_) {
      int32_t size = (*scan_attrs_)[scan_idx].size;
      return block_->GetPreMin(scan_idx, size, pre_min);
    }
    int32_t size = (*convert_->version_conv_->blk_attrs_)[scan_idx].size;
    return convert_->GetPreMin(this, scan_idx, size, pre_min);
  }
  KStatus GetVarPreMax(uint32_t scan_idx, TSSlice& pre_max) {
    if (!convert_) {
      return block_->GetVarPreMax(scan_idx, pre_max);
    }
    return convert_->GetVarPreMax(this, scan_idx, pre_max);
  }
  KStatus GetVarPreMin(uint32_t scan_idx, TSSlice& pre_min) {
    if (!convert_) {
      return block_->GetVarPreMin(scan_idx, pre_min);
    }
    return convert_->GetVarPreMin(this, scan_idx, pre_min);
  }

  KStatus UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>* schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
    return block_->UpdateFirstLastCandidates(ts_scan_cols, schema, first_col_idxs, last_col_idxs, candidates);
  }

  void SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span);

  void SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span);

  void TrimBack(int row_num) {
    assert(row_num <= nrow_);
    assert(block_ != nullptr);
    nrow_ -= row_num;
  }

  void TrimFront(int row_num) {
    assert(row_num <= nrow_);
    assert(block_ != nullptr);
    start_row_ += row_num;
    nrow_ -= row_num;
  }
};
}  // namespace kwdbts
