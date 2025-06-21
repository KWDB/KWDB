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

#include "ts_agg.h"
#include "ts_block.h"
#include "ts_blkspan_type_convert.h"
#include "ts_iterator_v2_impl.h"

namespace kwdbts {
bool TsBlock::HasPreAgg(uint32_t begin_row_idx, uint32_t row_num) {
  return false;
}

KStatus TsBlock::GetPreCount(uint32_t blk_col_idx, uint16_t& count) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetPreSum(uint32_t blk_col_idx, int32_t size, void* &pre_sum, bool& is_overflow) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetPreMax(uint32_t blk_col_idx, void* &pre_max) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetPreMin(uint32_t blk_col_idx, int32_t size, void* &pre_min) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetVarPreMax(uint32_t blk_col_idx, TSSlice& pre_max) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetVarPreMin(uint32_t blk_col_idx, TSSlice& pre_min) {
  return KStatus::FAIL;
}

KStatus TsBlock::UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
  return KStatus::SUCCESS;
}

TsBlockSpan::TsBlockSpan(TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
                         std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version)
    : entity_id_(entity_id), block_(block), start_row_(start), nrow_(nrow),
      convert_(*this, tbl_schema_mgr, scan_version == 0 ? block->GetTableVersion() : scan_version) {
  assert(nrow_ >= 1);
  has_pre_agg_ = block_->HasPreAgg(start_row_, nrow_);
}

bool TsBlockSpan::operator<(const TsBlockSpan& other) const {
  if (entity_id_ != other.entity_id_) {
    return entity_id_ < other.entity_id_;
  } else {
    timestamp64 ts = block_->GetTS(start_row_);
    timestamp64 other_ts = other.block_->GetTS(other.start_row_);
    if (ts != other_ts) {
      return ts < other_ts;
    } else {
      uint64_t seq_no = *block_->GetLSNAddr(start_row_);
      uint64_t other_seq_no = *other.block_->GetLSNAddr(other.start_row_);
      return seq_no > other_seq_no;
    }
  }
}

void TsBlockSpan::operator=(TsBlockSpan &other) {
  this->entity_id_ = other.entity_id_;
  this->block_ = other.block_;
  this->start_row_ = other.start_row_;
  this->nrow_ = other.nrow_;
  this->has_pre_agg_ = other.has_pre_agg_;
  this->convert_ = std::move(other.convert_);
}

TSEntityID TsBlockSpan::GetEntityID() const {
  return entity_id_;
}

int TsBlockSpan::GetRowNum() const {
  return nrow_;
}

int TsBlockSpan::GetStartRow() const {
  return start_row_;
}

std::shared_ptr<TsBlock> TsBlockSpan::GetTsBlock() const {
  return block_;
}

TSTableID TsBlockSpan::GetTableID() const {
  return block_->GetTableId();
}

uint32_t TsBlockSpan::GetTableVersion() const {
  return block_->GetTableVersion();
}

timestamp64 TsBlockSpan::GetTS(uint32_t row_idx) const {
  return block_->GetTS(start_row_ + row_idx);
}

timestamp64 TsBlockSpan::GetFirstTS() const {
  return block_->GetTS(start_row_);
}

timestamp64 TsBlockSpan::GetLastTS() const {
  return block_->GetTS(start_row_ + nrow_ - 1);
}

uint64_t* TsBlockSpan::GetLSNAddr(int row_idx) const {
  return block_->GetLSNAddr(start_row_ + row_idx);
}

void TsBlockSpan::GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
  *min_ts = block_->GetTS(start_row_);
  *max_ts = block_->GetTS(start_row_ + nrow_ - 1);
}

bool TsBlockSpan::IsColExist(uint32_t scan_idx) {
  return convert_.IsColExist(scan_idx);
}

bool TsBlockSpan::IsColNotNull(uint32_t scan_idx) {
  return convert_.IsColNotNull(scan_idx);
}

bool TsBlockSpan::IsSameType(uint32_t scan_idx) {
  return convert_.IsSameType(scan_idx);
}

int32_t TsBlockSpan::GetColSize(uint32_t scan_idx) {
  return convert_.GetColSize(scan_idx);
}

int32_t TsBlockSpan::GetColType(uint32_t scan_idx) {
  return convert_.GetColType(scan_idx);
}

bool TsBlockSpan::IsVarLenType(uint32_t scan_idx) {
  return convert_.IsVarLenType(scan_idx);
}

KStatus TsBlockSpan::GetColBitmap(uint32_t scan_idx, TsBitmap& bitmap) {
  return convert_.GetColBitmap(scan_idx, bitmap);
}

// dest type is fixed len datatype.
KStatus TsBlockSpan::GetFixLenColAddr(uint32_t scan_idx, char** value, TsBitmap& bitmap) {
  return convert_.GetFixLenColAddr(scan_idx, value, bitmap);
}

// dest type is varlen datatype.
KStatus TsBlockSpan::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx, DataFlags& flag, TSSlice& data) {
  return convert_.GetVarLenTypeColAddr(row_idx, scan_idx, flag, data);
}

KStatus TsBlockSpan::GetCount(uint32_t scan_idx, uint32_t& count) {
  TsBitmap bitmap;
  auto s = GetColBitmap(scan_idx, bitmap);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  for (int row_idx = start_row_; row_idx < start_row_ + nrow_; ++row_idx) {
    if (bitmap[row_idx] != DataFlags::kValid) {
      continue;
    }
    ++count;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSpan::GetSum(uint32_t scan_idx, void *&pre_sum, bool &is_overflow) {
}

KStatus TsBlockSpan::GetMax(uint32_t scan_idx, void *&pre_max) {
}

KStatus TsBlockSpan::GetMin(uint32_t scan_idx, void *&pre_min) {
}

KStatus TsBlockSpan::GetVarMax(uint32_t scan_idx, TSSlice &pre_max) {
}

KStatus TsBlockSpan::GetVarMin(uint32_t scan_idx, TSSlice &pre_min) {
}

bool TsBlockSpan::HasPreAgg() {
  return has_pre_agg_;
}

KStatus TsBlockSpan::GetPreCount(uint32_t scan_idx, uint16_t& count) {
  return convert_.GetPreCount(scan_idx, count);
}

KStatus TsBlockSpan::GetPreSum(uint32_t scan_idx, void* &pre_sum, bool& is_overflow) {
  int32_t size = convert_.version_conv_->blk_attrs_[scan_idx].size;
  return convert_.GetPreSum(scan_idx, size, pre_sum, is_overflow);
}

KStatus TsBlockSpan::GetPreMax(uint32_t scan_idx, void* &pre_max) {
  return convert_.GetPreMax(scan_idx, pre_max);
}

KStatus TsBlockSpan::GetPreMin(uint32_t scan_idx, void* &pre_min) {
  int32_t size = convert_.version_conv_->blk_attrs_[scan_idx].size;
  return convert_.GetPreMin(scan_idx, size, pre_min);
}

KStatus TsBlockSpan::GetVarPreMax(uint32_t scan_idx, TSSlice& pre_max) {
  return convert_.GetVarPreMax(scan_idx, pre_max);
}

KStatus TsBlockSpan::GetVarPreMin(uint32_t scan_idx, TSSlice& pre_min) {
  return convert_.GetVarPreMin(scan_idx, pre_min);
}

KStatus TsBlockSpan::UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
  return block_->UpdateFirstLastCandidates(ts_scan_cols, schema, first_col_idxs, last_col_idxs, candidates);
}

void TsBlockSpan::SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span) {
  assert(row_num <= nrow_);
  front_span = make_shared<TsBlockSpan>(entity_id_, block_, start_row_, row_num,
                                        convert_.tbl_schema_mgr_, convert_.version_conv_->scan_version_);
  // change current span info
  start_row_ += row_num;
  nrow_ -= row_num;
  convert_ = TSBlkDataTypeConvert(*this, convert_.tbl_schema_mgr_, convert_.version_conv_->scan_version_);
}

void TsBlockSpan::SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span) {
  assert(row_num <= nrow_);
  back_span = make_shared<TsBlockSpan>(entity_id_, block_, start_row_ + nrow_ - row_num, row_num,
                                       convert_.tbl_schema_mgr_);
  // change current span info
  nrow_ -= row_num;
  convert_ = TSBlkDataTypeConvert(*this, convert_.tbl_schema_mgr_, convert_.version_conv_->scan_version_);
}

void TsBlockSpan::Truncate(int row_num) {
  start_row_ += row_num;
  nrow_ -= row_num;
  convert_ = TSBlkDataTypeConvert(*this, convert_.tbl_schema_mgr_, convert_.version_conv_->scan_version_);
}

void TsBlockSpan::Clear() {
  block_ = nullptr;
  entity_id_ = 0;
  start_row_ = 0;
  nrow_ = 0;
  convert_ = {};
}

}  // namespace kwdbts
