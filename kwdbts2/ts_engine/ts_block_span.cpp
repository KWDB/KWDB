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

KStatus TsBlock::GetAggResult(uint32_t begin_row_idx, uint32_t row_num, uint32_t blk_col_idx,
                               const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type,
                               const Sumfunctype agg_type, TSSlice& agg_data, bool& is_overflow) {
  TSBlkDataTypeConvert convert(this, begin_row_idx, row_num);

  if (!isVarLenType(dest_type.type)) {
    char* value = nullptr;
    TsBitmap bitmap;
    auto s = convert.GetFixLenColAddr(blk_col_idx, schema, dest_type, &value, bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
      return s;
    }

    AggCalculatorV2 calc(value, &bitmap, static_cast<DATATYPE>(dest_type.type), dest_type.size, row_num);
    uint64_t local_count = 0;

    s = calc.MergeAggResultFromBlock(agg_data, agg_type, blk_col_idx, is_overflow);
    if (s != KStatus::SUCCESS) {
      return s;
    }
  } else {
    std::vector<string> var_rows;
    KStatus ret;
    for (int i = 0; i < row_num; ++i) {
      TSSlice slice;
      DataFlags flag;
      ret = convert.GetVarLenTypeColAddr(i, blk_col_idx, schema, dest_type, flag, slice);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetVarLenTypeColAddr failed.");
        return ret;
      }
      if (flag == DataFlags::kValid) {
        var_rows.emplace_back(slice.data, slice.len);
      }
    }
    VarColAggCalculatorV2 calc(var_rows);
    calc.MergeAggResultFromBlock(agg_data, agg_type);
  }
  return KStatus::SUCCESS;
}

KStatus TsBlock::UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
  return KStatus::SUCCESS;
}

KStatus TsBlock::GetFirstAndLastInfo(
    uint32_t begin_row_idx,
    uint32_t row_num,
    uint32_t col_id,
    const std::vector<AttributeInfo>& schema,
    const AttributeInfo& dest_type,
    Sumfunctype agg_type,
    int64_t* out_ts,
    int* out_row_idx) {

  TSBlkDataTypeConvert convert(this, begin_row_idx, row_num);

  bool is_first = (
      agg_type == Sumfunctype::FIRST || agg_type == Sumfunctype::FIRSTTS ||
      agg_type == Sumfunctype::FIRST_ROW || agg_type == Sumfunctype::FIRSTROWTS);

  bool is_row_based = (
      agg_type == Sumfunctype::FIRST_ROW || agg_type == Sumfunctype::FIRSTROWTS ||
      agg_type == Sumfunctype::LAST_ROW  || agg_type == Sumfunctype::LASTROWTS);

  int64_t best_ts = is_first ? INT64_MAX : INT64_MIN;
  int best_idx = -1;

  // Unified scan logic: forward for FIRST, backward for LAST
  auto scan = [&](auto getter) {
    if (is_first) {
      for (int i = 0; i < row_num; ++i) {
        if (!getter(i)) continue;
        int64_t ts = GetTS(begin_row_idx + i);
        best_ts = ts;
        best_idx = i;
        break;  // earliest match found
      }
    } else {
      for (int i = row_num - 1; i >= 0; --i) {
        if (!getter(i)) continue;
        int64_t ts = GetTS(begin_row_idx + i);
        best_ts = ts;
        best_idx = i;
        break;  // latest match found
      }
    }
  };

  if (is_row_based) {
    // All rows are eligible regardless of column nullability
    scan([](int) { return true; });
  } else if (!isVarLenType(dest_type.type)) {
    // Fixed-length column: use bitmap to check nulls
    char* value = nullptr;
    TsBitmap bitmap;
    KStatus s = convert.GetFixLenColAddr(col_id, schema, dest_type, &value, bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
      return s;
    }

    scan([&](int i) {
      return bitmap[i] != DataFlags::kNull;
    });
  } else {
    // Variable-length column: use per-row flag to check nulls
    scan([&](int i) {
      TSSlice slice;
      DataFlags flag;
      KStatus ret = convert.GetVarLenTypeColAddr(i, col_id, schema, dest_type, flag, slice);
      return ret == KStatus::SUCCESS && flag == DataFlags::kValid;
    });
  }

  if (out_ts) *out_ts = best_ts;
  if (out_row_idx) *out_row_idx = best_idx;
  return KStatus::SUCCESS;
}

TsBlockSpan::TsBlockSpan(TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow_)  // NOLINT(runtime/init)
    : entity_id_(entity_id), block_(block), start_row_(start), nrow_(nrow_), convert_(*this) {  // NOLINT(runtime/init)
  assert(nrow_ >= 1);
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

KStatus TsBlockSpan::GetColBitmap(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema, TsBitmap& bitmap) {
  return convert_.GetColBitmap(blk_col_idx, schema, bitmap);
}

// dest type is fixed len datatype.
KStatus TsBlockSpan::GetFixLenColAddr(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, char** value, TsBitmap& bitmap) {
  return convert_.GetFixLenColAddr(blk_col_idx, schema, dest_type, value, bitmap);
}

// dest type is varlen datatype.
KStatus TsBlockSpan::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, DataFlags& flag, TSSlice& data) {
  return convert_.GetVarLenTypeColAddr(row_idx, blk_col_idx, schema, dest_type, flag, data);
}

KStatus TsBlockSpan::GetAggResult(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, const Sumfunctype agg_type, TSSlice& agg_data, bool& is_overflow) {
  return block_->GetAggResult(
    start_row_, nrow_, blk_col_idx, schema, dest_type, agg_type, agg_data, is_overflow);
}

KStatus TsBlockSpan::UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
  return block_->UpdateFirstLastCandidates(ts_scan_cols, schema, first_col_idxs, last_col_idxs, candidates);
}

KStatus TsBlockSpan::GetFirstAndLastInfo(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, Sumfunctype agg_type, int64_t* out_ts, int* out_row_idx) {
  return block_->GetFirstAndLastInfo(
    start_row_, nrow_, blk_col_idx, schema, dest_type, agg_type, out_ts, out_row_idx);
}

void TsBlockSpan::SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span) {
  assert(row_num <= nrow_);
  front_span = make_shared<TsBlockSpan>(entity_id_, block_, start_row_, row_num);
  // change current span info
  start_row_ += row_num;
  nrow_ -= row_num;
}

void TsBlockSpan::SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span) {
  assert(row_num <= nrow_);
  back_span = make_shared<TsBlockSpan>(entity_id_, block_, start_row_ + nrow_ - row_num, row_num);
  convert_ = TSBlkDataTypeConvert(*this);
  // change current span info
  nrow_ -= row_num;
}

void TsBlockSpan::Truncate(int row_num) {
  start_row_ += row_num;
  nrow_ -= row_num;
  convert_ = TSBlkDataTypeConvert(*this);
}

void TsBlockSpan::Clear() {
  block_ = nullptr;
  entity_id_ = 0;
  start_row_ = 0;
  nrow_ = 0;
  convert_ = {};
}

}  // namespace kwdbts
