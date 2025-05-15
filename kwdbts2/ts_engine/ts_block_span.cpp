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

KStatus TsBlock::GetAggResult(uint32_t begin_row_idx, uint32_t row_num, uint32_t col_id,
                               const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type,
                               const std::vector<Sumfunctype>& agg_types, uint64_t* count_ptr,
                               void* max_addr, void* min_addr, void* sum_addr) {
  TSBlkDataTypeConvert convert(this, begin_row_idx, row_num);

  if (!isVarLenType(dest_type.type)) {
    bool need_max = false, need_min = false, need_sum = false, need_count = false;
    for (auto agg_type : agg_types) {
      switch (agg_type) {
        case Sumfunctype::MAX:   need_max = true; break;
        case Sumfunctype::MIN:   need_min = true; break;
        case Sumfunctype::SUM:   need_sum = true; break;
        case Sumfunctype::COUNT: need_count = true; break;
        default:
          LOG_ERROR("Unsupported aggregation type: %d", static_cast<int>(agg_type));
          return KStatus::FAIL;
      }
    }

    char* value = nullptr;
    TsBitmap bitmap;
    auto s = convert.GetFixLenColAddr(col_id, schema, dest_type, &value, bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
      return s;
    }

    AggCalculatorV2 calc(value, &bitmap, static_cast<DATATYPE>(dest_type.type), dest_type.size, row_num);
    uint64_t local_count = 0;

    bool overflow = calc.MergeAggResultFromBlock(
        need_count ? *count_ptr : local_count,
        need_max ? max_addr : nullptr,
        need_min ? min_addr : nullptr,
        need_sum ? sum_addr : nullptr);

    assert(!overflow);

    return KStatus::SUCCESS;
  } else {
    bool need_count = false;
    for (auto agg_type : agg_types) {
      if (agg_type == Sumfunctype::COUNT) {
        need_count = true;
      } else {
        LOG_ERROR("VarLenType only supports COUNT currently: type = %d", dest_type.type);
        return KStatus::FAIL;
      }
    }

    if (!need_count) {
      LOG_ERROR("No supported aggregation type for VarLenType");
      return KStatus::FAIL;
    }

    std::vector<std::string> var_mem(row_num);
    unsigned char* bitmap = static_cast<unsigned char*>(malloc(KW_BITMAP_SIZE(row_num)));
    if (bitmap == nullptr) {
      return KStatus::FAIL;
    }
    memset(bitmap, 0x00, KW_BITMAP_SIZE(row_num));
    for (uint32_t i = 0; i < row_num; ++i) {
      TSSlice var_data;
      DataFlags flag;

      auto s = convert.GetVarLenTypeColAddr(i + begin_row_idx, col_id, schema, dest_type, flag, var_data);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetVarLenTypeColAddr failed at row %u", i);
        return s;
      }
      if (flag != DataFlags::kValid) {
        set_null_bitmap(bitmap, i);
      }
    }

    VarColAggCalculatorV2 calc(var_mem, bitmap, dest_type.size, row_num);
    std::string dummy_max, dummy_min;
    uint16_t local_count = 0;
    calc.CalcAllAgg(dummy_max, dummy_min, local_count);

    if (count_ptr) {
      *count_ptr += local_count;
    }
    return KStatus::SUCCESS;
  }
}


KStatus TsBlock::GetLastInfo(uint32_t begin_row_idx,
                             uint32_t row_num,
                             uint32_t col_id,
                             const std::vector<AttributeInfo>& schema,
                             const AttributeInfo& dest_type,
                             int64_t* out_ts,
                             int* out_row_idx) {
  TSBlkDataTypeConvert convert(this, begin_row_idx, row_num);
  if (out_ts) *out_ts = INT64_MIN;
  if (out_row_idx) *out_row_idx = -1;

  if (!isVarLenType(dest_type.type)) {
    char* value = nullptr;
    TsBitmap bitmap;
    auto s = convert.GetFixLenColAddr(col_id, schema, dest_type, &value, bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
      return s;
    }

    int64_t max_ts = INT64_MIN;
    int best_idx = -1;
    for (int i = 0; i < row_num; ++i) {
      if (bitmap[i] == DataFlags::kNull) continue;
      int64_t ts = GetTS(i);
      if (ts > max_ts) {
        max_ts = ts;
        best_idx = i;
      }
    }

    if (out_ts) *out_ts = max_ts;
    if (out_row_idx) *out_row_idx = best_idx;
    return KStatus::SUCCESS;
  }

  LOG_ERROR("VarLenType not supported in GetLastInfo: type = %d", dest_type.type);
  return KStatus::FAIL;
}

TsBlockSpan::TsBlockSpan(TSTableID table_id, uint32_t table_version, TSEntityID entity_id,
            std::shared_ptr<TsBlock> block, int start, int nrow_)  // NOLINT(runtime/init)
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

uint64_t* TsBlockSpan::GetLSNAddr(int row_idx) const {
  return block_->GetLSNAddr(start_row_ + row_idx);
}

void TsBlockSpan::GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
  *min_ts = block_->GetTS(start_row_);
  *max_ts = block_->GetTS(start_row_ + nrow_ - 1);
}

// dest type is fixed len datatype.
KStatus TsBlockSpan::GetFixLenColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, char** value, TsBitmap& bitmap) {
  return convert_.GetFixLenColAddr(col_id, schema, dest_type, value, bitmap);
}

// dest type is varlen datatype.
KStatus TsBlockSpan::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, DataFlags& flag, TSSlice& data) {
  return convert_.GetVarLenTypeColAddr(row_idx, col_idx, schema, dest_type, flag, data);
}

KStatus TsBlockSpan::GetAggResult(uint32_t col_id, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, std::vector<Sumfunctype> agg_types, uint64_t* count_ptr,
 void* max_addr, void* min_addr, void* sum_addr) {
  return block_->GetAggResult(
    start_row_, nrow_, col_id, schema, dest_type, agg_types, count_ptr, max_addr, min_addr, sum_addr);
}

KStatus TsBlockSpan::GetLastInfo(uint32_t col_id, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& dest_type, int64_t* out_ts, int* out_row_idx) {
  return block_->GetLastInfo(start_row_, nrow_, col_id, schema, dest_type, out_ts, out_row_idx);
}

void TsBlockSpan::SplitFront(int row_num, TsBlockSpan* front_span) {
  assert(row_num <= nrow_);
  front_span->block_ = block_;
  front_span->entity_id_ = entity_id_;
  front_span->start_row_ = start_row_;
  front_span->nrow_ = row_num;
  front_span->convert_ = TSBlkDataTypeConvert(*front_span);
  convert_ = TSBlkDataTypeConvert(*this);
  // change current span info
  start_row_ += row_num;
  nrow_ -= row_num;
}

void TsBlockSpan::SplitBack(int row_num, TsBlockSpan* back_span) {
  assert(row_num <= nrow_);
  back_span->block_ = block_;
  back_span->entity_id_ = entity_id_;
  back_span->start_row_ = start_row_ + nrow_ - row_num;
  back_span->nrow_ = row_num;
  back_span->convert_ = TSBlkDataTypeConvert(*back_span);
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
