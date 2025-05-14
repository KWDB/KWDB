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

namespace kwdbts {

KStatus TsBlock::GetAggResult(uint32_t begin_row_idx,
                               uint32_t row_num,
                               uint32_t col_id,
                               const std::vector<AttributeInfo>& schema,
                               const AttributeInfo& dest_type,
                               std::vector<Sumfunctype> agg_types,
                               std::vector<TSSlice>& agg_data) {
  TSBlkDataTypeConvert convert(this, begin_row_idx, row_num);
  agg_data.clear();

  if (!isVarLenType(dest_type.type)) {
    bool need_max = false, need_min = false, need_sum = false;
    for (auto agg_type : agg_types) {
      switch (agg_type) {
        case Sumfunctype::MAX: need_max = true; break;
        case Sumfunctype::MIN: need_min = true; break;
        case Sumfunctype::SUM: need_sum = true; break;
        case Sumfunctype::COUNT: break;
        default:
          LOG_ERROR("Unsupported aggregation type: %d", static_cast<int>(agg_type));
          return KStatus::FAIL;
      }
    }

    // size_t value_size = dest_type.size;
    size_t value_size = sizeof(int64_t);;
    size_t total_size = 2 + 3 * value_size;
    char* allc_mem = reinterpret_cast<char*>(malloc(total_size));
    if (!allc_mem) {
      LOG_ERROR("Failed to allocate memory for aggregation result.");
      return KStatus::FAIL;
    }
    memset(allc_mem, 0, total_size);

    char* value = nullptr;
    TsBitmap bitmap;
    auto s = convert.GetFixLenColAddr(col_id, schema, dest_type, &value, bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
      free(allc_mem);
      return s;
    }

    AggCalculatorV2 calc(value, &bitmap, static_cast<DATATYPE>(dest_type.type), dest_type.size, row_num);
    bool overflow = calc.CalcAllAgg(
      *reinterpret_cast<uint16_t*>(allc_mem),
      need_max ? allc_mem + 2 : nullptr,
      need_min ? allc_mem + 2 + value_size : nullptr,
      need_sum ? allc_mem + 2 + 2 * value_size : nullptr);
    assert(!overflow);

    for (auto agg_type : agg_types) {
      TSSlice slice;

      if (agg_type == Sumfunctype::COUNT) {
        slice.len = sizeof(k_uint64);
        slice.data = static_cast<char*>(malloc(slice.len));
        if (!slice.data) {
          LOG_ERROR("Failed to allocate memory for COUNT slice");
          free(allc_mem);
          return KStatus::FAIL;
        }
        k_uint64 count_value = static_cast<k_uint64>(row_num);
        memcpy(slice.data, &count_value, slice.len);
      } else {
        slice.len = value_size;
        slice.data = static_cast<char*>(malloc(value_size));
        if (!slice.data) {
          LOG_ERROR("Failed to allocate memory for agg slice");
          free(allc_mem);
          return KStatus::FAIL;
        }

        size_t offset = 0;
        switch (agg_type) {
          case Sumfunctype::MAX: offset = 2; break;
          case Sumfunctype::MIN: offset = 2 + value_size; break;
          case Sumfunctype::SUM: offset = 2 + 2 * value_size; break;
          default: break;
        }

        memcpy(slice.data, allc_mem + offset, value_size);
      }

      agg_data.push_back(slice);
    }

    free(allc_mem);
    return KStatus::SUCCESS;
  }

  LOG_ERROR("VarLenType aggregation not supported yet: type = %d", dest_type.type);
  return KStatus::FAIL;
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
 const AttributeInfo& dest_type, std::vector<Sumfunctype> agg_types, std::vector<TSSlice>& agg_data) {
  return block_->GetAggResult(start_row_, nrow_, col_id, schema, dest_type, agg_types, agg_data);
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
