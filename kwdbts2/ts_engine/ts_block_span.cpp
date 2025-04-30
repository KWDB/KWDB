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

#include "ts_block.h"
#include "ts_blkspan_type_convert.h"

namespace kwdbts {


KStatus TsBlock::GetAggResult(uint32_t begin_row_idx, uint32_t row_num, uint32_t col_id,
const std::vector<AttributeInfo>& schema,
const AttributeInfo& dest_type, std::vector<Sumfunctype> agg_types, std::vector<TSSlice>& agg_data) {
  TSBlkSpanDataTypeConvert convert(this, begin_row_idx, row_num);
  // char* value;
  // TsBitmap bitmap;
  // agg_data.resize(agg_types.size());
  // if (!isVarLenType(dest_type.type)) {
  //   char* allc_mem = reinterpret_cast<char*>(malloc((agg_types.size()) * dest_type.size) + 16);
  //   auto s = GetFixLenColAddr(col_id, schema, dest_type, &value, bitmap);
  //   if (s != KStatus::SUCCESS) {
  //     LOG_ERROR("GetFixLenColAddr failed.");
  //     return s;
  //   }
  //   for (size_t i = 0; i < agg_types.size(); i++) {
  //     switch (agg_types[i]) {
  //       case Sumfunctype::MAX:
  //         /* code */
  //         break;
  //       case Sumfunctype::MIN:
  //         break;
  //       case Sumfunctype::SUM:
  //         break;
  //       case Sumfunctype::COUNT:
  //         break;
  //       default:
  //         break;
  //     }
  //   }
  //   AggCalculatorV2 v2((void*)value, &bitmap, dest_type.type, dest_type.size, row_num_);
  //   bool overflow = v2.CalcAllAgg(*(reinterpret_cast<uint16_t*>(allc_mem)), allc_mem + 8, allc_mem + 16, allc_mem + 24);
  //   assert(!overflow);
  // } else {
  //   // VarColAggCalculatorV2 v2()
  // }
  return KStatus::SUCCESS;
}


TsBlockSpan::TsBlockSpan(TSTableID table_id, uint32_t table_version, TSEntityID entity_id,
            std::shared_ptr<TsBlock> block, int start, int nrow_)
    : entity_id_(entity_id), block_(block), start_row_(start), nrow_(nrow_), convert_(*this) {
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
      uint64_t seq_no = *block_->GetSeqNoAddr(start_row_);
      uint64_t other_seq_no = *other.block_->GetSeqNoAddr(other.start_row_);
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

uint64_t* TsBlockSpan::GetSeqNoAddr(int row_idx) const {
  return block_->GetSeqNoAddr(start_row_ + row_idx);
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

void TsBlockSpan::SplitFront(int row_num, TsBlockSpan* front_span) {
  assert(row_num <= nrow_);
  front_span->block_ = block_;
  front_span->entity_id_ = entity_id_;
  front_span->start_row_ = start_row_;
  front_span->nrow_ = row_num;
  front_span->convert_ = {*front_span};
  convert_ = {*this};
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
  back_span->convert_ = {*back_span};
  convert_ = {*this};
  // change current span info
  nrow_ -= row_num;
}

void TsBlockSpan::Truncate(int row_num) {
  start_row_ += row_num;
  nrow_ -= row_num;
  convert_ = {*this};
}

void TsBlockSpan::Clear() {
  block_ = nullptr;
  entity_id_ = 0;
  start_row_ = 0;
  nrow_ = 0;
  convert_ = {};
}

}  // namespace kwdbts
