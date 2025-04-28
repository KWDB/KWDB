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
 const AttributeInfo& desc_type, char** value, TsBitmap& bitmap) {
  return convert_.GetFixLenColAddr(col_id, schema, desc_type, value, bitmap);
}

// dest type is varlen datatype.
KStatus TsBlockSpan::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& desc_type, DataFlags& flag, TSSlice& data) {
  return convert_.GetVarLenTypeColAddr(row_idx, col_idx, schema, desc_type, flag, data);
}

KStatus TsBlockSpan::GetAggResult(uint32_t col_id, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& desc_type, std::vector<Sumfunctype> agg_types, std::vector<TSSlice>& agg_data) {
  return convert_.GetAggResult(col_id, schema, desc_type, agg_types, agg_data);
}

}  // namespace kwdbts
