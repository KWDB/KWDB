// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ts_payload.h"

namespace kwdbts {

static inline bool __isVarType(int type) { return ((type == VARSTRING) || (type == VARBINARY)); }

TsRawPayloadRowParser::TsRawPayloadRowParser(const std::vector<AttributeInfo>& data_schema)
    : schema_(data_schema) {
  auto bitmap_len = (schema_.size() + 7) / 8;
  int cur_offset = bitmap_len;
  col_offset_.resize(schema_.size());
  for (size_t i = 0; i < schema_.size(); i++) {
    col_offset_[i] = cur_offset;
    if (__isVarType(schema_[i].type)) {
      cur_offset += 8;
    } else {
      cur_offset += schema_[i].size;
    }
  }
}

bool TsRawPayloadRowParser::GetColValueAddr(const TSSlice& row_data, int col_id,
                                            TSSlice* col_data) {
  assert(col_id < schema_.size());
  assert(row_data.len > col_offset_[col_id]);
  if (!__isVarType(schema_[col_id].type)) {
    col_data->data = row_data.data + col_offset_[col_id];
    col_data->len = schema_[col_id].size;
  } else {
    size_t actual_offset = KUint64(row_data.data + col_offset_[col_id]);
    col_data->len = KUint16(row_data.data + actual_offset);
    col_data->data = row_data.data + actual_offset + 2;
  }
  return true;
}

TsRawPayload::TsRawPayload(const TSSlice& raw, const std::vector<AttributeInfo>& data_schema)
    : payload_(raw), metric_schema_(data_schema), row_parser_(data_schema) {
  assert(raw.len > header_size_);
  uint16_t ptag_len = KUint16(payload_.data + header_size_);
  primary_key_.data = payload_.data + header_size_ + sizeof(ptag_len);
  primary_key_.len = ptag_len;
  assert(primary_key_.data - payload_.data + 4 < payload_.len);
  tag_datas_.data = primary_key_.data + primary_key_.len + 4;
  tag_datas_.len = KUint32(tag_datas_.data - 4);
  assert(tag_datas_.len <= payload_.len);

  char* mem = tag_datas_.data + tag_datas_.len;
  auto metric_datas_len = KUint32(mem);
  mem += 4;
  auto count = GetRowCount();
  row_data_.reserve(count);
  for (size_t i = 0; i < count; i++) {
    auto row_size = KUint32(mem);
    mem += 4;
    row_data_.push_back({mem, row_size});
    mem += row_size;
  }
  assert(mem - payload_.data == payload_.len);
  can_parse_ = !metric_schema_.empty();
}

}  // namespace kwdbts
