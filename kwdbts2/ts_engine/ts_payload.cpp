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

#include <vector>
#include <string>
#include "ts_payload.h"
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {

TsRawPayloadRowParser::TsRawPayloadRowParser(const std::vector<AttributeInfo>* data_schema)
    : schema_(data_schema) {
  size_t col_size = schema_ != nullptr ? schema_->size() : 0;
  auto bitmap_len = (col_size + 7) / 8;
  int cur_offset = bitmap_len;
  col_offset_.resize(col_size);
  for (size_t i = 0; i < col_size; i++) {
    col_offset_[i] = cur_offset;
    if (__isVarType((*schema_)[i].type)) {
      cur_offset += 8;
    } else {
      cur_offset += (*schema_)[i].size;
    }
  }
}

bool TsRawPayloadRowParser::GetColValueAddr(const TSSlice& row_data, int col_id,
                                            TSSlice* col_data) {
  bool parse_ok = false;
  // run here means column value is not null.
  bool end_col_value = (col_id == schema_->size() - 1);
  if (col_id < schema_->size() && row_data.len > col_offset_[col_id]) {
    if (!__isVarType((*schema_)[col_id].type)) {
      col_data->data = row_data.data + col_offset_[col_id];
      col_data->len = (*schema_)[col_id].size;
      auto end_offset = col_offset_[col_id] + col_data->len;
      if (end_offset <= row_data.len) {
        parse_ok = true;
      }
    } else {
      size_t actual_offset = KUint64(row_data.data + col_offset_[col_id]);
      col_data->len = KUint16(row_data.data + actual_offset);
      auto end_offset = actual_offset + col_data->len + kStringLenLen;
      if (end_offset <= row_data.len) {
        col_data->data = row_data.data + actual_offset + kStringLenLen;
        parse_ok = true;
      }
    }
  }
  if (!parse_ok) {
    std::string hex_str;
    BinaryToHexStr(row_data, hex_str);
    LOG_ERROR("TsRawPayloadRowParser::Parse failed. data len:%lu, hex: %s", row_data.len, hex_str.c_str());
    LOG_ERROR("print metric schema while payload parse, schema size: %lu", schema_->size());
    for (size_t i = 0; i < schema_->size(); i++) {
      LOG_ERROR("scheme idx[%lu]: %s.", i, schema_->at(i).toString().c_str());
    }
    return false;
  }
  return true;
}

KStatus TsRawPayload::ParsePayLoadStruct(const TSSlice &raw) {
  payload_ = raw;
  bool parse_ok = false;
  bool all_row_ok = true;
  if (raw.len > header_size_) {
    uint16_t ptag_len = KUint16(payload_.data + header_size_);
    primary_key_.data = payload_.data + header_size_ + sizeof(ptag_len);
    primary_key_.len = ptag_len;
    if (primary_key_.data - payload_.data + 4 < payload_.len) {
      tag_datas_.data = primary_key_.data + primary_key_.len + 4;
      tag_datas_.len = KUint32(tag_datas_.data - 4);
      char* mem = tag_datas_.data + tag_datas_.len;
      if (mem - payload_.data + 4 <= payload_.len) {
        auto metric_datas_len = KUint32(mem);
        mem += 4;
        if (GetRowType() != DataTagFlag::TAG_ONLY) {
          auto count = GetRowCount();
          row_data_.reserve(count);
          for (size_t i = 0; i < count; i++) {
            if (mem + 4 - payload_.data > payload_.len) {
              LOG_ERROR("cannot read row[%lu] length.", i);
              all_row_ok = false;
              break;
            }
            auto row_size = KUint32(mem);
            mem += 4;
            row_data_.push_back({mem, row_size});
            mem += row_size;
          }
        } else {
          mem += metric_datas_len;
        }
      }
      if ((mem - payload_.data == payload_.len) && all_row_ok) {
        parse_ok = true;
      }
    }
  }
  if (!parse_ok) {
    std::string hex_str;
    BinaryToHexStr(raw, hex_str);
    LOG_ERROR("TsRawPayload::Parse table[%lu] failed. data len:%lu, hex: %s", GetTableID(), raw.len, hex_str.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

TsRawPayload::TsRawPayload(const std::vector<AttributeInfo>* data_schema)
    : row_parser_(data_schema) {
  can_parse_ = data_schema != nullptr;
}

}  // namespace kwdbts
