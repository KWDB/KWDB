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
#include "data_type.h"
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {

TsRawPayloadRowParser::TsRawPayloadRowParser(const std::vector<AttributeInfo>* data_schema,
const std::vector<uint32_t>& valid_col_idx)
    : schema_(data_schema), actual_col_idx_in_schema_(valid_col_idx) {
  if (data_schema == nullptr) {
    return;
  }
  size_t col_size = 0;
  if (valid_col_idx.size() > 0) {
    col_size = valid_col_idx.size();
    col_id_map_valid_list_.resize(schema_->size(), UINT32_MAX);
    for (size_t i = 0; i < valid_col_idx.size(); i++) {
      col_id_map_valid_list_[valid_col_idx[i]] = i;
    }
  } else {
    col_size = schema_->size();
  }
  auto bitmap_len = (col_size + 7) / 8;
  int cur_offset = bitmap_len;
  col_offset_.resize(col_size);
  for (size_t i = 0; i < col_size; i++) {
    col_offset_[i] = cur_offset;
    auto scheme_idx = i;
    if (valid_col_idx.size() > 0) {
      scheme_idx = valid_col_idx[i];
    }
    if (__isVarType((*schema_)[scheme_idx].type)) {
      cur_offset += 8;
    } else {
      cur_offset += (*schema_)[scheme_idx].size;
    }
  }
}

void TsRawPayloadRowParser::FailReport(TSSlice row_data) const {
  std::string hex_str;
  BinaryToHexStr(row_data, hex_str);
  LOG_ERROR("TsRawPayloadRowParser::Parse failed. data len:%lu, hex: %s", row_data.len, hex_str.c_str());
  LOG_ERROR("print metric schema while payload parse, schema size: %lu", schema_->size());
  for (size_t i = 0; i < schema_->size(); i++) {
    LOG_ERROR("scheme idx[%lu]: %s.", i, schema_->at(i).toString().c_str());
  }
}

bool TsRawPayloadSparseRowWithTypeParser::Init(TSSlice row) {
  TSSlice valid_info;
  uint32_t pd_version;
  TSSlice actual_data;
  TsRawPayloadRowBuilder::ParseWithValidColInfo(row, valid_info, pd_version, row_type_, actual_data);
  tuple_ = actual_data;
  if (!TsRawPayload::ParseValidInfo(row_type_, valid_info, actual_col_idx_in_schema_)) {
    LOG_ERROR("parse valid info failed.");
    return false;
  }
  if (actual_col_idx_in_schema_.size() == 0 || actual_col_idx_in_schema_.size() == schema_->size()) {
    actual_col_idx_in_schema_.clear();
  }
  parser_ = new TsRawPayloadRowParser(schema_, actual_col_idx_in_schema_);
  if (parser_ == nullptr) {
    LOG_ERROR("new TsRawPayloadRowParser failed.");
    return false;
  }
  return true;
}

bool TsRawPayload::parservalidColInfo(TSPayloadRowStructType type, char*& mem) {
  valid_col_info_.data = mem;
  valid_col_info_.len = 0;
  valid_col_idx_list_.clear();
  if (type == TS_PAYLOAD_ROW_TYPE_BITMAP) {
    auto bitmap_num = KUint32(mem);
    auto bitmap_len = (bitmap_num + 7) / 8;
    valid_col_info_.len = bitmap_len + 4;
  } else if (type == TS_PAYLOAD_ROW_TYPE_VECTOR) {
    auto col_num = KUint32(mem);
    auto col_list_len = 4 * col_num;
    valid_col_info_.len = col_list_len + 4;
  }
  mem += valid_col_info_.len;

  if (!TsRawPayload::ParseValidInfo(type, valid_col_info_, valid_col_idx_list_)) {
    LOG_ERROR("parse valid info failed.");
    return false;
  }
  // if all cols are valid, no need to transform idx. run as TS_PAYLOAD_ROW_TYPE_TUPLE
  if (data_schema_!= nullptr && valid_col_idx_list_.size() == data_schema_->size()) {
    valid_col_idx_list_.clear();
  }
  return true;
}

TSSlice TsRawPayload::GenRowDataWithValidInfo(int row) {
  assert(row < row_data_.size());
  return  TsRawPayloadRowBuilder::DataAddValidColInfo(row_data_[row], valid_col_info_, GetPayloadVersion(), row_type_);
}
KStatus TsRawPayload::ParsePayLoadStruct(const TSSlice &raw) {
  payload_ = raw;
  if (GetPayloadVersion() == 0 || GetPayloadVersion() > MAX_PAYLOAD_VERSION) {
    LOG_ERROR("unsupported payload version. %d", GetPayloadVersion());
    return KStatus::FAIL;
  }
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
        if (GetRowType() != DataTagFlag::TAG_ONLY && parse_metric_) {
          if (GetPayloadVersion() == MAX_PAYLOAD_VERSION) {
            row_type_ = (TSPayloadRowStructType)KUint16(mem);
            mem += 2;
            if (!parservalidColInfo(row_type_, mem)) {
              LOG_ERROR("parse none col info failed.");
              return KStatus::FAIL;
            }
          } else {
            row_type_ = TS_PAYLOAD_ROW_TYPE_TUPLE;
          }
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
  row_parser_ = new TsRawPayloadRowParser(data_schema_, valid_col_idx_list_);
  if (row_parser_ == nullptr) {
    LOG_ERROR("new TsRawPayloadRowParser failed.");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsRawPayload::ParseBatchDataStruct(const TSSlice &raw) {
  payload_ = raw;
  if (GetPayloadVersion() == 0 || GetPayloadVersion() > MAX_PAYLOAD_VERSION) {
    LOG_ERROR("unsupported payload version. %d", GetPayloadVersion());
    return KStatus::FAIL;
  }
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
        if (GetPayloadVersion() == MAX_PAYLOAD_VERSION) {
          row_type_ = (TSPayloadRowStructType)KUint16(mem);
          mem += 2;
          if (!parservalidColInfo(row_type_, mem)) {
            LOG_ERROR("parse none col info failed.");
            return KStatus::FAIL;
          }
        } else {
          row_type_ = TS_PAYLOAD_ROW_TYPE_TUPLE;
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

TsRawPayload::TsRawPayload(const std::vector<AttributeInfo>* data_schema, bool parse_metric)
    : data_schema_(data_schema), parse_metric_(parse_metric) {
}

}  // namespace kwdbts
