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

#include <vector>
#include <memory>
#include <set>
#include <utility>
#include "payload.h"
#include "ts_payload.h"

namespace kwdbts {

#define IS_VAR_DATATYPE(type) ((type) == DATATYPE::VARSTRING || (type) == DATATYPE::VARBINARY)

bool TsRawPayloadRowBuilder::Build(TSSlice* row_data, bool need_malloc, const std::vector<uint32_t>& valid_col_idx) {
  if (col_value_.empty() || col_value_[0].len == 0) {
    return false;
  }
  std::vector<AttributeInfo> nonone_col_schema;
  std::vector<TSSlice> nonone_col_values;
  if (valid_col_idx.empty()) {
    nonone_col_schema = schema_;
    nonone_col_values = col_value_;
  } else {
    for (auto col_id : valid_col_idx) {
      nonone_col_schema.push_back(schema_[col_id]);
      nonone_col_values.push_back(col_value_[col_id]);
    }
  }

  size_t bitmap_len;
  size_t fixed_tuple_len;
  size_t var_part_len;
  getNoNoneRowInfo(nonone_col_schema, nonone_col_values, bitmap_len, fixed_tuple_len, var_part_len);

  char* mem = nullptr;
  if (need_malloc) {
    row_data->len = bitmap_len + fixed_tuple_len + var_part_len;
    mem = reinterpret_cast<char*>(malloc(row_data->len));
    row_data->data = mem;
  } else {
    mem = row_data->data;
  }
  std::memset(mem, 0, row_data->len);
  size_t cur_var_offset = bitmap_len + fixed_tuple_len;
  size_t cur_tuple_offset = bitmap_len;
  for (size_t i = 0; i < nonone_col_schema.size(); i++) {
    if (isVarLenType(nonone_col_schema[i].type)) {
      KUint64(mem + cur_tuple_offset) = cur_var_offset;
      cur_var_offset += 2;
      if (nonone_col_values[i].data != nullptr) {
        KUint16(mem + cur_var_offset - 2) = nonone_col_values[i].len;
        std::memcpy(mem + cur_var_offset, nonone_col_values[i].data, nonone_col_values[i].len);
        cur_var_offset += nonone_col_values[i].len;
      } else {
        setRowDeleted(mem, i + 1);
        KUint16(mem + cur_var_offset - 2) = 0;
      }
      cur_tuple_offset += 8;
    } else {
      if (nonone_col_values[i].data != nullptr) {
        std::memcpy(mem + cur_tuple_offset, nonone_col_values[i].data, nonone_col_values[i].len);
      } else {
        setRowDeleted(mem, i + 1);
      }
      cur_tuple_offset += nonone_col_schema[i].size;
    }
  }
  assert(cur_tuple_offset == bitmap_len + fixed_tuple_len);
  assert(cur_var_offset == row_data->len);
  return true;
}
TSSlice TsRawPayloadRowBuilder::DataAddValidColInfo(TSSlice row_data, TSSlice valid_info,
uint32_t pd_version, TSPayloadRowStructType type) {
  TSSlice ret;
  ret.len = row_data.len + valid_info.len + 4 + 2 + 2;
  ret.data = reinterpret_cast<char*>(std::malloc(ret.len));
  if (ret.data == nullptr) {
    return ret;
  }
  KUint32(ret.data) = pd_version;
  KUint16(ret.data + 4) = type;
  KUint16(ret.data + 4 + 2) = valid_info.len;
  std::memcpy(ret.data + 4 + 2 + 2, valid_info.data, valid_info.len);
  std::memcpy(ret.data + 4 + 2 + 2 +  valid_info.len, row_data.data, row_data.len);
  return ret;
}
void TsRawPayloadRowBuilder::ParseWithValidColInfo(TSSlice row_data, TSSlice& valid_info,
uint32_t& pd_version, TSPayloadRowStructType& type, TSSlice& actual_data) {
  pd_version = KUint32(row_data.data);
  type = (TSPayloadRowStructType)(KUint16(row_data.data + 4));
  valid_info.len = KUint16(row_data.data + 4 + 2);
  valid_info.data = row_data.data + 4 + 2 + 2;
  actual_data.len = row_data.len - valid_info.len - 4 - 2 - 2;
  actual_data.data = row_data.data + 4 + 2 + 2 + valid_info.len;
}
bool TSRowPayloadSparseBuilder::Init(const std::vector<TagInfo>& tag_schema,
const std::vector<AttributeInfo>& data_schema, int row_num, TSPayloadRowStructType type) {
  tag_schema_ = tag_schema;
  data_schema_ = data_schema;
  count_ = row_num;
  type_ = type;
  for (size_t i = 0; i < count_; i++) {
    auto cur_row = std::make_unique<TsRawPayloadRowBuilder>(data_schema_);
    if (cur_row == nullptr) {
      LOG_ERROR("new TSRowPayloadSparseBuilder failed");
      return false;
    }
    rows_.push_back(std::move(cur_row));
  }
  SetTagMem();
  return true;
}

void TSRowPayloadSparseBuilder::SetTagMem() {
  tag_value_mem_bitmap_len_ = (tag_schema_.size() + 7) / 8;  // bitmap
  tag_value_mem_len_ = tag_value_mem_bitmap_len_;
  for (auto tag : tag_schema_) {
    if (IS_VAR_DATATYPE(tag.m_data_type) && !tag.isPrimaryTag()) {
      tag_value_mem_len_ += sizeof(intptr_t);
    } else {
      tag_value_mem_len_ += tag.m_size;
    }
    if (tag.isPrimaryTag()) {
      primary_key_info_.push_back(tag);
    }
  }
  tag_value_mem_ = reinterpret_cast<char*>(std::malloc(tag_value_mem_len_));
  std::memset(tag_value_mem_, 0xFF, tag_value_mem_bitmap_len_);  // bitmap  set all tag null
  std::memset(tag_value_mem_ + tag_value_mem_bitmap_len_, 0, tag_value_mem_len_ - tag_value_mem_bitmap_len_);
}

const char* TSRowPayloadSparseBuilder::GetTagAddr() {
  return tag_value_mem_;
}

void TSRowPayloadSparseBuilder::Reset() {
  primary_tags_.clear();
  if (tag_value_mem_) {
    free(tag_value_mem_);
    tag_value_mem_ = nullptr;
  }
  SetTagMem();
  for (size_t i = 0; i < count_; i++) {
    rows_[i]->Reset();
  }
}

bool TSRowPayloadSparseBuilder::SetTagValue(int tag_idx, char* mem, int count) {
  if (tag_idx >= tag_schema_.size()) {
    return false;
  }
  auto tag_schema = tag_schema_[tag_idx];
  int col_data_offset = tag_value_mem_bitmap_len_ + tag_schema.m_offset;
  // all types of tag all store same.
  char* tag_in_mem = nullptr;
  if (!tag_schema.isPrimaryTag() && IS_VAR_DATATYPE(tag_schema.m_data_type)) {  // re_alloc  var type data space.
    int cur_offset = tag_value_mem_len_;
    tag_value_mem_ = reinterpret_cast<char*>(std::realloc(tag_value_mem_, tag_value_mem_len_ + count + 2));
    tag_value_mem_len_ = tag_value_mem_len_ + count + 2;
    KUint16(tag_value_mem_ + cur_offset) = count;
    tag_in_mem = tag_value_mem_ + cur_offset + 2;
    std::memcpy(tag_in_mem, mem, count);
    KUint64(tag_value_mem_ + col_data_offset) = cur_offset;
  } else {
    tag_in_mem = tag_value_mem_ + col_data_offset;
    std::memcpy(tag_in_mem, mem, count);
  }
  if (tag_schema.isPrimaryTag()) {  // primary key store in tuple.
    int offset = (tag_in_mem - tag_value_mem_);
    primary_tags_.push_back({offset, count});
  }
  unset_null_bitmap((unsigned char *)tag_value_mem_, tag_idx);
  return true;
}

bool TSRowPayloadSparseBuilder::SetColumnValue(int row_num, int col_idx, char* mem, size_t length) {
  if (row_num >= count_ || col_idx >= data_schema_.size()) {
    return false;
  }
  rows_[row_num]->SetColValue(col_idx, TSSlice{mem, length});
  return true;
}

bool TSRowPayloadSparseBuilder::SetColumnNull(int row_num, int col_idx) {
  if (row_num >= count_ || col_idx >= data_schema_.size()) {
    return false;
  }
  rows_[row_num]->SetColNull(col_idx);
  return true;
}

bool TSRowPayloadSparseBuilder::SetValidCols(const std::vector<uint32_t>& cols) {
  if (count_ > 0) {
    LOG_ERROR("count not empty, can not set valid cols, this should be generated aotmically.");
    return false;
  }
  valid_col_idx_ = cols;
  return true;
}
bool TSRowPayloadSparseBuilder::Build(TSTableID table_id, uint32_t table_version, TSSlice *payload) {
  if (tag_schema_.empty() || data_schema_.empty() || primary_tags_.empty()) {
    return false;
  }
  int header_size = Payload::header_size_;
  k_uint32 header_len = header_size;
  k_int16 primary_len_len = 2;
  // primary tag

  k_int32 primary_tag_len = 0;
  assert(primary_tags_.size() == primary_key_info_.size());
  for (int i = 0; i < primary_tags_.size(); ++i) {
    primary_tag_len += primary_key_info_[i].m_size;
  }
  assert(primary_tag_len != 0);
  char* primary_keys_mem = reinterpret_cast<char*>(malloc(primary_tag_len));
  memset(primary_keys_mem, 0, primary_tag_len);
  int begin_offset = 0;
  for (int i = 0; i < primary_tags_.size(); ++i) {
    std::memcpy(primary_keys_mem + begin_offset, tag_value_mem_ + primary_tags_[i].offset_, primary_tags_[i].len_);
    begin_offset += primary_key_info_[i].m_size;
  }

  k_int32 tag_len_len = 4;
  // tag value
  k_int32 tag_value_len =  tag_value_mem_len_;
  // data part
  k_int32 data_len_len = 4;

  TSSlice valid_col_info{nullptr, 0};
  if (!genValidColInfo(valid_col_info, valid_col_idx_)) {
    LOG_ERROR("genValidColInfo failed");
    return false;
  }

  k_int32 data_len = 0;
  std::vector<TSSlice> row_datas;
  for (size_t i = 0; i < rows_.size(); i++) {
    TSSlice row_data;
    auto ret = rows_[i]->Build(&row_data, true, valid_col_idx_);
    assert(ret);
    row_datas.push_back(row_data);
    data_len += 4 + row_data.len;
  }
  data_len += 2 + valid_col_info.len;

  k_uint32 payload_length = header_len + primary_len_len + primary_tag_len
                            + tag_len_len + tag_value_len + data_len_len + data_len;

  char* value = reinterpret_cast<char*>(malloc(payload_length));
  std::memset(value, 0, payload_length);
  char* value_idx = value;
  // header part
  KInt32(value_idx + TsRawPayload::row_num_offset_) = count_;
  KUint8(value_idx + TsRawPayload::row_type_offset_) =
    (count_ == 0 && (valid_col_idx_.size() == 0)) ? TAG_ONLY : DATA_AND_TAG;
  KUint64(value_idx + TsRawPayload::table_id_offset_) = table_id;
  KUint32(value_idx + TsRawPayload::ts_version_offset_) = table_version;
  KUint32(value_idx + TsRawPayload::payload_version_offset_) = MAX_PAYLOAD_VERSION;

  value_idx += header_len;
  // set primary tag
  KInt16(value_idx) = primary_tag_len;
  value_idx += primary_len_len;
  std::memcpy(value_idx, primary_keys_mem, primary_tag_len);
  primary_offset_ = value_idx - value;
  value_idx += primary_tag_len;

  // set tag
  KInt32(value_idx) = tag_value_len;
  value_idx += tag_len_len;
  std::memcpy(value_idx, tag_value_mem_, tag_value_len);
  tag_offset_ = value_idx - value;
  value_idx += tag_value_len;

  // set data_len_len
  KInt32(value_idx) = data_len;
  // data part header, add row_type and valid_col_info
  data_offset_ = value_idx - value;
  value_idx += data_len_len;
  KUint16(value_idx) = type_;
  value_idx += 2;
  memcpy(value_idx, valid_col_info.data, valid_col_info.len);
  value_idx += valid_col_info.len;
  // data part, add row_datas.
  for (size_t i = 0; i < row_datas.size(); i++) {
    KUint32(value_idx) = row_datas[i].len;
    value_idx += 4;
    std::memcpy(value_idx, row_datas[i].data, row_datas[i].len);
    value_idx += row_datas[i].len;
  }

  payload->data = value;
  payload->len = value_idx - value;
  assert(payload->len == payload_length);
  // set hashpoint  todo(liangbo01) no need now
  uint32_t hashpoint = 2;
  std::memcpy(&payload->data[Payload::hash_point_id_offset_], &hashpoint, sizeof(uint16_t));

  free(primary_keys_mem);
  for (size_t i = 0; i < row_datas.size(); i++) {
    free(row_datas[i].data);
  }
  free(valid_col_info.data);
  return true;
}

bool TSRowPayloadSparseBuilder::genValidColInfo(TSSlice& col_none_mem, std::vector<uint32_t>& valid_col_idx) {
  if (TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE == type_) {
    return true;
  }
  if (count_ > 0) {
    assert(valid_col_idx.empty());
    std::set<uint32_t> nonone_col_ids;
    for (size_t i = 0; i < rows_.size(); i++) {
      nonone_col_ids.merge(rows_[i]->GetNononeCols());
    }
    valid_col_idx.insert(valid_col_idx.begin(), nonone_col_ids.begin(), nonone_col_ids.end());
  }
  switch (type_) {
    case TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE:
      // not run here.
      break;
    case TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_VECTOR:
      col_none_mem.data = reinterpret_cast<char*>(malloc(valid_col_idx.size() * 4 + 4));
      memset(col_none_mem.data, 0, valid_col_idx.size() * 4 + 4);
      col_none_mem.len += 4;
      for (size_t i = 0; i < valid_col_idx.size(); i++) {
        KUint32(col_none_mem.data + col_none_mem.len) = valid_col_idx[i];
        col_none_mem.len += 4;
      }
      KUint32(col_none_mem.data) = col_none_mem.len / 4 - 1;
      break;
    case TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_BITMAP:
      col_none_mem.len = (data_schema_.size() + 7) / 8 + 4;
      col_none_mem.data = reinterpret_cast<char*>(malloc(col_none_mem.len));
      memset(col_none_mem.data, 0xff, col_none_mem.len);
      KUint32(col_none_mem.data) = data_schema_.size();
      for (size_t i = 0; i < valid_col_idx.size(); i++) {
        setRowValid(col_none_mem.data + 4, valid_col_idx[i] + 1);
      }
      break;
    default:
      break;
  }
  return true;
}

}  //  namespace kwdbts
