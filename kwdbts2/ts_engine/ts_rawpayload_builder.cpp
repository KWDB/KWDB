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

#include "ts_payload.h"

namespace kwdbts {

#define IS_VAR_DATATYPE(type) ((type) == DATATYPE::VARSTRING || (type) == DATATYPE::VARBINARY)


bool TsRawPayloadRowBuilder::Build(TSSlice* row_data) {
  if (col_value_.size() == 0 || col_value_[0].len == 0) {
    return false;
  }
  size_t bitmap_len = (schema_.size() + 7) / 8;
  size_t fixed_tuple_len = 0;
  size_t var_part_len = 0;
  for (size_t i = 0; i < schema_.size(); i++) {
    if (isVarLenType(schema_[i].type)) {
      fixed_tuple_len += 8;
      size_t cur_col_var_len = 2;
      if (col_value_[i].data != nullptr) {
        cur_col_var_len += col_value_[i].len;
      }
      var_part_len += cur_col_var_len;
    } else {
      fixed_tuple_len += schema_[i].size;
    }
  }
  row_data->len = bitmap_len + fixed_tuple_len + var_part_len;
  char* mem = reinterpret_cast<char*>(malloc(row_data->len));
  std::memset(mem, 0, row_data->len);
  row_data->data = mem;
  size_t cur_var_offset = bitmap_len + fixed_tuple_len;
  size_t cur_tuple_offset = bitmap_len;
  for (size_t i = 0; i < schema_.size(); i++) {
    if (isVarLenType(schema_[i].type)) {
      KUint64(mem + cur_tuple_offset) = cur_var_offset;
      cur_var_offset += 2;
      if (col_value_[i].data != nullptr) {
        KUint16(mem + cur_var_offset - 2) = col_value_[i].len;
        std::memcpy(mem + cur_var_offset, col_value_[i].data, col_value_[i].len);
        cur_var_offset += col_value_[i].len;
      } else {
        setRowDeleted(mem, i + 1);
        KUint16(mem + cur_var_offset - 2) = 0;
      }
      cur_tuple_offset += 8;
    } else {
      if (col_value_[i].data != nullptr) {
        std::memcpy(mem + cur_tuple_offset, col_value_[i].data, col_value_[i].len);
      } else {
        setRowDeleted(mem, i + 1);
      }
      cur_tuple_offset += schema_[i].size;
    }
  }
  assert(cur_tuple_offset == bitmap_len + fixed_tuple_len);
  assert(cur_var_offset == row_data->len);
  return true;
}

TSRowPayloadBuilder::TSRowPayloadBuilder(const std::vector<TagInfo>& tag_schema,
                                   const std::vector<AttributeInfo>& data_schema, int row_num)
    : tag_schema_(tag_schema), data_schema_(data_schema), count_(row_num) {
  for (size_t i = 0; i < count_; i++) {
    rows_.emplace_back(new TsRawPayloadRowBuilder(data_schema_));
  }
  SetTagMem();
}

void TSRowPayloadBuilder::SetTagMem() {
  tag_value_mem_bitmap_len_ = (tag_schema_.size() + 7) / 8;  // bitmap
  tag_value_mem_len_ = tag_value_mem_bitmap_len_;
  for (const auto tag : tag_schema_) {
    if (IS_VAR_DATATYPE(tag.m_data_type)) {
      // not allocate space now. Then insert tag value, resize this tmp space.
      if (tag.m_tag_type == PRIMARY_TAG) {
        // primary tag all store in tuple.
        tag_value_mem_len_ += tag.m_length;
      } else {
        tag_value_mem_len_ += sizeof(intptr_t);
      }
    } else {
      tag_value_mem_len_ += tag.m_size;
    }
  }
  tag_value_mem_ = reinterpret_cast<char*>(std::malloc(tag_value_mem_len_));
  std::memset(tag_value_mem_, 0xFF, tag_value_mem_bitmap_len_);  // bitmap  set all tag null
  std::memset(tag_value_mem_ + tag_value_mem_bitmap_len_, 0, tag_value_mem_len_ - tag_value_mem_bitmap_len_);
}

const char* TSRowPayloadBuilder::GetTagAddr() {
  return tag_value_mem_;
}

void TSRowPayloadBuilder::Reset() {
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

bool TSRowPayloadBuilder::SetTagValue(int tag_idx, char* mem, int count) {
  if (tag_idx >= tag_schema_.size()) {
    return false;
  }
  auto tag_schema = tag_schema_[tag_idx];
  int col_data_offset = tag_value_mem_bitmap_len_ + tag_schema.m_offset;
  if (tag_schema.isPrimaryTag()) {  // primary key store in tuple.
    std::memcpy(tag_value_mem_ + col_data_offset, mem, count);
    primary_tags_.push_back({col_data_offset, count});
  } else {
    // all types of tag all store same.
    if (IS_VAR_DATATYPE(tag_schema.m_data_type)) {  // re_alloc  var type data space.
      int cur_offset = tag_value_mem_len_;
      tag_value_mem_ = reinterpret_cast<char*>(std::realloc(tag_value_mem_, tag_value_mem_len_ + count + 2));
      tag_value_mem_len_ = tag_value_mem_len_ + count + 2;
      KUint16(tag_value_mem_ + cur_offset) = count;
      std::memcpy(tag_value_mem_ + cur_offset + 2, mem, count);
      KUint64(tag_value_mem_ + col_data_offset) = cur_offset;
    } else {
      std::memcpy(tag_value_mem_ + col_data_offset, mem, count);
    }
  }
  unset_null_bitmap((unsigned char *)tag_value_mem_, tag_idx);
  return true;
}

bool TSRowPayloadBuilder::SetColumnValue(int row_num, int col_idx, char* mem, size_t length) {
  if (row_num >= count_ || col_idx >= data_schema_.size()) {
    return false;
  }
  rows_[row_num]->SetColValue(col_idx, TSSlice{mem, length});
  return true;
}

bool TSRowPayloadBuilder::SetColumnNull(int row_num, int col_idx) {
  if (row_num >= count_ || col_idx >= data_schema_.size()) {
    return false;
  }
  rows_[row_num]->SetColNull(col_idx);
  return true;
}

bool TSRowPayloadBuilder::Build(TSTableID table_id, uint32_t table_version, TSSlice *payload) {
  if (tag_schema_.empty() || data_schema_.empty() || primary_tags_.empty()) {
    return false;
  }
  int header_size = Payload::header_size_;
  k_uint32 header_len = header_size;
  k_int16 primary_len_len = 2;
  // primary tag

  k_int32 primary_tag_len = 0;
  for (int i = 0; i < primary_tags_.size(); ++i) {
    primary_tag_len += primary_tags_[i].len_;
  }
  char* primary_keys_mem = reinterpret_cast<char*>(malloc(primary_tag_len));
  int begin_offset = 0;
  for (int i = 0; i < primary_tags_.size(); ++i) {
    std::memcpy(primary_keys_mem + begin_offset, tag_value_mem_ + primary_tags_[i].offset_, primary_tags_[i].len_);
    begin_offset += primary_tags_[i].len_;
  }

  k_int32 tag_len_len = 4;
  // tag value
  k_int32 tag_value_len =  tag_value_mem_len_;
  // data part
  k_int32 data_len_len = 4;

  k_int32 data_len = 0;
  std::vector<TSSlice> row_datas;
  for (size_t i = 0; i < rows_.size(); i++) {
    TSSlice row_data;
    auto ret = rows_[i]->Build(&row_data);
    assert(ret);
    row_datas.push_back(row_data);
    data_len += 4 + row_data.len;
  }

  k_uint32 payload_length = header_len + primary_len_len + primary_tag_len
                            + tag_len_len + tag_value_len + data_len_len + data_len;

  char* value = reinterpret_cast<char*>(malloc(payload_length));
  std::memset(value, 0, payload_length);
  char* value_idx = value;
  // header part
  KInt32(value_idx + TsRawPayload::row_num_offset_) = count_;
  KUint32(value_idx + TsRawPayload::ts_version_offset_) = data_schema_[0].version;
  KUint64(value_idx + TsRawPayload::table_id_offset_) = table_id;
  KUint32(value_idx + TsRawPayload::ts_version_offset_) = table_version;

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
  data_offset_ = value_idx - value;
  value_idx += data_len_len;
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
  return true;
}

}  //  namespace kwdbts
