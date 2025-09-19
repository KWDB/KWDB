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

#include "payload_builder.h"
#include "ts_table.h"

namespace kwdbts {

#define IS_VAR_DATATYPE(type) ((type) == DATATYPE::VARSTRING || (type) == DATATYPE::VARBINARY)

PayloadBuilder::PayloadBuilder(const std::vector<TagInfo>& tag_schema,
                                   const std::vector<AttributeInfo>& data_schema)
    : tag_schema_(tag_schema), data_schema_(data_schema) {
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
  memset(tag_value_mem_, 0xFF, tag_value_mem_bitmap_len_);  // bitmap  set all tag null
  memset(tag_value_mem_ + tag_value_mem_bitmap_len_, 0, tag_value_mem_len_ - tag_value_mem_bitmap_len_);
}

const char* PayloadBuilder::GetTagAddr() {
  return tag_value_mem_;
}

bool PayloadBuilder::SetTagValue(int tag_idx, char* mem, int count) {
  if (tag_idx >= tag_schema_.size()) {
    return false;
  }
  auto tag_schema = tag_schema_[tag_idx];
  int col_data_offset = tag_value_mem_bitmap_len_ + tag_schema.m_offset;
  if (tag_schema.isPrimaryTag()) {  // primary key store in tuple.
    memcpy(tag_value_mem_ + col_data_offset, mem, count);
    primary_tags_.push_back({col_data_offset, count});
  } else {
    // all types of tag all store same.
    if (IS_VAR_DATATYPE(tag_schema.m_data_type)) {  // re_alloc  var type data space.
      int cur_offset = tag_value_mem_len_;
      tag_value_mem_ = reinterpret_cast<char*>(std::realloc(tag_value_mem_, tag_value_mem_len_ + count + 2));
      tag_value_mem_len_ = tag_value_mem_len_ + count + 2;
      KUint16(tag_value_mem_ + cur_offset) = count;
      memcpy(tag_value_mem_ + cur_offset + 2, mem, count);
      KUint64(tag_value_mem_ + col_data_offset) = cur_offset;
    } else {
      memcpy(tag_value_mem_ + col_data_offset, mem, count);
    }
  }
  unset_null_bitmap((unsigned char *)tag_value_mem_, tag_idx);
  return true;
}

bool PayloadBuilder::SetDataRows(int count) {
  if (count_ != 0) {
    return false;
  }
  count_ = count;
  int batch_bitmap = (count + 7) / 8;
  int row_size = 0;
  bool exist_var_type = false;
  for (auto& data_schema : data_schema_) {
    data_schema_offset_.push_back(row_size);
    if (IS_VAR_DATATYPE(data_schema.type)) {
      row_size += sizeof(intptr_t);
      exist_var_type = true;
    } else {
      row_size += data_schema.size;
    }
  }
  fix_data_mem_len_ = batch_bitmap * data_schema_.size() + row_size * count;
  fix_data_mem_ = reinterpret_cast<char*>(malloc(fix_data_mem_len_));
//  memset(fix_data_mem_, 0xFF, fix_data_mem_len_);
  memset(fix_data_mem_, 0, fix_data_mem_len_);
  if (exist_var_type) {
    tmp_var_type_mem_len_ = 1024;
    tmp_var_type_mem_ = reinterpret_cast<char*>(std::malloc(tmp_var_type_mem_len_));
  }
  return true;
}

bool PayloadBuilder::SetColumnValue(int row_num, int col_idx, char* mem, int length) {
  if (row_num >= count_ || col_idx >= data_schema_.size()) {
    return false;
  }
  int batch_bitmap_size = (count_ + 7) / 8;
  int row_offset = data_schema_offset_[col_idx];
  int col_data_bitmap_offset = row_offset * count_ + col_idx * batch_bitmap_size;
  char* fix_data_col_batch = fix_data_mem_ + col_data_bitmap_offset;
  if (IS_VAR_DATATYPE(data_schema_[col_idx].type)) {
    char* cur_col_value_addr = fix_data_col_batch + batch_bitmap_size + row_num * sizeof(intptr_t);
    while (tmp_var_type_mem_used_ + length + 2 > tmp_var_type_mem_len_) {
      tmp_var_type_mem_len_ *= 2;
      tmp_var_type_mem_ = reinterpret_cast<char*>(
        std::realloc((unsigned  char*)tmp_var_type_mem_, tmp_var_type_mem_len_));
    }
    int var_type_offset = tmp_var_type_mem_used_;
    KUint16(tmp_var_type_mem_ + var_type_offset) = length;
    memcpy(tmp_var_type_mem_ + 2 + var_type_offset, mem, length);
    tmp_var_type_mem_used_ += length + 2;
    KUint64(cur_col_value_addr) = fix_data_mem_len_ - col_data_bitmap_offset + var_type_offset;
  } else {
    char* cur_col_value_addr = fix_data_col_batch + batch_bitmap_size + row_num * data_schema_[col_idx].size;
    memcpy(cur_col_value_addr, mem, length);
  }
  unset_null_bitmap((unsigned  char*)fix_data_col_batch, row_num);
  return true;
}

bool PayloadBuilder::SetColumnNull(int row_num, int col_idx) {
  if (row_num >= count_ || col_idx >= data_schema_.size()) {
    return false;
  }
  int batch_bitmap_size = (count_ + 7) / 8;
  int row_offset = data_schema_offset_[col_idx];
  int col_data_bitmap_offset = row_offset * count_ + col_idx * batch_bitmap_size;
  char* fix_data_col_batch = fix_data_mem_ + col_data_bitmap_offset;
  set_null_bitmap((unsigned  char*)fix_data_col_batch, row_num);
  // LOG_INFO("Set data to null at column[%d:%s] row_num[%d]", col_idx, data_schema_[col_idx].name, row_num)
  return true;
}

bool PayloadBuilder::Build(TSSlice *payload, uint64_t hash_num) {
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
    memcpy(primary_keys_mem + begin_offset, tag_value_mem_ + primary_tags_[i].offset_, primary_tags_[i].len_);
    begin_offset += primary_tags_[i].len_;
  }

  k_int32 tag_len_len = 4;
  // tag value
  k_int32 tag_value_len =  tag_value_mem_len_;
  // data part
  k_int32 data_len_len = 4;
  k_int32 data_len = fix_data_mem_len_ + tmp_var_type_mem_used_;
  k_uint32 payload_length = header_len + primary_len_len + primary_tag_len
                            + tag_len_len + tag_value_len + data_len_len + data_len;

  char* value = reinterpret_cast<char*>(malloc(payload_length));
  memset(value, 0, payload_length);
  char* value_idx = value;
  // header part
  KInt32(value_idx + Payload::row_num_offset_) = count_;
  KUint32(value_idx + Payload::ts_version_offset_) = data_schema_[0].version;
  value_idx += header_len;
  // set primary tag
  KInt16(value_idx) = primary_tag_len;
  value_idx += primary_len_len;
  memcpy(value_idx, primary_keys_mem, primary_tag_len);
  primary_offset_ = value_idx - value;
  value_idx += primary_tag_len;

  // set tag
  KInt32(value_idx) = tag_value_len;
  value_idx += tag_len_len;
  memcpy(value_idx, tag_value_mem_, tag_value_len);
  tag_offset_ = value_idx - value;
  value_idx += tag_value_len;

  // set data_len_len
  KInt32(value_idx) = data_len;
  data_offset_ = value_idx - value;
  value_idx += data_len_len;
  memcpy(value_idx, fix_data_mem_, fix_data_mem_len_);
  memcpy(value_idx + fix_data_mem_len_, tmp_var_type_mem_, tmp_var_type_mem_used_);
  value_idx += data_len;
  payload->data = value;
  payload->len = value_idx - value;
    // set hashpoint
  uint32_t hashpoint = GetConsistentHashId(value + primary_offset_, primary_tag_len, hash_num);
  memcpy(&payload->data[Payload::hash_point_id_offset_], &hashpoint, sizeof(uint16_t));

  free(primary_keys_mem);
  return true;
}

}  //  namespace kwdbts
