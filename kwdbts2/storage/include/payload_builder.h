
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

#pragma once

#include <memory>
#include <vector>
#include "data_type.h"
#include "kwdb_type.h"
#include "payload.h"
#include "mmap/mmap_tag_column_table.h"

namespace kwdbts {

typedef struct {
  int offset_;
  int len_;
} PrimaryTagInfo;


/**
 * @brief build payload struct data.
 *        Builder function return the memroy ,which need free above.
*/
class PayloadBuilder {
 private:
  std::vector<TagInfo> tag_schema_;
  std::vector<AttributeInfo> data_schema_;
  // TSSlice primary_tag_{nullptr, 0};
  std::vector<int32_t> data_schema_offset_;
  std::vector<PrimaryTagInfo> primary_tags_;
  int count_{0};
  int primary_offset_;
  int tag_offset_;
  int data_offset_;

  char* tag_value_mem_{nullptr};
  int tag_value_mem_len_{0};
  int tag_value_mem_bitmap_len_{0};

  char* fix_data_mem_{nullptr};
  int fix_data_mem_len_{0};

  char* tmp_var_type_mem_{nullptr};
  int tmp_var_type_mem_len_{0};
  int tmp_var_type_mem_used_{0};

 public:
  PayloadBuilder(const std::vector<TagInfo>& tag_schema,
                   const std::vector<AttributeInfo>& data_schema);

  // just copy other tag value info
  PayloadBuilder(PayloadBuilder& other) {
    tag_schema_ = other.tag_schema_;
    data_schema_ = other.data_schema_;
    data_schema_offset_ = other.data_schema_offset_;
    primary_tags_ = other.primary_tags_;
    count_ = other.count_;
    primary_offset_ = other.primary_offset_;
    tag_offset_ = other.tag_offset_;
    data_offset_ = other.data_offset_;

    tag_value_mem_len_ = other.tag_value_mem_len_;
    tag_value_mem_bitmap_len_ = other.tag_value_mem_bitmap_len_;
    if (other.tag_value_mem_ != nullptr) {
      tag_value_mem_ = reinterpret_cast<char*>(malloc(tag_value_mem_len_));
      memcpy(tag_value_mem_, other.tag_value_mem_, tag_value_mem_len_);
    }

    fix_data_mem_ = nullptr;
    fix_data_mem_len_ = 0;
    tmp_var_type_mem_ = nullptr;
    tmp_var_type_mem_len_  = 0;
    tmp_var_type_mem_used_ = 0;
  }

  ~PayloadBuilder() {
    if (tag_value_mem_) {
      free(tag_value_mem_);
    }
    if (fix_data_mem_) {
      free(fix_data_mem_);
    }
    if (tmp_var_type_mem_) {
      free(tmp_var_type_mem_);
    }
  }

  const char* GetTagAddr();

  bool SetTagValue(int tag_id, char* mem, int length);

  bool SetDataRows(int count);

  bool SetColumnValue(int row_num, int col_idx, char* mem, int length);

  bool SetColumnNull(int row_num, int col_idx);

  bool Build(TSSlice *payload, uint64_t hash_num);
};

}  // namespace kwdbts
