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

#include <string>
#include <string_view>
#include "mmap/mmap_tag_column_table.h"
#include "test_util.h"
#include "payload_builder.h"

using namespace kwdbts;  // NOLINT

struct ColDataTypes {
  DATATYPE type;
  int size;
  TagType tag_type;
};

std::vector<ColDataTypes> tag_col_types({
  {DATATYPE::TIMESTAMP64, 8, TagType::PRIMARY_TAG},
  {DATATYPE::INT64, 8, TagType::GENERAL_TAG}});

std::vector<ColDataTypes> metric_col_types({{DATATYPE::TIMESTAMP64, 16, TagType::UNKNOWN_TAG},
                                            {DATATYPE::VARSTRING, 1024, TagType::UNKNOWN_TAG},
                                            {DATATYPE::INT32, 4, TagType::UNKNOWN_TAG},
                                            {DATATYPE::VARBINARY, 1123, TagType::UNKNOWN_TAG},
                                            {DATATYPE::VARSTRING, 12334, TagType::UNKNOWN_TAG}});

class TestRowPayloadBuilder : public testing::Test {
 public:
  std::vector<TagInfo> tag_schema_;
  std::vector<AttributeInfo> data_schema_;
  TSTableID table_id_ = 12343;
  uint32_t table_version_ = 345;
  TestRowPayloadBuilder() {
    // add tag columns
    tag_schema_.clear();
    size_t offset = 0;
    for (int i = 0; i < tag_col_types.size(); i++) {
      TagInfo info;
      info.m_id  = i + 1;
      info.m_tag_type = tag_col_types[i].tag_type;
      info.m_data_type = tag_col_types[i].type;
      info.m_length = tag_col_types[i].size;
      info.m_size = tag_col_types[i].size;
      info.m_offset = offset;
      offset += info.m_size;
      tag_schema_.push_back(info);
    }
    data_schema_.clear();
    for (int i = 0; i < metric_col_types.size(); i++) {
      AttributeInfo info;
      info.id = i + 1;
      info.length = metric_col_types[i].size;
      info.max_len = metric_col_types[i].size;
      info.size = metric_col_types[i].size;
      info.type = metric_col_types[i].type;
      data_schema_.push_back(info);
    }
  }

  ~TestRowPayloadBuilder() {
  }

  TSSlice GenPayload(KTimestamp primary_tag, int data_count) {
    TSRowPayloadBuilder pay_build(tag_schema_, data_schema_, data_count);
    for (size_t i = 0; i < tag_schema_.size(); i++) {
      KTimestamp cur_value = primary_tag + i;
      pay_build.SetTagValue(i, reinterpret_cast<char*>(&cur_value), sizeof(KTimestamp));
    }
    for (size_t j = 0; j < data_count; j++) {
      for (size_t i = 0; i < data_schema_.size(); i++) {
        KTimestamp cur_value = primary_tag + i + j;
        switch (data_schema_[i].type) {
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
          pay_build.SetColumnValue(j, i, reinterpret_cast<char*>(&cur_value), sizeof(cur_value));
          break;
        case DATATYPE::INT32: {
          uint32_t value = cur_value;
          pay_build.SetColumnValue(j, i, reinterpret_cast<char*>(&value), sizeof(value));
          break;
        }
        case DATATYPE::VARSTRING: {
          string const_var_data = intToString(cur_value);
          pay_build.SetColumnValue(j, i, const_var_data.data(), const_var_data.length());
          break;
        }
        case DATATYPE::VARBINARY:  {
          pay_build.SetColumnValue(j, i, reinterpret_cast<char*>(&cur_value), sizeof(cur_value));
          break;
        }
        default:
          break;
        }
      }
    }
    TSSlice payload_slice;
    bool s = pay_build.Build(table_id_, table_version_, &payload_slice);
    EXPECT_EQ(s, true);
    return payload_slice;
  }

  void CheckPayload(const TSSlice &raw, KTimestamp primary_tag, int data_count) {
    TsRawPayload pay(raw, data_schema_);
    ASSERT_EQ(pay.GetRowCount(), data_count);
    ASSERT_EQ(pay.GetTableID(), table_id_);
    ASSERT_EQ(pay.GetTableVersion(), table_version_);
    for (size_t j = 0; j < data_count; j++) {
      for (size_t i = 0; i < data_schema_.size(); i++) {
        TSSlice col_data;
        auto ok = pay.GetColValue(j, i, &col_data);
        ASSERT_TRUE(ok);
        KTimestamp cur_value = primary_tag + i + j;
        switch (data_schema_[i].type) {
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
          ASSERT_EQ(KTimestamp(col_data.data), cur_value);
          break;
        case DATATYPE::INT32: {
          ASSERT_EQ(cur_value, KUint32(col_data.data));
          break;
        }
        case DATATYPE::VARSTRING: {
          auto const_var_data = std::stoll(std::string{col_data.data, col_data.len});
          ASSERT_EQ(const_var_data, cur_value);
          break;
        }
        case DATATYPE::VARBINARY:  {
          ASSERT_EQ(KTimestamp(col_data.data), cur_value);
          break;
        }
        default:
          break;
        }
      }
    }
  }
};

// Create and delete empty tables
TEST_F(TestRowPayloadBuilder, empty) {
}

// Test simple data types
TEST_F(TestRowPayloadBuilder, create) {
  int count = 1;
  KTimestamp primary_tag = 10010;
  TSSlice payload_slice = GenPayload(primary_tag, count);
  CheckPayload(payload_slice, primary_tag, count);
  free(payload_slice.data);
}

// Test data with variable length type fields
TEST_F(TestRowPayloadBuilder, create_1) {
  int count = 100;
  KTimestamp primary_tag = 10086;
  TSSlice payload_slice = GenPayload(primary_tag, count);
  CheckPayload(payload_slice, primary_tag, count);
  free(payload_slice.data);
}
