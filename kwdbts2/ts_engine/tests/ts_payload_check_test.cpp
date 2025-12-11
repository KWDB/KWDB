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

#include <gtest/gtest.h>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <array>
#include <random>
#include "libkwdbts2.h"
#include "ts_ts_lsn_span_utils.h"
#include  "ts_payload.h"

using namespace kwdbts;

struct ColDataTypes {
  DATATYPE type;
  int size;
  TagType tag_type;
};

std::vector<ColDataTypes> tag_col_types({
  {DATATYPE::TIMESTAMP64, 8, TagType::PRIMARY_TAG},
  {DATATYPE::INT64, 8, TagType::GENERAL_TAG},
  {DATATYPE::VARSTRING, 1024, TagType::PRIMARY_TAG}});

std::vector<ColDataTypes> metric_col_types({{DATATYPE::TIMESTAMP64, 16, TagType::UNKNOWN_TAG},
                                            {DATATYPE::VARSTRING, 1024, TagType::UNKNOWN_TAG},
                                            {DATATYPE::INT32, 4, TagType::UNKNOWN_TAG},
                                            {DATATYPE::VARBINARY, 1123, TagType::UNKNOWN_TAG},
                                            {DATATYPE::VARSTRING, 12334, TagType::UNKNOWN_TAG}});

class TestBinaryToHexstr : public testing::Test {
 public:
  std::vector<TagInfo> tag_schema_;
  std::vector<AttributeInfo> data_schema_;

 TestBinaryToHexstr() {
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
      if (isVarLenType(tag_col_types[i].type)) {
        info.m_size = 8;
      }
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
    bool s = pay_build.Build(100, 101, &payload_slice);
    EXPECT_EQ(s, true);
    return payload_slice;
  }
};

TEST_F(TestBinaryToHexstr, BinaryToHexStrTest) {
  char buff[128];
  TSSlice data{buff, 128};
  memset(buff, 254, 128);
  std::string hex;
  BinaryToHexStr(data, hex);
  // std::cout << "|" << hex << "|" << std::endl;
  TSSlice buff_bak;
  HexStrToBinary(hex, buff_bak);
  ASSERT_EQ(buff_bak.len, data.len);
  ASSERT_TRUE(0 == memcmp(data.data, buff_bak.data, data.len));
  memset(buff, 12, 100);
  BinaryToHexStr(data, hex);
  // std::cout << "|" << hex << "|" << std::endl;
  free(buff_bak.data);
  HexStrToBinary(hex, buff_bak);
  ASSERT_EQ(buff_bak.len, data.len);
  ASSERT_TRUE(0 == memcmp(data.data, buff_bak.data, data.len));
  free(buff_bak.data);
  srand(time(nullptr));
  for (size_t i = 0; i < 128; i++) {
    buff[i] = (rand() % 256);
  }
  BinaryToHexStr(data, hex);
  // std::cout << "|" << hex << "|" << std::endl;
  HexStrToBinary(hex, buff_bak);
  ASSERT_EQ(buff_bak.len, data.len);
  ASSERT_TRUE(0 == memcmp(data.data, buff_bak.data, data.len));
  free(buff_bak.data);

  uint8_t buff_1[1024];
    for (size_t i = 0; i < 1024; i++) {
    buff_1[i] = i;
  }
  TSSlice data1{reinterpret_cast<char*>(buff_1), 1024};
  BinaryToHexStr(data1, hex);
  // std::cout << "|" << hex << "|" << std::endl;
  HexStrToBinary(hex, buff_bak);
  ASSERT_EQ(buff_bak.len, data1.len);
  ASSERT_TRUE(0 == memcmp(data1.data, buff_bak.data, data1.len));
  free(buff_bak.data);
}

TEST_F(TestBinaryToHexstr, payloadToHexStrTest) {
  TSSlice payload = GenPayload(10010, 3);
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  KUint32(payload.data + TsRawPayload::row_num_offset_) = 2;
  s = p.ParsePayLoadStruct(payload);
  ASSERT_EQ(s, KStatus::FAIL);
  KUint32(payload.data + TsRawPayload::row_num_offset_) = 4;
  s = p.ParsePayLoadStruct(payload);
  ASSERT_EQ(s, KStatus::FAIL);
  free(payload.data);
}

TEST_F(TestBinaryToHexstr, payloadRowToHexStrTest) {
  TSSlice payload = GenPayload(10010, 3);
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto row_data = p.GetRowData(0);
  TsRawPayloadRowParser row_p(&data_schema_);
  ASSERT_TRUE(!row_p.IsColNull(row_data, data_schema_.size() - 1));
  TSSlice col_data;
  ASSERT_TRUE(row_p.GetColValueAddr(row_data, data_schema_.size() - 1, &col_data));

  row_data.len -= 1;
  ASSERT_TRUE(!row_p.GetColValueAddr(row_data, data_schema_.size() - 1, &col_data));
  row_data.len += 2;
  ASSERT_TRUE(row_p.GetColValueAddr(row_data, data_schema_.size() - 1, &col_data));
  free(payload.data);
}

TEST_F(TestBinaryToHexstr, HexStrToPayloadTest) {
  std::string hex_str = "1111111111111111111111111111111111111111111111111111111111111111111111111111111";
  TSSlice payload;
  HexStrToBinary(hex_str, payload);
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  ASSERT_EQ(s, KStatus::FAIL);
  free(payload.data);
}
