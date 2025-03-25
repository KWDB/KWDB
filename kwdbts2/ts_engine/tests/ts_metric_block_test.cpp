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
#include "ts_env.h"
#include "ts_metric_block.h"

using namespace kwdbts;  // NOLINT

#define Def_Column(col_var, col_id, pname, ptype, poffset, psize, plength, pencoding, \
       pflag, pmax_len, pversion, pattrtype)      \
    struct AttributeInfo col_var;          \
    {col_var.id = col_id; strcpy(col_var.name, pname); col_var.type = ptype; col_var.offset = poffset; }  \
    {col_var.size = psize; col_var.length = plength; }      \
    {col_var.encoding = pencoding; col_var.flag = pflag; }      \
    {col_var.max_len = pmax_len; col_var.version = pversion; }      \
    {col_var.col_flag = (ColumnFlag)pattrtype; }

class TsMetricBlockTest : public ::testing::Test {
 public:
  std::vector<AttributeInfo> schema_;

 public:
  TsMetricBlockTest() {
    kwdbts::TsEnvInstance::GetInstance().Compressor()->Init();
    genSchema();
  }

  ~TsMetricBlockTest() {}

  void genSchema() {
    Def_Column(col_1, 1, "k_ts", DATATYPE::TIMESTAMP64_LSN, 0, 16, 16, 0, AINFO_NOT_NULL, 3, 1, 0);
    Def_Column(col_2, 2, "v1_value", DATATYPE::INT32,   0, 8, 8, 0, 0, 0, 1, 0);
    Def_Column(col_3, 3, "v2_value", DATATYPE::DOUBLE,  0, 8, 8, 0, 0, 0, 1, 0);
    schema_.clear();
    schema_.push_back(col_1);
    schema_.push_back(col_2);
    schema_.push_back(col_3);
  }

  void genData(DATATYPE type, uint32_t count, char* mem, int start_value) {
    switch (type) {
    case DATATYPE::TIMESTAMP64_LSN :
      for (size_t i = 0; i < count; i++) {
        KUint64(mem + i * 16) = start_value + 1000 * i;
      }
      break;
    case DATATYPE::INT32:
      for (size_t i = 0; i < count; i++) {
        KUint32(mem + 4 * i) = start_value + i * 3;
      }
      break;

    case DATATYPE::DOUBLE:
      for (size_t i = 0; i < count; i++) {
        KDouble64(mem + i * 8) = start_value + i * 12.3;
      }
      break;
    default:
      break;
    }
  }
  void checkData(DATATYPE type, uint32_t count, char* mem, int start_value) {
    switch (type) {
    case DATATYPE::TIMESTAMP64_LSN :
      for (size_t i = 0; i < count; i++) {
        EXPECT_EQ(KUint64(mem + i * 16), start_value + 1000 * i);
      }
      break;
    case DATATYPE::INT32:
      for (size_t i = 0; i < count; i++) {
        EXPECT_EQ(KUint32(mem + 4 * i), start_value + i * 3);
      }
      break;

    case DATATYPE::DOUBLE:
      for (size_t i = 0; i < count; i++) {
        EXPECT_EQ(KDouble64(mem + i * 8), start_value + i * 12.3);
      }
      break;
    default:
      break;
    }
  }
  void markBitmap(TSSlice bitmap) {
    srand(time(NULL));
    for (size_t i = 0; i < bitmap.len; i++) {
      KUint8(bitmap.data + i) = rand() % 255;
    }
  }
  void checkBitmap(TSSlice bitmap, TSSlice bitmap_1) {
    EXPECT_EQ(bitmap.len, bitmap_1.len);
    EXPECT_EQ(0, memcmp(bitmap.data, bitmap_1.data, bitmap.len));
  }

  void genBlockColData(uint32_t col_type_len, uint32_t rows, TsBlockColData* data)  {
    data->data.len = col_type_len * rows;
    data->data.data = reinterpret_cast<char*>(malloc(data->data.len));
    data->bitmap.len = (rows + 7) / 8;
    data->bitmap.data = reinterpret_cast<char*>(malloc(data->bitmap.len));
    data->need_free = true;
  }
};

TEST_F(TsMetricBlockTest, simpleBuilderAndParser) {
  int rows = 10;
  TsMetricBlockBuilder builder(schema_, rows);
  std::vector<TsBlockColData> datas;
  datas.resize(schema_.size());
  for (size_t i = 0; i < schema_.size(); i++){
    genBlockColData(schema_[i].size, rows, &datas[i]);
    genData((DATATYPE)(schema_[i].type), rows, datas[i].data.data, i);
  }

  TSSlice compressed = builder.Build(datas);
  EXPECT_TRUE(compressed.len != 0);
  TsMetricBlockParser parser(schema_, rows);
  EXPECT_TRUE(parser.Parse(compressed));
  for (size_t i = 0; i < schema_.size(); i++) {
    TsBlockColData col_data;
    EXPECT_TRUE(parser.GetColData(i, &col_data));
    checkData((DATATYPE)(schema_[i].type), rows, col_data.data.data, i);
  }
  free(compressed.data);
}

TEST_F(TsMetricBlockTest, simpleBuilderAndParserWithBitmap) {
  int rows = 100;
  TsMetricBlockBuilder builder(schema_, rows);
  std::vector<TsBlockColData> datas;
  datas.resize(schema_.size());
  for (size_t i = 0; i < schema_.size(); i++){
    genBlockColData(schema_[i].size, rows, &datas[i]);
    genData((DATATYPE)(schema_[i].type), rows, datas[i].data.data, i);
    markBitmap(datas[i].bitmap);
  }

  TSSlice compressed = builder.Build(datas);
  EXPECT_TRUE(compressed.len != 0);
  TsMetricBlockParser parser(schema_, rows);
  EXPECT_TRUE(parser.Parse(compressed));
  for (size_t i = 0; i < schema_.size(); i++) {
    TsBlockColData col_data;
    EXPECT_TRUE(parser.GetColData(i, &col_data));
    checkData((DATATYPE)(schema_[i].type), rows, col_data.data.data, i);
    checkBitmap(datas[i].bitmap, col_data.bitmap);
  }
  free(compressed.data);
}
