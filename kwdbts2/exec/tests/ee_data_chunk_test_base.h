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

#include <algorithm>
#include <random>
#include <numeric>
#include <gtest/gtest.h>
#include <string>
#include "ee_kwthd_context.h"
#include "ee_exec_pool.h"

namespace kwdbts {
struct TestRowData {
  k_int64 v1;
  k_double64 v2;
  string v3;
  bool v4;
};

class DataChunkTestBase : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    g_pstBufferPoolInfo = kwdbts::EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    EXPECT_EQ((g_pstBufferPoolInfo != nullptr), true);
  }

  static void TearDownTestCase() {
    kwdbts::KStatus status = kwdbts::EE_MemPoolCleanUp(g_pstBufferPoolInfo);
    EXPECT_EQ(status, kwdbts::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }
  void SetUp() override {
    CreateColInfo(col_info_);
    row_data_ = new TestRowData[row_num_];
    CreateRowData(row_data_, row_num_);
    CreateDataChunk(data_chunk_, col_info_, 4, row_data_, row_num_);
  }

  void TearDown() override {
    SafeDeleteArray(row_data_);
  }

 public:
  void CreateColInfo(ColumnInfo* col_info) {
    col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ,
                             KWDBTypeFamily::TimestampTZFamily);
    col_info[1] =
        ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
    col_info[2] =
        ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
    col_info[3] =
        ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);
  }

  void CreateRowData(TestRowData* row_data, int row_num) {
    indices_.resize(row_num);
    std::iota(indices_.begin(), indices_.end(), 0);

    // init random seed
    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(indices_.begin(), indices_.end(), g);
    for (int i = 0; i < row_num; i++) {
      int j = indices_[i];
      row_data[j].v1 = i;
      row_data[j].v2 = i * 1.1;
      row_data[j].v3 = "host_" + std::to_string(i);
      row_data[j].v4 = i % 2 == 0;
    }
  }

  void CreateDataChunk(DataChunkPtr& chunk, ColumnInfo* col_info, int col_num,
                       TestRowData* row_data, int row_num) {
    chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num, row_num);
    ASSERT_EQ(chunk->Initialize(), true);
    for (int i = 0; i < row_num; i++) {
      chunk->AddCount();
      chunk->InsertData(i, 0, reinterpret_cast<char*>(&row_data[i].v1),
                        sizeof(k_int64));
      chunk->InsertData(i, 1, reinterpret_cast<char*>(&row_data[i].v2),
                        sizeof(k_double64));
      chunk->InsertData(i, 2, const_cast<char*>(row_data[i].v3.c_str()),
                        row_data[i].v3.length());
      chunk->InsertData(i, 3, reinterpret_cast<char*>(&row_data[i].v4),
                        sizeof(bool));
    }
    ASSERT_EQ(chunk->Count(), row_num);
    ASSERT_EQ(chunk->Capacity(), row_num);
    ASSERT_EQ(chunk->isFull(), true);
    ASSERT_EQ(chunk->ColumnNum(), 4);
    ASSERT_EQ(chunk->RowSize(), 51);
  }

  k_bool AssertEqualData(DatumPtr data, k_uint32 row, k_int32 col) {
    k_uint32 sorted_row = indices_[row];
    switch (col_info_[col].storage_type) {
      case roachpb::DataType::TIMESTAMPTZ: {
        k_int64 check_ts;
        memcpy(&check_ts, data, col_info_[col].storage_len);
        return check_ts == row_data_[sorted_row].v1;
      }
      case roachpb::DataType::DOUBLE: {
        k_double64 check_double;
        memcpy(&check_double, data, col_info_[col].storage_len);
        return check_double == row_data_[sorted_row].v2;
      }
      case roachpb::DataType::CHAR: {
        k_uint16 len;
        auto ptr = data;
        memcpy(&len, ptr, sizeof(k_uint16));
        ptr += sizeof(k_uint16);
        string check_char = string(ptr, len);
        return check_char == row_data_[sorted_row].v3;
      }
      case roachpb::DataType::BOOL: {
        bool check_bool;
        memcpy(&check_bool, data, col_info_[col].storage_len);
        return check_bool == row_data_[sorted_row].v4;
      }
      default:
        return false;
    }
  }
  std::vector<int> indices_;
  DataChunkPtr data_chunk_;
  ColumnInfo col_info_[4];
  TestRowData* row_data_;
  k_int32 row_num_{100};
  k_int32 col_num_{4};
};

}  // namespace kwdbts
