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

#include "ee_common.h"
#include "ee_kwthd_context.h"
#include "ee_data_chunk.h"
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_data_chunk_container.h"

using namespace kwdbts;  // NOLINT

// TestDataChunkContainer for multiple model processing
class TestDataChunkContainer : public ::testing::Test {  // inherit testing::Test
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
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestDataChunkContainer() = default;
};

// DataChunkContainer test cases for multiple model processing
TEST_F(TestDataChunkContainer, TestAddAndGetData) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);

  k_uint32 capacity{1024};
  k_int32 col_num = 5;
  ColumnInfo col_info[5];

  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  string v3 = "host_0";
  bool v4 = true;

  col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
  col_info[1] = ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info[2] = ColumnInfo(8, roachpb::DataType::DECIMAL, KWDBTypeFamily::DecimalFamily);
  col_info[3] = ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info[4] = ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  std::vector<Field*> output_fields;
  output_fields.push_back(new FieldLonglong(0, col_info[0].storage_type, col_info[0].storage_len));
  output_fields.push_back(new FieldDouble(1, col_info[1].storage_type, col_info[1].storage_len));
  output_fields.push_back(new FieldDouble(2, col_info[2].storage_type, col_info[2].storage_len));
  output_fields.push_back(new FieldChar(3, col_info[3].storage_type, col_info[3].storage_len));
  output_fields.push_back(new FieldBool(4, col_info[4].storage_type, col_info[4].storage_len));

  // case 0: data chunk kept in memory with default memory size 256M
  // case 1: data chunk materialized in mmap file with 1024 data size threshold
  vector<int> data_size_threshold = {DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY, 256 * 1024};
  vector<bool> data_chunk_is_materialized = {false, true};
  for (int case_num = 0; case_num < 2; ++case_num) {
    DataChunkContainer* data_chunk_container = new DataChunkContainer(data_size_threshold[case_num]);
    for (int i = 0; i < 10; ++i) {
      DataChunkPtr chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
      ASSERT_EQ(chunk->Initialize(), true);
      k_int32 row = chunk->NextLine();
      ASSERT_EQ(row, -1);

      k_int64 int_val = v1;
      k_double64 double_val = v2;
      string str_val = v3;
      bool bool_val = v4;
      for (int j = 0; j < capacity; ++j) {
        chunk->AddCount();
        int_val += 1;
        double_val += 1;
        str_val.at(5) += 1;
        bool_val = !bool_val;
        chunk->InsertData(j, 0, reinterpret_cast<char*>(&int_val), sizeof(k_int64));
        chunk->InsertData(j, 1, reinterpret_cast<char*>(&double_val), sizeof(k_double64));
        chunk->InsertDecimal(j, 2, reinterpret_cast<char*>(&double_val), true);
        chunk->InsertData(j, 3, const_cast<char*>(str_val.c_str()), str_val.length());
        chunk->InsertData(j, 4, reinterpret_cast<char*>(&bool_val), sizeof(bool));
      }
      ASSERT_EQ(data_chunk_container->AddDataChunk(chunk), KStatus::SUCCESS);
    }

    ASSERT_EQ(data_chunk_container->IsMaterialized(), data_chunk_is_materialized[case_num]);

    // check the data is expected
    for (int i = 0; i < 10; ++i) {
      DataChunkPtr& data = data_chunk_container->GetDataChunk(i);
      ASSERT_EQ(data->NextLine(), 0);
      ASSERT_EQ(data->Count(), capacity);
      ASSERT_EQ(data->Capacity(), capacity);
      ASSERT_EQ(data->isFull(), true);
      ASSERT_EQ(data->ColumnNum(), 5);
      ASSERT_EQ(data->RowSize(), 60);

      k_int64 int_val = v1;
      k_double64 double_val = v2;
      string str_val = v3;
      bool bool_val = v4;
      k_int64 check_ts;
      k_double64 check_double;
      k_uint16 len3 = 0;
      char char_v3[16];
      bool check_bool;

      for (int j = 0; j < capacity; ++j) {
        int_val += 1;
        double_val += 1;
        str_val.at(5) += 1;
        bool_val = !bool_val;

        auto ptr1 = data->GetData(j, 0);
        memcpy(&check_ts, ptr1, col_info[0].storage_len);
        ASSERT_EQ(check_ts, int_val);

        auto ptr2 = data->GetData(j, 1);
        memcpy(&check_double, ptr2, col_info[1].storage_len);
        ASSERT_EQ(check_double, double_val);

        auto ptr3 = data->GetData(j, 2) + BOOL_WIDE;
        memcpy(&check_double, ptr3, col_info[2].storage_len);
        ASSERT_EQ(check_double, double_val);

        auto ptr4 = data->GetData(j, 3, len3);
        memcpy(char_v3, ptr4, len3);
        string check_char = string(char_v3, len3);
        ASSERT_EQ(check_char, str_val);

        auto ptr5 = data->GetData(j, 4);
        memcpy(&check_bool, ptr5, col_info[4].storage_len);
        ASSERT_EQ(check_bool, bool_val);
      }
    }
    delete data_chunk_container;
  }

  for (auto field : output_fields) {
    SafeDeletePointer(field);
  }
}

TEST_F(TestDataChunkContainer, TestGetBatchCount) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);

  k_uint32 capacity{1024};
  k_int32 col_num = 2;
  ColumnInfo col_info[2];

  col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
  col_info[1] = ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);

  DataChunkContainer* data_chunk_container = new DataChunkContainer(DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY);
  
  // Test GetBatchCount with no data chunks
  ASSERT_EQ(data_chunk_container->GetBatchCount(), 0);
  
  // Add one data chunk and test
  DataChunkPtr chunk1 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk1->Initialize(), true);
  chunk1->AddCount();
  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  chunk1->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk1->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  ASSERT_EQ(data_chunk_container->AddDataChunk(chunk1), KStatus::SUCCESS);
  ASSERT_EQ(data_chunk_container->GetBatchCount(), 1);
  
  // Add another data chunk and test
  DataChunkPtr chunk2 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk2->Initialize(), true);
  chunk2->AddCount();
  chunk2->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk2->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  ASSERT_EQ(data_chunk_container->AddDataChunk(chunk2), KStatus::SUCCESS);
  ASSERT_EQ(data_chunk_container->GetBatchCount(), 2);
  
  delete data_chunk_container;
}

TEST_F(TestDataChunkContainer, TestReserveEnoughCapacity) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);

  k_uint32 capacity{1024};
  k_int32 col_num = 2;
  ColumnInfo col_info[2];

  col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
  col_info[1] = ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);

  // Create container with small threshold to force mmap usage
  DataChunkContainer* data_chunk_container = new DataChunkContainer(1024);
  
  // Add first chunk to trigger mmap creation
  DataChunkPtr chunk1 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk1->Initialize(), true);
  chunk1->AddCount();
  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  chunk1->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk1->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  ASSERT_EQ(data_chunk_container->AddDataChunk(chunk1), KStatus::SUCCESS);
  
  // Add second chunk - this should test Reserve with enough capacity
  DataChunkPtr chunk2 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk2->Initialize(), true);
  chunk2->AddCount();
  chunk2->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk2->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  ASSERT_EQ(data_chunk_container->AddDataChunk(chunk2), KStatus::SUCCESS);
  
  // Verify both chunks were added
  ASSERT_EQ(data_chunk_container->GetBatchCount(), 2);
  
  delete data_chunk_container;
}

TEST_F(TestDataChunkContainer, TestEdgeCases) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);

  k_uint32 capacity{1024};
  k_int32 col_num = 2;
  ColumnInfo col_info[2];

  col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
  col_info[1] = ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);

  // Test with zero capacity threshold
  DataChunkContainer* data_chunk_container = new DataChunkContainer(0);
  
  // Add a chunk - should go directly to mmap
  DataChunkPtr chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk->Initialize(), true);
  chunk->AddCount();
  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  ASSERT_EQ(data_chunk_container->AddDataChunk(chunk), KStatus::SUCCESS);
  
  // Verify the chunk was added
  ASSERT_EQ(data_chunk_container->GetBatchCount(), 1);
  ASSERT_TRUE(data_chunk_container->IsMaterialized());
  
  delete data_chunk_container;
}
