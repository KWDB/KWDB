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
//

#include <ee_tag_row_batch.h>
#include "ee_kwthd_context.h"
#include "ee_disk_data_container.h"
#include "ee_data_container.h"
#include "ee_data_chunk.h"
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
using namespace kwdbts;  // NOLINT

class TestDiskDataContainer : public ::testing::Test {  // inherit testing::Test
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
  TestDiskDataContainer() = default;
};

TEST_F(TestDiskDataContainer, TestDiskDataContainer) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);
  DataChunkPtr chunk = nullptr;

  k_uint32 total_sample_rows{1};
  ColumnInfo col_info[5];
  k_int32 col_num = 5;

  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  string v3 = "host_0";
  bool v4 = true;

  col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
  col_info[1] = ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info[2] = ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info[3] = ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info[4] = ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  // check insert
  chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
  ASSERT_EQ(chunk->Initialize(), true);
  k_int32 row = chunk->NextLine();
  ASSERT_EQ(row, -1);

  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  chunk->InsertDecimal(0, 2, reinterpret_cast<char*>(&v2), true);
  chunk->InsertData(0, 3, const_cast<char*>(v3.c_str()), v3.length());
  chunk->InsertData(0, 4, reinterpret_cast<char*>(&v4), sizeof(bool));

  ASSERT_EQ(chunk->NextLine(), 0);
  ASSERT_EQ(chunk->Count(), 1);
  ASSERT_EQ(chunk->Capacity(), total_sample_rows);
  ASSERT_EQ(chunk->isFull(), true);
  ASSERT_EQ(chunk->ColumnNum(), 5);
  ASSERT_EQ(chunk->RowSize(), 59);

  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // test single chunk append
  {
    DataContainerPtr tempTable =
      std::make_unique<kwdbts::DiskDataContainer>(order_info, col_info, col_num);
    tempTable->Init();

    tempTable->Append(chunk);

    ASSERT_EQ(tempTable->Count(), 1);

    DataChunkPtr chunkx = nullptr;
    tempTable->Sort();
    tempTable->NextChunk(chunkx);


    auto ptr1 = chunkx->GetData(0, 0);
    k_int64 check_ts;
    memcpy(&check_ts, ptr1, col_info[0].storage_len);
    ASSERT_EQ(check_ts, v1);

    auto ptr2 = chunkx->GetData(0, 1);
    k_double64 check_double;
    memcpy(&check_double, ptr2, col_info[1].storage_len);
    ASSERT_EQ(check_double, v2);

    auto ptr3 = chunkx->GetData(0, 1);
    memcpy(&check_double, ptr3, col_info[2].storage_len);
    ASSERT_EQ(check_double, v2);

    k_uint16 len3 = 0;
    auto ptr4 = chunkx->GetData(0, 3, len3);
    char char_v3[len3];
    memcpy(char_v3, ptr4, len3);
    string check_char = string(char_v3, len3);
    ASSERT_EQ(check_char, v3);

    auto ptr5 = chunkx->GetData(0, 4);
    bool check_bool;
    memcpy(&check_bool, ptr5, col_info[4].storage_len);
    ASSERT_EQ(check_bool, v4);
  }

  DataChunkPtr chunk2;
  chunk2 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
  ASSERT_EQ(chunk2->Initialize(), true);
  chunk2->InsertData(ctx, chunk.get(), nullptr);
  ASSERT_EQ(chunk2->Count(), 1);

  {
    k_int64 check_ts = 0;
    auto ptr11 = chunk2->GetData(0, 0);
    memcpy(&check_ts, ptr11, col_info[0].storage_len);
    ASSERT_EQ(check_ts, v1);
  }

  DataContainerPtr tempTable2 =
    std::make_unique<kwdbts::DiskDataContainer>(order_info, col_info, col_num);
  tempTable2->Init();

  tempTable2->Append(chunk);
  tempTable2->Append(chunk2);

  ASSERT_EQ(tempTable2->Count(), 2);

  // 测试NextLine()、GetData()等接口
  { 
    tempTable2->Sort();
    
    // 测试NextLine()
    k_int32 line = tempTable2->NextLine();
    ASSERT_EQ(line, 0);
    line = tempTable2->NextLine();
    ASSERT_EQ(line, 1);
    line = tempTable2->NextLine();
    ASSERT_EQ(line, -1);
  }
  tempTable2.reset();
  
  // 测试多批次数据的排序和合并
  {
    DataContainerPtr tempTable3 = 
      std::make_unique<kwdbts::DiskDataContainer>(order_info, col_info, col_num);
    tempTable3->Init();
    
    // 添加多个chunk
    for (int i = 0; i < 5; i++) {
      DataChunkPtr chunk_i = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
      ASSERT_EQ(chunk_i->Initialize(), true);
      chunk_i->AddCount();
      
      k_int64 v1_i = v1 + i;
      k_double64 v2_i = v2 + i;
      string v3_i = v3 + "_" + std::to_string(i);
      bool v4_i = (i % 2 == 0);
      
      chunk_i->InsertData(0, 0, reinterpret_cast<char*>(&v1_i), sizeof(k_int64));
      chunk_i->InsertData(0, 1, reinterpret_cast<char*>(&v2_i), sizeof(k_double64));
      chunk_i->InsertDecimal(0, 2, reinterpret_cast<char*>(&v2_i), true);
      chunk_i->InsertData(0, 3, const_cast<char*>(v3_i.c_str()), v3_i.length());
      chunk_i->InsertData(0, 4, reinterpret_cast<char*>(&v4_i), sizeof(bool));
      
      tempTable3->Append(chunk_i);
    }
    
    ASSERT_EQ(tempTable3->Count(), 5);
    
    // 排序并测试
    tempTable3->Sort();
    
    DataChunkPtr result_chunk = nullptr;
    EEIteratorErrCode code = tempTable3->NextChunk(result_chunk);
    ASSERT_EQ(code, EEIteratorErrCode::EE_OK);
    ASSERT_NE(result_chunk, nullptr);
    ASSERT_EQ(result_chunk->Count(), 5);
  }
  
  // 测试NextChunk函数中read_merge_infos_->chunk_infos_.size()不等于1的情况
  {
    DataContainerPtr tempTable4 = 
      std::make_unique<kwdbts::DiskDataContainer>(order_info, col_info, col_num);
    tempTable4->Init();
    
    // 添加10个chunk，形成多个batch
    for (int i = 0; i < 10; i++) {
      DataChunkPtr chunk_i = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
      ASSERT_EQ(chunk_i->Initialize(), true);
      chunk_i->AddCount();
      
      k_int64 v1_i = v1 + i;
      k_double64 v2_i = v2 + i;
      string v3_i = v3 + "_" + std::to_string(i);
      bool v4_i = (i % 2 == 0);
      
      chunk_i->InsertData(0, 0, reinterpret_cast<char*>(&v1_i), sizeof(k_int64));
      chunk_i->InsertData(0, 1, reinterpret_cast<char*>(&v2_i), sizeof(k_double64));
      chunk_i->InsertDecimal(0, 2, reinterpret_cast<char*>(&v2_i), true);
      chunk_i->InsertData(0, 3, const_cast<char*>(v3_i.c_str()), v3_i.length());
      chunk_i->InsertData(0, 4, reinterpret_cast<char*>(&v4_i), sizeof(bool));
      
      tempTable4->Append(chunk_i);
    }
    
    ASSERT_EQ(tempTable4->Count(), 10);
    
    // 排序 - 这会触发SortAndFlushLastChunk中的force_merge条件
    tempTable4->Sort();
    
    // 调用NextChunk，此时read_merge_infos_->chunk_infos_.size()=10，不等于1
    DataChunkPtr result_chunk = nullptr;
    EEIteratorErrCode code = tempTable4->NextChunk(result_chunk);
    ASSERT_EQ(code, EEIteratorErrCode::EE_OK);
    ASSERT_NE(result_chunk, nullptr);
  }
  
  // 专门测试第446行的第二个条件：(chunk_index+1) % MAX_CHUNK_BATCH_NUM == 0
  {
    DataContainerPtr tempTable5 = 
      std::make_unique<kwdbts::DiskDataContainer>(order_info, col_info, col_num);
    tempTable5->Init();
    
    // 添加6个chunk，这样当添加第7个chunk时，会触发batch满的条件
    // (6+1) % 7 = 0，会进入第446行的分支
    for (int i = 0; i < 6; i++) {
      DataChunkPtr chunk_i = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
      ASSERT_EQ(chunk_i->Initialize(), true);
      chunk_i->AddCount();
      
      k_int64 v1_i = v1 + i;
      k_double64 v2_i = v2 + i;
      string v3_i = v3 + "_" + std::to_string(i);
      bool v4_i = (i % 2 == 0);
      
      chunk_i->InsertData(0, 0, reinterpret_cast<char*>(&v1_i), sizeof(k_int64));
      chunk_i->InsertData(0, 1, reinterpret_cast<char*>(&v2_i), sizeof(k_double64));
      chunk_i->InsertDecimal(0, 2, reinterpret_cast<char*>(&v2_i), true);
      chunk_i->InsertData(0, 3, const_cast<char*>(v3_i.c_str()), v3_i.length());
      chunk_i->InsertData(0, 4, reinterpret_cast<char*>(&v4_i), sizeof(bool));
      
      tempTable5->Append(chunk_i);
    }
    
    ASSERT_EQ(tempTable5->Count(), 6);
    
    // 排序以完成所有处理
    tempTable5->Sort();
    
    // 验证数据正确性
    DataChunkPtr result_chunk = nullptr;
    EEIteratorErrCode code = tempTable5->NextChunk(result_chunk);
    ASSERT_EQ(code, EEIteratorErrCode::EE_OK);
    ASSERT_NE(result_chunk, nullptr);
    ASSERT_EQ(result_chunk->Count(), 6);
  }

}
