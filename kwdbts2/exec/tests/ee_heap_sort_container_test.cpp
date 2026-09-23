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

#include <queue>
#include <limits>

#include <ee_tag_row_batch.h>
#include "ee_kwthd_context.h"
#define private public
#define protected public
#include "ee_heap_sort_container.h"
#include "ee_data_container.h"
#include "ee_data_chunk.h"
#undef protected
#undef private
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_data_chunk_test_base.h"
using namespace kwdbts;  // NOLINT

class TestHeapSortContainer : public DataChunkTestBase {
 public:
  TestHeapSortContainer() = default;
};

class OwnedColumnInfoChunk : public DataChunk {
 public:
  OwnedColumnInfoChunk(ColumnInfo* col_info, k_uint32 col_num, k_uint32 capacity)
      : DataChunk(col_info, col_num, capacity), owned_col_info_(col_info) {}

  ~OwnedColumnInfoChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

std::unique_ptr<DataChunk> MakeIntVarcharChunk(
    const std::vector<std::pair<k_int32, std::string>>& rows) {
  auto* col_info = new ColumnInfo[2];
  col_info[0] = ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  col_info[1] =
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(
      col_info, 2, static_cast<k_uint32>(rows.size()));
  EXPECT_TRUE(chunk->Initialize());
  for (size_t i = 0; i < rows.size(); ++i) {
    chunk->AddCount();
    auto value = rows[i].first;
    chunk->InsertData(i, 0, reinterpret_cast<char*>(&value), sizeof(value));
    chunk->InsertData(i, 1, const_cast<char*>(rows[i].second.data()),
                      rows[i].second.size());
  }
  return chunk;
}

TEST_F(TestHeapSortContainer, TestDiskDataContainer) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // test single chunk append
  {
    DataContainerPtr tempDataContainer =
        std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_,
                                                    col_num_, row_num_);
    tempDataContainer->Init();

    tempDataContainer->Append(data_chunk_);

    ASSERT_EQ(tempDataContainer->Count(), row_num_);

    tempDataContainer->Sort();
    
    for (int i = 0; i < row_num_; i++) {
      auto ptr1 = tempDataContainer->GetData(i, 0);
      ASSERT_TRUE(AssertEqualData(ptr1, i, 0));

      auto ptr2 = tempDataContainer->GetData(i, 1);
      ASSERT_TRUE(AssertEqualData(ptr2, i, 1));
      auto ptr3 = tempDataContainer->GetData(i, 2);
      ASSERT_TRUE(AssertEqualData(ptr3, i, 2));
      auto ptr4 = tempDataContainer->GetData(i, 3);
      ASSERT_TRUE(AssertEqualData(ptr4, i, 3));
    }
  }
}

TEST_F(TestHeapSortContainer, HeapContainerHandlesQueueAppendAndSelectors) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});
  HeapSortContainer container(order_info, col_info_, col_num_, 10);
  ASSERT_EQ(container.Init(), KStatus::SUCCESS);

  DataChunkPtr empty_chunk = std::make_unique<kwdbts::DataChunk>(col_info_, 4, 0);
  ASSERT_EQ(empty_chunk->Initialize(), true);
  EXPECT_EQ(container.Append(empty_chunk), KStatus::SUCCESS);
  EXPECT_EQ(container.Count(), 0U);
  EXPECT_EQ(container.NextChunk(empty_chunk), EEIteratorErrCode::EE_ERROR);

  DataChunkPtr second_chunk;
  CreateDataChunk(second_chunk, col_info_, col_num_, row_data_, row_num_);

  std::queue<DataChunkPtr> buffer;
  buffer.push(std::move(data_chunk_));
  buffer.push(std::move(second_chunk));
  EXPECT_EQ(container.Append(buffer), KStatus::SUCCESS);
  EXPECT_TRUE(buffer.empty());
  EXPECT_EQ(container.Count(), 10U);

  k_uint16 raw_len = 0;
  EXPECT_NE(container.GetData(0, 0), nullptr);
  EXPECT_NE(container.GetData(0, 2, raw_len), nullptr);
  EXPECT_GT(raw_len, 0);
  EXPECT_FALSE(container.IsNull(0, 2));

  EXPECT_EQ(container.Sort(), KStatus::SUCCESS);
  k_int64 previous_order_key = std::numeric_limits<k_int64>::min();
  for (int i = 0; i < 10; ++i) {
    ASSERT_GE(container.NextLine(), 0);
    EXPECT_FALSE(container.IsNull(2));
    auto* ordered_value = reinterpret_cast<k_int64*>(container.GetData(i, 0));
    ASSERT_NE(ordered_value, nullptr);
    EXPECT_GE(*ordered_value, previous_order_key);
    previous_order_key = *ordered_value;

    auto* current_value = reinterpret_cast<k_int64*>(container.GetData(0));
    ASSERT_NE(current_value, nullptr);
    EXPECT_EQ(*current_value, *ordered_value);

    k_uint16 str_len = 0;
    EXPECT_NE(container.GetData(i, 2, str_len), nullptr);
    EXPECT_GT(str_len, 0);
    EXPECT_NE(container.GetData(i, 1), nullptr);
    EXPECT_NE(container.GetData(i, 3), nullptr);
  }
  EXPECT_EQ(container.NextLine(), -1);
}

TEST_F(TestHeapSortContainer, ComputeCapacityFallsBackToOneForWideRows) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});

  ColumnInfo wide_column[] = {
      ColumnInfo(131072, roachpb::DataType::CHAR,
                 KWDBTypeFamily::StringFamily)};
  HeapSortContainer container(order_info, wide_column, 1);
  ASSERT_EQ(container.Init(), KStatus::SUCCESS);

  DataChunkPtr chunk = std::make_unique<kwdbts::DataChunk>(wide_column, 1, 2);
  ASSERT_EQ(chunk->Initialize(), true);
  chunk->AddCount();
  chunk->InsertData(0, 0, const_cast<char*>("a"), 1);
  chunk->AddCount();
  chunk->InsertData(1, 0, const_cast<char*>("b"), 1);

  EXPECT_EQ(container.Append(chunk), KStatus::SUCCESS);
  EXPECT_EQ(container.Count(), 1U);
}

TEST_F(TestHeapSortContainer, HeapContainerCoversDefaultCapacityAndInputChunkAccess) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});

  ColumnInfo columns[] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily),
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};

  HeapSortContainer default_capacity_container(order_info, columns, 2);
  ASSERT_EQ(default_capacity_container.Init(), KStatus::SUCCESS);

  auto one_row_chunk = MakeIntVarcharChunk({{1, "alpha"}});
  ASSERT_EQ(default_capacity_container.Append(one_row_chunk), KStatus::SUCCESS);
  EXPECT_GT(default_capacity_container.Count(), 0U);
  EXPECT_EQ(default_capacity_container.NextLine(), 0);

  HeapSortContainer overflow_container(order_info, columns, 2, 1);
  ASSERT_EQ(overflow_container.Init(), KStatus::SUCCESS);

  auto overflow_chunk = MakeIntVarcharChunk({{2, "beta"}, {1, "alpha"}});
  ASSERT_EQ(overflow_container.Append(overflow_chunk), KStatus::SUCCESS);
  EXPECT_EQ(overflow_container.Count(), 1U);

  k_uint16 len = 0;
  auto* input_ptr = overflow_container.GetData(UINT32_MAX, 1, len);
  ASSERT_NE(input_ptr, nullptr);
  EXPECT_EQ(std::string(input_ptr, len), "alpha");

  ASSERT_EQ(overflow_container.Sort(), KStatus::SUCCESS);
  EXPECT_FALSE(overflow_container.IsNull(0, 1));
}

TEST_F(TestHeapSortContainer, HeapContainerCoversResetAndFailurePaths) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});

  ColumnInfo columns[] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily),
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};

  auto* saved_pool = g_pstBufferPoolInfo;
  g_pstBufferPoolInfo = nullptr;
  {
    HeapSortContainer init_fail_container(order_info, columns, 2, 2);
    EXPECT_EQ(init_fail_container.Init(), KStatus::FAIL);
  }
  g_pstBufferPoolInfo = saved_pool;

  HeapSortContainer reset_container(order_info, columns, 2, 2);
  ASSERT_EQ(reset_container.Init(), KStatus::SUCCESS);
  auto reset_chunk = MakeIntVarcharChunk({{1, "reset"}});
  ASSERT_EQ(reset_container.Append(reset_chunk), KStatus::SUCCESS);
  ASSERT_NE(reset_container.mem_chunk_ptr_, nullptr);
  reset_container.Reset();
  EXPECT_EQ(reset_container.mem_chunk_ptr_, nullptr);

  HeapSortContainer append_fail_container(order_info, columns, 2, 2);
  ASSERT_EQ(append_fail_container.Init(), KStatus::SUCCESS);
  append_fail_container.mem_chunk_ptr_->GetVarStrContainer()->len =
      static_cast<k_int32>(EE_MaxAllocSize);
  auto append_fail_chunk = MakeIntVarcharChunk({{2, "fail"}});
  EXPECT_EQ(append_fail_container.Append(append_fail_chunk), KStatus::FAIL);

  HeapSortContainer queue_fail_container(order_info, columns, 2, 2);
  ASSERT_EQ(queue_fail_container.Init(), KStatus::SUCCESS);
  queue_fail_container.mem_chunk_ptr_->GetVarStrContainer()->len =
      static_cast<k_int32>(EE_MaxAllocSize);
  std::queue<DataChunkPtr> buffer;
  buffer.push(MakeIntVarcharChunk({{3, "queue"}}));
  EXPECT_EQ(queue_fail_container.Append(buffer), KStatus::FAIL);
  EXPECT_EQ(buffer.size(), 1U);
}

// Test HeapSortContainer with TIMESTAMP_TZ type
TEST_F(TestHeapSortContainer, TestTimestampTypeComparison) {
  struct TestRowDataTs {
    k_int64 v1;
    k_double64 v2;
    string v3;
    bool v4;
  };

  std::vector<int> indices_ts;
  DataChunkPtr data_chunk_ts;
  ColumnInfo col_info_ts[4];
  TestRowDataTs* row_data_ts;
  k_int32 row_num_ts{100};
  k_int32 col_num_ts{4};

  col_info_ts[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ,
                            KWDBTypeFamily::TimestampFamily);
  col_info_ts[1] = 
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_ts[2] = 
      ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info_ts[3] = 
      ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  row_data_ts = new TestRowDataTs[row_num_ts];

  indices_ts.resize(row_num_ts);
  std::iota(indices_ts.begin(), indices_ts.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_ts.begin(), indices_ts.end(), g);
  for (int i = 0; i < row_num_ts; i++) {
    int j = indices_ts[i]; 
    row_data_ts[j].v1 = i;
    row_data_ts[j].v2 = i * 1.1;
    row_data_ts[j].v3 = "host_" + std::to_string(i);
    row_data_ts[j].v4 = i % 2 == 0;
  }

  data_chunk_ts = std::make_unique<kwdbts::DataChunk>(col_info_ts, col_num_ts, row_num_ts);
  ASSERT_EQ(data_chunk_ts->Initialize(), true);
  for (int i = 0; i < row_num_ts; i++) {
    data_chunk_ts->AddCount();
    data_chunk_ts->InsertData(i, 0, reinterpret_cast<char*>(&row_data_ts[i].v1),
                      sizeof(k_int64));
    data_chunk_ts->InsertData(i, 1, reinterpret_cast<char*>(&row_data_ts[i].v2),    
                      sizeof(k_double64));
    data_chunk_ts->InsertData(i, 2, const_cast<char*>(row_data_ts[i].v3.c_str()),
                      row_data_ts[i].v3.length());
    data_chunk_ts->InsertData(i, 3, reinterpret_cast<char*>(&row_data_ts[i].v4),  
                      sizeof(bool));
  }

  // Create order info with BOOL column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});
  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer = 
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_ts, 
                                                  col_num_ts, row_num_ts);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_ts);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_ts);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();
 
  auto ptr1 = tempDataContainer->GetData(0, 0);
  k_uint32 sorted_row = indices_ts[0];
  k_int64 check_ts;
  memcpy(&check_ts, ptr1, col_info_ts[0].storage_len);
  ASSERT_EQ(check_ts, row_data_ts[sorted_row].v1);

  delete[] row_data_ts;
}

// Test HeapSortContainer with INT type
TEST_F(TestHeapSortContainer, TestIntTypeComparison) {
  struct TestRowDataInt {
    k_int32 v1;
    k_double64 v2;
    string v3;
    bool v4;
  };

  std::vector<int> indices_int;
  DataChunkPtr data_chunk_int;
  ColumnInfo col_info_int[4];
  TestRowDataInt* row_data_int;
  k_int32 row_num_int{100};
  k_int32 col_num_int{4};

  col_info_int[0] = ColumnInfo(4, roachpb::DataType::INT,
                            KWDBTypeFamily::IntFamily);
  col_info_int[1] = 
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_int[2] = 
      ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info_int[3] = 
      ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  row_data_int = new TestRowDataInt[row_num_int];

  indices_int.resize(row_num_int);
  std::iota(indices_int.begin(), indices_int.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_int.begin(), indices_int.end(), g);
  for (int i = 0; i < row_num_int; i++) {
    int j = indices_int[i]; 
    row_data_int[j].v1 = i;
    row_data_int[j].v2 = i * 1.1;
    row_data_int[j].v3 = "host_" + std::to_string(i);
    row_data_int[j].v4 = i % 2 == 0;
  }

  data_chunk_int = std::make_unique<kwdbts::DataChunk>(col_info_int, col_num_int, row_num_int);
  ASSERT_EQ(data_chunk_int->Initialize(), true);
  for (int i = 0; i < row_num_int; i++) {
    data_chunk_int->AddCount();
    data_chunk_int->InsertData(i, 0, reinterpret_cast<char*>(&row_data_int[i].v1),
                      sizeof(k_int32));
    data_chunk_int->InsertData(i, 1, reinterpret_cast<char*>(&row_data_int[i].v2),    
                      sizeof(k_double64));
    data_chunk_int->InsertData(i, 2, const_cast<char*>(row_data_int[i].v3.c_str()),
                      row_data_int[i].v3.length());
    data_chunk_int->InsertData(i, 3, reinterpret_cast<char*>(&row_data_int[i].v4),  
                      sizeof(bool));
  }

  // Create order info with INT column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer = 
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_int, 
                                                  col_num_int, row_num_int);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_int);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_int);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();
 
  auto ptr1 = tempDataContainer->GetData(0, 0);
  k_uint32 sorted_row = indices_int[0];
  k_int32 check_int;
  memcpy(&check_int, ptr1, col_info_int[0].storage_len);
  ASSERT_EQ(check_int, row_data_int[sorted_row].v1);

  // Clean up
  delete[] row_data_int;
}

// Test HeapSortContainer with SMALLINT type
TEST_F(TestHeapSortContainer, TestSIntTypeComparison) {
  struct TestRowDataSInt {
    k_int16 v1;
    k_double64 v2;
    string v3;
    bool v4;
  };

  std::vector<int> indices_sint;
  DataChunkPtr data_chunk_sint;
  ColumnInfo col_info_sint[4];
  TestRowDataSInt* row_data_sint;
  k_int32 row_num_sint{100};
  k_int32 col_num_sint{4};

  col_info_sint[0] = ColumnInfo(2, roachpb::DataType::SMALLINT,
                            KWDBTypeFamily::IntFamily);
  col_info_sint[1] = 
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_sint[2] = 
      ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info_sint[3] = 
      ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  row_data_sint = new TestRowDataSInt[row_num_sint];

  indices_sint.resize(row_num_sint);
  std::iota(indices_sint.begin(), indices_sint.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_sint.begin(), indices_sint.end(), g);
  for (int i = 0; i < row_num_sint; i++) {
    int j = indices_sint[i]; 
    row_data_sint[j].v1 = i;
    row_data_sint[j].v2 = i * 1.1;
    row_data_sint[j].v3 = "host_" + std::to_string(i);
    row_data_sint[j].v4 = i % 2 == 0;
  }

  data_chunk_sint = std::make_unique<kwdbts::DataChunk>(col_info_sint, col_num_sint, row_num_sint);
  ASSERT_EQ(data_chunk_sint->Initialize(), true);
  for (int i = 0; i < row_num_sint; i++) {
    data_chunk_sint->AddCount();
    data_chunk_sint->InsertData(i, 0, reinterpret_cast<char*>(&row_data_sint[i].v1),
                      sizeof(k_int16));
    data_chunk_sint->InsertData(i, 1, reinterpret_cast<char*>(&row_data_sint[i].v2),    
                      sizeof(k_double64));
    data_chunk_sint->InsertData(i, 2, const_cast<char*>(row_data_sint[i].v3.c_str()),
                      row_data_sint[i].v3.length());
    data_chunk_sint->InsertData(i, 3, reinterpret_cast<char*>(&row_data_sint[i].v4),  
                      sizeof(bool));
  }

  // Create order info with SMALLINT column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer = 
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_sint, 
                                                  col_num_sint, row_num_sint);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_sint);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_sint);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();
 
  auto ptr1 = tempDataContainer->GetData(0, 0);
  k_uint32 sorted_row = indices_sint[0];
  k_int16 check_sint;
  memcpy(&check_sint, ptr1, col_info_sint[0].storage_len);
  ASSERT_EQ(check_sint, row_data_sint[sorted_row].v1);

  // Clean up
  delete[] row_data_sint;
}

// Test HeapSortContainer with FLOAT type
TEST_F(TestHeapSortContainer, TestFloatTypeComparison) {
  struct TestRowDataFloat {
    k_float32 v1;
    k_double64 v2;
    string v3;
    bool v4;
  };

  std::vector<int> indices_float;
  DataChunkPtr data_chunk_float;
  ColumnInfo col_info_float[4];
  TestRowDataFloat* row_data_float;
  k_int32 row_num_float{100};
  k_int32 col_num_float{4};

  col_info_float[0] = ColumnInfo(4, roachpb::DataType::FLOAT,
                            KWDBTypeFamily::DecimalFamily);
  col_info_float[1] = 
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_float[2] = 
      ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info_float[3] = 
      ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  row_data_float = new TestRowDataFloat[row_num_float];

  indices_float.resize(row_num_float);
  std::iota(indices_float.begin(), indices_float.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_float.begin(), indices_float.end(), g);
  for (int i = 0; i < row_num_float; i++) {
    int j = indices_float[i]; 
    row_data_float[j].v1 = i * 1.1f;
    row_data_float[j].v2 = i * 2.2;
    row_data_float[j].v3 = "host_" + std::to_string(i);
    row_data_float[j].v4 = i % 2 == 0;
  }

  data_chunk_float = std::make_unique<kwdbts::DataChunk>(col_info_float, col_num_float, row_num_float);
  ASSERT_EQ(data_chunk_float->Initialize(), true);
  for (int i = 0; i < row_num_float; i++) {
    data_chunk_float->AddCount();
    data_chunk_float->InsertData(i, 0, reinterpret_cast<char*>(&row_data_float[i].v1),
                      sizeof(k_float32));
    data_chunk_float->InsertData(i, 1, reinterpret_cast<char*>(&row_data_float[i].v2),    
                      sizeof(k_double64));
    data_chunk_float->InsertData(i, 2, const_cast<char*>(row_data_float[i].v3.c_str()),
                      row_data_float[i].v3.length());
    data_chunk_float->InsertData(i, 3, reinterpret_cast<char*>(&row_data_float[i].v4),  
                      sizeof(bool));
  }

  // Create order info with FLOAT column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer = 
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_float, 
                                                  col_num_float, row_num_float);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_float);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_float);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();
 
  auto ptr1 = tempDataContainer->GetData(0, 0);
  k_uint32 sorted_row = indices_float[0];
  k_float32 check_float;
  memcpy(&check_float, ptr1, col_info_float[0].storage_len);
  ASSERT_EQ(check_float, row_data_float[sorted_row].v1);

  // Clean up
  delete[] row_data_float;
}

// Test HeapSortContainer with Bool type
TEST_F(TestHeapSortContainer, TestBoolTypeComparison) {
  struct TestRowDataBool {
    bool v1;  // bool type at first position
    k_int64 v2;
    k_double64 v3;
    string v4;
  };

  std::vector<int> indices_bool;
  DataChunkPtr data_chunk_bool;
  ColumnInfo col_info_bool[4];
  TestRowDataBool* row_data_bool;
  k_int32 row_num_bool{100};
  k_int32 col_num_bool{4};

  col_info_bool[0] = ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);
  col_info_bool[1] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ,
                            KWDBTypeFamily::TimestampFamily);
  col_info_bool[2] = 
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_bool[3] = 
      ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);

  row_data_bool = new TestRowDataBool[row_num_bool];

  indices_bool.resize(row_num_bool);
  std::iota(indices_bool.begin(), indices_bool.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_bool.begin(), indices_bool.end(), g);
  for (int i = 0; i < row_num_bool; i++) {
    int j = indices_bool[i]; 
    row_data_bool[j].v1 = i % 2 == 0;
    row_data_bool[j].v2 = i;
    row_data_bool[j].v3 = i * 1.1;
    row_data_bool[j].v4 = "host_" + std::to_string(i);
  }

  data_chunk_bool = std::make_unique<kwdbts::DataChunk>(col_info_bool, col_num_bool, row_num_bool);
  ASSERT_EQ(data_chunk_bool->Initialize(), true);
  for (int i = 0; i < row_num_bool; i++) {
    data_chunk_bool->AddCount();
    data_chunk_bool->InsertData(i, 0, reinterpret_cast<char*>(&row_data_bool[i].v1),
                      sizeof(bool));
    data_chunk_bool->InsertData(i, 1, reinterpret_cast<char*>(&row_data_bool[i].v2),
                      sizeof(k_int64));
    data_chunk_bool->InsertData(i, 2, reinterpret_cast<char*>(&row_data_bool[i].v3),    
                      sizeof(k_double64));
    data_chunk_bool->InsertData(i, 3, const_cast<char*>(row_data_bool[i].v4.c_str()),
                      row_data_bool[i].v4.length());
  }

  // Create order info with BOOL column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});

  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer = 
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_bool, 
                                                  col_num_bool, row_num_bool);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_bool);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_bool);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();
 
  // Verify the first row's bool value is false (since we sorted in ASC order)
  auto ptr1 = tempDataContainer->GetData(0, 0);
  bool check_bool;
  memcpy(&check_bool, ptr1, col_info_bool[0].storage_len);
  ASSERT_EQ(check_bool, false);

  // Clean up
  delete[] row_data_bool;
}

// Test HeapSortContainer with String type
TEST_F(TestHeapSortContainer, TestStringTypeComparison) {
  struct TestRowDataStr {
    string v1;  // string type at first position
    k_int64 v2;
    k_double64 v3;
    bool v4;
  };

  std::vector<int> indices_str;
  DataChunkPtr data_chunk_str;
  ColumnInfo col_info_str[4];
  TestRowDataStr* row_data_str;
  k_int32 row_num_str{100};
  k_int32 col_num_str{4};

  col_info_str[0] = ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info_str[1] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ,
                            KWDBTypeFamily::TimestampFamily);
  col_info_str[2] = 
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_str[3] = 
      ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  row_data_str = new TestRowDataStr[row_num_str];

  indices_str.resize(row_num_str);
  std::iota(indices_str.begin(), indices_str.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_str.begin(), indices_str.end(), g);
  for (int i = 0; i < row_num_str; i++) {
    int j = indices_str[i]; 
    row_data_str[j].v1 = "host_" + std::to_string(i);
    row_data_str[j].v2 = i;
    row_data_str[j].v3 = i * 1.1;
    row_data_str[j].v4 = i % 2 == 0;
  }

  data_chunk_str = std::make_unique<kwdbts::DataChunk>(col_info_str, col_num_str, row_num_str);
  ASSERT_EQ(data_chunk_str->Initialize(), true);
  for (int i = 0; i < row_num_str; i++) {
    data_chunk_str->AddCount();
    data_chunk_str->InsertData(i, 0, const_cast<char*>(row_data_str[i].v1.c_str()),
                      row_data_str[i].v1.length());
    data_chunk_str->InsertData(i, 1, reinterpret_cast<char*>(&row_data_str[i].v2),
                      sizeof(k_int64));
    data_chunk_str->InsertData(i, 2, reinterpret_cast<char*>(&row_data_str[i].v3),    
                      sizeof(k_double64));
    data_chunk_str->InsertData(i, 3, reinterpret_cast<char*>(&row_data_str[i].v4),  
                      sizeof(bool));
  }

  // Create order info with String column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});

  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer = 
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_str, 
                                                  col_num_str, row_num_str);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_str);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_str);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();
 
  // Verify the first row's string value is "host_0" (since we sorted in ASC order)
  auto ptr1 = tempDataContainer->GetData(0, 0);
  k_uint32 sorted_row = indices_str[0];
  k_uint16 len;
  memcpy(&len, ptr1, sizeof(k_uint16));
  ptr1 += sizeof(k_uint16);
  string check_char = string(ptr1, len);
  ASSERT_EQ(check_char, row_data_str[sorted_row].v1);

  // Clean up
  delete[] row_data_str;
}

// Test HeapSortContainer with DECIMAL type
TEST_F(TestHeapSortContainer, TestDecimalTypeComparison) {
  struct TestRowDataDecimal {
    k_int64 v1; // Using k_int64 to represent decimal values
    k_double64 v2;
    string v3;
    bool v4;
  };

  std::vector<int> indices_decimal;
  DataChunkPtr data_chunk_decimal;
  ColumnInfo col_info_decimal[4];
  TestRowDataDecimal* row_data_decimal;
  k_int32 row_num_decimal{100};
  k_int32 col_num_decimal{4};

  col_info_decimal[0] = ColumnInfo(8, roachpb::DataType::BIGINT,
                            KWDBTypeFamily::DecimalFamily);
  col_info_decimal[1] =
      ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info_decimal[2] =
      ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info_decimal[3] =
      ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  row_data_decimal = new TestRowDataDecimal[row_num_decimal];

  indices_decimal.resize(row_num_decimal);
  std::iota(indices_decimal.begin(), indices_decimal.end(), 0);

  // init random seed
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(indices_decimal.begin(), indices_decimal.end(), g);
  for (int i = 0; i < row_num_decimal; i++) {
    int j = indices_decimal[i];
    row_data_decimal[j].v1 = i * 1000; // Using integer values to represent decimal
    row_data_decimal[j].v2 = i * 2.2;
    row_data_decimal[j].v3 = "host_" + std::to_string(i);
    row_data_decimal[j].v4 = i % 2 == 0;
  }
data_chunk_decimal = std::make_unique<kwdbts::DataChunk>(col_info_decimal, col_num_decimal, row_num_decimal);
  ASSERT_EQ(data_chunk_decimal->Initialize(), true);
  for (int i = 0; i < row_num_decimal; i++) {
    data_chunk_decimal->AddCount();
    data_chunk_decimal->InsertData(i, 0, reinterpret_cast<char*>(&row_data_decimal[i].v1),
                      sizeof(k_int64));
    data_chunk_decimal->InsertData(i, 1, reinterpret_cast<char*>(&row_data_decimal[i].v2),
                      sizeof(k_double64));
    data_chunk_decimal->InsertData(i, 2, const_cast<char*>(row_data_decimal[i].v3.c_str()),
                      row_data_decimal[i].v3.length());
    data_chunk_decimal->InsertData(i, 3, reinterpret_cast<char*>(&row_data_decimal[i].v4),
                      sizeof(bool));
  }

  // Create order info with DECIMAL column as the first sort key
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // Create HeapSortContainer which uses OrderColumnCompare
  DataContainerPtr tempDataContainer =
      std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_decimal,
                                                  col_num_decimal, row_num_decimal);
  tempDataContainer->Init();

  // Append data chunk
  tempDataContainer->Append(data_chunk_decimal);

  // Verify data count
  ASSERT_EQ(tempDataContainer->Count(), row_num_decimal);

  // Sort the container (this will call OrderColumnCompare::operator())
  tempDataContainer->Sort();

  auto ptr1 = tempDataContainer->GetData(0, 0);
  k_int64 check_decimal;
  memcpy(&check_decimal, ptr1, col_info_decimal[0].storage_len);
  ASSERT_EQ(check_decimal, 0); // First value should be 0 after sorting

  // Clean up
  delete[] row_data_decimal;
}
