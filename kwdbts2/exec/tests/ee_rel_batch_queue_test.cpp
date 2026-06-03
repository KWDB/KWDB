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

#include <chrono>
#include <thread>

#include "ee_tag_row_batch.h"
#include "ee_kwthd_context.h"
#include "ee_data_chunk.h"
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_rel_batch_queue.h"

using namespace kwdbts;  // NOLINT

namespace {

std::vector<Field*> MakeSingleIntOutputFields() {
  std::vector<Field*> output_fields;
  output_fields.push_back(new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)));
  return output_fields;
}

void DeleteFields(std::vector<Field*>& output_fields) {
  for (auto* field : output_fields) {
    SafeDeletePointer(field);
  }
  output_fields.clear();
}

std::unique_ptr<DataChunk> MakeSingleIntChunk(k_int32 value) {
  ColumnInfo col_info[1];
  col_info[0] = ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                           KWDBTypeFamily::IntFamily);
  auto chunk = std::make_unique<DataChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&value), sizeof(value));
  return chunk;
}

}  // namespace

// TestRelBatchQueue for multiple model processing
class TestRelBatchQueue : public ::testing::Test {  // inherit testing::Test
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
  TestRelBatchQueue() = default;
};

// RelBatchQueue test cases for multiple model processing
TEST_F(TestRelBatchQueue, TestPutAndGetBatch) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);
  DataChunkPtr chunk = nullptr;

  k_uint32 capacity{1};
  ColumnInfo col_info[5];
  k_int32 col_num = 5;

  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  string v3 = "host_0";
  bool v4 = true;
  k_int64 d1 = 15623456;
  k_double64 d2 = 39.789;
  string d3 = "kwbase";
  bool d4 = false;

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

  chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk->Initialize(), true);
  k_int32 row = chunk->NextLine();
  ASSERT_EQ(row, -1);

  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
  chunk->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
  chunk->InsertDecimal(0, 2, reinterpret_cast<char*>(&v2), true);
  chunk->InsertData(0, 3, const_cast<char*>(v3.c_str()), v3.length());
  chunk->InsertData(0, 4, reinterpret_cast<char*>(&v4), sizeof(bool));

  RelBatchQueue* relBatch = new RelBatchQueue();
  ASSERT_EQ(relBatch->Init(output_fields), KStatus::SUCCESS);
  relBatch->Add(ctx, chunk->GetData(), capacity);
  DataChunkPtr data;
  ASSERT_EQ(relBatch->Next(ctx, data), EEIteratorErrCode::EE_OK);

  ASSERT_EQ(data->NextLine(), 0);
  ASSERT_EQ(data->Count(), 1);
  ASSERT_EQ(data->Capacity(), capacity);
  ASSERT_EQ(data->isFull(), true);
  ASSERT_EQ(data->ColumnNum(), 5);
  ASSERT_EQ(data->RowSize(), 60);

  auto ptr1 = chunk->GetData(0, 0);
  k_int64 check_ts;
  memcpy(&check_ts, ptr1, col_info[0].storage_len);
  ASSERT_EQ(check_ts, v1);

  ptr1 = data->GetData(0, 0);
  memcpy(&check_ts, ptr1, col_info[0].storage_len);
  ASSERT_EQ(check_ts, v1);

  auto ptr2 = data->GetData(0, 1);
  k_double64 check_double;
  memcpy(&check_double, ptr2, col_info[1].storage_len);
  ASSERT_EQ(check_double, v2);

  auto ptr3 = data->GetData(0, 2) + BOOL_WIDE;
  memcpy(&check_double, ptr3, col_info[2].storage_len);
  ASSERT_EQ(check_double, v2);

  k_uint16 len3 = 0;
  auto ptr4 = data->GetData(0, 3, len3);
  char char_v3[len3];
  memcpy(char_v3, ptr4, len3);
  string check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, v3);

  auto ptr5 = data->GetData(0, 4);
  bool check_bool;
  memcpy(&check_bool, ptr5, col_info[4].storage_len);
  ASSERT_EQ(check_bool, v4);

  DataChunkPtr chunk2;
  chunk2 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  ASSERT_EQ(chunk2->Initialize(), true);
  chunk2->AddCount();
  chunk2->InsertData(0, 0, reinterpret_cast<char*>(&d1), sizeof(k_int64));
  chunk2->InsertData(0, 1, reinterpret_cast<char*>(&d2), sizeof(k_double64));
  chunk2->InsertDecimal(0, 2, reinterpret_cast<char*>(&d2), true);
  chunk2->InsertData(0, 3, const_cast<char*>(d3.c_str()), d3.length());
  chunk2->InsertData(0, 4, reinterpret_cast<char*>(&d4), sizeof(bool));


  relBatch->Add(ctx, chunk2->GetData(), capacity);
  ASSERT_EQ(relBatch->Next(ctx, data), EEIteratorErrCode::EE_OK);

  ptr1 = data->GetData(0, 0);
  memcpy(&check_ts, ptr1, col_info[0].storage_len);
  ASSERT_EQ(check_ts, d1);

  ptr2 = data->GetData(0, 1);
  memcpy(&check_double, ptr2, col_info[1].storage_len);
  ASSERT_EQ(check_double, d2);

  ptr3 = data->GetData(0, 2) + BOOL_WIDE;
  memcpy(&check_double, ptr3, col_info[2].storage_len);
  ASSERT_EQ(check_double, d2);

  len3 = 0;
  ptr4 = data->GetData(0, 3, len3);
  memcpy(char_v3, ptr4, len3);
  check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, d3);

  len3 = 0;
  ptr4 = chunk2->GetData(0, 3, len3);
  memcpy(char_v3, ptr4, len3);
  check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, d3);

  ptr5 = data->GetData(0, 4);
  memcpy(&check_bool, ptr5, col_info[4].storage_len);
  ASSERT_EQ(check_bool, d4);

  SafeDeletePointer(relBatch);

  for (auto field : output_fields) {
    SafeDeletePointer(field);
  }
}

TEST_F(TestRelBatchQueue, TestDoneNullBatchAndNotifyErrorBranches) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);

  {
    RelBatchQueue queue;
    auto output_fields = MakeSingleIntOutputFields();
    ASSERT_EQ(queue.Init(output_fields), KStatus::SUCCESS);
    EXPECT_EQ(queue.Add(ctx, nullptr, 0), KStatus::SUCCESS);
    DataChunkPtr chunk = nullptr;
    EXPECT_EQ(queue.Next(ctx, chunk), EEIteratorErrCode::EE_END_OF_RECORD);
    DeleteFields(output_fields);
  }

  {
    RelBatchQueue queue;
    auto output_fields = MakeSingleIntOutputFields();
    ASSERT_EQ(queue.Init(output_fields), KStatus::SUCCESS);
    EXPECT_EQ(queue.Add(ctx, nullptr, 1), KStatus::FAIL);
    DeleteFields(output_fields);
  }

  {
    RelBatchQueue queue;
    auto output_fields = MakeSingleIntOutputFields();
    ASSERT_EQ(queue.Init(output_fields), KStatus::SUCCESS);
    EEIteratorErrCode next_code = EEIteratorErrCode::EE_OK;
    std::thread waiter([&]() {
      kwdbContext_t worker_ctx;
      InitServerKWDBContext(&worker_ctx);
      DataChunkPtr chunk = nullptr;
      next_code = queue.Next(&worker_ctx, chunk);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.NotifyError();
    waiter.join();
    EXPECT_EQ(next_code, EEIteratorErrCode::EE_ERROR);
    DeleteFields(output_fields);
  }

  {
    RelBatchQueue queue;
    auto chunk = MakeSingleIntChunk(42);
    KStatus add_status = KStatus::SUCCESS;
    std::thread producer([&]() {
      kwdbContext_t worker_ctx;
      InitServerKWDBContext(&worker_ctx);
      add_status = queue.Add(&worker_ctx, chunk->GetData(), 1);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.NotifyError();
    producer.join();
    EXPECT_EQ(add_status, KStatus::FAIL);
  }
}
