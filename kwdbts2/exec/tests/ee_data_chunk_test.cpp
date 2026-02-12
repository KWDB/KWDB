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

#include "ee_tag_row_batch.h"
#include "ee_kwthd_context.h"
#include "ee_data_chunk.h"
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"

using namespace kwdbts;  // NOLINT
class TestDataChunk : public ::testing::Test {  // inherit testing::Test
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
  TestDataChunk() = default;
};

TEST_F(TestDataChunk, TestChunk) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);
  std::queue<DataChunkPtr> queue_data_chunk;
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

  Field** renders = static_cast<Field **>(malloc(col_num * sizeof(Field *)));
  renders[0] = new FieldLonglong(0, col_info[0].storage_type, col_info[0].storage_len);
  renders[1] = new FieldDouble(1, col_info[1].storage_type, col_info[1].storage_len);
  renders[2] = new FieldDouble(2, col_info[2].storage_type, col_info[2].storage_len);
  renders[3] = new FieldChar(3, col_info[3].storage_type, col_info[3].storage_len);
  renders[4] = new FieldBool(4, col_info[4].storage_type, col_info[4].storage_len);

  TSTagReaderSpec spec;
  spec.set_tableid(1);
  spec.set_tableversion(1);

  for (auto & info : col_info) {
    TSCol* col = spec.add_colmetas();
    col->set_storage_type(info.storage_type);
    col->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
    col->set_storage_len(info.storage_len);
  }

  TABLE table(0, 1);
  table.Init(ctx, &spec);
  TagRowBatchPtr tag_data_handle = std::make_shared<TagRowBatch>();
  tag_data_handle->Init(&table);

  current_thd = KNEW KWThdContext();
  current_thd->SetRowBatch(tag_data_handle.get());

  for (int i = 0; i < col_num; i++) {
    renders[i]->table_ = &table;
    renders[i]->is_chunk_ = true;
  }

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

  auto ptr1 = chunk->GetData(0, 0);
  k_int64 check_ts;
  memcpy(&check_ts, ptr1, col_info[0].storage_len);
  ASSERT_EQ(check_ts, v1);

  auto ptr2 = chunk->GetData(0, 1);
  k_double64 check_double;
  memcpy(&check_double, ptr2, col_info[1].storage_len);
  ASSERT_EQ(check_double, v2);

  auto ptr3 = chunk->GetData(0, 1);
  memcpy(&check_double, ptr3, col_info[2].storage_len);
  ASSERT_EQ(check_double, v2);

  k_uint16 len3 = 0;
  auto ptr4 = chunk->GetData(0, 3, len3);
  char char_v3[len3];
  memcpy(char_v3, ptr4, len3);
  string check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, v3);

  auto ptr5 = chunk->GetData(0, 4);
  bool check_bool;
  memcpy(&check_bool, ptr5, col_info[4].storage_len);
  ASSERT_EQ(check_bool, v4);

  DataChunkPtr chunk2;
  chunk2 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
  ASSERT_EQ(chunk2->Initialize(), true);
  chunk2->InsertData(ctx, chunk.get(), nullptr);
  // current_thd->SetDataChunk(chunk.get());
  // chunk2->InsertData(ctx, chunk.get(), renders);
  ASSERT_EQ(chunk2->Count(), 1);

  check_ts = 0;
  auto ptr11 = chunk2->GetData(0, 0);
  memcpy(&check_ts, ptr11, col_info[0].storage_len);
  ASSERT_EQ(check_ts, v1);

  // check append
  DataChunkPtr chunk3, chunk4;
  chunk3 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
  ASSERT_EQ(chunk3->Initialize(), true);
  queue_data_chunk.push(std::move(chunk));

  chunk3->Append(queue_data_chunk);
  ASSERT_EQ(chunk3->Count(), 1);
  ASSERT_EQ(chunk3->EstimateCapacity(col_info, col_num), 1099);

  check_ts = 0;
  auto ptr31 = chunk2->GetData(0, 0);
  memcpy(&check_ts, ptr31, col_info[0].storage_len);
  ASSERT_EQ(check_ts, v1);

  // check line
  row = chunk3->NextLine();
  ASSERT_EQ(row, 0);
  row = chunk3->NextLine();
  ASSERT_EQ(row, -1);
  chunk3->ResetLine();
  row = chunk3->NextLine();
  ASSERT_EQ(row, 0);

  chunk3->setDisorder(true);
  ASSERT_EQ(chunk3->isDisorder(), true);

  ASSERT_EQ(chunk3->Capacity(), 1);

  // check null
  ASSERT_EQ(chunk3->IsNull(0, 3), false);
  chunk3->SetNull(0,3);
  ASSERT_EQ(chunk3->IsNull(0, 3), true);
  ASSERT_EQ(chunk3->IsNull(0, 4), false);
  chunk3->SetNotNull(0, 3);
  ASSERT_EQ(chunk3->IsNull(0, 3), false);
  chunk3->SetAllNull();
  ASSERT_EQ(chunk3->IsNull(0, 3), true);
  ASSERT_EQ(chunk3->IsNull(3), true);

  // check encoding
  EE_StringInfo info = ee_makeStringInfo();
  EE_StringInfo info_pg = ee_makeStringInfo();
  for (row = 0; row < chunk3->Count(); ++row) {
     chunk3->PgResultData(ctx, row, info_pg, {});
     for (size_t col = 0; col < chunk3->ColumnNum(); ++col) {
       ASSERT_EQ(chunk2->EncodingValue(ctx, 0, col, info), SUCCESS);
     }
  }

  free(info->data);
  delete info;

  free(info_pg->data);
  delete info_pg;

  // check copy
  chunk4 = std::make_unique<kwdbts::DataChunk>(col_info, col_num, total_sample_rows);
  ASSERT_EQ(chunk4->Initialize(), true);
  chunk4->Append(chunk2.get(), 0, 1);

  // check row batch
  KStatus status = chunk4->AddRowBatchData(ctx, tag_data_handle.get(), renders, true);
  ASSERT_EQ(status, SUCCESS);
  status = chunk4->AddRowBatchData(ctx, tag_data_handle.get(), renders, false);
  ASSERT_EQ(status, SUCCESS);

  ASSERT_EQ(chunk4->Count(), 1);


  for (int i = 0; i < col_num; i++) {
   delete renders[i];
  }

  free(renders);
}
