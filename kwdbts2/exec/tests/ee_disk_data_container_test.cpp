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
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestDiskDataContainer() = default;
};

TEST_F(TestDiskDataContainer, TestDiskDataContainer) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);
  std::queue<DataChunkPtr> queue_data_chunk;
  DataChunkPtr chunk;

  k_uint32 total_sample_rows{1};
  std::vector<ColumnInfo> col_info;
  col_info.reserve(1);

  k_int64 v1 = 15600000000;
  k_double64 v2 = 10.55;
  string v3 = "host_0";
  bool v4 = true;

  col_info.emplace_back(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
  col_info.emplace_back(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info.emplace_back(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
  col_info.emplace_back(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  col_info.emplace_back(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

  // check insert
  chunk = std::make_unique<kwdbts::DataChunk>(col_info, total_sample_rows);
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

  // test single chunk append
  {
    DataContainerPtr tempTable =
      std::make_unique<kwdbts::DiskDataContainer>(col_info);
    tempTable->Init();

    tempTable->Append(chunk.get());

    ASSERT_EQ(tempTable->Count(), 1);

    auto ptr1 = tempTable->GetData(0, 0);
    k_int64 check_ts;
    memcpy(&check_ts, ptr1, col_info[0].storage_len);
    ASSERT_EQ(check_ts, v1);

    auto ptr2 = tempTable->GetData(0, 1);
    k_double64 check_double;
    memcpy(&check_double, ptr2, col_info[1].storage_len);
    ASSERT_EQ(check_double, v2);

    auto ptr3 = tempTable->GetData(0, 1);
    memcpy(&check_double, ptr3, col_info[2].storage_len);
    ASSERT_EQ(check_double, v2);

    k_uint16 len3 = 0;
    auto ptr4 = tempTable->GetData(0, 3, len3);
    char char_v3[len3];
    memcpy(char_v3, ptr4, len3);
    string check_char = string(char_v3, len3);
    ASSERT_EQ(check_char, v3);

    auto ptr5 = tempTable->GetData(0, 4);
    bool check_bool;
    memcpy(&check_bool, ptr5, col_info[4].storage_len);
    ASSERT_EQ(check_bool, v4);
  }

  DataChunkPtr chunk2;
  chunk2 = std::make_unique<kwdbts::DataChunk>(col_info, total_sample_rows);
  ASSERT_EQ(chunk2->Initialize(), true);
  chunk2->InsertData(ctx, chunk.get(), nullptr);
  ASSERT_EQ(chunk2->Count(), 1);

  {
    k_int64 check_ts = 0;
    auto ptr11 = chunk2->GetData(0, 0);
    memcpy(&check_ts, ptr11, col_info[0].storage_len);
    ASSERT_EQ(check_ts, v1);
  }

  queue_data_chunk.push(std::move(chunk));
  queue_data_chunk.push(std::move(chunk2));

  DataContainerPtr tempTable2 =
    std::make_unique<kwdbts::DiskDataContainer>(col_info);
  tempTable2->Init();

  tempTable2->Append(queue_data_chunk);

  ASSERT_EQ(tempTable2->Count(), 2);

  // test the first row
  {
    auto ptr1 = tempTable2->GetData(0, 0);
    k_int64 check_ts;
    memcpy(&check_ts, ptr1, col_info[0].storage_len);
    ASSERT_EQ(check_ts, v1);

    auto ptr2 = tempTable2->GetData(0, 1);
    k_double64 check_double;
    memcpy(&check_double, ptr2, col_info[1].storage_len);
    ASSERT_EQ(check_double, v2);

    auto ptr3 = tempTable2->GetData(0, 1);
    memcpy(&check_double, ptr3, col_info[2].storage_len);
    ASSERT_EQ(check_double, v2);

    k_uint16 len3 = 0;
    auto ptr4 = tempTable2->GetData(0, 3, len3);
    char char_v3[len3];
    memcpy(char_v3, ptr4, len3);
    string check_char = string(char_v3, len3);
    ASSERT_EQ(check_char, v3);

    auto ptr5 = tempTable2->GetData(0, 4);
    bool check_bool;
    memcpy(&check_bool, ptr5, col_info[4].storage_len);
    ASSERT_EQ(check_bool, v4);
  }

  // test the second row
  {
    auto ptr1 = tempTable2->GetData(1, 0);
    k_int64 check_ts;
    memcpy(&check_ts, ptr1, col_info[0].storage_len);
    ASSERT_EQ(check_ts, v1);

    auto ptr2 = tempTable2->GetData(1, 1);
    k_double64 check_double;
    memcpy(&check_double, ptr2, col_info[1].storage_len);
    ASSERT_EQ(check_double, v2);

    auto ptr3 = tempTable2->GetData(1, 1);
    memcpy(&check_double, ptr3, col_info[2].storage_len);
    ASSERT_EQ(check_double, v2);

    k_uint16 len3 = 0;
    auto ptr4 = tempTable2->GetData(1, 3, len3);
    char char_v3[len3];
    memcpy(char_v3, ptr4, len3);
    string check_char = string(char_v3, len3);
    ASSERT_EQ(check_char, v3);

    auto ptr5 = tempTable2->GetData(1, 4);
    bool check_bool;
    memcpy(&check_bool, ptr5, col_info[4].storage_len);
    ASSERT_EQ(check_bool, v4);
  }
}