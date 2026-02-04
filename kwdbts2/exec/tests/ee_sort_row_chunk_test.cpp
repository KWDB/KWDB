// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_sort_row_chunk.h"

#include "cm_assert.h"
#include "ee_kwthd_context.h"
#include "ee_string_info.h"
#include "ee_tag_row_batch.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ts_utils.h"

using namespace kwdbts;  // NOLINT
DataChunkPtr chunk;
SortRowChunkPtr row_chunk_1{nullptr};
SortRowChunkPtr row_chunk_2{nullptr};
ColumnInfo col_info[5];
k_int32 col_num{5};
k_int64 v1 = 15600000000;
k_double64 v2 = 10.55;
string v3 = "host_0";
bool v4 = true;
const k_uint32 total_sample_rows{1};
class TestSortRowChunk : public ::testing::Test {  // inherit testing::Test
 protected:
  static void SetUpTestCase() {
    g_pstBufferPoolInfo = kwdbts::EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    EXPECT_EQ((g_pstBufferPoolInfo != nullptr), true);

    kwdbContext_t context;
    kwdbContext_p ctx = &context;
    InitServerKWDBContext(ctx);

    col_info[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ,
                             KWDBTypeFamily::TimestampTZFamily);
    col_info[1] =
        ColumnInfo(8, roachpb::DataType::DOUBLE, KWDBTypeFamily::DecimalFamily);
    col_info[2] = ColumnInfo(8, roachpb::DataType::DECIMAL,
                             KWDBTypeFamily::DecimalFamily);
    col_info[3] =
        ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
    col_info[4] =
        ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

    chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num,
                                                total_sample_rows);
    ASSERT_EQ(chunk->Initialize(), true);
    k_int32 row = chunk->NextLine();
    ASSERT_EQ(row, -1);

    chunk->AddCount();
    chunk->InsertData(0, 0, reinterpret_cast<char*>(&v1), sizeof(k_int64));
    chunk->InsertData(0, 1, reinterpret_cast<char*>(&v2), sizeof(k_double64));
    chunk->InsertDecimal(0, 2, reinterpret_cast<char*>(&v2), true);
    chunk->InsertData(0, 3, const_cast<char*>(v3.c_str()), v3.length());
    chunk->InsertData(0, 4, reinterpret_cast<char*>(&v4), sizeof(bool));
  }

  static void TearDownTestCase() {
    kwdbts::KStatus status = kwdbts::EE_MemPoolCleanUp(g_pstBufferPoolInfo);
    EXPECT_EQ(status, kwdbts::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestSortRowChunk() = default;
};

TEST_F(TestSortRowChunk, TestChunk) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // check append
  k_uint64 chunk_size = 64 * 1024;
  row_chunk_1 = std::make_unique<kwdbts::SortRowChunk>(
      col_info, order_info, col_num, chunk_size, UINT32_MAX, 0, false);
  ASSERT_EQ(row_chunk_1->Initialize(), true);
  k_uint32 i = 0;
  ASSERT_EQ(row_chunk_1->Append(chunk, i, i + 1), KStatus::SUCCESS);
  ASSERT_EQ(row_chunk_1->NextLine(), 0);
  ASSERT_EQ(row_chunk_1->Count(), 1);
  ASSERT_EQ(row_chunk_1->GetColumnInfo(), static_cast<ColumnInfo*>(col_info));

  row_chunk_2 = std::make_unique<kwdbts::SortRowChunk>(
      col_info, order_info, col_num, chunk_size, UINT32_MAX, 0, false);

  ASSERT_EQ(row_chunk_2->Initialize(), true);

  // check CopyWithSort
  ASSERT_EQ(row_chunk_2->CopyWithSortFrom(row_chunk_1), KStatus::SUCCESS);

  ASSERT_EQ(row_chunk_2->DecodeData(), KStatus::SUCCESS);
  // check getData
  ASSERT_EQ(row_chunk_1->DecodeData(), KStatus::SUCCESS);
  ASSERT_EQ(*reinterpret_cast<k_int64*>(row_chunk_1->GetData(0, 0)), v1);
  ASSERT_EQ(*reinterpret_cast<k_double64*>(row_chunk_1->GetData(0, 1)), v2);

  k_bool is_double2;
  DatumPtr decimal_v2 = row_chunk_1->GetData(0, 2);
  memcpy(&is_double2, decimal_v2, BOOL_WIDE);
  ASSERT_EQ(is_double2, true);
  k_double64 check_double;
  memcpy(&check_double, decimal_v2 + BOOL_WIDE, sizeof(k_double64));
  ASSERT_EQ(check_double, v2);

  k_uint16 len3;
  DatumPtr char_v3 = row_chunk_1->GetData(0, 3, len3);
  ASSERT_EQ(len3, v3.length());
  string check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, v3);
  ASSERT_EQ(*reinterpret_cast<k_bool*>(row_chunk_1->GetData(0, 4)), v4);

  // check getData
  ASSERT_EQ(row_chunk_2->NextLine(), 0);
  ASSERT_EQ(row_chunk_2->Count(), 1);

  ASSERT_EQ(*reinterpret_cast<k_int64*>(row_chunk_2->GetData(0)), v1);
  ASSERT_EQ(*reinterpret_cast<k_double64*>(row_chunk_2->GetData(1)), v2);

  decimal_v2 = row_chunk_2->GetData(2);
  memcpy(&is_double2, decimal_v2, BOOL_WIDE);
  ASSERT_EQ(is_double2, true);
  memcpy(&check_double, decimal_v2 + BOOL_WIDE, sizeof(k_double64));
  ASSERT_EQ(check_double, v2);

  char_v3 = row_chunk_2->GetData(0, 3, len3);
  ASSERT_EQ(len3, v3.length());
  check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, v3);
  ASSERT_EQ(*reinterpret_cast<k_bool*>(row_chunk_2->GetData(4)), v4);

  // check Append
  row_chunk_2->Reset();

  ASSERT_EQ(row_chunk_2->Append(row_chunk_1, row_chunk_1->GetData()),
            KStatus::SUCCESS);

  ASSERT_EQ(row_chunk_2->NextLine(), 0);
  ASSERT_EQ(row_chunk_2->Count(), 1);

  // check pointer offset change function
  row_chunk_2->ChangePointerToOffset();
  char_v3 = row_chunk_2->GetData(0, 3, len3);
  ASSERT_EQ(len3, v3.length());
  check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, v3);
  row_chunk_2->ChangeOffsetToPointer();
  char_v3 = row_chunk_2->GetData(0, 3, len3);
  ASSERT_EQ(len3, v3.length());
  check_char = string(char_v3, len3);
  ASSERT_EQ(check_char, v3);
  // check line
  ASSERT_EQ(row_chunk_2->NextLine(), -1);

  // check one sortrowchunk append to other
  k_uint64 one_row_chunk_size = 100;
  auto row_chunk_3 = std::make_unique<kwdbts::SortRowChunk>(
      col_info, order_info, col_num, one_row_chunk_size, UINT32_MAX, 0, false);
  ASSERT_EQ(row_chunk_3->Initialize(), true);
  ASSERT_EQ(row_chunk_3->Append(row_chunk_1, row_chunk_1->GetData()),
            KStatus::SUCCESS);
  // check append fail
  ASSERT_EQ(row_chunk_3->Append(row_chunk_1, row_chunk_1->GetData()),
            KStatus::FAIL);

  // check expand fail
  ASSERT_EQ(row_chunk_3->Expand(100, true), KStatus::FAIL);
  ASSERT_EQ(row_chunk_3->Expand(100, false), KStatus::FAIL);
}
