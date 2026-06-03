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

#include <memory>
#include <string>
#include <vector>

#include "ee_aggregate_func.h"
#include "ee_mempool.h"
#include "gtest/gtest.h"

namespace kwdbts {
namespace {

DataChunkPtr MakeDistinctChunk() {
  static ColumnInfo columns[] = {
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily),
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily)};
  columns[0].allow_null = false;
  columns[1].allow_null = false;

  DataChunkPtr chunk = std::make_unique<DataChunk>(columns, 2, 3);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount(3);

  k_int32 group = 1;
  k_int32 value_a = 10;
  k_int32 value_b = 11;
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&group), sizeof(group));
  chunk->InsertData(0, 1, reinterpret_cast<char*>(&value_a), sizeof(value_a));
  chunk->InsertData(1, 0, reinterpret_cast<char*>(&group), sizeof(group));
  chunk->InsertData(1, 1, reinterpret_cast<char*>(&value_a), sizeof(value_a));
  chunk->InsertData(2, 0, reinterpret_cast<char*>(&group), sizeof(group));
  chunk->InsertData(2, 1, reinterpret_cast<char*>(&value_b), sizeof(value_b));
  return chunk;
}

class AggregateFuncFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
  }

  void TearDown() override {
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }
};

TEST_F(AggregateFuncFactoryTest, DistinctTrackingUsesInternalSeenSet) {
  auto chunk = MakeDistinctChunk();
  std::vector<roachpb::DataType> col_types{roachpb::DataType::INT,
                                           roachpb::DataType::INT};
  std::vector<k_uint32> col_lens{sizeof(k_int32), sizeof(k_int32)};
  std::vector<k_uint32> group_cols{0};
  std::vector<bool> allow_null{false, false};

  std::unique_ptr<AggregateFunc> agg(
      AggregateFuncFactory::CreateCount(roachpb::DataType::INT, 0, 1, 8));
  ASSERT_NE(agg, nullptr);

  k_bool is_distinct = false;
  EXPECT_EQ(agg->isDistinct(chunk.get(), 0, col_types, col_lens, group_cols,
                            &is_distinct, allow_null),
            0);
  EXPECT_TRUE(is_distinct);

  EXPECT_EQ(agg->isDistinct(chunk.get(), 1, col_types, col_lens, group_cols,
                            &is_distinct, allow_null),
            0);
  EXPECT_FALSE(is_distinct);

  EXPECT_EQ(agg->isDistinct(chunk.get(), 2, col_types, col_lens, group_cols,
                            &is_distinct, allow_null),
            0);
  EXPECT_TRUE(is_distinct);
}

TEST_F(AggregateFuncFactoryTest, CreatesMaxMinAndAnyNotNullFactories) {
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMax(roachpb::DataType::BOOL, 0, 1, 1));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxAggregate<k_bool>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 1U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMax(roachpb::DataType::VARCHAR, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE((dynamic_cast<MaxAggregate<String, true>*>(agg.get()) !=
                 nullptr));
    EXPECT_EQ(agg->GetLen(), 8U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMax(roachpb::DataType::DECIMAL, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxAggregate<k_decimal>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  EXPECT_EQ(AggregateFuncFactory::CreateMax(roachpb::DataType::UNKNOWN, 0, 1,
                                            8),
            nullptr);

  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMin(roachpb::DataType::INT, 0, 1, 4));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinAggregate<k_int32>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 4U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMin(roachpb::DataType::CHAR, 0, 1, 5));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinAggregate<String>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 5U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMin(roachpb::DataType::DECIMAL, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinAggregate<k_decimal>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  EXPECT_EQ(AggregateFuncFactory::CreateMin(roachpb::DataType::UNKNOWN, 0, 1,
                                            8),
            nullptr);

  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateAnyNotNull(
        roachpb::DataType::BIGINT, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AnyNotNullAggregate<k_int64>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 8U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateAnyNotNull(
        roachpb::DataType::VARCHAR, 0, 1, 6));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<AnyNotNullAggregate<std::string, true>*>(agg.get()) !=
         nullptr));
    EXPECT_EQ(agg->GetLen(), 6U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateAnyNotNull(
        roachpb::DataType::DECIMAL, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AnyNotNullAggregate<k_decimal>*>(agg.get()),
              nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  EXPECT_EQ(AggregateFuncFactory::CreateAnyNotNull(roachpb::DataType::UNKNOWN,
                                                   0, 1, 8),
            nullptr);
}

TEST_F(AggregateFuncFactoryTest, CoversRemainingFactoryTypeAliases) {
  const std::vector<roachpb::DataType> int64_types = {
      roachpb::DataType::TIMESTAMP,      roachpb::DataType::TIMESTAMPTZ,
      roachpb::DataType::TIMESTAMP_MICRO, roachpb::DataType::TIMESTAMP_NANO,
      roachpb::DataType::TIMESTAMPTZ_MICRO,
      roachpb::DataType::TIMESTAMPTZ_NANO, roachpb::DataType::DATE,
      roachpb::DataType::BIGINT};
  const std::vector<roachpb::DataType> fixed_string_types = {
      roachpb::DataType::CHAR, roachpb::DataType::BINARY,
      roachpb::DataType::NCHAR};
  const std::vector<roachpb::DataType> var_string_types = {
      roachpb::DataType::VARCHAR, roachpb::DataType::NVARCHAR,
      roachpb::DataType::VARBINARY};

  for (auto type : int64_types) {
    std::unique_ptr<AggregateFunc> max_agg(
        AggregateFuncFactory::CreateMax(type, 0, 1, 8));
    ASSERT_NE(max_agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxAggregate<k_int64>*>(max_agg.get()), nullptr);

    std::unique_ptr<AggregateFunc> min_agg(
        AggregateFuncFactory::CreateMin(type, 0, 1, 8));
    ASSERT_NE(min_agg, nullptr);
    EXPECT_NE(dynamic_cast<MinAggregate<k_int64>*>(min_agg.get()), nullptr);

    std::unique_ptr<AggregateFunc> any_agg(
        AggregateFuncFactory::CreateAnyNotNull(type, 0, 1, 8));
    ASSERT_NE(any_agg, nullptr);
    EXPECT_NE(dynamic_cast<AnyNotNullAggregate<k_int64>*>(any_agg.get()),
              nullptr);
  }

  for (auto type : fixed_string_types) {
    std::unique_ptr<AggregateFunc> max_agg(
        AggregateFuncFactory::CreateMax(type, 0, 1, 6));
    ASSERT_NE(max_agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxAggregate<String>*>(max_agg.get()), nullptr);

    std::unique_ptr<AggregateFunc> min_agg(
        AggregateFuncFactory::CreateMin(type, 0, 1, 6));
    ASSERT_NE(min_agg, nullptr);
    EXPECT_NE(dynamic_cast<MinAggregate<String>*>(min_agg.get()), nullptr);

    std::unique_ptr<AggregateFunc> any_agg(
        AggregateFuncFactory::CreateAnyNotNull(type, 0, 1, 6));
    ASSERT_NE(any_agg, nullptr);
    EXPECT_NE(dynamic_cast<AnyNotNullAggregate<std::string>*>(any_agg.get()),
              nullptr);
  }

  for (auto type : var_string_types) {
    std::unique_ptr<AggregateFunc> max_agg(
        AggregateFuncFactory::CreateMax(type, 0, 1, 6));
    ASSERT_NE(max_agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<MaxAggregate<String, true>*>(max_agg.get()) != nullptr));

    std::unique_ptr<AggregateFunc> min_agg(
        AggregateFuncFactory::CreateMin(type, 0, 1, 6));
    ASSERT_NE(min_agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<MinAggregate<String, true>*>(min_agg.get()) != nullptr));

    std::unique_ptr<AggregateFunc> any_agg(
        AggregateFuncFactory::CreateAnyNotNull(type, 0, 1, 6));
    ASSERT_NE(any_agg, nullptr);
    EXPECT_TRUE((dynamic_cast<AnyNotNullAggregate<std::string, true>*>(
                     any_agg.get()) != nullptr));
  }
}

TEST_F(AggregateFuncFactoryTest, CreatesSumCountAverageAndTwaFactories) {
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSum(roachpb::DataType::SMALLINT, 0, 1, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<SumAggregate<k_int16, k_decimal>*>(agg.get()) !=
         nullptr));
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSum(roachpb::DataType::DOUBLE, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<SumAggregate<k_double64, k_double64>*>(agg.get()) !=
         nullptr));
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSum(roachpb::DataType::DECIMAL, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<SumAggregate<k_decimal, k_decimal>*>(agg.get()) !=
         nullptr));
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  EXPECT_EQ(AggregateFuncFactory::CreateSum(roachpb::DataType::BOOL, 0, 1, 1),
            nullptr);

  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSumInt(roachpb::DataType::INT, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<SumIntAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateCount(roachpb::DataType::INT, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<CountAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateCountRow(0, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<CountRowAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSTDDEVRow(roachpb::DataType::DOUBLE, 0, 1,
                                              8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<STDDEVRowAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateAVGRow(roachpb::DataType::FLOAT, 0, 1, 4));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AVGRowAggregate<k_float32>*>(agg.get()), nullptr);
  }
  EXPECT_EQ(
      AggregateFuncFactory::CreateAVGRow(roachpb::DataType::VARCHAR, 0, 1, 8),
      nullptr);

  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateTwa(
        roachpb::DataType::DOUBLE, 0, 1, 8, 2, 1.5));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<TwaAggregate<k_double64>*>(agg.get()), nullptr);
  }
  EXPECT_EQ(
      AggregateFuncFactory::CreateTwa(roachpb::DataType::VARCHAR, 0, 1, 8, 2,
                                      1.5),
      nullptr);
}

TEST_F(AggregateFuncFactoryTest, CoversRemainingNumericAggregateFactories) {
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSum(roachpb::DataType::INT, 0, 1, 4));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<SumAggregate<k_int32, k_decimal>*>(agg.get()) !=
         nullptr));
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSum(roachpb::DataType::BIGINT, 0, 1, 8));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<SumAggregate<k_int64, k_decimal>*>(agg.get()) !=
         nullptr));
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateSum(roachpb::DataType::FLOAT, 0, 1, 4));
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(
        (dynamic_cast<SumAggregate<k_float32, k_double64>*>(agg.get()) !=
         nullptr));
  }

  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateAVGRow(roachpb::DataType::SMALLINT, 0, 1,
                                           2));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AVGRowAggregate<k_int16>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateAVGRow(roachpb::DataType::INT, 0, 1, 4));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AVGRowAggregate<k_int32>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateAVGRow(roachpb::DataType::BIGINT, 0, 1,
                                           8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AVGRowAggregate<k_int64>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateAVGRow(roachpb::DataType::DOUBLE, 0, 1,
                                           8));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<AVGRowAggregate<k_double64>*>(agg.get()), nullptr);
  }

  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateTwa(roachpb::DataType::SMALLINT, 0, 1, 2,
                                        2, 1.5));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<TwaAggregate<k_int16>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateTwa(roachpb::DataType::INT, 0, 1, 4, 2,
                                        1.5));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<TwaAggregate<k_int32>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateTwa(roachpb::DataType::BIGINT, 0, 1, 8, 2,
                                        1.5));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<TwaAggregate<k_int64>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateTwa(roachpb::DataType::FLOAT, 0, 1, 4, 2,
                                        1.5));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<TwaAggregate<k_float32>*>(agg.get()), nullptr);
  }
}

TEST_F(AggregateFuncFactoryTest, CreatesFirstLastElapsedAndExtendFactories) {
  std::string interval = "'00:00:01':::INTERVAL";

  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateLast(
        roachpb::DataType::VARCHAR, 0, 1, 12, 2, 1000));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<LastAggregate<true>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 12U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateLast(
        roachpb::DataType::DECIMAL, 0, 1, 8, 2, 1000));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateLastTS(0, 1, 8, 2, 1000));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<LastTSAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateLastRow(
        roachpb::DataType::DOUBLE, 0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<LastRowAggregate<>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 8U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateLastRowTS(0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<LastRowTSAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateFirst(
        roachpb::DataType::VARCHAR, 0, 1, 10, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<FirstAggregate<true>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 10U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateFirstRow(
        roachpb::DataType::DECIMAL, 0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateFirstTS(0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<FirstTSAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateFirstRowTS(0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<FirstRowTSAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateElapsed(
        roachpb::DataType::TIMESTAMP, 0, 1, 8, interval));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<ElapsedAggregate*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::CHAR, 0, 1,
                                              16, 2, true));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<String>*>(agg.get()), nullptr);
    EXPECT_EQ(agg->GetLen(), 16U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateMinExtend(
        roachpb::DataType::DECIMAL, 0, 1, 16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_decimal>*>(agg.get()),
              nullptr);
    EXPECT_EQ(agg->GetLen(), 16U);
  }
  EXPECT_EQ(AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::UNKNOWN,
                                                  0, 1, 16, 2, false),
            nullptr);
  EXPECT_EQ(AggregateFuncFactory::CreateMinExtend(roachpb::DataType::UNKNOWN,
                                                  0, 1, 16, 2, false),
            nullptr);
}

TEST_F(AggregateFuncFactoryTest, CoversRemainingFirstLastAndExtendAliases) {
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateLast(
        roachpb::DataType::INT, 0, 1, 4, 2, 1000));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 4U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateLastRow(
        roachpb::DataType::VARCHAR, 0, 1, 10, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 10U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateLastRow(
        roachpb::DataType::DECIMAL, 0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateFirst(
        roachpb::DataType::DECIMAL, 0, 1, 8, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 8U + BOOL_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateFirst(
        roachpb::DataType::INT, 0, 1, 4, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 4U);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateFirstRow(
        roachpb::DataType::VARCHAR, 0, 1, 9, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 9U + STRING_WIDE);
  }
  {
    std::unique_ptr<AggregateFunc> agg(AggregateFuncFactory::CreateFirstRow(
        roachpb::DataType::INT, 0, 1, 4, 2));
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->GetLen(), 4U);
  }

  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::BOOL, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<k_bool>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::SMALLINT, 0,
                                              1, 16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<k_int16>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::INT, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<k_int32>*>(agg.get()), nullptr);
  }
  for (auto type : {roachpb::DataType::TIMESTAMP,
                    roachpb::DataType::TIMESTAMPTZ,
                    roachpb::DataType::TIMESTAMP_MICRO,
                    roachpb::DataType::TIMESTAMP_NANO,
                    roachpb::DataType::TIMESTAMPTZ_MICRO,
                    roachpb::DataType::TIMESTAMPTZ_NANO,
                    roachpb::DataType::DATE,
                    roachpb::DataType::BIGINT}) {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(type, 0, 1, 16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<k_int64>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::FLOAT, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<k_float32>*>(agg.get()),
              nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(roachpb::DataType::DOUBLE, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<k_double64>*>(agg.get()),
              nullptr);
  }
  for (auto type : {roachpb::DataType::VARCHAR, roachpb::DataType::NCHAR,
                    roachpb::DataType::NVARCHAR,
                    roachpb::DataType::BINARY,
                    roachpb::DataType::VARBINARY}) {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMaxExtend(type, 0, 1, 16, 2, true));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MaxExtendAggregate<String>*>(agg.get()), nullptr);
  }

  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(roachpb::DataType::BOOL, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_bool>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(roachpb::DataType::SMALLINT, 0,
                                              1, 16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_int16>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(roachpb::DataType::INT, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_int32>*>(agg.get()), nullptr);
  }
  for (auto type : {roachpb::DataType::TIMESTAMP,
                    roachpb::DataType::TIMESTAMPTZ,
                    roachpb::DataType::TIMESTAMP_MICRO,
                    roachpb::DataType::TIMESTAMP_NANO,
                    roachpb::DataType::TIMESTAMPTZ_MICRO,
                    roachpb::DataType::TIMESTAMPTZ_NANO,
                    roachpb::DataType::DATE,
                    roachpb::DataType::BIGINT}) {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(type, 0, 1, 16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_int64>*>(agg.get()), nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(roachpb::DataType::FLOAT, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_float32>*>(agg.get()),
              nullptr);
  }
  {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(roachpb::DataType::DOUBLE, 0, 1,
                                              16, 2, false));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<k_double64>*>(agg.get()),
              nullptr);
  }
  for (auto type : {roachpb::DataType::CHAR, roachpb::DataType::VARCHAR,
                    roachpb::DataType::NCHAR, roachpb::DataType::NVARCHAR,
                    roachpb::DataType::BINARY,
                    roachpb::DataType::VARBINARY}) {
    std::unique_ptr<AggregateFunc> agg(
        AggregateFuncFactory::CreateMinExtend(type, 0, 1, 16, 2, true));
    ASSERT_NE(agg, nullptr);
    EXPECT_NE(dynamic_cast<MinExtendAggregate<String>*>(agg.get()), nullptr);
  }
}

}  // namespace
}  // namespace kwdbts
