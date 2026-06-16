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

#include <ctime>
#include <limits>
#include <string>
#include <vector>

#include "ee_common.h"
#include "ee_field_agg.h"
#include "ee_field_const.h"
#include "ee_mempool.h"
#include "ee_ryu_dbconvert.h"
#include "gtest/gtest.h"
#include "ts_object_error.h"

namespace kwdbts {
namespace {

KTimestampTz ToTimestampMs(int year, int month, int day) {
  std::tm tm = {};
  tm.tm_year = year - 1900;
  tm.tm_mon = month - 1;
  tm.tm_mday = day;
  return static_cast<KTimestampTz>(timegm(&tm) * 1000);
}

class CommonUtilsTest : public ::testing::Test {
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

TEST_F(CommonUtilsTest, DetectsFirstAndLastAggregateFunctions) {
  for (auto func : {TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTTS,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTTS,
                    TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS}) {
    EXPECT_TRUE(IsFirstLastAggFunc(func));
  }
  EXPECT_FALSE(IsFirstLastAggFunc(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM));
}

TEST_F(CommonUtilsTest, ParsesUnicodeHexAndTypeConversions) {
  EXPECT_EQ(parseUnicode2Utf8("A\\u0042"), "AB");
  EXPECT_EQ(parseUnicode2Utf8("\\U00000043"), "C");
  EXPECT_EQ(parseUnicode2Utf8("\\u07FF"), "\337\277");
  EXPECT_EQ(parseUnicode2Utf8("\\u4F60"), "\344\275\240");
  EXPECT_EQ(parseUnicode2Utf8("\\U0001F600"), "\360\237\230\200");
  EXPECT_EQ(parseUnicode2Utf8("a\\\\b"), "a\\b");
  EXPECT_EQ(parseUnicode2Utf8("a\\'b"), "a'b");
  EXPECT_EQ(parseUnicode2Utf8("\\"), "\\");
  EXPECT_EQ(parseUnicode2Utf8("\\x"), "\\x");

  EXPECT_EQ(parseHex2String("\\x414243"), "ABC");
  EXPECT_EQ(parseHex2String("plain"), "plain");

  ErrorInfo err_info;
  EXPECT_EQ(tryAlterType("12", DATATYPE::INT16, err_info), 0);
  EXPECT_TRUE(err_info.isOK());

  err_info.clear();
  EXPECT_EQ(tryAlterType("abc", DATATYPE::INT32, err_info), KWEPERM);
  EXPECT_EQ(err_info.errcode, KWEPERM);

  err_info.clear();
  EXPECT_EQ(tryAlterType("40000", DATATYPE::INT16, err_info), KWEPERM);
  EXPECT_EQ(err_info.errcode, KWEPERM);

  err_info.clear();
  EXPECT_EQ(tryAlterType("3.14", DATATYPE::DOUBLE, err_info), 0);
  EXPECT_TRUE(err_info.isOK());

  err_info.clear();
  EXPECT_EQ(tryAlterType("21", DATATYPE::INT64, err_info), 0);
  EXPECT_TRUE(err_info.isOK());

  err_info.clear();
  EXPECT_EQ(tryAlterType("2.5", DATATYPE::FLOAT, err_info), 0);
  EXPECT_TRUE(err_info.isOK());

  err_info.clear();
  EXPECT_EQ(tryAlterType("4.2tail", DATATYPE::FLOAT, err_info), KWEPERM);
  EXPECT_EQ(err_info.errcode, KWEPERM);
}

TEST_F(CommonUtilsTest, AddsDurationsWithAndWithoutCalendarSemantics) {
  const KTimestampTz base = ToTimestampMs(2023, 1, 31);
  EXPECT_EQ(TimeAddDuration(base, 0, false, false), base);
  EXPECT_EQ(TimeAddDuration(1000, 500, false, false), 1500);
  EXPECT_EQ(TimeAddDuration(base, 1, true, false), ToTimestampMs(2023, 2, 28));
  EXPECT_EQ(TimeAddDuration(base, 1, true, true), ToTimestampMs(2024, 1, 31));
}

TEST_F(CommonUtilsTest, CreatesAggregateFieldsForSupportedTypes) {
  FieldConstInt bool_field(roachpb::DataType::BOOL, 1, sizeof(k_bool));
  FieldConstInt short_field(roachpb::DataType::SMALLINT, 2, sizeof(k_int16));
  FieldConstInt int_field(roachpb::DataType::INT, 3, sizeof(k_int32));
  FieldConstInt bigint_field(roachpb::DataType::BIGINT, 4, sizeof(k_int64));
  FieldConstDouble float_field(roachpb::DataType::FLOAT, 1.5);
  FieldConstDouble double_field(roachpb::DataType::DOUBLE, 2.5);
  FieldConstString string_field(roachpb::DataType::VARCHAR, "value");
  FieldConstDouble decimal_field(roachpb::DataType::DECIMAL, 3.5);
  FieldConstInt invalid_field(roachpb::DataType::UNKNOWN, 1, sizeof(k_int32));

  FieldAggNum* agg_field = nullptr;
  EXPECT_EQ(CreateAggField(0, &bool_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggBool*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(1, &short_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggShort*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(2, &int_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggInt*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(3, &bigint_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggLonglong*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(4, &float_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggFloat*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(5, &double_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggDouble*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(6, &string_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggString*>(agg_field), nullptr);
  delete agg_field;

  EXPECT_EQ(CreateAggField(7, &decimal_field, nullptr, &agg_field),
            KStatus::SUCCESS);
  EXPECT_NE(dynamic_cast<FieldAggDecimal*>(agg_field), nullptr);
  delete agg_field;

  agg_field = nullptr;
  EXPECT_EQ(CreateAggField(8, &invalid_field, nullptr, &agg_field),
            KStatus::FAIL);
  EXPECT_EQ(agg_field, nullptr);
}

TEST_F(CommonUtilsTest, CreatesAggregateFieldsForAliasTypes) {
  FieldAggNum* agg_field = nullptr;

  for (auto type : {roachpb::DataType::TIMESTAMP,
                    roachpb::DataType::TIMESTAMPTZ,
                    roachpb::DataType::TIMESTAMP_MICRO,
                    roachpb::DataType::TIMESTAMP_NANO,
                    roachpb::DataType::TIMESTAMPTZ_MICRO,
                    roachpb::DataType::TIMESTAMPTZ_NANO,
                    roachpb::DataType::DATE}) {
    FieldConstInt field(type, 5, sizeof(k_int64));
    EXPECT_EQ(CreateAggField(0, &field, nullptr, &agg_field),
              KStatus::SUCCESS);
    EXPECT_NE(dynamic_cast<FieldAggLonglong*>(agg_field), nullptr);
    delete agg_field;
    agg_field = nullptr;
  }

  for (auto type : {roachpb::DataType::CHAR, roachpb::DataType::NCHAR,
                    roachpb::DataType::NVARCHAR,
                    roachpb::DataType::BINARY,
                    roachpb::DataType::VARBINARY}) {
    FieldConstString field(type, "value");
    EXPECT_EQ(CreateAggField(0, &field, nullptr, &agg_field),
              KStatus::SUCCESS);
    EXPECT_NE(dynamic_cast<FieldAggString*>(agg_field), nullptr);
    delete agg_field;
    agg_field = nullptr;
  }
}

TEST_F(CommonUtilsTest, WritesFieldValuesIntoDataChunk) {
  ColumnInfo columns[] = {
      ColumnInfo(sizeof(k_bool), roachpb::DataType::BOOL,
                 KWDBTypeFamily::BoolFamily),
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily),
      ColumnInfo(sizeof(k_double64), roachpb::DataType::DOUBLE,
                 KWDBTypeFamily::FloatFamily),
      ColumnInfo(8, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily),
      ColumnInfo(sizeof(k_double64), roachpb::DataType::DECIMAL,
                 KWDBTypeFamily::DecimalFamily),
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily)};
  columns[0].allow_null = false;
  columns[1].allow_null = false;
  columns[2].allow_null = false;
  columns[3].allow_null = false;
  columns[4].allow_null = false;
  columns[5].allow_null = true;

  DataChunkPtr chunk = std::make_unique<DataChunk>(columns, 6, 1);
  ASSERT_TRUE(chunk->Initialize());
  chunk->AddCount();

  FieldConstInt bool_field(roachpb::DataType::BOOL, 1, sizeof(k_bool));
  FieldConstInt int_field(roachpb::DataType::INT, 42, sizeof(k_int32));
  FieldConstDouble double_field(roachpb::DataType::DOUBLE, 6.25);
  FieldConstString string_field(roachpb::DataType::VARCHAR, "hello");
  FieldConstDouble decimal_field(roachpb::DataType::DECIMAL, 12.5);
  decimal_field.set_sql_type(roachpb::DataType::DOUBLE);
  FieldConstNull null_field(roachpb::DataType::INT, sizeof(k_int32));
  null_field.set_allow_null(true);

  Field* fields[] = {&bool_field, &int_field, &double_field, &string_field,
                     &decimal_field, &null_field};
  FieldsToChunk(fields, 6, 0, chunk);

  EXPECT_FALSE(chunk->IsNull(0, 0));
  EXPECT_FALSE(chunk->IsNull(0, 1));
  EXPECT_FALSE(chunk->IsNull(0, 2));
  EXPECT_FALSE(chunk->IsNull(0, 3));
  EXPECT_FALSE(chunk->IsNull(0, 4));
  EXPECT_TRUE(chunk->IsNull(0, 5));

  EXPECT_EQ(*reinterpret_cast<k_bool*>(chunk->GetDataPtr(0, 0)), 1);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(chunk->GetDataPtr(0, 1)), 42);
  EXPECT_DOUBLE_EQ(*reinterpret_cast<k_double64*>(chunk->GetDataPtr(0, 2)),
                   6.25);

  k_uint16 str_len = 0;
  DatumPtr str_data = chunk->GetData(0, 3, str_len);
  EXPECT_EQ(str_len, 5);
  EXPECT_EQ(std::string(str_data, str_len), "hello");

  DatumPtr decimal_raw = chunk->GetData(0, 4);
  EXPECT_TRUE(*reinterpret_cast<k_bool*>(decimal_raw));
  EXPECT_DOUBLE_EQ(
      *reinterpret_cast<k_double64*>(decimal_raw + BOOL_WIDE), 12.5);
}

TEST_F(CommonUtilsTest, WritesTimestampAndNumericAliasesIntoDataChunk) {
  ColumnInfo columns[] = {
      ColumnInfo(sizeof(k_int64), roachpb::DataType::TIMESTAMP,
                 KWDBTypeFamily::TimestampTZFamily),
      ColumnInfo(sizeof(k_int64), roachpb::DataType::TIMESTAMPTZ,
                 KWDBTypeFamily::TimestampTZFamily),
      ColumnInfo(sizeof(k_int64), roachpb::DataType::TIMESTAMP_MICRO,
                 KWDBTypeFamily::TimestampTZFamily),
      ColumnInfo(sizeof(k_int64), roachpb::DataType::TIMESTAMP_NANO,
                 KWDBTypeFamily::TimestampTZFamily),
      ColumnInfo(sizeof(k_int64), roachpb::DataType::TIMESTAMPTZ_MICRO,
                 KWDBTypeFamily::TimestampTZFamily),
      ColumnInfo(sizeof(k_int64), roachpb::DataType::TIMESTAMPTZ_NANO,
                 KWDBTypeFamily::TimestampTZFamily),
      ColumnInfo(sizeof(k_int64), roachpb::DataType::DATE,
                 KWDBTypeFamily::DateFamily),
      ColumnInfo(sizeof(k_int16), roachpb::DataType::SMALLINT,
                 KWDBTypeFamily::IntFamily),
      ColumnInfo(sizeof(k_float32), roachpb::DataType::FLOAT,
                 KWDBTypeFamily::FloatFamily)};
  for (auto& column : columns) {
    column.allow_null = false;
  }

  DataChunkPtr chunk = std::make_unique<DataChunk>(columns, 9, 1);
  ASSERT_TRUE(chunk->Initialize());
  chunk->AddCount();

  FieldConstInt ts_field(roachpb::DataType::TIMESTAMP, 100, sizeof(k_int64));
  FieldConstInt tstz_field(roachpb::DataType::TIMESTAMPTZ, 101,
                           sizeof(k_int64));
  FieldConstInt ts_micro_field(roachpb::DataType::TIMESTAMP_MICRO, 102,
                               sizeof(k_int64));
  FieldConstInt ts_nano_field(roachpb::DataType::TIMESTAMP_NANO, 103,
                              sizeof(k_int64));
  FieldConstInt tstz_micro_field(roachpb::DataType::TIMESTAMPTZ_MICRO, 104,
                                 sizeof(k_int64));
  FieldConstInt tstz_nano_field(roachpb::DataType::TIMESTAMPTZ_NANO, 105,
                                sizeof(k_int64));
  FieldConstInt date_field(roachpb::DataType::DATE, 106, sizeof(k_int64));
  FieldConstInt short_field(roachpb::DataType::SMALLINT, 7, sizeof(k_int16));
  FieldConstDouble float_field(roachpb::DataType::FLOAT, 3.5);

  Field* fields[] = {&ts_field,
                     &tstz_field,
                     &ts_micro_field,
                     &ts_nano_field,
                     &tstz_micro_field,
                     &tstz_nano_field,
                     &date_field,
                     &short_field,
                     &float_field};
  FieldsToChunk(fields, 9, 0, chunk);

  for (int index = 0; index < 7; ++index) {
    EXPECT_EQ(*reinterpret_cast<k_int64*>(chunk->GetDataPtr(0, index)),
              100 + index);
  }
  EXPECT_EQ(*reinterpret_cast<k_int16*>(chunk->GetDataPtr(0, 7)), 7);
  EXPECT_FLOAT_EQ(*reinterpret_cast<k_float32*>(chunk->GetDataPtr(0, 8)), 3.5F);
}

TEST_F(CommonUtilsTest, WritesStringAliasesIntoDataChunk) {
  ColumnInfo columns[] = {
      ColumnInfo(8, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily),
      ColumnInfo(8, roachpb::DataType::NCHAR, KWDBTypeFamily::StringFamily),
      ColumnInfo(8, roachpb::DataType::NVARCHAR, KWDBTypeFamily::StringFamily),
      ColumnInfo(8, roachpb::DataType::BINARY, KWDBTypeFamily::StringFamily),
      ColumnInfo(8, roachpb::DataType::VARBINARY,
                 KWDBTypeFamily::StringFamily)};
  for (auto& column : columns) {
    column.allow_null = false;
  }

  DataChunkPtr chunk = std::make_unique<DataChunk>(columns, 5, 1);
  ASSERT_TRUE(chunk->Initialize());
  chunk->AddCount();

  FieldConstString char_field(roachpb::DataType::CHAR, "ch");
  FieldConstString nchar_field(roachpb::DataType::NCHAR, "nc");
  FieldConstString nvarchar_field(roachpb::DataType::NVARCHAR, "nv");
  FieldConstString binary_field(roachpb::DataType::BINARY, "bi");
  FieldConstString varbinary_field(roachpb::DataType::VARBINARY, "vb");

  Field* fields[] = {&char_field, &nchar_field, &nvarchar_field, &binary_field,
                     &varbinary_field};
  FieldsToChunk(fields, 5, 0, chunk);

  for (int index = 0; index < 5; ++index) {
    EXPECT_FALSE(chunk->IsNull(0, index));
  }
}

TEST_F(CommonUtilsTest, RyuConversionFunctionsHandleFixedScientificAndSpecialValues) {
  char buffer[128] = {0};

  d2fixed_buffered(123.25, 2, buffer);
  EXPECT_STREQ(buffer, "123.25");

  memset(buffer, 0, sizeof(buffer));
  d2fixed_buffered(0.00123, 5, buffer);
  EXPECT_STREQ(buffer, "0.00123");

  memset(buffer, 0, sizeof(buffer));
  d2exp_buffered(123.25, 2, buffer);
  EXPECT_STREQ(buffer, "1.23e+02");

  memset(buffer, 0, sizeof(buffer));
  d2exp_buffered(0.00123, 2, buffer);
  EXPECT_STREQ(buffer, "1.23e-03");

  memset(buffer, 0, sizeof(buffer));
  int len = d2fixed_buffered_n(-42.5, 1, buffer);
  EXPECT_EQ(std::string(buffer, len), "-42.5");

  memset(buffer, 0, sizeof(buffer));
  len = ryu_snprintf_f(123.25, 2, buffer, sizeof(buffer));
  EXPECT_EQ(len, 6);
  EXPECT_STREQ(buffer, "123.25");

  memset(buffer, 0, sizeof(buffer));
  len = ryu_snprintf_g(3.14159, 17, buffer, sizeof(buffer), false, -1);
  EXPECT_EQ(len, 7);
  EXPECT_STREQ(buffer, "3.14159");

  memset(buffer, 0, sizeof(buffer));
  len = ryu_snprintf_g(-0.0, 17, buffer, sizeof(buffer), false, -1);
  EXPECT_EQ(len, 2);
  EXPECT_STREQ(buffer, "-0");

  memset(buffer, 0, sizeof(buffer));
  len = ryu_snprintf_g(std::numeric_limits<double>::quiet_NaN(), 17,
                       buffer, sizeof(buffer), false, -1);
  EXPECT_EQ(len, 3);
  EXPECT_STREQ(buffer, "nan");

  memset(buffer, 0, sizeof(buffer));
  len = d2exp_buffered_n(-42.5, 1, buffer);
  EXPECT_EQ(std::string(buffer, len), "-4.2e+01");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(0.0, buffer);
  EXPECT_STREQ(buffer, "0E0");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(123000.0, buffer);
  EXPECT_STREQ(buffer, "1.23e+05");
  d2s_exp_to_fixed(buffer);
  EXPECT_STREQ(buffer, "123000");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(0.00123, buffer);
  EXPECT_STREQ(buffer, "1.23e-03");
  d2s_exp_to_fixed(buffer);
  EXPECT_STREQ(buffer, "0.00123");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(std::numeric_limits<double>::infinity(), buffer);
  EXPECT_STREQ(buffer, "Infinity");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(-std::numeric_limits<double>::infinity(), buffer);
  EXPECT_STREQ(buffer, "-Infinity");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(std::numeric_limits<double>::quiet_NaN(), buffer);
  EXPECT_STREQ(buffer, "NaN");
}

TEST_F(CommonUtilsTest, RyuConversionFunctionsHandleRoundingNegativeZeroAndDenormals) {
  char buffer[256] = {0};

  d2fixed_buffered(-0.0, 3, buffer);
  EXPECT_STREQ(buffer, "-0.000");

  memset(buffer, 0, sizeof(buffer));
  d2fixed_buffered(9.999, 2, buffer);
  EXPECT_STREQ(buffer, "10.00");

  memset(buffer, 0, sizeof(buffer));
  d2fixed_buffered(999.995, 2, buffer);
  EXPECT_STREQ(buffer, "1000.00");

  memset(buffer, 0, sizeof(buffer));
  d2fixed_buffered(0.0000001234, 10, buffer);
  EXPECT_STREQ(buffer, "0.0000001234");

  memset(buffer, 0, sizeof(buffer));
  d2fixed_buffered(std::numeric_limits<double>::denorm_min(), 20, buffer);
  EXPECT_STREQ(buffer, "0.00000000000000000000");

  memset(buffer, 0, sizeof(buffer));
  d2exp_buffered(-0.0, 2, buffer);
  EXPECT_STREQ(buffer, "-0.00e+00");

  memset(buffer, 0, sizeof(buffer));
  d2exp_buffered(9.999, 2, buffer);
  EXPECT_STREQ(buffer, "1.00e+01");

  memset(buffer, 0, sizeof(buffer));
  d2exp_buffered(999.995, 2, buffer);
  EXPECT_STREQ(buffer, "1.00e+03");

  memset(buffer, 0, sizeof(buffer));
  d2exp_buffered(std::numeric_limits<double>::denorm_min(), 2, buffer);
  EXPECT_STREQ(buffer, "4.94e-324");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(1.0, buffer);
  EXPECT_STREQ(buffer, "1e+00");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(-1.0, buffer);
  EXPECT_STREQ(buffer, "-1e+00");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(1.5, buffer);
  EXPECT_STREQ(buffer, "1.5e+00");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(1e23, buffer);
  EXPECT_STREQ(buffer, "1e+23");
  d2s_exp_to_fixed(buffer);
  EXPECT_STREQ(buffer, "100000000000000000000000");

  memset(buffer, 0, sizeof(buffer));
  d2s_buffered(std::numeric_limits<double>::denorm_min(), buffer);
  EXPECT_STREQ(buffer, "5e-324");
}

}  // namespace
}  // namespace kwdbts
