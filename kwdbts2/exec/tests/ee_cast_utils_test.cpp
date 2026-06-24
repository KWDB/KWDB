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

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

#include "ee_cast_utils.h"
#include "ee_ryu_dbconvert.h"
#include "gtest/gtest.h"

namespace kwdbts {

KStatus fetchNum2(char* ptr, int64_t& num);
void deleteZero(double input, char* str, int32_t length);
int64_t strnatoi(char* num, int32_t len);
char* forward2TimeStringEnd(char* str);
int32_t parseFraction(char* str, char** end);
KStatus parseTimezone(char* str, int64_t* tzOffset);
KStatus parseTimeWithTz(KString timestr, k_int64 scale, k_int64* time,
                        char delim);
bool checkTzPresent(KString& str);
KStatus parseLocaltimeDst(KString timestr, k_int64 scale, k_int64* utime,
                          char delim);
KStatus kwdbParseTime(KString& timestr, k_int64 scale, k_int64* utime);

namespace {

TEST(CastUtilsTest, ParsesNumericFragmentsAndOverflowChecks) {
  char digit[] = "7";
  char letter[] = "x";
  EXPECT_TRUE(isNumber(digit));
  EXPECT_FALSE(isNumber(letter));

  char numeric[] = "-00123.45";
  char* ptr = numeric;
  int positive = 0;
  char* real_start = nullptr;
  EXPECT_EQ(fetchNum0(ptr, positive, real_start), 3);
  EXPECT_EQ(positive, -1);
  EXPECT_EQ(std::string(real_start, 3), "123");
  EXPECT_EQ(*ptr, '.');

  char zero_value[] = "+000";
  ptr = zero_value;
  positive = 0;
  real_start = nullptr;
  EXPECT_EQ(fetchNum0(ptr, positive, real_start), 0);
  EXPECT_EQ(positive, 1);
  EXPECT_NE(real_start, nullptr);

  int64_t fetched = 0;
  char int_string[] = "12345";
  EXPECT_EQ(fetchNum2(int_string, fetched), KStatus::SUCCESS);
  EXPECT_EQ(fetched, 12345);

  char bad_int_string[] = "12x";
  EXPECT_EQ(fetchNum2(bad_int_string, fetched), KStatus::FAIL);

  char empty_int_string[] = "";
  EXPECT_EQ(fetchNum2(empty_int_string, fetched), KStatus::FAIL);

  char int_overflow[] = "9223372036854775808";
  EXPECT_EQ(fetchNum2(int_overflow, fetched), KStatus::FAIL);

  char int_underflow[] = "-9223372036854775809";
  EXPECT_EQ(fetchNum2(int_underflow, fetched), KStatus::FAIL);

  EXPECT_FALSE(checkOverflow(1, 10, 10, 5));
  EXPECT_TRUE(checkOverflow(1, std::numeric_limits<int64_t>::max(), 10, 0));
  EXPECT_TRUE(checkOverflow(0, 1, 10, 1));
  EXPECT_TRUE(checkOverflow(-1, std::numeric_limits<int64_t>::min(), 10, 1));

  char empty_numeric[] = "";
  ptr = empty_numeric;
  positive = 0;
  real_start = nullptr;
  EXPECT_EQ(fetchNum0(ptr, positive, real_start), 0);
}

TEST(CastUtilsTest, ConvertsBooleansNumbersAndIntegers) {
  int bool_output = -1;
  char true_string[] = "TrUe";
  EXPECT_TRUE(boolStrToInt(true_string, bool_output));
  EXPECT_EQ(bool_output, 1);

  char false_string[] = "FALSE";
  EXPECT_TRUE(boolStrToInt(false_string, bool_output));
  EXPECT_EQ(bool_output, 0);

  char invalid_bool[] = "yes";
  EXPECT_FALSE(boolStrToInt(invalid_bool, bool_output));

  double as_double = 0.0;
  char double_string[] = "12.5";
  EXPECT_EQ(strToDouble(double_string, as_double), KStatus::SUCCESS);
  EXPECT_DOUBLE_EQ(as_double, 12.5);

  char bool_double[] = "true";
  EXPECT_EQ(strToDouble(bool_double, as_double), KStatus::SUCCESS);
  EXPECT_DOUBLE_EQ(as_double, 1.0);

  char invalid_double[] = "1x";
  EXPECT_EQ(strToDouble(invalid_double, as_double), KStatus::FAIL);

  char huge_double[] = "1e309";
  EXPECT_EQ(strToDouble(huge_double, as_double), KStatus::FAIL);

  int64_t as_int64 = 0;
  char int64_string[] = "123";
  EXPECT_EQ(strToInt64(int64_string, as_int64), KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 123);

  char rounded_up[] = "12.6";
  EXPECT_EQ(strToInt64(rounded_up, as_int64), KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 13);

  char rounded_down_negative[] = "-12.6";
  EXPECT_EQ(strToInt64(rounded_down_negative, as_int64), KStatus::SUCCESS);
  EXPECT_EQ(as_int64, -13);

  char scientific[] = "1.2e3";
  EXPECT_EQ(strToInt64(scientific, as_int64), KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 1200);

  char bool_int[] = "false";
  EXPECT_EQ(strToInt64(bool_int, as_int64), KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 0);

  char invalid_int[] = ".e5";
  EXPECT_EQ(strToInt64(invalid_int, as_int64), KStatus::FAIL);

  char overflow_int[] = "92233720368547758070";
  EXPECT_EQ(strToInt64(overflow_int, as_int64), KStatus::FAIL);

  char invalid_suffix[] = "1a";
  EXPECT_EQ(strToInt64(invalid_suffix, as_int64), KStatus::FAIL);

  char invalid_decimal_suffix[] = "1.2x";
  EXPECT_EQ(strToInt64(invalid_decimal_suffix, as_int64), KStatus::FAIL);

  char invalid_exponent[] = "1e+";
  EXPECT_EQ(strToInt64(invalid_exponent, as_int64), KStatus::FAIL);

  char zero_after_negative_exponent[] = "0.4e-1";
  EXPECT_EQ(strToInt64(zero_after_negative_exponent, as_int64),
            KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 0);

  char rounded_with_negative_exponent[] = "15e-1";
  EXPECT_EQ(strToInt64(rounded_with_negative_exponent, as_int64),
            KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 2);

  char padded_with_spaces[] = "   42";
  EXPECT_EQ(strToInt64(padded_with_spaces, as_int64), KStatus::SUCCESS);
  EXPECT_EQ(as_int64, 42);
}

TEST(CastUtilsTest, FormatsNumbersAndParsesTimeHelpers) {

  char sql_float_buffer[32] = {0};
  int sql_float_len = ryu_snprintf_g(1e17, 17, sql_float_buffer,
                                     sizeof(sql_float_buffer), false, -1);
  EXPECT_EQ(sql_float_len, 5);
  EXPECT_STREQ(sql_float_buffer, "1e+17");

  memset(sql_float_buffer, 0, sizeof(sql_float_buffer));
  sql_float_len = ryu_snprintf_g(2e17, 17, sql_float_buffer,
                                 sizeof(sql_float_buffer), false, -1);
  EXPECT_EQ(sql_float_len, 5);
  EXPECT_STREQ(sql_float_buffer, "2e+17");

  char zero_buffer[] = "12.3400";
  deleteZero(12.34, zero_buffer, static_cast<int32_t>(strlen(zero_buffer)));
  EXPECT_STREQ(zero_buffer, "12.34");

  char integer_buffer[] = "1.000";
  deleteZero(1.0, integer_buffer, static_cast<int32_t>(strlen(integer_buffer)));
  EXPECT_STREQ(integer_buffer, "1");

  char decimal_digits[] = "1234";
  EXPECT_EQ(strnatoi(decimal_digits, 10), 1234);

  char hex_digits[] = "0x1f";
  EXPECT_EQ(strnatoi(hex_digits, 4), 31);

  char bad_digits[] = "12a";
  EXPECT_EQ(strnatoi(bad_digits, 3), 0);

  char upper_hex_digits[] = "0X1F";
  EXPECT_EQ(strnatoi(upper_hex_digits, 4), 31);

  char time_string[] = "12:34:56.789+08:00";
  EXPECT_EQ(*forward2TimeStringEnd(time_string), '.');

  char fraction_string[] = "123456789tail";
  char* end = nullptr;
  EXPECT_EQ(parseFraction(fraction_string, &end), 123456789);
  EXPECT_STREQ(end, "tail");

  char invalid_fraction[] = "tail";
  EXPECT_EQ(parseFraction(invalid_fraction, &end), -1);

  int64_t tz_offset = 0;
  char tz_string[] = "+08:30";
  EXPECT_EQ(parseTimezone(tz_string, &tz_offset), KStatus::SUCCESS);
  EXPECT_EQ(tz_offset, -(8 * 3600 + 30 * 60));

  char compact_tz[] = "-0800";
  EXPECT_EQ(parseTimezone(compact_tz, &tz_offset), KStatus::SUCCESS);
  EXPECT_EQ(tz_offset, 8 * 3600);

  char invalid_tz[] = "+13:00";
  EXPECT_EQ(parseTimezone(invalid_tz, &tz_offset), KStatus::FAIL);

  char invalid_tz_chars[] = "+08x0";
  EXPECT_EQ(parseTimezone(invalid_tz_chars, &tz_offset), KStatus::FAIL);

  char invalid_tz_minute[] = "+12:01";
  EXPECT_EQ(parseTimezone(invalid_tz_minute, &tz_offset), KStatus::FAIL);

  k_int64 utc_time = 0;
  k_int64 parsed_time = 0;
  EXPECT_EQ(parseTimeWithTz("1970-01-01T00:00:01Z", 1000, &utc_time, 'T'),
            KStatus::SUCCESS);
  EXPECT_GT(utc_time, 0);

  k_int64 zero_offset_time = 0;
  EXPECT_EQ(parseTimeWithTz("1970-01-01T00:00:01+00:00", 1000,
                            &zero_offset_time, 'T'),
            KStatus::SUCCESS);
  EXPECT_EQ(zero_offset_time, utc_time);

  k_int64 space_delim_time = 0;
  EXPECT_EQ(parseTimeWithTz("1970-01-01 00:00:01Z", 1000, &space_delim_time, 0),
            KStatus::SUCCESS);
  EXPECT_EQ(space_delim_time, utc_time);

  EXPECT_EQ(parseTimeWithTz("1970-01-01T00:00:01.123+08:00", 1000,
                            &parsed_time, 'T'),
            KStatus::SUCCESS);
  EXPECT_LT(parsed_time, utc_time);

  EXPECT_EQ(parseTimeWithTz("1970-01-01T00:00:01.123A", 1000, &parsed_time, 'T'),
            KStatus::FAIL);
  EXPECT_EQ(parseTimeWithTz("1970-01-01T00:00:01.123Zx", 1000, &parsed_time,
                            'T'),
            KStatus::FAIL);
  EXPECT_EQ(parseTimeWithTz("1970-01-01 00:00:01Z", 1000, &parsed_time, 'X'),
            KStatus::FAIL);

  KString with_tz = "1970-01-01T00:00:01Z";
  EXPECT_TRUE(checkTzPresent(with_tz));

  KString without_tz = "1970-01-01 00:00:01";
  EXPECT_FALSE(checkTzPresent(without_tz));

  k_int64 local_time = 0;
  EXPECT_EQ(parseLocaltimeDst(without_tz, 1000, &local_time, 0),
            KStatus::SUCCESS);
  EXPECT_GT(local_time, 0);

  KString local_time_t = "1970-01-01T00:00:01";
  EXPECT_EQ(parseLocaltimeDst(local_time_t, 1000, &parsed_time, 'T'),
            KStatus::SUCCESS);
  EXPECT_EQ(parsed_time, local_time);

  KString invalid_date = "2023-02-29";
  EXPECT_EQ(parseLocaltimeDst(invalid_date, 1000, &parsed_time, 0),
            KStatus::FAIL);

  KString leap_invalid = "2024-02-30";
  EXPECT_EQ(parseLocaltimeDst(leap_invalid, 1000, &parsed_time, 0),
            KStatus::FAIL);

  KString parsed_with_tz = "1970-01-01T00:00:01Z";
  parsed_time = 0;
  EXPECT_EQ(kwdbParseTime(parsed_with_tz, 1000, &parsed_time),
            KStatus::SUCCESS);
  EXPECT_EQ(parsed_time, utc_time);

  KString parsed_without_tz = "1970-01-01 00:00:01";
  EXPECT_EQ(kwdbParseTime(parsed_without_tz, 1000, &parsed_time),
            KStatus::SUCCESS);
  EXPECT_EQ(parsed_time, local_time);

  KString parsed_without_tz_t = "1970-01-01T00:00:01";
  EXPECT_EQ(kwdbParseTime(parsed_without_tz_t, 1000, &parsed_time),
            KStatus::SUCCESS);
  EXPECT_EQ(parsed_time, local_time);

  KString parsed_date = "1970-01-01";
  EXPECT_EQ(kwdbParseTime(parsed_date, 1000, &parsed_time), KStatus::SUCCESS);
  EXPECT_EQ(parsed_time, 0);

  EXPECT_EQ(convertStringToTimestamp("", 1000, &parsed_time), KStatus::FAIL);
  EXPECT_EQ(convertStringToTimestamp("1970-01-01T00:00:01Z", 1000,
                                     &parsed_time),
            KStatus::SUCCESS);
  EXPECT_EQ(parsed_time, utc_time);
  EXPECT_EQ(convertStringToTimestamp("not-a-time", 1000, &parsed_time),
            KStatus::FAIL);
}

}  // namespace
}  // namespace kwdbts
