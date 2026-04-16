// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <gtest/gtest.h>
#include "utils/date_time_util.h"
#include <chrono>
#include <thread>
#include <cstring>

using namespace std;
using namespace kwdbts;

extern long int kw_time_zone;

class DateTimeUtilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    kw_time_zone = 0;
  }

  void TearDown() override {
  }
};

TEST_F(DateTimeUtilTest, BasicDateTimeConstructor) {
  Date32 d32;
  EXPECT_TRUE(d32.isValid());
}

TEST_F(DateTimeUtilTest, BasicDateTimeCopyConstructor) {
  Date32 d32;
  d32.setYear(2023);
  d32.setMonth(12);
  d32.setDay(25);

  Date32 d32_copy(d32);
  EXPECT_EQ(d32_copy.getYear(), 2023);
  EXPECT_EQ(d32_copy.getMonth(), 12);
  EXPECT_EQ(d32_copy.getDay(), 25);
}

TEST_F(DateTimeUtilTest, BasicDateTimeSetFormat) {
  Date32 d32;
  string new_format = "%Y/%m/%d";
  d32.setTimeFormat(new_format);
  EXPECT_EQ(d32.format(), new_format);
}

TEST_F(DateTimeUtilTest, Date32Constructor) {
  Date32 d32_with_str("2023-12-25");
  EXPECT_TRUE(d32_with_str.isValid());

  Date32 d32_with_ymd(2023, 12, 25);
  EXPECT_EQ(d32_with_ymd.getYear(), 2023);
  EXPECT_EQ(d32_with_ymd.getMonth(), 12);
  EXPECT_EQ(d32_with_ymd.getDay(), 25);

  struct tm ts;
  setTimeLocal(ts, 2023, 12, 25);
  Date32 d32_with_tm(ts);
  EXPECT_TRUE(d32_with_tm.isValid());
}

TEST_F(DateTimeUtilTest, Date32SetAndGet) {
  Date32 d32;
  d32.setYear(2024);
  d32.setMonth(2);
  d32.setDay(29);

  EXPECT_EQ(d32.getYear(), 2024);
  EXPECT_EQ(d32.getMonth(), 2);
  EXPECT_EQ(d32.getDay(), 29);

  const void* data = d32.getData();
  EXPECT_NE(data, nullptr);
}

TEST_F(DateTimeUtilTest, Date32DateTimeOperations) {
  Date32 d32;
  struct tm ts;
  setTimeLocal(ts, 2024, 3, 15);

  char buffer[sizeof(Date32::DTS)] = {0};
  d32.setDateTime(ts, buffer);

  d32.setData(buffer);
  EXPECT_EQ(d32.getYear(), 2024);
  EXPECT_EQ(d32.getMonth(), 3);
  EXPECT_EQ(d32.getDay(), 15);

  bool result = d32.setDateTime("2024-04-20", buffer);
  EXPECT_TRUE(result);
  d32.setData(buffer);
  EXPECT_EQ(d32.getYear(), 2024);
  EXPECT_EQ(d32.getMonth(), 4);
  EXPECT_EQ(d32.getDay(), 20);

  result = d32.setDateTime("invalid date", buffer);
  EXPECT_FALSE(result);
}

TEST_F(DateTimeUtilTest, Date32InvalidDateHandling) {
  Date32 d32;
  char buffer[sizeof(Date32::DTS)] = {0};

  d32.toDateTime(2024, 13, 32, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_FALSE(d32.isValid());

  d32.clear(buffer);
  d32.setData(buffer);
  EXPECT_FALSE(d32.isValid());

  d32.toDateTime(2024, 6, 15, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_TRUE(d32.isValid());
}

TEST_F(DateTimeUtilTest, Date32YearMonthDayValidations) {
  Date32 d32;
  char buffer[sizeof(Date32::DTS)] = {0};

  d32.toDateTime(2024, 2, 29, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_TRUE(d32.isValid());

  d32.toDateTime(2023, 2, 29, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_FALSE(d32.isValid());

  d32.toDateTime(1000, 1, 1, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_TRUE(d32.isValid());

  d32.toDateTime(9999, 12, 31, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_TRUE(d32.isValid());
}

TEST_F(DateTimeUtilTest, Date32Clone) {
  Date32 d32;
  d32.setYear(2024);
  d32.setMonth(5);
  d32.setDay(20);

  EXPECT_EQ(d32.getYear(), 2024);
  EXPECT_EQ(d32.getMonth(), 5);
  EXPECT_EQ(d32.getDay(), 20);

  BasicDateTime* clone = d32.clone();
  ASSERT_NE(clone, nullptr);

  Date32* d32_clone = dynamic_cast<Date32*>(clone);
  ASSERT_NE(d32_clone, nullptr);

  delete clone;
}

TEST_F(DateTimeUtilTest, Date32CloneWithData) {
  Date32 d32;
  char buffer[sizeof(Date32::DTS)] = {0};
  d32.toDateTime(2024, 12, 25, 0, 0, 0, buffer);
  d32.setData(buffer);

  EXPECT_EQ(d32.getYear(), 2024);
  EXPECT_EQ(d32.getMonth(), 12);
  EXPECT_EQ(d32.getDay(), 25);

  BasicDateTime* clone = d32.clone();
  ASSERT_NE(clone, nullptr);

  Date32* d32_clone = dynamic_cast<Date32*>(clone);
  ASSERT_NE(d32_clone, nullptr);

  delete clone;
}

TEST_F(DateTimeUtilTest, Date32ToIDTYPE) {
  Date32 d32;
  d32.setYear(2024);
  d32.setMonth(8);
  d32.setDay(15);

  IDTYPE id = d32.toIDTYPE();
  if (d32.isValid()) {
    EXPECT_NE(id, 0);
  }
}

TEST_F(DateTimeUtilTest, DateTime32Constructor) {
  DateTime32 dt32;

  DateTime32 dt32_with_str("2023-12-25 14:30:45");
  EXPECT_TRUE(dt32_with_str.isValid());

  struct tm ts;
  setTimeLocal(ts, 2023, 12, 25, 14, 30, 45);
  DateTime32 dt32_with_tm(ts);
  EXPECT_TRUE(dt32_with_tm.isValid());
}

TEST_F(DateTimeUtilTest, DateTime32SetAndGet) {
  DateTime32 dt32;
  dt32.setYear(2024);
  dt32.setMonth(3);
  dt32.setDay(15);
  dt32.setHour(10);
  dt32.setMinute(30);
  dt32.setSecond(45);

  EXPECT_EQ(dt32.getYear(), 2024);
  EXPECT_EQ(dt32.getMonth(), 3);
  EXPECT_EQ(dt32.getDay(), 15);
  EXPECT_EQ(dt32.getHour(), 10);
  EXPECT_EQ(dt32.getMinute(), 30);
  EXPECT_EQ(dt32.getSecond(), 45);
}

TEST_F(DateTimeUtilTest, DateTime32RangeValidation) {
  DateTime32 dt32;
  char buffer[sizeof(uint32_t)] = {0};

  dt32.toDateTime(2000, 1, 1, 0, 0, 0, buffer);
  dt32.setData(buffer);
  EXPECT_TRUE(dt32.isValid());

  dt32.toDateTime(2063, 12, 31, 23, 59, 59, buffer);
  dt32.setData(buffer);
  EXPECT_TRUE(dt32.isValid());

  dt32.toDateTime(2064, 1, 1, 0, 0, 0, buffer);
  dt32.setData(buffer);
  EXPECT_FALSE(dt32.isValid());

  dt32.toDateTime(1999, 12, 31, 23, 59, 59, buffer);
  dt32.setData(buffer);
  EXPECT_FALSE(dt32.isValid());
}

TEST_F(DateTimeUtilTest, DateTime32ClearAndClone) {
  DateTime32 dt32;
  char buffer[sizeof(uint32_t)] = {0};

  dt32.toDateTime(2024, 5, 20, 15, 30, 0, buffer);
  dt32.setData(buffer);
  EXPECT_EQ(dt32.getYear(), 2024);

  dt32.clear(buffer);
  dt32.setData(buffer);

  dt32.toDateTime(2024, 5, 20, 15, 30, 0, buffer);
  dt32.setData(buffer);

  BasicDateTime* clone = dt32.clone();
  ASSERT_NE(clone, nullptr);
  DateTime32* dt32_clone = dynamic_cast<DateTime32*>(clone);
  ASSERT_NE(dt32_clone, nullptr);

  delete clone;
}

TEST_F(DateTimeUtilTest, DateTime32StringFormatting) {
  DateTime32 dt32;
  char buffer[sizeof(uint32_t)] = {0};
  dt32.toDateTime(2024, 6, 10, 8, 5, 30, buffer);
  dt32.setData(buffer);

  char out_buffer[100] = {0};
  int len = dt32.strformat(dt32.getData(), out_buffer, 100, "%Y-%m-%d %H:%M:%S");
  EXPECT_GT(len, 0);
}

TEST_F(DateTimeUtilTest, DateTime64Constructor) {
  DateTime64 dt64_with_str("2024-12-25 14:30:45");
  EXPECT_TRUE(dt64_with_str.isValid());

  struct tm ts;
  setTimeLocal(ts, 2024, 12, 25, 14, 30, 45);
  DateTime64 dt64_with_tm(ts);
  EXPECT_TRUE(dt64_with_tm.isValid());
}

TEST_F(DateTimeUtilTest, DateTime64SetAndGet) {
  DateTime64 dt64;
  dt64.setYear(2024);
  dt64.setMonth(12);
  dt64.setDay(25);
  dt64.setHour(23);
  dt64.setMinute(59);
  dt64.setSecond(58);

  EXPECT_EQ(dt64.getYear(), 2024);
  EXPECT_EQ(dt64.getMonth(), 12);
  EXPECT_EQ(dt64.getDay(), 25);
  EXPECT_EQ(dt64.getHour(), 23);
  EXPECT_EQ(dt64.getMinute(), 59);
  EXPECT_EQ(dt64.getSecond(), 58);
}

TEST_F(DateTimeUtilTest, DateTime64WideRangeSupport) {
  DateTime64 dt64;
  char buffer[sizeof(DateTime64::DTS)] = {0};

  dt64.toDateTime(1000, 1, 1, 0, 0, 0, buffer);
  dt64.setData(buffer);
  EXPECT_TRUE(dt64.isValid());
  EXPECT_EQ(dt64.getYear(), 1000);

  dt64.toDateTime(9999, 12, 31, 23, 59, 59, buffer);
  dt64.setData(buffer);
  EXPECT_TRUE(dt64.isValid());
  EXPECT_EQ(dt64.getYear(), 9999);

  dt64.toDateTime(2024, 2, 29, 12, 0, 0, buffer);
  dt64.setData(buffer);
  EXPECT_TRUE(dt64.isValid());
}

TEST_F(DateTimeUtilTest, DateTime64LeapYearHandling) {
  DateTime64 dt64;
  char buffer[sizeof(DateTime64::DTS)] = {0};

  dt64.toDateTime(2024, 2, 29, 12, 0, 0, buffer);
  dt64.setData(buffer);
  EXPECT_TRUE(dt64.isValid());

  dt64.toDateTime(2023, 2, 29, 12, 0, 0, buffer);
  dt64.setData(buffer);
  EXPECT_TRUE(dt64.isValid());
}

TEST_F(DateTimeUtilTest, DateTime64CloneAndCopy) {
  DateTime64 dt64;
  char buffer[sizeof(DateTime64::DTS)] = {0};
  dt64.toDateTime(2024, 10, 1, 9, 0, 0, buffer);
  dt64.setData(buffer);
  EXPECT_EQ(dt64.getYear(), 2024);

  BasicDateTime* clone = dt64.clone();
  ASSERT_NE(clone, nullptr);
  DateTime64* dt64_clone = dynamic_cast<DateTime64*>(clone);
  ASSERT_NE(dt64_clone, nullptr);

  delete clone;
}

TEST_F(DateTimeUtilTest, DateTimeDOSConstructor) {
  DateTimeDOS dtdos;

  struct tm ts;
  setTimeLocal(ts, 2024, 3, 15, 10, 30, 45);
  DateTimeDOS dtdos_with_tm(ts);
  EXPECT_TRUE(dtdos_with_tm.isValid());
}

TEST_F(DateTimeUtilTest, DateTimeDOSSetAndGet) {
  DateTimeDOS dtdos;
  dtdos.setYear(2024);
  dtdos.setMonth(7);
  dtdos.setDay(4);
  dtdos.setHour(14);
  dtdos.setMinute(30);
  dtdos.setSecond(15);

  EXPECT_EQ(dtdos.getYear(), 2024);
  EXPECT_EQ(dtdos.getMonth(), 7);
  EXPECT_EQ(dtdos.getDay(), 4);
  EXPECT_EQ(dtdos.getHour(), 14);
  EXPECT_EQ(dtdos.getMinute(), 30);
}

TEST_F(DateTimeUtilTest, DateTimeDOSClearAndValid) {
  DateTimeDOS dtdos;
  dtdos.setYear(2024);

  char buffer[sizeof(DateTimeDOS::DTS)] = {0};
  dtdos.clear(buffer);
  dtdos.setData(buffer);
  EXPECT_FALSE(dtdos.isValidTime(dtdos.getData()));
}

TEST_F(DateTimeUtilTest, TimeConstructor) {
  Time time_obj;
  EXPECT_EQ(time_obj.precision(), 0);
}

TEST_F(DateTimeUtilTest, TimeSetAndGet) {
  Time time_obj;
  char buffer[sizeof(int32_t)] = {0};
  struct tm ts;
  setTimeLocal(ts, 2024, 3, 15, 14, 30, 45);
  time_obj.setDateTime(ts, buffer);
  time_obj.setData(buffer);

  EXPECT_EQ(time_obj.hour(time_obj.getData()), 14);
  EXPECT_EQ(time_obj.minute(time_obj.getData()), 30);
  EXPECT_EQ(time_obj.second(time_obj.getData()), 45);
  EXPECT_EQ(time_obj.secondNumber(time_obj.getData()), 14*3600+30*60+45);
}

TEST_F(DateTimeUtilTest, TimeStringParsing) {
  Time time_obj;
  char buffer[sizeof(int32_t)] = {0};

  bool result = time_obj.setDateTime("14:30:45", buffer);
  EXPECT_TRUE(result);
  time_obj.setData(buffer);
  EXPECT_EQ(time_obj.hour(time_obj.getData()), 14);
  EXPECT_EQ(time_obj.minute(time_obj.getData()), 30);
  EXPECT_EQ(time_obj.second(time_obj.getData()), 45);

  result = time_obj.setDateTime("invalid", buffer);
  EXPECT_FALSE(result);
}

TEST_F(DateTimeUtilTest, Time64Constructor) {
  Time64 time64(3);
  EXPECT_EQ(time64.precision(), 3);
}

TEST_F(DateTimeUtilTest, Time64PrecisionHandling) {
  for (int prec = 0; prec <= 9; prec++) {
    Time64 time64(prec);
    EXPECT_EQ(time64.precision(), prec);
  }
}

TEST_F(DateTimeUtilTest, Time64SetAndGet) {
  Time64 time64(3);
  int64_t time_value = 14 * 3600 + 30 * 60 + 45;
  time_value = time_value * 1000 + 500;
  time64.setData(&time_value);

  EXPECT_EQ(time64.hour(time64.getData()), 14);
  EXPECT_EQ(time64.minute(time64.getData()), 30);
  EXPECT_EQ(time64.second(time64.getData()), 45);
}

TEST_F(DateTimeUtilTest, Time64StringFormatting) {
  Time64 time64(3);
  int64_t time_value = (10 * 3600 + 15 * 60 + 30) * 1000 + 123;
  time64.setData(&time_value);

  char buffer[100] = {0};
  int len = time64.strformat(time64.getData(), buffer, 100, "");
  EXPECT_GT(len, 0);
}

TEST_F(DateTimeUtilTest, TimeStampDateTimeConstructorAndBasics) {
  TimeStampDateTime tsdt;
  EXPECT_EQ(tsdt.size(), sizeof(timestamp));
}

TEST_F(DateTimeUtilTest, TimeStampDateTimeSetAndGet) {
  TimeStampDateTime tsdt;
  char buffer[sizeof(timestamp)] = {0};

  struct tm ts;
  setTimeLocal(ts, 2024, 3, 15, 14, 30, 45);
  tsdt.setDateTime(ts, buffer);
  tsdt.setData(buffer);

  EXPECT_EQ(tsdt.year(tsdt.getData()), 2024);
  EXPECT_EQ(tsdt.month(tsdt.getData()), 3);
  EXPECT_EQ(tsdt.day(tsdt.getData()), 15);
  EXPECT_EQ(tsdt.hour(tsdt.getData()), 14);
  EXPECT_EQ(tsdt.minute(tsdt.getData()), 30);
  EXPECT_EQ(tsdt.second(tsdt.getData()), 45);
}

TEST_F(DateTimeUtilTest, TimeStampDateTimeStringParsing) {
  TimeStampDateTime tsdt;
  char buffer[sizeof(timestamp)] = {0};

  bool result = tsdt.setDateTime("2024-03-15 14:30:45", buffer);
  EXPECT_TRUE(result);

  result = tsdt.setDateTime("invalid", buffer);
  EXPECT_FALSE(result);
}

TEST_F(DateTimeUtilTest, TimeStampDateTimeAddMonth) {
  TimeStampDateTime tsdt;
  char buffer[sizeof(timestamp)] = {0};

  bool result = tsdt.setDateTime("2024-01-31 10:00:00", buffer);
  EXPECT_TRUE(result);
  tsdt.setData(buffer);
  tsdt.addMonth(1, buffer);
  tsdt.setData(buffer);
  EXPECT_EQ(tsdt.month(tsdt.getData()), 2);
}

TEST_F(DateTimeUtilTest, TimeStamp64DateTimeConstructorAndPrecision) {
  TimeStamp64DateTime tsdt64;
  tsdt64.setPrecision(3);
  EXPECT_EQ(tsdt64.precision(), 3);
  EXPECT_TRUE(tsdt64.isTimeStamp64());
}

TEST_F(DateTimeUtilTest, TimeStamp64DateTimeSubSecondHandling) {
  TimeStamp64DateTime tsdt64;
  tsdt64.setPrecision(3);
  char buffer[sizeof(timestamp64)] = {0};

  bool result = tsdt64.setDateTime("2024-03-15 14:30:45.123", buffer);
  EXPECT_TRUE(result);
  tsdt64.setData(buffer);

  timestamp64 ts_value = *(timestamp64*)tsdt64.getData();
  timestamp64 ms_part = ts_value % 1000;
  EXPECT_EQ(ms_part, 123);
}

TEST_F(DateTimeUtilTest, TimeStamp64DateTimeTimestampConversion) {
  TimeStamp64DateTime tsdt64;
  tsdt64.setPrecision(3);
  char buffer[sizeof(timestamp64)] = {0};
  tsdt64.setDateTime("2024-03-15 14:30:45.123", buffer);
  tsdt64.setData(buffer);

  timestamp ts = tsdt64.toTimeStamp(tsdt64.getData());
  EXPECT_GT(ts, 0);
}

TEST_F(DateTimeUtilTest, IsLeapYear) {
  EXPECT_TRUE(isLeapYear(2020));
  EXPECT_TRUE(isLeapYear(2024));
  EXPECT_TRUE(isLeapYear(2000));
  EXPECT_FALSE(isLeapYear(2021));
  EXPECT_FALSE(isLeapYear(1900));
  EXPECT_FALSE(isLeapYear(2100));
}

TEST_F(DateTimeUtilTest, GetWeekDay) {
  int wd = getWeekDay(2024, 3, 15);
  EXPECT_EQ(wd, 5);

  wd = getWeekDay(2024, 3, 16);
  EXPECT_EQ(wd, 6);

  wd = getWeekDay(2024, 3, 17);
  EXPECT_EQ(wd, 0);
}

TEST_F(DateTimeUtilTest, GetDayOfMonth) {
  EXPECT_EQ(getDayOfMonth(2024, 0), 31);
  EXPECT_EQ(getDayOfMonth(2024, 1), 29);
  EXPECT_EQ(getDayOfMonth(2023, 1), 28);
  EXPECT_EQ(getDayOfMonth(2024, 3), 30);
  EXPECT_EQ(getDayOfMonth(2024, 11), 31);
}

TEST_F(DateTimeUtilTest, TimeFormatMaxLen) {
  string fmt = "%Y-%m-%d %H:%M:%S";
  int len = timeFormatMaxLen(fmt);
  EXPECT_GT(len, 0);

  fmt = "%Y%m%d";
  len = timeFormatMaxLen(fmt);
  EXPECT_EQ(len, 9);
}

TEST_F(DateTimeUtilTest, Now) {
  uint32_t t1 = now();
  this_thread::sleep_for(chrono::milliseconds(100));
  uint32_t t2 = now();
  EXPECT_GE(t2, t1);
}

TEST_F(DateTimeUtilTest, NowString) {
  string now_str = nowString();
  EXPECT_FALSE(now_str.empty());
  EXPECT_EQ(now_str[0], '[');
  EXPECT_EQ(now_str[now_str.length()-1], ']');
}

TEST_F(DateTimeUtilTest, ToYearWeek) {
  int32_t yw = toYearWeek("2024W15");
  EXPECT_NE(yw, 0);
  EXPECT_EQ(yw >> 16, 2024);
  EXPECT_EQ(yw & 0xFFFF, 15);

  yw = toYearWeek("invalid");
  EXPECT_EQ(yw, 0);
}

TEST_F(DateTimeUtilTest, PrecisionToMultiple) {
  EXPECT_EQ(precisionToMultiple(0), 1);
  EXPECT_EQ(precisionToMultiple(1), 10);
  EXPECT_EQ(precisionToMultiple(2), 100);
  EXPECT_EQ(precisionToMultiple(3), 1000);
  EXPECT_EQ(precisionToMultiple(6), 1000000);
  EXPECT_EQ(precisionToMultiple(9), 1000000000);
}

TEST_F(DateTimeUtilTest, StrToDateTime) {
  DateTimeData dtd;
  memset(&dtd, 0, sizeof(dtd));

  int result = strToDateTime("2024-03-15 14:30:45", dtd);
  EXPECT_EQ(result, 0);

  result = strToDateTime("2024-03-15", dtd);
  EXPECT_EQ(result, 0);

  result = strToDateTime("invalid", dtd);
  EXPECT_EQ(result, -1);
}

TEST_F(DateTimeUtilTest, DateConversions) {
  Date32 d32;
  d32.setYear(2024);
  d32.setMonth(6);
  d32.setDay(15);

  DateTimeDOS dtdos;
  DateTime32 dt32;
  DateTime64 dt64;

  char dos_buffer[sizeof(DateTimeDOS::DTS)] = {0};
  char dt32_buffer[sizeof(uint32_t)] = {0};
  char dt64_buffer[sizeof(DateTime64::DTS)] = {0};

  Date32ToDateTimeDOS(d32, dtdos, dos_buffer);
  Date32ToDateTime32(d32, dt32, dt32_buffer);
  Date32ToDateTime64(d32, dt64, dt64_buffer);
}

TEST_F(DateTimeUtilTest, DateTimeConversions) {
  DateTimeDOS dtdos;
  dtdos.setYear(2024);
  dtdos.setMonth(6);
  dtdos.setDay(15);

  Date32 d32;
  DateTimeDOSToDate32(dtdos, d32);
  EXPECT_EQ(d32.getYear(), 2024);
}

TEST_F(DateTimeUtilTest, GetDateTimeFactory) {
  BasicDateTime* bdt;

  bdt = getDateTime(DATE32, 0);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(DATETIME32, 0);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(DATETIME64, 0);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(DATETIMEDOS, 0);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(TIMESTAMP, 0);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(TIMESTAMP64, 3);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(TIME, 0);
  EXPECT_NE(bdt, nullptr);
  delete bdt;

  bdt = getDateTime(TIME64, 3);
  EXPECT_NE(bdt, nullptr);
  delete bdt;
}

TEST_F(DateTimeUtilTest, BoundaryDateTests) {
  Date32 d32;
  char buffer[sizeof(Date32::DTS)] = {0};

  d32.toDateTime(1000, 1, 1, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_TRUE(d32.isValid());

  d32.toDateTime(9999, 12, 31, 0, 0, 0, buffer);
  d32.setData(buffer);
  EXPECT_TRUE(d32.isValid());
}

TEST_F(DateTimeUtilTest, DateTimeToString) {
  Date32 d32;
  d32.setYear(2024);
  d32.setMonth(12);
  d32.setDay(25);

  string str = d32.toString(d32.getData());
  EXPECT_FALSE(str.empty());
}

TEST_F(DateTimeUtilTest, Date32DirectSetAndGet) {
  Date32 d32;

  d32.setYear(2024);
  d32.setMonth(6);
  d32.setDay(15);

  EXPECT_EQ(d32.getYear(), 2024);
  EXPECT_EQ(d32.getMonth(), 6);
  EXPECT_EQ(d32.getDay(), 15);

  const void* data = d32.getData();
  ASSERT_NE(data, nullptr);

  Date32 d32_copy;
  d32_copy.setData(const_cast<void*>(data));

  EXPECT_EQ(d32_copy.getYear(), 2024);
  EXPECT_EQ(d32_copy.getMonth(), 6);
  EXPECT_EQ(d32_copy.getDay(), 15);
}