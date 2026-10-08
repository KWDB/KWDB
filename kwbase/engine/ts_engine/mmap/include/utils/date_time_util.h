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

#pragma once

#include <vector>
#include <string.h>
#include <algorithm>
#include <iostream>
#include <time.h>
#include <set>
#include "data_type.h"
#include "settings.h"

extern long int kw_time_zone;

using namespace std;


using namespace kwdbts;

// format will be modified.
int timeFormatMaxLen(string &fmt);

void setTimeLocal(tm &t, int year, int month, int day, int hour = 0,
                  int minute = 0, int second = 0);

int getWeekDay(int year, int month, int day);

inline bool isLeapYear(int year) {
  return ((((year & 3) == 0) && (year % 100 != 0)) || (year % 400 == 0));
}

uint32_t now();

#define WEEKNUMBERMASK		0x0000FFFF

class DateTime64;
class DateTime32;

void DateTime64ToDateTime32(DateTime64 &dt64, DateTime32 &dt32);

extern int day_of_year[13];

// for month [0-11] in struc_tm
inline int day_of_month[12] =
  { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
inline int day_of_leap_year_month[12] =
  { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

inline int getDayOfMonth(int year, int mon)
{ return (isLeapYear(year)) ? day_of_leap_year_month[mon] : day_of_month[mon]; }

struct DateTimeData {
  struct tm stm;
  timestamp64 ts;
  int type;           // datetime type: Date32, DateTime64, TimeStamp64
  int prec;           // TimeStamp64 precision
};

class BasicDateTime {
protected:
  int cur_year_;
  bool is_valid_;
  string fmt_;
  int str_max_len_;
  int max_year_;

  // Days since 0000-00-00 rename dayNumber to days for subclass call
  int days(int year, int month, int day) const;

  // Seconds since 00:00:00 rename secondNumber to seconds for subclass call
  inline int seconds(int hour, int minute, int second) const
  {return (hour*3600 + minute*60 + second);}

  timestamp toTimeStamp(struct tm &ts);

public:
  BasicDateTime(const string &format);

  BasicDateTime(const BasicDateTime &rhs);

  virtual ~BasicDateTime();

  bool & isValid() { return is_valid_; }

  virtual bool isValidTime(void *data) { return true; }

  // check NULL or valid ZERO time string
  virtual bool isValidTime(const char *str) const = 0;

  virtual void setPrecision(int precison);

  virtual int precision() const;

  void setTimeFormat(const string &fmt) { fmt_ = fmt; }

  string & format() { return fmt_; }

  void getTimeStructure(struct tm &ts, void *data) {
    ::setTimeLocal(ts, year(data), month(data), day(data),
                   hour(data), minute(data), second(data));
  }

  virtual int size() const;

  virtual void * getData() const;

  virtual void getData(void *data) const = 0;

  virtual void setData(void *data) = 0;

  virtual void clear(void *dt) = 0;

  virtual void setDateTime(struct tm &ts, void *dt) = 0;

  virtual bool setDateTime(const char *time_str, void *dt) = 0;

  virtual int year(void *dt) = 0;

  virtual int month(void *dt) = 0;

  virtual int day(void *dt) = 0;

  int weekday(void *dt) { return getWeekDay(year(dt), month(dt), day(dt)); }

  virtual timestamp toTimeStamp(void *dt);

  virtual int hour(void *dt);
  virtual int minute(void *dt);
  virtual int second(void *dt);
  virtual int millisecond(void *dt);

  virtual void setYear(int y, void *dt);
  virtual void setMonth(int m, void *dt);
  virtual void setDay(int d, void *dt);
  virtual void setHour(int y, void *dt);
  virtual void setMinute(int m, void *dt);
  virtual void setSecond(int d, void *dt);
  virtual void setMilliSecond(int ms, void *dt);

  void now(void *dt);

  virtual void addMonth(int64_t m, void *dt);

  inline int strformat(void *data, char *s)
  { return strformat(data, s, str_max_len_, fmt_); }

  virtual int strformat(void *data, char *s, size_t max, const string &fmt_str);

  string toString(void *data);

  int32_t timeToSecond(void *dt);

  virtual BasicDateTime * clone() const = 0;

  // check is timestamp64 for save data
  virtual bool isTimeStamp64() { return false; }
};

class Time: public BasicDateTime {
protected:
  int32_t sec_;
  int year_;
  int16_t month_;
  int16_t day_;
  void setYMD();
  void setYMD(struct tm &ts);

  inline int32_t SplitHour(const int32_t &val) { return val / 3600; }
  inline int32_t SplitMinute(const int32_t &val) { return ((val % 3600) / 60); }
  inline int32_t SplitSecond(const int32_t &val) { return val % 60; }

public:
  Time();
  virtual ~Time();
  virtual int precision() const;

  virtual bool isValidTime(void *data);

  virtual bool isValidTime(const char *str) const;

  virtual void clear(void *dt);

  virtual void setDateTime(struct tm &ts, void *dt);

  virtual bool setDateTime(const char *time_str, void *dt);

  virtual int year(void *s);
  virtual int month(void *s);
  virtual int day(void *s);
  virtual int secondNumber(void *dt);
  virtual int hour(void *dt);
  virtual int minute(void *dt);
  virtual int second(void *dt);
  virtual int size() const;
  virtual void * getData() const;
  virtual void getData(void *data) const;
  virtual void setData(void *data);

  virtual int strformat(void *data, char *s, size_t max, const string &fmt_str);

  virtual BasicDateTime * clone() const;
};

class Time64: public Time {
private:
    timestamp64 ts_data_;
    string dec_fmt_;
    int prec_;              // for timestamp64 precision
    uint32_t multiple_;     // for converting to timestamp64

public:
    Time64(int prec=0);
    virtual ~Time64();
    virtual int precision() const;
    virtual int secondNumber(void *dt);
    virtual int hour(void *dt);
    virtual int minute(void *dt);
    virtual int second(void *dt);
    virtual int size() const;
    virtual void * getData() const;
    virtual void getData(void *data) const;
    virtual void setData(void *data);

    virtual int strformat(void *data, char *s, size_t max, const string &fmt_str);
    virtual BasicDateTime * clone() const;
};


/*  Date32
 *  31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16
 *  |<---               year                   --->|
 *  15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0
 *  |<-----  month   ----->|<------  day  -------->|
 *
 *  year: 0 - 65535
 *  month: 1 - 12
 *  day: 1 - 31
 */
class Date32: public BasicDateTime {
public:
  struct DTS              // DateTime Structure
  {
    uint8_t day;
    uint8_t month;
    uint16_t year;
  };

private:
  DTS date_;

public:
  Date32(): BasicDateTime(kwdbts::EngineOptions::dateFormat()) {};

  Date32(void *data): BasicDateTime(kwdbts::EngineOptions::dateFormat())
  { setData(data); }

  Date32(const string &time_str): BasicDateTime(kwdbts::EngineOptions::dateFormat())
  { setDateTime(time_str.c_str(), &date_); }

  virtual ~Date32() {};

  Date32(int year, int month, int day): BasicDateTime(kwdbts::EngineOptions::dateFormat()) {
    date_.year = year;
    date_.month = month;
    date_.day = day;
    is_valid_ = true;
  }

  Date32(struct tm &ts): BasicDateTime(kwdbts::EngineOptions::dateFormat()) {
    setDateTime(ts, &date_);
    is_valid_ = true;
  }
  virtual bool isValidTime(void *data);

  virtual bool isValidTime(const char *str) const;

  void clear(void *dt);

  virtual void setDateTime(struct tm &ts, void *dt);

  virtual bool setDateTime(const char *time_str, void *dt);

  void setYear_(int y, void *dt) { ((DTS *)dt)->year = y; }
  void setMonth_(int m, void *dt) { ((DTS *)dt)->month = m; }
  void setDay_(int d, void *dt) { ((DTS *)dt)->day = d; }

  void setYear(int y) { date_.year = y; }
  void setMonth(int m) { date_.month = m; }
  void setDay(int d) { date_.day = d; }

  virtual void setYear(int y, void *dt) { setYear_(y, dt); }
  virtual void setMonth(int m, void *dt) { setMonth_(m, dt); }
  virtual void setDay(int d, void *dt) { setDay_(d, dt); }

  virtual int year(void *s) { return ((DTS *)s)->year; }

  virtual int month(void *s) { return ((DTS *)s)->month; }

  virtual int day(void *s) { return ((DTS *)s)->day; }

  int getYear() const { return date_.year; }

  int getMonth() const { return date_.month; }

  int getDay() const { return date_.day; }

  virtual int size() const { return sizeof (date_); }

  virtual void * getData() const { return (void *) &date_; }

  virtual void getData(void *data) const;

  void getData_(void *data) const { *((DTS *) data) = date_; }

  IDTYPE toIDTYPE()
  { return (IDTYPE) (*(reinterpret_cast<uint32_t *> (&date_))); }

  virtual void setData(void *data) { date_ = *((DTS*) data); }

  virtual BasicDateTime * clone() const;

  inline void toDateTime(int year, int mon, int day, int hour, int min,
    int sec,  void *dt) {
    if ((year > 9999) || (year < 1000) ||
    (mon > 12) || (day > getDayOfMonth(year, mon-1))) { // mon should -1 because month array range is [0-11]
      clear(dt);
    } else {
      setYear_(year, dt);
      setMonth_(mon, dt);
      setDay_(day, dt);
      is_valid_ = true;
    }
  };
};

/*
 * 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16
 * |<---- year-1980--->|<- month ->|<--- day ---->|
 * 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0
 * |<--- hour--->|<---- minute --->|<- second/2 ->|
 */

#define DTDOSYEARMASK	0xFE00				// 1111 1110 0000 0000
#define DTDOSMONTHMASK	0x01E0				// 0000 0001 1110 0000
#define DTDOSDAYMASK	0x001F		        // 0000 0000 0001 1111
#define DTDOSHOURMASK	0xF800				// 1111 1000 0000 0000
#define DTDOSMINUTEMASK	0x07E0				// 0000 0111 1110 0000
#define DTDOSSECONDMASK	0x001F				// 0000 0000 0001 1111
class DateTimeDOS: public BasicDateTime {
public:
  struct DTS {
    uint16_t low;
    uint16_t high;
    bool operator==(const DTS &rhs)
    { return (*((int*)this) == *((int *)&rhs)); }
  };
private:
  DTS date_;

public:
  DateTimeDOS(): BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {};

  DateTimeDOS(void *data): BasicDateTime(kwdbts::EngineOptions::dateTimeFormat())
  { setData(data); }

  DateTimeDOS(const string &time_str): BasicDateTime(kwdbts::EngineOptions::dateTimeFormat())
  { setDateTime(time_str.c_str(), &date_); }

  virtual ~DateTimeDOS() {}

  DateTimeDOS(struct tm &ts): BasicDateTime(kwdbts::EngineOptions::dateTimeFormat())
  { setDateTime(ts, &date_); }

  virtual bool isValidTime(void *data) const;

  virtual bool isValidTime(const char *str) const;

  void clear(void *dt);

  void setDateTime(struct tm &ts, void *dt);

  virtual bool setDateTime(const char *time_str, void *dt);

  void setYear_(int year, void *dt) {
    ((DTS *)dt)->high =
      (((year - 1980) << 9) | (((DTS *)dt)->high & ~DTDOSYEARMASK));
  }
  void setMonth_(int month, void *dt) {
    ((DTS *)dt)->high =
      ((month << 5) | (((DTS *)dt)->high & ~DTDOSMONTHMASK));
  }

  void setDay_(int day, void *dt)
  { ((DTS *)dt)->high = (day | (((DTS *)dt)->high & ~DTDOSDAYMASK)); }

  void setHour_(int hour, void *dt) {
    ((DTS *)dt)->low = ((hour << 11) | (((DTS *)dt)->low & ~DTDOSHOURMASK));
  }

  void setMinute_(int minute, void *dt) {
    ((DTS *)dt)->low =
      ((minute << 5) | (((DTS *)dt)->low & ~DTDOSMINUTEMASK));
  }

  void setSecond_(int second, void *dt) {
    ((DTS *)dt)->low =
      (second >> 1 | (((DTS *)dt)->low & ~DTDOSSECONDMASK));
  }

  void setYear(int year)
  { date_.high = (((year - 1980) << 9) | (date_.high & ~DTDOSYEARMASK)); }

  void setMonth(int month)
  { date_.high = ((month << 5) | (date_.high & ~DTDOSMONTHMASK)); }

  void setDay(int day)
  { date_.high = (day | (date_.high & ~DTDOSDAYMASK)); }

  void setHour(int hour)
  { date_.low = ((hour << 11) | (date_.low & ~DTDOSHOURMASK)); }

  void setMinute(int minute)
  { date_.low = ((minute << 5) | (date_.low & ~DTDOSMINUTEMASK)); }

  void setSecond(int second)
  { date_.low = (second >> 1 | (date_.low & ~DTDOSSECONDMASK)); }

  virtual void setYear(int y, void *dt) { setYear_(y, dt); }
  virtual void setMonth(int m, void *dt) { setMonth_(m, dt); }
  virtual void setDay(int d, void *dt) { setDay_(d, dt); }
  virtual void setHour(int h, void *dt) { setHour_(h, dt); }
  virtual void setMinute(int m, void *dt) { setMinute_(m, dt); }
  virtual void setSecond(int s, void *dt) { setSecond_(s, dt); }

  virtual int year(void *dt)
  { return ((((DTS*) dt)->high & DTDOSYEARMASK) >> 9) + 1980; }

  virtual int month(void *dt)
  { return ((((DTS *)dt)->high & DTDOSMONTHMASK) >> 5); }

  virtual int day(void *dt)
  { return (((DTS *)dt)->high & DTDOSDAYMASK); }

  virtual int hour(void *dt)
  { return (((DTS *)dt)->low & DTDOSHOURMASK) >> 11; }

  virtual int minute(void *dt)
  { return (((DTS *)dt)->low & DTDOSMINUTEMASK) >> 5; }

  virtual int second(void *dt)
  { return (((DTS *)dt)->low & DTDOSSECONDMASK) << 1; }

  int getYear() const
  { return ((date_.high & DTDOSYEARMASK) >> 9) + 1980; }

  int getMonth() const
  { return ((date_.high & DTDOSMONTHMASK) >> 5); }

  int getDay() const
  { return (date_.high & DTDOSDAYMASK); }

  int getHour() const { return (date_.low & DTDOSHOURMASK) >> 11; }

  int getMinute() const  { return (date_.low & DTDOSMINUTEMASK) >> 5; }

  int getSecond() const { return (date_.low & DTDOSSECONDMASK) << 1; }

  int getWeekDay() const
  { return ::getWeekDay(getYear(), getMonth(), getDay()); }

  virtual int size() const { return sizeof (date_); }

  virtual void * getData() const  { return (void *) &date_; }
  virtual void getData(void *data) const;

  int getData_(void *data) const {
    *((DTS *) data) = date_;
    return 0;
  }

  IDTYPE toIDTYPE()
  { return (IDTYPE) (*(reinterpret_cast<uint32_t *> (&date_))); }

  virtual void setData(void * data) { date_ = *((DTS *) data); }

  virtual BasicDateTime * clone() const;
};

/*
 * 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16
 * |<-- year-2000-->|<- month ->|<--- day ---->|<-
 * 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0
 * -- hour -->|<---- minute --->|<--- second  --->|
 */

#define DT32YEARMASK    0xFC000000      // 1111 1100 0000 0000
#define DT32MONTHMASK   0x03C00000      // 0000 0011 1100 0000
#define DT32DAYMASK     0x003E0000      // 0000 0000 0011 1110
#define DT32HOURMASK    0x0001F000      // 0000 0000 0000 0001 1111 0000
#define DT32MINUTEMASK  0x00000FC0      // 0000 1111 1100 0000
#define DT32SECONDMASK  0x0000003F      // 0000 0000 0011 1111
class DateTime32: public BasicDateTime {
protected:
  uint32_t date_;
public:
  DateTime32();
  DateTime32(void *data);
  DateTime32(const string &time_str);
  DateTime32(struct tm &ts);
  virtual ~DateTime32();

  virtual bool isValidTime(void *data);

  virtual bool isValidTime(const char *str) const;

  void clear(void *dt);

  virtual void setDateTime(struct tm &ts, void *dt);

  virtual bool setDateTime(const char *time_str, void *dt);

  void setYear_(int year, void *dt) {
    int dt32_year = year - 2000;
    if (dt32_year & 0xFFFFFFC0)       //  2000 <= year <= 2063
      clear(dt);
    else
      *(uint32_t *)dt =
        (((dt32_year) << 26) | (*(uint32_t *)dt & ~DT32YEARMASK));
  }

  void setMonth_(int month, void *dt)
  { *(uint32_t *)dt = ((month << 22) | (*(uint32_t *)dt & ~DT32MONTHMASK)); }

  void setDay_(int day, void *dt)
  { *(uint32_t *)dt = ((day << 17) | (*(uint32_t *)dt & ~DT32DAYMASK)); }

  void setHour_(int hour, void *dt)
  { *(uint32_t *)dt = ((hour << 12) | (*(uint32_t *)dt & ~DT32HOURMASK)); }

  void setMinute_(int minute, void *dt)
  { *(uint32_t *)dt = ((minute << 6) | (*(uint32_t *)dt & ~DT32MINUTEMASK)); }

  void setSecond_(int second, void *dt) {
    *(uint32_t *)dt =
      ((second & DT32SECONDMASK) | (*(uint32_t *)dt & ~DT32SECONDMASK));
  }

  void setYear(int year)
  { date_ = (((year - 2000) << 26) | (date_ & ~DT32YEARMASK)); }

  void setMonth(int month)
  { date_ = ((month << 22) | (date_ & ~DT32MONTHMASK)); }

  void setDay(int day) { date_ = ((day << 17) | (date_ & ~DT32DAYMASK)); }

  void setHour(int hour) { date_ = ((hour << 12) | (date_ & ~DT32HOURMASK)); }

  void setMinute(int minute)
  { date_ = ((minute << 6) | (date_ & ~DT32MINUTEMASK)); }

  void setSecond(int second)
  { date_ = ((second & DT32SECONDMASK) | (date_ & ~DT32SECONDMASK)); }

  virtual void setYear(int y, void *dt) { setYear_(y, dt); }
  virtual void setMonth(int m, void *dt) { setMonth_(m, dt); }
  virtual void setDay(int d, void *dt) { setDay_(d, dt); }
  virtual void setHour(int h, void *dt) { setHour_(h, dt); }
  virtual void setMinute(int m, void *dt) { setMinute_(m, dt); }
  virtual void setSecond(int s, void *dt) { setSecond_(s, dt); }

  virtual int year(void *dt)
  { return (((*(uint32_t *)dt) & DT32YEARMASK) >> 26) + 2000; }

  virtual int month(void *dt)
  { return (((*(uint32_t *)dt) & DT32MONTHMASK) >> 22); }

  virtual int day(void *dt)
  { return (((*(uint32_t *)dt) & DT32DAYMASK) >> 17); }

  virtual int hour(void *dt)
  { return ((*(uint32_t *)dt) & DT32HOURMASK) >> 12; }

  virtual int minute(void *dt)
  { return ((*(uint32_t *)dt) & DT32MINUTEMASK) >> 6; }

  virtual int second(void *dt)
  { return ((*(uint32_t *)dt) & DT32SECONDMASK); }

  inline void toDateTime(int year, int mon, int day, int hour, int min,
    int sec,  void *dt) {
    int dt32_year = year - 2000;
    if (dt32_year & 0xFFFFFFC0) {        //  2000 <= year <= 2063
      clear(dt);
    } else {
      setYear_(year, dt);
      setMonth_(mon, dt);
      setDay_(day, dt);
      setHour_(hour, dt);
      setMinute_(min, dt);
      setSecond_(sec, dt);
      is_valid_ = true;
    }
  }

  int getYear() const { return ((date_ & DT32YEARMASK) >> 26) + 2000; }

  int getMonth() const { return ((date_ & DT32MONTHMASK) >> 22); }

  int getDay() const { return ((date_ & DT32DAYMASK) >> 17); }

  int getHour() const { return (date_ & DT32HOURMASK) >> 12; }

  int getMinute() const { return (date_ & DT32MINUTEMASK) >> 6; }

  int getSecond() const { return (date_ & DT32SECONDMASK); }

  virtual int size() const { return sizeof (date_); }

  virtual void * getData() const { return (void *) &date_; }
  virtual void getData(void *data) const;

  int getData_(void *data) const {
    *((uint32_t *) data) = date_;
    return 0;
  }

  IDTYPE toIDTYPE()
  { return (IDTYPE) (*(reinterpret_cast<uint32_t *> (&date_))); }

  virtual void setData(void * data) { date_ = *((uint32_t *) data); }

  virtual BasicDateTime * clone() const;

  void initTs(struct tm *ts) {
    ts->tm_gmtoff = 0;
    ts->tm_hour = 0;
    ts->tm_isdst = 0;
    ts->tm_mday = 0;
    ts->tm_min = 0;
    ts->tm_mon = 0;
    ts->tm_sec = 0;
    ts->tm_wday = 0;
    ts->tm_yday = 0;
    ts->tm_year = 0;
    ts->tm_zone = 0;
  }
};


/*  DateTime64
 *
 *  63 62 61 60 59 58 57 56 55 54 53 52 51 50 49 48
 *  |<---               year                   --->|
 *  47 46 45 44 43 42 41 40 39 38 37 36 35 34 33 32
 *  |<-----  month   ----->|<------  day  -------->|
 *  31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16
 *  |<-----   hour   ----->|<----    minute   ---->|
 *  15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0
 *  |<---    second   ---->|<---     weekday   --->|
 *
 *  year: 0 - 65535
 *  month: 0 - 11
 *  day: 1 - 31
 *  hour: 0 - 23
 *  quarter: 0 - 3
 *  minute: 0 - 59
 *  second: 0 - 59
 *  weekday: 0 - 6
 */

class DateTime64: public BasicDateTime {
public:
  struct DTS
  {
    uint8_t weekday;
    uint8_t second;
    uint8_t minute;
    uint8_t hour;
    uint8_t day;
    uint8_t month;
    uint16_t year;
  };

private:
  DTS date_;
  DateTime32 dt32_;

public:
  DateTime64();
  DateTime64(void *data);
  DateTime64(const string &time_str);
  DateTime64(struct tm &ts);
  virtual ~DateTime64();

  virtual bool isValidTime(void *data);

  virtual bool isValidTime(const char *str) const;

  virtual void clear(void *dt);

  virtual void setDateTime(struct tm &ts, void *dt);

  // return 1 on success. 0 on error
  virtual bool setDateTime(const char *str, void *dt);

  void setYear_(int year, void *dt) {
    if (year & 0xFFFF0000)
      clear(dt);
    else
      ((DTS *)dt)->year = year;
  }

  void setMonth_(int month, void *dt) { ((DTS *)dt)->month = month; }

  void setDay_(int day, void *dt) { ((DTS *)dt)->day = day; }

  void setHour_(int hour, void *dt) { ((DTS *)dt)->hour = hour; }

  void setMinute_(int minute, void *dt) { ((DTS *)dt)->minute = minute; }

  void setSecond_(int second, void *dt) { ((DTS *)dt)->second = second; }

  void setMilliSecond_(int millisecond, void *dt) { /* TODO */ }

  void setWeekDay_(int weekday, void *dt) { ((DTS *)dt)->weekday = weekday; }

  void setYear(int year) { date_.year = year;}

  void setMonth(int month) { date_.month = month; }

  void setDay(int day) { date_.day = day; }

  void setHour(int hour) { date_.hour = hour; }

  void setMinute(int minute) { date_.minute = minute; }

  void setSecond(int second) { date_.second = second; }

  void setMilliSecond(int millisecond) { /* TODO */ }

  void setWeekDay(int weekday) { date_.weekday = weekday; }

  virtual void setYear(int y, void *dt) { setYear_(y, dt); }
  virtual void setMonth(int m, void *dt) { setMonth_(m, dt); }
  virtual void setDay(int d, void *dt) { setDay_(d, dt); }
  virtual void setHour(int h, void *dt) { setHour_(h, dt); }
  virtual void setMinute(int m, void *dt) { setMinute_(m, dt); }
  virtual void setSecond(int s, void *dt) { setSecond_(s, dt); }
  virtual void setMilliSecond(int ms, void *dt) { /* TODO */ }

  virtual int year(void *s) { return ((DTS *)s)->year; }

  virtual int month(void *s) { return ((DTS *)s)->month; }

  virtual int day(void *s) { return ((DTS *)s)->day; }

  virtual int hour(void *dt) { return ((DTS *)dt)->hour; }

  virtual int minute(void *dt) { return ((DTS *)dt)->minute; }

  virtual int second(void *dt) { return ((DTS *)dt)->second; }

  int getYear() const { return date_.year; }

  int getMonth() const { return date_.month; }

  int getDay() const { return date_.day; }

  int getHour() const { return date_.hour; }

  int getMinute() const { return date_.minute; }

  int getSecond() const { return date_.second; }

  int getMilliSecond() const { return 0; /* TODO */ }

  virtual int size() const { return sizeof (date_); }

  void * getData() const { return (void *) &date_; }
  virtual void getData(void *data) const;

  int getData_(void *data) const {
#if defined(__UNALIGNED_ACCESS__)
    *((DTS *) data) = date_;
#else
    memcpy(data, &date_, sizeof(date_));
#endif
    return 0;
  }

  IDTYPE toIDTYPE() {
    if (sizeof(IDTYPE) == sizeof(date_))
      return *reinterpret_cast<IDTYPE *>(&date_);
    DateTime64ToDateTime32(*this, dt32_);
    return dt32_.toIDTYPE();
  }

  virtual void setData(void * data) { date_ = *((DTS *) data); }

  virtual BasicDateTime * clone() const;

  void initTs(struct tm *ts) {
    ts->tm_gmtoff = 0;
    ts->tm_hour = 0;
    ts->tm_isdst = 0;
    ts->tm_mday = 0;
    ts->tm_min = 0;
    ts->tm_mon = 0;
    ts->tm_sec = 0;
    ts->tm_wday = 0;
    ts->tm_yday = 0;
    ts->tm_year = 0;
    ts->tm_zone = 0;
  }

  inline void toDateTime(int year, int mon, int day, int hour, int min,
    int sec,  void *dt) {
    if (year < 1000 || year > 9999) {        //  1000 <= year <= 9999
      clear(dt);
    } else {
      setYear_(year, dt);
      setMonth_(mon, dt);
      setDay_(day, dt);
      setHour_(hour, dt);
      setMinute_(min, dt);
      setSecond_(sec, dt);
      setWeekDay_(0, dt);
      is_valid_ = true;
    }
  }
};

class TimeStampDateTime: public BasicDateTime {
private:
  struct tm stm_;
  timestamp ts_;
public:
  TimeStampDateTime();

  virtual ~TimeStampDateTime();

  virtual bool isValidTime(const char *str) const;

  virtual void clear(void *dt);

  virtual void setDateTime(struct tm &ts, void *dt);

  virtual bool setDateTime(const char *time_str, void *dt);

  virtual timestamp toTimeStamp(void *dt);

  virtual timestamp64 toTimeStamp64(void *dt, int prec);

  virtual int year(void *d);

  virtual int month(void *d);

  virtual int day(void *d);

  virtual int hour(void *dt);

  virtual int minute(void *dt);

  virtual int second(void *dt);

  virtual int size() const;

  virtual void * getData() const;
  virtual void getData(void *data) const;

//    IDTYPE toIDTYPE() {
//        return (IDTYPE) (*(reinterpret_cast<uint32_t *> (&date_)));
//    }

  virtual void setData(void *data);

  virtual int strformat(void *data, char *s, size_t max, const string &fmt_str);

  virtual BasicDateTime * clone() const;

  virtual void addMonth(int64_t m, void *dt);

  virtual int dayNumber(void *dt);

  virtual int secondNumber(void *dt);
};

//// timestamp64 to timestamp second
inline timestamp64 toTimestampSecond(timestamp64 t, int multiple_)
{ return (t / multiple_); }

inline timestamp64 toTimestampDecimal(timestamp64 t, int multiple_)
{ return (t % multiple_); }

// timestamp precision to multiple
// max 10 elements for timestamp(9)
extern const uint32_t precToMultiple[10];

inline uint32_t precisionToMultiple(int prec)
{ return precToMultiple[prec]; }

// TIMESTAMP64:  timestamp with subsecond up to nanosecond(10^-9)
class TimeStamp64DateTime: public BasicDateTime {
private:
  struct tm stm_;
  timestamp64 ts_data_;
  string dec_fmt_;
  int prec_;              // for timestamp64 precision
  uint32_t multiple_;     // for converting to timestamp64
  timestamp64 getSubSecond(char *p);

public:
  TimeStamp64DateTime();
  TimeStamp64DateTime(const TimeStamp64DateTime &rhs);

  virtual ~TimeStamp64DateTime();

  virtual bool isValidTime(const char *str) const;

  virtual void clear(void *dt);

  virtual void setPrecision(int precision);

  virtual int precision() const;

  virtual void setDateTime(struct tm &ts, void *dt);

  virtual bool setDateTime(const char *time_str, void *dt);

  virtual timestamp toTimeStamp(void *dt);

  virtual timestamp64 toTimeStamp64(void *dt, int prec);

  virtual int year(void *d);

  virtual int month(void *d);

  virtual int day(void *d);

  virtual int hour(void *dt);

  virtual int minute(void *dt);

  virtual int second(void *dt);

  virtual void addMonth(int64_t m, void *dt);

  virtual int size() const;

  virtual void * getData() const;
  virtual void getData(void *data) const;

  virtual void setData(void *data);

  virtual int strformat(void *data, char *s, size_t max, const string &fmt_str);

  virtual BasicDateTime * clone() const;

  void initTs(struct tm *ts) {
    ts->tm_gmtoff = 0;
    ts->tm_hour = 0;
    ts->tm_isdst = 0;
    ts->tm_mday = 0;
    ts->tm_min = 0;
    ts->tm_mon = 0;
    ts->tm_sec = 0;
    ts->tm_wday = 0;
    ts->tm_yday = 0;
    ts->tm_year = 0;
    ts->tm_zone = 0;
  }

  // check is timestamp64 for save data
  virtual bool isTimeStamp64() { return true; }

  virtual int dayNumber(void *dt);

  virtual int secondNumber(void *dt);

  virtual int64_t milliSecondNumber(void *dt);
};


BasicDateTime *getDateTime(int type, int precision);

namespace kwdbts {
inline Date32& s_Date32() { static Date32 d32; return d32; }
inline DateTimeDOS& s_DateTimeDOS()
{ static DateTimeDOS dtdos; return dtdos; }
inline DateTime32& s_DateTime32() { static DateTime32 dt32; return dt32; }
inline DateTime64& s_DateTime64() { static DateTime64 dt64; return dt64; }
};


inline void Date32ToDateTimeDOS(Date32 &d32, DateTimeDOS &dt32, void *dt) {
    dt32.setYear_(d32.getYear(), dt);
    dt32.setMonth_(d32.getMonth(), dt);
    dt32.setDay_(d32.getDay(), dt);
    dt32.setHour_(0, dt);
    dt32.setMinute_(0, dt);
    dt32.setSecond_(0, dt);
}

inline void Date32ToDateTime32(Date32 &d32, DateTime32 &dt32, void *dt)
{ dt32.toDateTime(d32.getYear(), d32.getMonth(), d32.getDay(), 0, 0, 0, dt); }

inline void Date32ToDateTime64(Date32 &d32, DateTime64 &dt64, void *dt) {
    dt64.clear(dt);
    dt64.setYear_(d32.getYear(), dt);
    dt64.setMonth_(d32.getMonth(), dt);
    dt64.setDay_(d32.getDay(), dt);
    dt64.setHour_(0, dt);
    dt64.setMinute_(0, dt);
    dt64.setSecond_(0, dt);
    dt64.setMilliSecond_(0, dt);
}

inline void DateTimeDOSToDate32(DateTimeDOS &dt32, Date32 &d32) {
    d32.setYear(dt32.getYear());
    d32.setMonth(dt32.getMonth());
    d32.setDay(dt32.getDay());
}

inline void DateTime32ToDate32(DateTime32 &dt32, Date32 &d32) {
  d32.setYear(dt32.getYear());
  d32.setMonth(dt32.getMonth());
  d32.setDay(dt32.getDay());
}

inline void DateTime32ToDateTimeDOS(DateTime32 &dt32, DateTimeDOS &ddos) {
    ddos.setYear(dt32.getYear());
    ddos.setMonth(dt32.getMonth());
    ddos.setDay(dt32.getDay());
    ddos.setHour(dt32.getHour());
    ddos.setMinute(dt32.getMinute());
    ddos.setSecond(dt32.getSecond());
}

inline void DateTime64ToDate32(DateTime64 &dt64, Date32 &d32) {
    d32.setYear(dt64.getYear());
    d32.setMonth(dt64.getMonth());
    d32.setDay(dt64.getDay());
}

inline void DateTime64ToDateTimeDOS(DateTime64 &dt64, DateTimeDOS &dt32) {
    dt32.setYear(dt64.getYear());
    dt32.setMonth(dt64.getMonth());
    dt32.setDay(dt64.getDay());
    dt32.setHour(dt64.getHour());
    dt32.setMinute(dt64.getMinute());
    dt32.setSecond(dt64.getSecond());
}

inline void DateTime64ToDateTime32(DateTime64 &dt64, DateTime32 &dt32) {
    dt32.toDateTime(dt64.getYear(), dt64.getMonth(), dt64.getDay(),
        dt64.getHour(), dt64.getMinute(), dt64.getSecond(), dt32.getData());
}

inline void DateTimeDOSToDateTime64(DateTimeDOS &dt32, DateTime64 &dt64) {
    dt64.clear(dt64.getData());
    dt64.setYear(dt32.getYear());
    dt64.setMonth(dt32.getMonth());
    dt64.setDay(dt32.getDay());
    dt64.setHour(dt32.getHour());
    dt64.setMinute(dt32.getMinute());
    dt64.setSecond(dt32.getSecond());
    //necessary evil to keep weekday field alive.
    // dt64.setWeekDay(0);
}

inline void DateTime32ToDateTime64(DateTime32 &dt32, DateTime64 &dt64) {
    dt64.clear(dt64.getData());
    dt64.setYear(dt32.getYear());
    dt64.setMonth(dt32.getMonth());
    dt64.setDay(dt32.getDay());
    dt64.setHour(dt32.getHour());
    dt64.setMinute(dt32.getMinute());
    dt64.setSecond(dt32.getSecond());
    //necessary evil to keep weekday field alive.
//    dt64.setWeekDay(0);
}

// convert string to year week format
int32_t toYearWeek(const char *str);

// now() in string format.
string nowString();

// string to DateTimeData
int strToDateTime(const char *s, DateTimeData &dtd);
