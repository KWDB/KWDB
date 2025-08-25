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


#include <cstring>
#include "utils/date_time_util.h"
#include "utils/big_table_utils.h"
#include "utils/string_utils.h"
#include "cm_func.h"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

char zero_date[] = "0-00-00";
char zero_date32[] = "2000-00-00";
char zero_datetime32[] = "2000-00-00 00:00:00";
char zero_datetime64[] = "0-00-00 00:00:00";

long int kw_time_zone;

int timeFormatMaxLen(string &fmt) {
  int n = 0;
  size_t sz = fmt.size();
  size_t fmt_tsz = sz - 1;
  for (size_t i = 0; i < sz; ++i) {
    if (i < fmt_tsz && fmt[i] == '%') {
      i++;
      int sym_sz;
      switch(fmt[i]) {
      case 'n': // New-line character ('\n')
      case 't': // Horizontal-tab character ('\t')
      case 'u': // ISO 8601 weekday as number with Monday as 1 (1-7)
      case 'w': // Weekday as a decimal number with Sunday as 0 (0-6)
      case '%': // A % sign    %
        sym_sz = 1; break;
      case 'C': // Year divided by 100 and truncated to integer (00-99)
      case 'd': // Day of the month, zero-padded (01-31)
      case 'e': // Day of the month, space-padded ( 1-31)
      case 'g': // Week-based year, last two digits (00-99)
      case 'H': // Hour in 24h format (00-23)
      case 'I': // Hour in 12h format (01-12)
      case 'm': // Month as a decimal number (01-12)
      case 'M': // Minute (00-59)
      case 'p': // AM or PM designation
      case 'S': // Second (00-61)
      case 'U': // Week number: (00-53)
      case 'V': // ISO 8601 week number (00-53)    34
      case 'W': // Week number: (00-53)  34
      case 'y': // Year, last two digits (00-99)   01
        sym_sz = 2; break;
      case 'a': // Abbreviated weekday name
      case 'b': // Abbreviated month name
      case 'h': // Abbreviated month name * (same as %b)
      case 'j': // Day of the year (001-366)
      case 'Z': // Timezone name or abbreviation * CDT
        sym_sz = 3; break;
      case 'G': // Week-based year 2001
      case 'Y': // Year    2001
        sym_sz = 4; break;
      case 'R': // 24-hour HH:MM time, equivalent to %H:%M
      case 'z': // ISO 8601 offset from UTC in timezone  +2359
        sym_sz = 5; break;
      case 'D': // Short MM/DD/YY date, equivalent to %m/%d/%y 08/23/01
      case 'T': // ISO 8601 time format (HH:MM:SS) = %H:%M:%S  14:55:02
      case 'x': // Date representation *   08/23/01
      case 'X': // Time representation *   14:55:02
        sym_sz = 8; break;
      case 'A': // Full weekday name
      case 'B': // Full month name
        sym_sz = 9; break;
      case 'F': // Short YYYY-MM-DD date = %Y-%m-%d  2001-08-23
        sym_sz = 10; break;
      case 'r': // 12-hour clock time *    02:55:02 pm
        sym_sz = 11; break;
      case 'c': // Date and time representation Thu Aug 23 14:55:02 2001
        sym_sz = 24; break;
      case 'i': // [MySQL] Minute (00 to 59)
        sym_sz = 2; fmt[i] = 'M'; break;
      case 'k': // [MySQL] Hour in 24h format (00-23)
        sym_sz = 2; fmt[i] = 'H'; break;
      case 'l': // [MySQL] Hour in 12h format (01-12)
        sym_sz = 2; fmt[i] = 'I'; break;
      case 's': // [MySQL] Second (00-59)
        sym_sz = 2; fmt[i] = 'S'; break;
      default:
        return -1;
      }
      n += sym_sz;
    }
    else
      n++;
  }
  return n + 1;  // 1 for NULL in strftime()
}

void setTimeLocal(tm &t, int year, int month, int day, int hour,
                  int minute, int second) {
  t.tm_year = year - 1900;
  t.tm_mon = month - 1;
  t.tm_mday = day;
  t.tm_hour = hour;
  t.tm_min = minute;
  t.tm_sec = second;
}

uint32_t now() {
  time_t t;
  time(&t);
  return (uint32_t)t;
}

BasicDateTime::BasicDateTime(const string &fmt) {
  time_t t = time(NULL);
  struct tm ts;
  ToGMT(t, ts);

  cur_year_ = ts.tm_year + 1900;
  // set valid and set invalid when conversion goes bad for save time.
  is_valid_ = true;
  fmt_ = fmt;
  str_max_len_ = timeFormatMaxLen(fmt_);
}

BasicDateTime::BasicDateTime(const BasicDateTime &rhs) {
  fmt_ = rhs.fmt_;
  cur_year_ = rhs.cur_year_;
  str_max_len_ = rhs.str_max_len_;
  is_valid_ = true;
}

BasicDateTime::~BasicDateTime() {}

int month_offset[13] = {0, 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };

int getWeekDay(int year, int month, int day) {
  year -= month < 3;
  return (year + year / 4 - year / 100 + year / 400 +
          month_offset[month] + day) % 7;
}

// Return days since 0000-00-00
// Reference MySQL calc_daynr
int BasicDateTime::days(int year, int month, int day) const {
  long sum;
  int temp;

  if (year == 0 && month == 0)
    return 0;

  sum = (long) (365 * year + 31 * ((int) month - 1) + (int) day);
  if (month <= 2)
    year--;
  else
    sum -= (long) ((int) month * 4 + 23) / 10;
  temp = (int) ((year / 100 + 1) * 3) >> 2;

  return (sum + (int) year / 4 - temp);
}

void BasicDateTime::now(void *dt) {
    time_t t = time(NULL);
    struct tm ts;
    ToGMT(t, ts);
    setDateTime(ts, dt);
}

void BasicDateTime::addMonth(int64_t n, void *dt) {
  int32_t mon = month(dt);
  int total_months = mon + n;
  int years_diff = total_months / 12;
  int months_diff = total_months % 12;
  if (months_diff <= 0) { // Fix bug when adddate to December
    months_diff += 12;
    years_diff--;
  }
  int32_t new_year = year(dt) + years_diff;
  int32_t new_month = months_diff;
  int32_t nd = getDayOfMonth(new_year, new_month - 1);
  if (day(dt) > nd) {
    setDay(nd, dt);
  }
  setMonth(new_month, dt);
  setYear(new_year, dt);
}

// for month [1-12]
int day_of_year[13] =
  { 0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

int BasicDateTime::size() const
{
    return sizeof(int32_t);
}

void * BasicDateTime::getData() const  { return nullptr; }


// https://gmbabar.wordpress.com/2010/12/01/mktime-slow-use-custom-function/
// tm to timestamp64 second only
timestamp64 tmToTimeStamp(const struct tm &stm) {
  if (stm.tm_year < 70)
    return 0;
  unsigned long tyears = stm.tm_year - 70;  // tm_year is from 1900.
  unsigned long leaps = (tyears + 2) / 4;   // no of next two lines until year 2100.
  if (stm.tm_year >= (2100-1900)) {
    int i = (stm.tm_year - 100) / 100;
    leaps -= ( (i/4)*3 + i%4 );
  }
  if (isLeapYear(stm.tm_year + 1900) && stm.tm_mon < 2)
    leaps--;
  unsigned long tdays = day_of_year[stm.tm_mon + 1] + stm.tm_mday - 1;
  tdays = tdays + (tyears * 365) + leaps;

  return (timestamp64) ((tdays * 86400) + (stm.tm_hour * 3600)
                        + (stm.tm_min * 60) + stm.tm_sec - kw_time_zone);
}

timestamp BasicDateTime::toTimeStamp(void *dt) {
  struct tm stm;
  getTimeStructure(stm, dt);
  return (timestamp)tmToTimeStamp(stm);
}

void BasicDateTime::setPrecision(int precision) {}

int BasicDateTime::precision() const { return 0; }

int BasicDateTime::hour(void *dt) { return 0; }
int BasicDateTime::minute(void *dt) { return 0; }
int BasicDateTime::second(void *dt) { return 0; }
int BasicDateTime::millisecond(void *dt) { return 0; }

void BasicDateTime::setYear(int y, void *dt) {}
void BasicDateTime::setMonth(int m, void *dt) {}
void BasicDateTime::setDay(int d, void *dt) {}
void BasicDateTime::setHour(int y, void *dt) {}
void BasicDateTime::setMinute(int m, void *dt) {}
void BasicDateTime::setSecond(int d, void *dt) {}
void BasicDateTime::setMilliSecond(int ms, void *dt) {}

int BasicDateTime::strformat(void *dt, char *s, size_t max,
  const string &fmt_str) {
  struct tm ts;
  getTimeStructure(ts, dt);
  ts.tm_wday = weekday(dt);
  return strftime(s, max, fmt_str.c_str(), &ts);
}

string BasicDateTime::toString(void *data) {
  string str;
  str.resize(str_max_len_);
  int sz = strformat(data, (char *)str.c_str());
  str.resize(sz);
  
  return str;
}

int32_t BasicDateTime::timeToSecond(void *dt)
{ return (hour(dt) * 3600 + minute(dt) * 60 + second(dt)); }

/////////
// Time
Time::Time(): BasicDateTime(kwdbts::s_defaultTimeFormat()) {
  str_max_len_ = MAX_TIME_STR_LEN;
  setYMD();
}

Time::~Time() {}

void Time::setYMD() {
  time_t t;
  time(&t);
  struct tm tm_now;
  ToGMT(t, tm_now);
  setYMD(tm_now);
}

void Time::setYMD(struct tm &ts) {
  year_ = ts.tm_year + 1900;;
  month_ = ts.tm_mon + 1;;
  day_ = ts.tm_mday;
}

bool Time::isValidTime(void *data) { return true; }

bool Time::isValidTime(const char *str) const { return true; }

void Time::clear(void *dt) { *((int32_t *)dt) = 0; }

void Time::setDateTime(struct tm &ts, void *dt) {
  setYMD(ts);
  *((int32_t *)dt) = (ts.tm_hour * 3600 + ts.tm_min * 60 + ts.tm_sec);
}

bool Time::setDateTime(const char *str,  void *dt) {
  struct tm ts;
  if ((strptime(str, fmt_.c_str(), &ts) == NULL)) {
    if (strptime(str, "%H:%M:%S", &ts) == NULL) {
      *((int32_t *)dt) = 0;
      is_valid_ = false;
    } else {
      time_t t;
      time(&t);
      struct tm tm_now;
      ToGMT(t, tm_now);
      ts.tm_year = tm_now.tm_year;
      ts.tm_mon = tm_now.tm_mon;
      ts.tm_mday = tm_now.tm_mday;
      is_valid_ = true;
    }
  } else {
    setDateTime(ts, dt);
    is_valid_ = true;
  }
  return is_valid_;
}

int Time::precision() const { return 0; }

int Time::year(void *s) { return year_; }

int Time::month(void *s) { return month_; }

int Time::day(void *s) { return day_; }

int Time::secondNumber(void *dt) { return (int)(*(int32_t *)dt); }

int Time::hour(void *dt) { return (*((int32_t *)dt) / 3600); }

int Time::minute(void *dt) {
  int32_t min_sec = *((int32_t *)dt);
  if (min_sec < 0)
    min_sec = - min_sec;
  return ((min_sec % 3600) / 60);
}

int Time::second(void *dt) { return /*abs(*/*((int32_t *)dt) % 60/*)*/; }

int Time::size() const { return sizeof(int32_t); }

void * Time::getData() const { return (void *)&sec_; }

void Time::getData(void *data) const { *((int32_t *)data) = sec_; }

void Time::setData(void *data) { sec_ = *((int32_t *)data); }

int Time::strformat(void *data, char *s, size_t max,
  const string &fmt_str) {
  int32_t tmp = abs(*(int32_t *)data);
  string fmt = "%02d:%02d:%02d";
  if(*(int32_t *)data < 0){
    fmt = "-" + fmt;
  }

  sprintf(s, fmt.c_str(), SplitHour(tmp), SplitMinute(tmp), SplitSecond(tmp));
  return strlen(s);
}

BasicDateTime * Time::clone() const { return (BasicDateTime *)new Time(); }

/////////
// Time64
Time64::Time64(int prec): Time() {
  prec_ = prec;
  multiple_ = precisionToMultiple(prec_);
  /*
  +1 for '.' precision
  -1 for cancel out the space that kept in timeFormatMaxLen
    , otherwise "2023-04-18 01:03:03.000" length is only 23, but str_max_len_ was 24
    , it will have a '\0' at the end of the string and cause 9091 error (see redmine 4233)
  */
  str_max_len_ += (prec_ + 1) - 1;
  dec_fmt_ = ".%0" + intToString(prec_) + "u";
}

Time64::~Time64() {}

int Time64::precision() const { return prec_; }

int Time64::secondNumber(void *dt) {
  return *((int64_t *)dt) / multiple_;
}

int Time64::hour(void *dt) {
  int64_t second = *((int64_t *)dt) / multiple_;
  return (second / 3600);
}

int Time64::minute(void *dt) {
  int64_t second = *((int64_t *)dt) / multiple_;
    return ((second % 3600) / 60);
}

int Time64::second(void *dt) {
  return (*((int64_t *)dt) / multiple_) % 60;
}

int Time64::size() const { return sizeof(int64_t); }

void * Time64::getData() const { return (void *)&ts_data_; }

void Time64::getData(void *data) const { *((int64_t *)data) = ts_data_; }

void Time64::setData(void *data) { ts_data_ = *((int64_t *)data); }

int Time64::strformat(void *data, char *s, size_t max, const string &fmt_str) {
  int64_t tmp = (*(int64_t *)data);
  string fmt = "%02d:%02d:%02d";
  if(tmp < 0){
    fmt = "-" + fmt;
    tmp = abs(tmp);
  }

  if (prec_ > 0 && prec_ <= 9) {
    int32_t old = (int32_t)(tmp / multiple_);
    int32_t msec = (int32_t)(tmp % multiple_);
    fmt = fmt + ".%0" + intToString(prec_) + "u";
    sprintf(s, fmt.c_str(), SplitHour(old), SplitMinute(old), SplitSecond(old), msec);
  } else {
    sprintf(s, fmt.c_str(), SplitHour(tmp), SplitMinute(tmp), SplitSecond(tmp));
  }

  return strlen(s);
}

BasicDateTime * Time64::clone() const { return (BasicDateTime *)new Time64(prec_); }

////////////
// Date32
bool Date32::isValidTime(void *data)
{ return ((day(data) != 0) && (month(data) != 0)); }

bool Date32::isValidTime(const char *str) const
{ return (*str == 0 || strcmp(str, zero_date) == 0); }

void Date32::clear(void *dt) {
  *((int32_t *)dt) = 0;
  is_valid_ = false;
}

bool Date32::setDateTime(const char *str, void *dt) {
  struct tm ts;
  ts.tm_year = 1900;
  ts.tm_mon = 0;
  ts.tm_mday = 1;
  ts.tm_hour = 0;
  ts.tm_min = 0;
  ts.tm_sec = 0;
  ts.tm_isdst = 0;
  char *p = strptime(str, fmt_.c_str(), &ts);
  if ((p == NULL)) {
    *((int32_t *)dt) = 0;
    is_valid_ = false;
  } else {
    toDateTime(ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday, 0,
      0, 0, dt);
  }
  return is_valid_;
}

void Date32::setDateTime(struct tm &ts, void *dt) {
  setYear_(ts.tm_year + 1900, dt);
  setMonth_(ts.tm_mon + 1, dt);
  setDay_(ts.tm_mday, dt);
}

void Date32::getData(void *data) const { getData_(data); }

BasicDateTime * Date32::clone() const { return (BasicDateTime *)new Date32(); }

///////////////
// DateTimeDOS

bool isValidDateTime32(const char *str) {
    return (*str == 0 || strcmp(str, zero_datetime32) == 0 ||
        strcmp(str, zero_date32) == 0);
}

bool DateTimeDOS::isValidTime(void *data) const
{ return (*((int32_t *)data) != 0); }

bool DateTimeDOS::isValidTime(const char *str) const {
    return isValidDateTime32(str);
}

void DateTimeDOS::clear(void *dt) {
  *((uint32_t *)dt) = 0;
  is_valid_ = false;
}

void DateTimeDOS::setDateTime(struct tm &ts, void *dt) {
//    date_.high = date_.low = 0;
  setMonth_(ts.tm_mon + 1, dt);
  setDay_(ts.tm_mday, dt);
  setHour_(ts.tm_hour, dt);
  setMinute_(ts.tm_min, dt);
  setSecond_(ts.tm_sec, dt);
  setYear_(ts.tm_year + 1900, dt);
}

bool DateTimeDOS::setDateTime(const char *str,  void *dt) {
  struct tm ts;
  if (strptime(str, fmt_.c_str(), &ts) == NULL) {
    Date32 d32;
    d32.setDateTime(str, d32.getData());
    if (d32.isValid()) {
      Date32ToDateTimeDOS(d32, *this, dt);
      is_valid_ = true;
    } else {
      *((int32_t *)dt) = 0;
      is_valid_ = false;
    }
  } else {
    setYear_(ts.tm_year + 1900, dt);
    setMonth_(ts.tm_mon + 1, dt);
    setDay_(ts.tm_mday, dt);
    setHour_(ts.tm_hour, dt);
    setMinute_(ts.tm_min, dt);
    setSecond_(ts.tm_sec, dt);
    is_valid_ = true;
  }
  return is_valid_;
}

void DateTimeDOS::getData(void *data) const { getData_(data); }

BasicDateTime * DateTimeDOS::clone() const
{ return (BasicDateTime *)new DateTimeDOS(); }

//////////////
// DateTime32
DateTime32::DateTime32(): BasicDateTime(kwdbts::EngineOptions::dateTimeFormat())
{ max_year_ = 63; }

DateTime32::DateTime32(void *data):
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 63;
  setData(data);
}

DateTime32::DateTime32(const string &time_str):
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 63;
  setDateTime(time_str.c_str(), &date_);
}

DateTime32::DateTime32(struct tm &ts):
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 63;
  setDateTime(ts, &date_);
}

DateTime32::~DateTime32() {}

bool DateTime32::isValidTime(void *data) { return (*((int32_t *)data) != 0); }

bool DateTime32::isValidTime(const char *str) const
{ return isValidDateTime32(str); }

void DateTime32::clear(void *dt) {
  *((uint32_t *)dt) = 0;
  is_valid_ = false;
}

void DateTime32::setDateTime(struct tm &ts, void *dt) {
  toDateTime(ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday, ts.tm_hour,
    ts.tm_min, ts.tm_sec, dt);
}

bool DateTime32::setDateTime(const char *str,  void *dt) {
  struct tm ts;
  initTs(&ts);
  char *p = strptime(str, fmt_.c_str(), &ts);
  if (p == NULL) {
    p = strptime(str, kwdbts::EngineOptions::dateFormat().c_str(), &ts);
    if ((p != nullptr) && (*p == '\0')) {
      toDateTime(ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday, ts.tm_hour,
		    ts.tm_min, ts.tm_sec, dt);
    } else {
      *((int32_t *)dt) = 0;
      is_valid_ = false;
    }
  } else {
    if ((p != nullptr) && (*p == '\0')) {
      toDateTime(ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday, ts.tm_hour,
        ts.tm_min, ts.tm_sec, dt);
      } else {
        is_valid_ = false;
      }
    }
  return is_valid_;
}

void DateTime32::getData(void *data) const { getData_(data); }

BasicDateTime * DateTime32::clone() const
{ return (BasicDateTime *)new DateTime32(); }


DateTime64::DateTime64(): BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 65535;
  setWeekDay(0);
}

DateTime64::DateTime64(void *data):
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 65535;
  setData(data);
}

DateTime64::DateTime64(const string &time_str):
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 65535;
  setDateTime(time_str.c_str(), &date_);
}

DateTime64::DateTime64(struct tm &ts):
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {
  max_year_ = 65535;
  setDateTime(ts, &date_);
}

DateTime64::~DateTime64() {}

bool DateTime64::isValidTime(void *data) { return *((int64_t *)data) != 0; }

bool DateTime64::isValidTime(const char *str) const {
  return (*str == 0 || strcmp(str, zero_date) == 0 ||
    strcmp(str, zero_datetime64) == 0);
}

void DateTime64::clear(void *dt) {
  *((uint64_t *)dt) = 0;
  is_valid_ = false;
}

void DateTime64::setDateTime(struct tm &ts, void *dt) {
  *((uint64_t *)dt) = 0;
  setMonth_(ts.tm_mon + 1, dt);
  setDay_(ts.tm_mday, dt);
  setHour_(ts.tm_hour, dt);
  setMinute_(ts.tm_min, dt);
  setSecond_(ts.tm_sec, dt);
  setYear_(ts.tm_year + 1900, dt);  // will clear data if out of ragne.
}

bool DateTime64::setDateTime(const char *str, void *dt) {
  struct tm ts;
  initTs(&ts);
  char *p = strptime(str, fmt_.c_str(), &ts);
  if (p == NULL) {
    p = strptime(str, kwdbts::EngineOptions::dateFormat().c_str(), &ts);
    if ((p != nullptr) && (*p == '\0')) {
      toDateTime(ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday, ts.tm_hour,
		    ts.tm_min, ts.tm_sec, dt);
    } else {
      *((int64_t *)dt) = 0;
      is_valid_ = false;
    }
  } else {
    if ((p != nullptr) && (*p == '\0')) {
      //    *((uint64_t *)dt) = 0;  // TODO: which is faster to zero weekday?
      toDateTime(ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday, ts.tm_hour,
        ts.tm_min, ts.tm_sec, dt);
      } else {
      is_valid_ = false;
      }
    }
  return is_valid_;
}

void DateTime64::getData(void *data) const { getData_(data); }

BasicDateTime * DateTime64::clone() const {
    return (BasicDateTime *)new DateTime64();
}

// TimeStamp
TimeStampDateTime::TimeStampDateTime():
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {}

TimeStampDateTime::~TimeStampDateTime() {};

bool TimeStampDateTime::isValidTime(const char *str) const
{ return true; }

void TimeStampDateTime::clear(void *dt) {
  *((timestamp *)dt) = 0;
  is_valid_ = false;
}

void TimeStampDateTime::setDateTime(struct tm &ts, void *dt)
{ *((timestamp *)dt) = tmToTimeStamp(ts); }

bool TimeStampDateTime::setDateTime(const char *str, void *dt) {
  struct tm ts;
  timestamp t;
  char *p = strptime(str, fmt_.c_str(), &ts);
  if (p == NULL) {
    t = strtoul((const char*)str, &p, 10);
    if (*p == '\0' || (*p == '.' && *(p+1) == '\0')) {
      if (t >= numeric_limits<uint32_t>::max())
        is_valid_ = false;
      else
        is_valid_ = true;
    } else { // NULL or invalid value
      t = 0;
      is_valid_ = false;
    }
  } else {
    if (*p == '\0') {
      t = tmToTimeStamp(ts);
      is_valid_ = true;
    } else {
      t = 0;
      is_valid_ = false;
    }
  }
  *((timestamp *)dt) = t;
  return is_valid_;
}

timestamp TimeStampDateTime::toTimeStamp(void *d)
{ return *(timestamp *)d; }

timestamp64 TimeStampDateTime::toTimeStamp64(void *d, int prec)
{ return (timestamp64)(*(timestamp *)d) * precisionToMultiple(prec); }

int TimeStampDateTime::dayNumber(void *dt){
    time_t tt = (time_t)(*(timestamp *)dt);
    ToGMT(tt, stm_);
    return days(stm_.tm_year + 1900, stm_.tm_mon + 1, stm_.tm_mday);
}

int TimeStampDateTime::secondNumber(void *dt){
    time_t tt = (time_t)(*(timestamp *)dt);
    ToGMT(tt, stm_);
    return seconds(stm_.tm_hour, stm_.tm_min, stm_.tm_sec);
}

int TimeStampDateTime::year(void *d) {
  time_t tt = (time_t)(*(timestamp *)d);
  ToGMT(tt, stm_);
  return stm_.tm_year + 1900;
}

int TimeStampDateTime::month(void *d) {
  time_t tt = (time_t)(*(timestamp *)d);
  ToGMT(tt, stm_);
  return stm_.tm_mon + 1;
}

int TimeStampDateTime::day(void *d) {
  time_t tt = (time_t)(*(timestamp *)d);
  ToGMT(tt, stm_);
  return stm_.tm_mday;
}

int TimeStampDateTime::hour(void *d) {
  time_t tt = (time_t)(*(timestamp *)d);
  ToGMT(tt, stm_);
  return stm_.tm_hour;
}

int TimeStampDateTime::minute(void *d) {
  time_t tt = (time_t)(*(timestamp *)d);
  ToGMT(tt, stm_);
  return stm_.tm_min;
}

int TimeStampDateTime::second(void *d) {
  time_t tt = (time_t)(*(timestamp *)d);
  ToGMT(tt, stm_);
  return stm_.tm_sec;
}

int TimeStampDateTime::size() const { return sizeof (ts_); }

void * TimeStampDateTime::getData() const { return (void *) &ts_; }

void TimeStampDateTime::getData(void *data) const
{ *((timestamp *)data) = ts_; }

void TimeStampDateTime::setData(void *data) { ts_ = *((timestamp *)data); }

int TimeStampDateTime::strformat(void *dt, char *s, size_t max,
  const string &fmt_str) {
  struct tm ts;
  // adjust to local time zone
  time_t tt = (time_t)(*(timestamp *)dt);
  ToGMT(tt, ts);
  return strftime(s, max, fmt_str.c_str(), &ts);
}

BasicDateTime * TimeStampDateTime::clone() const
{ return (BasicDateTime *)new TimeStampDateTime(*this); }

/*
 * Implement addMonth for timestamp.
 *
 * Used by adddate/addtime/subdate/subtime query if interval type is month/year
 */
void TimeStampDateTime::addMonth(int64_t n, void *dt) {
  time_t tt = (time_t)(*(timestamp *)dt);
  ToGMT(tt, stm_);
  int tm_total_months = stm_.tm_mon + n;
  int tm_years_diff = tm_total_months / 12;
  int tm_months_diff = tm_total_months % 12;
  if (tm_months_diff < 0) {
	tm_months_diff += 12;
	tm_years_diff--;
  }
  stm_.tm_year += tm_years_diff;
  stm_.tm_mon = tm_months_diff;
  int32_t nd = getDayOfMonth(stm_.tm_year, stm_.tm_mon);
  if (stm_.tm_mday > nd) {
    stm_.tm_mday = nd;
  }
  tt = tmToTimeStamp(stm_);
  *((timestamp*) dt) = tt;
}

// maximum 9 elements
const uint32_t precToMultiple[10] = {1, 10, 100, 1000, 10000, 100000,
  1000000, 10000000, 100000000, 1000000000};

// TIMESTAMP64
TimeStamp64DateTime::TimeStamp64DateTime():
  BasicDateTime(kwdbts::EngineOptions::dateTimeFormat()) {}

TimeStamp64DateTime::TimeStamp64DateTime(const TimeStamp64DateTime &rhs):
  BasicDateTime(rhs) {
  prec_ = rhs.prec_;
  setPrecision(prec_);
}

TimeStamp64DateTime::~TimeStamp64DateTime() {}

bool TimeStamp64DateTime::isValidTime(const char *str) const { return true; }

void TimeStamp64DateTime::clear(void *dt) {
  *((timestamp64 *)dt) = 0;
  is_valid_ = false;
}

void TimeStamp64DateTime::setPrecision(int precision) {
  prec_ = precision;
  multiple_ = precisionToMultiple(prec_);
  /* 
  +1 for '.' precision
  -1 for cancel out the space that kept in timeFormatMaxLen
    , otherwise "2023-04-18 01:03:03.000" length is only 23, but str_max_len_ was 24
    , it will have a '\0' at the end of the string and cause 9091 error (see redmine 4233)
  */
  str_max_len_ += (prec_ + 1) - 1;    
  dec_fmt_ = ".%0" + intToString(prec_) + "u";
}

int TimeStamp64DateTime::precision() const { return prec_; }

void TimeStamp64DateTime::setDateTime(struct tm &ts, void *dt)
{ *((timestamp64 *)dt) = tmToTimeStamp(ts); }

timestamp64 TimeStamp64DateTime::getSubSecond(char *p) {
  char *e;
  p++;
  timestamp64 ms = strtoul((const char*)p, &e, 10);
  if (*e != '\0') {   // invalid subsecond
    is_valid_ = false;
  } else {
    if (ms != 0) {
      int n = e - p;
      if (n < prec_) {
        ms *= precToMultiple[prec_ - n];
      }
      if (n > prec_) {
        ms /= precToMultiple[n - prec_];
      }
    }
    is_valid_ = true;
  }
  return ms;
}

bool TimeStamp64DateTime::setDateTime(const char *str, void *dt) {
  struct tm ts;
  timestamp64 t;
  timestamp64 ms;
  initTs(&ts);
  char *p = strptime(str, fmt_.c_str(), &ts);
  if (p == NULL) {
    t = strtoul((const char*)str, &p, 10);
    if (*p == '\0') {
      *((timestamp64 *)dt) = t;
      is_valid_ = true;
      return is_valid_;
    }
//    if (*p == '.') {
//      ms = getSubSecond(p);
//    } else if (*p != '\0') { // NULL or invalid value    if (*p == '.') {
    else { // NULL or invalid value
      p = strptime(str, kwdbts::EngineOptions::dateFormat().c_str(), &ts);
      if (p == NULL || *p != '\0') {
        is_valid_ = false;
      } else {
        ms = 0;
        ts.tm_hour = ts.tm_min = ts.tm_sec = 0;
        is_valid_ = true;
        t = tmToTimeStamp(ts);
      }
    }
  } else {
    if (*p == '.') {
      ms = getSubSecond(p);
    } else if (*p == '\0') {
      is_valid_ = true;
      ms = 0;
    } else {  // invalid value
      is_valid_ = false;
    }
    t = tmToTimeStamp(ts);
  }
  if (is_valid_)
    t = t * multiple_ + ms;
  else
    t = 0;
  *((timestamp64 *)dt) = t;
  return is_valid_;
}

timestamp TimeStamp64DateTime::toTimeStamp(void *d)
{ return (timestamp)toTimestampSecond((*(timestamp64 *)d), multiple_); }

timestamp64 TimeStamp64DateTime::toTimeStamp64(void *d, int prec) {
  if (prec_ == prec) {
    return *((timestamp64 *)d);
  }
  if (prec > prec_) {
    return *((timestamp64 *)d) * precisionToMultiple(prec - prec_);
  }
  return *((timestamp64 *)d) / precisionToMultiple(prec_ - prec);
}

int TimeStamp64DateTime::dayNumber(void *dt){
    time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)dt, multiple_);
    ToGMT(tt, stm_);
    return days(stm_.tm_year + 1900, stm_.tm_mon + 1, stm_.tm_mday);
}

int TimeStamp64DateTime::secondNumber(void *dt){
    time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)dt, multiple_);
    ToGMT(tt, stm_);
    return seconds(stm_.tm_hour, stm_.tm_min, stm_.tm_sec)*multiple_
           +(*(timestamp64 *)dt)%multiple_;
}

int64_t TimeStamp64DateTime::milliSecondNumber(void *dt) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)dt, multiple_);
  ToGMT(tt, stm_);
  int64_t secs = seconds(stm_.tm_hour, stm_.tm_min, stm_.tm_sec);
  return (int64_t)secs*multiple_ + (*(int64_t *)dt)%multiple_;
}

int TimeStamp64DateTime::year(void *d) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)d, multiple_);
  ToGMT(tt, stm_);
  return stm_.tm_year + 1900;
}

int TimeStamp64DateTime::month(void *d) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)d, multiple_);
  ToGMT(tt, stm_);
  return stm_.tm_mon + 1;
}

int TimeStamp64DateTime::day(void *d) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)d, multiple_);
  ToGMT(tt, stm_);
  return stm_.tm_mday;
}

int TimeStamp64DateTime::hour(void *d) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)d, multiple_);
  ToGMT(tt, stm_);
  return stm_.tm_hour;
}

int TimeStamp64DateTime::minute(void *d) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)d, multiple_);
  ToGMT(tt, stm_);
  return stm_.tm_min;
}

int TimeStamp64DateTime::second(void *d) {
  time_t tt = (time_t)toTimestampSecond(*(timestamp64 *)d, multiple_);
  ToGMT(tt, stm_);
  return stm_.tm_sec;
}

// day of month [0-11]


void TimeStamp64DateTime::addMonth(int64_t n, void *dt) {
  time_t tt = (time_t) toTimestampSecond(*(timestamp64*) dt, multiple_);
  ToGMT(tt, stm_);
  int tm_total_months = stm_.tm_mon + n;
  int tm_years_diff = tm_total_months / 12;
  int tm_months_diff = tm_total_months % 12;
  if (tm_months_diff < 0) {
	tm_months_diff += 12;
	tm_years_diff--;
  }
  // Update stm_.tm_year and stm_.tm_mon to new ones
  stm_.tm_year += tm_years_diff;
  stm_.tm_mon = tm_months_diff;
  
  int32_t nd = getDayOfMonth(stm_.tm_year, stm_.tm_mon);
  if (stm_.tm_mday > nd) {
    stm_.tm_mday = nd;
  }
  tt = tmToTimeStamp(stm_);
  *((timestamp64*) dt) = tt * multiple_
    + toTimestampDecimal(*(timestamp64*) dt, multiple_);
}

int TimeStamp64DateTime::size() const { return sizeof (ts_data_); }

void * TimeStamp64DateTime::getData() const { return (void *)&ts_data_; }

void TimeStamp64DateTime::getData(void *data) const
{ *((timestamp64 *)data) = ts_data_; }

void TimeStamp64DateTime::setData(void *data)
{ ts_data_ = *((timestamp64 *)data); }

int TimeStamp64DateTime::strformat(void *dt, char *s, size_t max,
  const string &fmt_str) {
  //TODO: will compiler optimize the following to single instruction?
  time_t tt = *(timestamp64 *)dt / multiple_;
  
  // adjust to local time zone
  ToGMT(tt, stm_);
  int len = strftime(s, max, fmt_str.c_str(), &stm_);

  // TODO: Refactor code to add virtual function needDecimal() in class
  // Add this if statement to fix date_format ending with dot issue

  // As for TimeStamp64, when calling select * from table, and when *s contains ms/us/ns, need to format date to the one ending with ms/us/ns.
  // This is a hack, where sub-seconds are added at the end if length of datetime_format > 20 (e.g."%Y-%m-%d %H:%M:%S") in order to avoid string comparison.

  // Note: cannot only check whether prec_ > 0 since when prec_ > 0, max will not always > 20.
  // TODO: Need to change if statement condition after date_format supports .%f

  if (max > 20) {
    uint64_t subsecond = *(timestamp64 *)dt % multiple_;
    sprintf((s + len), dec_fmt_.c_str(), (uint32_t)subsecond);
    return str_max_len_;
  }
  // Return len instead of str_max_len_ to avoid adding extra chars if length of datetime_format <= 20 (e.g "%Y-%m-%d %H:%M")
  return len;
}

BasicDateTime * TimeStamp64DateTime::clone() const
{ return (BasicDateTime *)new TimeStamp64DateTime(*this); }


BasicDateTime * getDateTime(int type, int precision) {
  BasicDateTime *bdt;
  switch(type){
    case DATE32: bdt = (BasicDateTime *)new Date32(); break;
    case DATETIME32: bdt = (BasicDateTime *)new DateTime32(); break;
    case DATETIME64: bdt = (BasicDateTime *)new DateTime64(); break;
    case DATETIMEDOS: bdt = (BasicDateTime *)new DateTimeDOS(); break;
    case TIMESTAMP:
    case INT32:
      bdt = (BasicDateTime *)new TimeStampDateTime(); break;
    case TIMESTAMP64:
    case INT64:
      bdt = (BasicDateTime *)new TimeStamp64DateTime(); break;
    case TIME: bdt = (BasicDateTime *)new Time(); break;
    case TIME64: bdt = (BasicDateTime *)new Time64(precision); break;
  }
  if (type == TIMESTAMP64)
    bdt->setPrecision(precision);
  return bdt;
}

int32_t toYearWeek(const char *str) {
    const char *w_pos = strpbrk(str, "wW");
    int32_t year_week;
    if (w_pos != NULL) {
        string str_year = string(str, w_pos - str);
        string str_week = string(w_pos + 1);
        year_week = stringToInt(str_year);
        year_week = year_week << 16;
        year_week = year_week | stringToInt(str_week);
    } else {
        year_week = 0;
    }
    return year_week;
}

int getSubSecond(char *p, timestamp64 &ss, int &prec) {
  char *e;
  p++;
  ss = strtoul((const char*)p, &e, 10);
  if (*e != '\0') {   // invalid subsecond
    return -1;
  } else {
    prec = e - p;
  }
  return 0;
}

int strToDateTime(const char *s, DateTimeData &dtd) {
  timestamp64 t;
  timestamp64 ss;
  char *p = strptime(s, kwdbts::EngineOptions::dateTimeFormat().c_str(), &dtd.stm);
  if (p == NULL) {
    dtd.ts = strtoul((const char*)s, &p, 10);
    if (*p ==  '\0') {
      dtd.prec = 0;
      dtd.type = TIMESTAMP64;
    } else {
      p = strptime(s, kwdbts::EngineOptions::dateFormat().c_str(), &dtd.stm);
      if (p == NULL || *p != '\0') {
        return -1;
      } else {
        dtd.stm.tm_hour = dtd.stm.tm_min = dtd.stm.tm_sec = 0;
        dtd.type = DATE32;
      }
    }
  } else {
    if (*p == '.') {
      if (getSubSecond(p, ss, dtd.prec) < 0)
        return -1;
      dtd.ts = tmToTimeStamp(dtd.stm);
      if (1 == dtd.prec) {
        ss *= 100;
      } else if (2 == dtd.prec) {
        ss *= 10;
      }
      dtd.prec = 3;
      dtd.ts = dtd.ts * precisionToMultiple(dtd.prec) + ss;
      dtd.type = TIMESTAMP64;
    } else if (*p == '\0') {
      dtd.ts = tmToTimeStamp(dtd.stm);
      dtd.prec = 3;
      dtd.ts = dtd.ts * precisionToMultiple(dtd.prec);
      dtd.type = TIMESTAMP64;
    } else
      return -1;
  }
  return 0;
}

string nowString() {
  time_t rawtime;
  struct tm lt;
  char buffer[64];

  time(&rawtime);
  ToGMT(rawtime, lt);
  strftime(buffer, sizeof(buffer)-1, "[%Y-%m-%d %H:%M:%S]", &lt);
  return string(buffer);
}
