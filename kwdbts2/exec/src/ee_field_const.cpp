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

#include "ee_field_const.h"

#include <iomanip>
#include <iostream>
#include <sstream>

namespace kwdbts {

char *FieldConstInt::get_ptr(RowBatch *batch) {
  return reinterpret_cast<char *>(&value_);
}

k_int64 FieldConstInt::ValInt() { return value_; }

k_int64 FieldConstInt::ValInt(k_char *ptr) {
  k_int64 val = 0;
  memcpy(&val, ptr, storage_len_);

  return val;
}

k_double64 FieldConstInt::ValReal() { return static_cast<k_double64>(value_); }

k_double64 FieldConstInt::ValReal(k_char *ptr) { return ValInt(ptr); }

String FieldConstInt::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", value_);
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldConstInt::ValStr(k_char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldConstInt::field_to_copy() {
  FieldConstInt *field = new FieldConstInt(*this);
  if (nullptr == field) {
    return nullptr;
  }

  return field;
}

k_bool FieldConstInt::fill_template_field(char *ptr) {
  memcpy(ptr, &value_, storage_len_);

  return 0;
}

// getYMDFormInterval parse the year, month, and day from the timestamp of interval type(10y, 12 years 10 days)
k_bool getYMDFormInterval(KString *value_, k_int32 *startPos, k_int32 *year, k_int32 *month, k_int32 *day) {
  k_char *endptr;
  k_bool isIntervalStyle = KFALSE;
  auto pos = value_->find_first_of("ymd");
  while (pos != std::string::npos) {
    if ((*value_)[pos] == 'y') {
      // xx years
      isIntervalStyle = KTRUE;
      *year =
          strtol(value_->substr(*startPos, pos - *startPos).c_str(), &endptr, 10);
    } else if ((*value_)[pos] == 'm') {
      // xx months
      isIntervalStyle = KTRUE;
      *month =
          strtol(value_->substr(*startPos, pos - *startPos).c_str(), &endptr, 10);
    } else if ((*value_)[pos] == 'd') {
      // xx days
      isIntervalStyle = KTRUE;
      *day =
          strtol(value_->substr(*startPos, pos - *startPos).c_str(), &endptr, 10);
    }
    *startPos = value_->find_first_of(" ", pos + 1) + 1;
    pos = value_->find_first_of("ymd", pos + 3);
  }
  return isIntervalStyle;
}

// getYMDFormTimestamp parse the year, month, and day from the timestamp of string (1999-01-01)
KStatus getYMDFormTimestamp(KString *value_, k_int32 *startPos, k_int32 *year, k_int32 *month, k_int32 *day) {
  k_char *endptr;
  // parse year, month, day
  auto pos = value_->find('-', *startPos);
  if (pos != std::string::npos) {
    *year =
        strtol(value_->substr(*startPos, pos - *startPos).c_str(), &endptr, 10) - 1900;
    *month = strtol(value_->substr(pos + 1, 2).c_str(), &endptr, 10)-1;
    pos += 3;
    *day = strtol(value_->substr(pos + 1, 2).c_str(), &endptr, 10);
    pos += 3;
    *startPos = pos;
  } else {
    *year = 70;
    *month = 0;
    *day = 1;
  }
  return KStatus::SUCCESS;
}

// getHMSFormTimestamp parse the hour, minutes, second, millisecond from the timestamp of string (12:12:12.111)
KStatus getHMSFormTimestamp(
    KString *value_, k_int32 *startPos, k_int32 *hour, k_int32 *minutes, k_int32 *second, k_int32 *millisecond) {
  // parse hour, minute, second
  k_char *endptr;
  auto pos = value_->find(':', *startPos);
  if (pos != std::string::npos) {
    *hour = strtol(value_->substr(*startPos, pos - *startPos).c_str(), &endptr, 10);
    *minutes = strtol(value_->substr(pos + 1, 2).c_str(), &endptr, 10);
    pos += 3;
    *second = strtol(value_->substr(pos + 1, 2).c_str(), &endptr, 10);
    pos += 3;
    // parser millisecond
    k_int8 size = 100;
    while ((*value_)[pos] != '\0') {
      auto ms = strtol(value_->substr(pos + 1, 1).c_str(), &endptr, 10);
      *millisecond += ms * size;
      size /= 10;
      pos++;
    }
  }
  return KStatus::SUCCESS;
}

/* getInterval add a timestamp (int64) to a interval(string)
 * interval format:
 * 15:52:01.001
 * 11:00:00
 * 1 days
 * 1 year 1 mon
 */
k_int64 getInterval(KString *value_, k_int64 *orgVal, k_bool negative) {
  struct tm ltm;
  k_int64 orgMS = *orgVal % 1000;
  k_int64 orgt = (*orgVal) / 1000;
  gmtime_r(&orgt, &ltm);
  k_int32 YorH, MorM, DorS, startPos;
  YorH = MorM = DorS = startPos = 0;
  k_bool dayStyle = getYMDFormInterval(value_, &startPos, &YorH, &MorM, &DorS);
  if (dayStyle) {
    if (!negative) {
      ltm.tm_year += YorH;
      ltm.tm_mon += MorM;
      ltm.tm_mday += DorS;
    } else {
      ltm.tm_year -= YorH;
      ltm.tm_mon -= MorM;
      ltm.tm_mday -= DorS;
    }
  }
  if (value_->at(startPos) == '\0') {
    return timegm(&ltm) * 1000 + orgMS;
  }
  if (value_->at(startPos) == '-') {
    negative = KTRUE;
    startPos++;
  }
  k_int32 intervalMS = YorH = MorM = DorS = 0;
  getHMSFormTimestamp(value_, &startPos, &YorH, &MorM, &DorS, &intervalMS);
  if (!negative) {
    ltm.tm_hour += YorH;
    ltm.tm_min += MorM;
    ltm.tm_sec += DorS;
    return timegm(&ltm) * 1000 + orgMS + intervalMS;
  }
  ltm.tm_hour -= YorH;
  ltm.tm_min -= MorM;
  ltm.tm_sec -= DorS;
  return timegm(&ltm) * 1000 + orgMS - intervalMS;
}

k_int64 getTimeFormTimestamp(KString *value_) {
  // get timestamp(int64) from timestamp of string type
  time_t now = time(nullptr);
  tm ltm{0};
  localtime_r(&now, &ltm);
  ltm.tm_year = ltm.tm_mon = ltm.tm_mday = ltm.tm_hour = ltm.tm_min = ltm.tm_sec = 0;
  k_int32 intervalMS, startPos;
  intervalMS = startPos = 0;
  k_bool negative = KFALSE;
  if (value_->at(startPos) == '-') {
    negative = KTRUE;
    startPos++;
  } else if (value_->length() >= 2 && value_->substr(value_->length() - 2) == "BC") {
    negative = KTRUE;
  }
  getYMDFormTimestamp(value_, &startPos, &ltm.tm_year, &ltm.tm_mon, &ltm.tm_mday);
  getHMSFormTimestamp(value_, &startPos, &ltm.tm_hour, &ltm.tm_min, &ltm.tm_sec, &intervalMS);
  if (negative) {
    ltm.tm_year = 0 - ltm.tm_year - 2 * 1900;
    auto a = mktime(&ltm) * 1000 + intervalMS;
    return  a + ltm.tm_gmtoff*1000;
  }
  return mktime(&ltm) * 1000 + intervalMS + ltm.tm_gmtoff*1000;
}

char *FieldConstInterval::get_ptr(RowBatch *batch) {
  return value_.data();
}

k_int64 FieldConstInterval::ValInt() {
  // get timestamp(int64) from timestamp of interval type  or string type
  k_int64 orgVal = 0;
  return getInterval(&value_, &orgVal, false);
}

k_int64 FieldConstInterval::ValInt(k_int64 *val, k_bool negative) {
  return getInterval(&value_, val, negative);
}

Field *FieldConstInterval::field_to_copy() {
  FieldConstInterval *field = new FieldConstInterval(*this);
  if (nullptr == field) {
    return nullptr;
  }

  return field;
}

char *FieldConstDouble::get_ptr(RowBatch *batch) {
  return reinterpret_cast<char *>(&value_);
}

k_int64 FieldConstDouble::ValInt() { return value_; }

k_int64 FieldConstDouble::ValInt(char *ptr) { return ValReal(ptr); }

k_double64 FieldConstDouble::ValReal() {
  return static_cast<k_double64>(value_);
}

k_double64 FieldConstDouble::ValReal(char *ptr) {
  k_double64 val = 0.0f;

  memcpy(&val, ptr, storage_len_);

  return val;
}

String FieldConstDouble::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", value_);
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldConstDouble::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldConstDouble::field_to_copy() {
  FieldConstDouble *field = new FieldConstDouble(*this);

  if (nullptr == field) {
    return nullptr;
  }

  return field;
}

k_bool FieldConstDouble::fill_template_field(char *ptr) {
  memcpy(ptr, &value_, storage_len_);

  return 0;
}

char *FieldConstString::get_ptr(RowBatch *batch) {
  return value_.data();
}

// %Y-%m-%d %H:%M:%S.ms
k_int64 FieldConstString::ValInt() {
  return getTimeFormTimestamp(&value_);
}

k_int64 FieldConstString::ValInt(char *ptr) {
  k_int64 val = 0;
  memcpy(&val, ptr, sizeof(k_int64));

  return val;
}

k_double64 FieldConstString::ValReal() { return ValInt(); }

k_double64 FieldConstString::ValReal(char *ptr) { return ValInt(ptr); }

String FieldConstString::ValStr() {
  return { const_cast<char *>(value_.data()), value_.size() };
  // return value_;
}

String FieldConstString::ValStr(char *ptr) {
  k_uint16 len = 0;
  memcpy(&len, ptr, sizeof(k_uint16));
  return {ptr + sizeof(k_uint16), len};
}

Field *FieldConstString::field_to_copy() {
  FieldConstString *field = new FieldConstString(*this);

  if (nullptr == field) {
    return nullptr;
  }

  return field;
}

k_bool FieldConstString::fill_template_field(char *ptr) {
  memcpy(ptr, value_.c_str(), storage_len_);

  return 0;
}

Field *FieldConstNull::field_to_copy() {
  FieldConstNull *field = new FieldConstNull(*this);

  return field;
}

}  // namespace kwdbts
