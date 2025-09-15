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

#include "duckdb/engine/ap_parse_query.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/engine/duckdb_exec.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace kwdb;
using kwdbts::KStatus;

namespace kwdbap {
//std::string parseUnicode2Utf8(const std::string &str) {
//  std::string utf8str;
//  for (size_t i = 0; i < str.size(); i++) {
//    if (str[i] == '\\') {
//      if (i + 10 <= str.size() && str[i + 1] == 'U') {
//        // Parse unicode escape sequences
//        int codepoint = std::stoi(str.substr(i + 2, 8), nullptr, 16);
//        i += 9;
//        if (codepoint <= 0x7F) {
//          utf8str += static_cast<char>(codepoint);
//        } else if (codepoint <= 0x7FF) {
//          utf8str += static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F));
//          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
//        } else if (codepoint <= 0xFFFF) {
//          utf8str += static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F));
//          utf8str += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
//          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
//        } else if (codepoint <= 0x10FFFF) {
//          utf8str += static_cast<char>(0xF0 | ((codepoint >> 18) & 0x07));
//          utf8str += static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F));
//          utf8str += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
//          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
//        } else {
//          // error
//          return str;
//        }
//      } else if (i + 4 < str.size() && str[i + 1] == 'u') {
//        // Parse unicode escape symbols
//        int codepoint = std::stoi(str.substr(i + 2, 4), nullptr, 16);
//        i += 5;
//        if (codepoint <= 0x7F) {
//          utf8str += static_cast<char>(codepoint);
//        } else if (codepoint <= 0x7FF) {
//          utf8str += static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F));
//          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
//        } else if (codepoint <= 0xFFFF) {
//          utf8str += static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F));
//          utf8str += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
//          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
//        } else {
//          // error
//          return str;
//        }
//      } else if (i + 1 < str.size()) {
//        if (str[i + 1] != '\'') {
//          utf8str += str[i];
//        }
//        i++;
//        if (str[i] != '\\') {
//          utf8str += str[i];
//        }
//      } else {
//        utf8str += str[i];
//      }
//    } else {
//      utf8str += str[i];
//    }
//  }
//  utf8str.erase(std::remove(utf8str.begin(), utf8str.end(), '\0'),
//                utf8str.end());
//  return utf8str;
//}
//
//std::string parseHex2String(const std::string &hexStr) {
//  std::string asciiStr;
//  if ((hexStr.substr(0, 2) != "\\x")) {
//    return hexStr;
//  }
//  for (size_t i = 2; i < hexStr.length(); i += 2) {
//    // extract two characters (hexadecimal numbers)
//    std::string hexByte = hexStr.substr(i, 2);
//    // convert hexadecimal numbers to integers
//    int value = std::stoi(hexByte, nullptr, 16);
//    // Convert integers to characters and add them to the result string
//    asciiStr += static_cast<char>(value);
//  }
//  return asciiStr;
//}
//
//char *forward2TimeStringEnd(char *str) {
//  int32_t i = 0;
//  int32_t numOfSep = 0;
//
//  while (str[i] != 0 && numOfSep < 2) {
//    if (str[i++] == ':') {
//      numOfSep++;
//    }
//  }
//
//  while (str[i] >= '0' && str[i] <= '9') {
//    i++;
//  }
//
//  return &str[i];
//}
//
//int64_t strnatoi(char *num, int32_t len) {
//  int64_t ret = 0, i, dig, base = 1;
//
//  if (len > (int32_t)strlen(num)) {
//    len = (int32_t)strlen(num);
//  }
//
//  if ((len > 2) && (num[0] == '0') && ((num[1] == 'x') || (num[1] == 'X'))) {
//    for (i = len - 1; i >= 2; --i, base *= 16) {
//      if (num[i] >= '0' && num[i] <= '9') {
//        dig = (num[i] - '0');
//      } else if (num[i] >= 'a' && num[i] <= 'f') {
//        dig = num[i] - 'a' + 10;
//      } else if (num[i] >= 'A' && num[i] <= 'F') {
//        dig = num[i] - 'A' + 10;
//      } else {
//        return 0;
//      }
//      ret += dig * base;
//    }
//  } else {
//    for (i = len - 1; i >= 0; --i, base *= 10) {
//      if (num[i] >= '0' && num[i] <= '9') {
//        dig = (num[i] - '0');
//      } else {
//        return 0;
//      }
//      ret += dig * base;
//    }
//  }
//
//  return ret;
//}
//
//bool checkTzPresent(KString &str) {
//  int32_t len = str.length();
//  char *seg = forward2TimeStringEnd(str.data());
//  int32_t seg_len = len - (int32_t)(seg - str.c_str());
//
//  char *c = &seg[seg_len - 1];
//  for (int32_t i = 0; i < seg_len; ++i) {
//    if (*c == 'Z' || *c == 'z' || *c == '+' || *c == '-') {
//      return true;
//    }
//    c--;
//  }
//  return false;
//}
//
//char *kwdbStrpTime(const char *buf, const char *fmt, struct tm *tm) {
//  return strptime(buf, fmt, tm);
//}
//
//KStatus parseTimezone(char *str, int64_t *tzOffset) {
//  int64_t hour = 0;
//
//  int32_t i = 0;
//  if (str[i] != '+' && str[i] != '-') {
//    return FAIL;
//  }
//
//  i++;
//
//  int32_t j = i;
//  while (str[j]) {
//    if ((str[j] >= '0' && str[j] <= '9') || str[j] == ':') {
//      ++j;
//      continue;
//    }
//
//    return FAIL;
//  }
//
//  char *sep = strchr(&str[i], ':');
//  if (sep != NULL) {
//    int32_t len = (int32_t)(sep - &str[i]);
//
//    hour = strnatoi(&str[i], len);
//    i += len + 1;
//  } else {
//    hour = strnatoi(&str[i], 2);
//    i += 2;
//  }
//
//  if (hour > 12 || hour < 0) {
//    return FAIL;
//  }
//
//  // return error if there're illegal charaters after min(2 Digits)
//  char *minStr = &str[i];
//  if (minStr[1] != '\0' && minStr[2] != '\0') {
//    return FAIL;
//  }
//
//  int64_t minute = strnatoi(&str[i], 2);
//  if (minute > 59 || (hour == 12 && minute > 0)) {
//    return FAIL;
//  }
//
//  if (str[0] == '+') {
//    *tzOffset = -(hour * 3600 + minute * 60);
//  } else {
//    *tzOffset = hour * 3600 + minute * 60;
//  }
//
//  return SUCCESS;
//}
//
//int32_t parseFraction(char *str, char **end) {
//  int32_t i = 0;
//  int64_t fraction = 0;
//
//  const int32_t NANO_SEC_FRACTION_LEN = 9;
//
//  int32_t factor[9] = {1,      10,      100,      1000,     10000,
//                       100000, 1000000, 10000000, 100000000};
//  int32_t times = 1;
//
//  while (str[i] >= '0' && str[i] <= '9') {
//    i++;
//  }
//
//  int32_t totalLen = i;
//  if (totalLen <= 0) {
//    return -1;
//  }
//
//  /* parse the fraction */
//  /* only use the initial 3 bits */
//  if (i >= NANO_SEC_FRACTION_LEN) {
//    i = NANO_SEC_FRACTION_LEN;
//  }
//  times = NANO_SEC_FRACTION_LEN - i;
//
//  fraction = strnatoi(str, i) * factor[times];
//  *end = str + totalLen;
//
//  return fraction;
//}
//
//KStatus parseTimeWithTz(KString timestr, k_int64 scale, k_int64 *time,
//                        char delim) {
//  // int64_t factor = TSDB_TICK_PER_SECOND(timePrec);
//  int64_t factor = 1000 * scale;
//  int64_t tzOffset = 0;
//
//  struct tm tm;
//  memset(&tm, 0, sizeof(tm));
//
//  char *str;
//  if (delim == 'T') {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
//  } else if (delim == 0) {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
//  } else {
//    str = NULL;
//  }
//
//  if (str == NULL) {
//    size_t pos = timestr.find('-');
//    if (pos == 0) {
//      pos = timestr.find('-', 1);
//    }
//    if (pos != std::string::npos) {
//      std::string num_str = timestr.substr(0, pos);
//      int year = std::stoi(num_str);
//      if (year > 2970 || year < 0) {
//        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
//                                      "Timestamp/TimestampTZ out of range");
//      }
//    }
//    return FAIL;
//  }
//
//  int64_t seconds = timegm(&tm);
//
//  int64_t fraction = 0;
//  str = forward2TimeStringEnd(timestr.data());
//
//  if (!I64_SAFE_MUL_CHECK(seconds, factor)) {
//    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
//                                  "Timestamp/TimestampTZ out of range");
//    return FAIL;
//  }
//  if ((str[0] == 'Z' || str[0] == 'z') && str[1] == '\0') {
//    /* utc time, no millisecond, return directly*/
//    *time = seconds * factor;
//  } else if (str[0] == '.') {
//    str += 1;
//    if ((fraction = parseFraction(str, &str)) < 0) {
//      return FAIL;
//    }
//    fraction /= (1000000 / scale);
//
//    *time = seconds * factor + fraction;
//
//    char seg = str[0];
//    if (seg != 'Z' && seg != 'z' && seg != '+' && seg != '-') {
//      return FAIL;
//    } else if ((seg == 'Z' || seg == 'z') && str[1] != '\0') {
//      return FAIL;
//    } else if (seg == '+' || seg == '-') {
//      // parse the timezone
//      if (parseTimezone(str, &tzOffset) == FAIL) {
//        return FAIL;
//      }
//
//      *time += tzOffset * factor;
//    }
//
//  } else if (str[0] == '+' || str[0] == '-') {
//    *time = seconds * factor + fraction;
//
//    // parse the timezone
//    if (parseTimezone(str, &tzOffset) == FAIL) {
//      return FAIL;
//    }
//
//    *time += tzOffset * factor;
//  } else {
//    return FAIL;
//  }
//
//  return SUCCESS;
//}
//
//time_t kwdbMktime(struct tm *timep) { return mktime(timep); }
//
//#define LEAP_YEAR_MONTH_DAY 29
//static inline bool validateTm(struct tm *pTm) {
//  if (pTm == NULL) {
//    return false;
//  }
//
//  int32_t dayOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
//
//  int32_t year = pTm->tm_year + 1900;
//  bool isLeapYear =
//      ((year % 100) == 0) ? ((year % 400) == 0) : ((year % 4) == 0);
//
//  if (isLeapYear && (pTm->tm_mon == 1)) {
//    if (pTm->tm_mday > LEAP_YEAR_MONTH_DAY) {
//      return false;
//    }
//  } else {
//    if (pTm->tm_mday > dayOfMonth[pTm->tm_mon]) {
//      return false;
//    }
//  }
//
//  return true;
//}
//
//KStatus parseLocaltimeDst(KString timestr, k_int64 scale, k_int64 *utime,
//                          char delim) {
//  *utime = 0;
//  struct tm tm;
//  memset(&tm, 0, sizeof(tm));
//
//  tm.tm_isdst = -1;
//
//  char *str;
//  int32_t len = timestr.length();
//  if (delim == 'T') {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
//  } else if (delim == 0) {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
//  } else {
//    str = NULL;
//  }
//
//  if (str == NULL || (((str - timestr.data()) < len) && (*str != '.')) ||
//      !validateTm(&tm)) {
//    // if parse failed, try "%Y-%m-%d" format
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d", &tm);
//    if (str == NULL || (((str - timestr.data()) < len) && (*str != '.')) ||
//        !validateTm(&tm)) {
//      return FAIL;
//    }
//  }
//  /* mktime will be affected by TZ, set by using kwdb_options */
//  int64_t seconds = kwdbMktime(&tm);
//
//  int64_t fraction = 0;
//  if (*str == '.') {
//    /* parse the second fraction part */
//    if ((fraction = parseFraction(str + 1, &str)) < 0) {
//      return FAIL;
//    }
//  }
//  fraction /= (1000000 / scale);
//
//  if (!I64_SAFE_MUL_CHECK(seconds, scale)) {
//    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
//                                  "Timestamp/TimestampTZ out of range");
//  }
//  // *utime = TSDB_TICK_PER_SECOND(timePrec) * seconds + fraction;
//  *utime = scale * seconds + fraction;
//  return SUCCESS;
//}
//
//KStatus kwdbParseTime(KString &timestr, k_int64 scale, k_int64 *utime) {
//  /* parse datatime string in with tz */
//  if (timestr.find('T') != std::string::npos) {
//    if (checkTzPresent(timestr)) {
//      return parseTimeWithTz(timestr, scale, utime, 'T');
//    } else {
//      return parseLocaltimeDst(timestr, scale, utime, 'T');
//    }
//  } else {
//    if (checkTzPresent(timestr)) {
//      return parseTimeWithTz(timestr, scale, utime, 0);
//    } else {
//      return parseLocaltimeDst(timestr, scale, utime, 0);
//    }
//  }
//}
//
//KStatus convertStringToTimestamp(KString inputData, k_int64 scale,
//                                 k_int64 *timeVal) {
//  // int32_t charLen = varDataLen(inputData);
//  if (inputData.empty()) {
//    return FAIL;
//  }
//  KStatus ret = kwdbParseTime(inputData, scale, timeVal);
//  if (ret != SUCCESS) {
//    EEPgErrorInfo::SetPgErrorInfo(
//        ERRCODE_INVALID_DATETIME_FORMAT,
//        "parsing as type timestamp: missing required date fields");
//  }
//  return ret;
//}

//int32_t parseFraction(char *str, char **end) {
//  int32_t i = 0;
//  int64_t fraction = 0;
//
//  const int32_t NANO_SEC_FRACTION_LEN = 9;
//
//  int32_t factor[9] = {1,      10,      100,      1000,     10000,
//                       100000, 1000000, 10000000, 100000000};
//  int32_t times = 1;
//
//  while (str[i] >= '0' && str[i] <= '9') {
//    i++;
//  }
//
//  int32_t totalLen = i;
//  if (totalLen <= 0) {
//    return -1;
//  }
//
//  /* parse the fraction */
//  /* only use the initial 3 bits */
//  if (i >= NANO_SEC_FRACTION_LEN) {
//    i = NANO_SEC_FRACTION_LEN;
//  }
//  times = NANO_SEC_FRACTION_LEN - i;
//
//  fraction = strnatoi(str, i) * factor[times];
//  *end = str + totalLen;
//
//  return fraction;
//}
//
//KStatus parseTimeWithTz(KString timestr, k_int64 scale, k_int64 *time,
//                        char delim) {
//  // int64_t factor = TSDB_TICK_PER_SECOND(timePrec);
//  int64_t factor = 1000 * scale;
//  int64_t tzOffset = 0;
//
//  struct tm tm;
//  memset(&tm, 0, sizeof(tm));
//
//  char *str;
//  if (delim == 'T') {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
//  } else if (delim == 0) {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
//  } else {
//    str = NULL;
//  }
//
//  if (str == NULL) {
//    size_t pos = timestr.find('-');
//    if (pos == 0) {
//      pos = timestr.find('-', 1);
//    }
//    if (pos != std::string::npos) {
//      std::string num_str = timestr.substr(0, pos);
//      int year = std::stoi(num_str);
//      if (year > 2970 || year < 0) {
//        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
//                                      "Timestamp/TimestampTZ out of range");
//      }
//    }
//    return FAIL;
//  }
//
//  int64_t seconds = timegm(&tm);
//
//  int64_t fraction = 0;
//  str = forward2TimeStringEnd(timestr.data());
//
//  if (!I64_SAFE_MUL_CHECK(seconds, factor)) {
//    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
//                                  "Timestamp/TimestampTZ out of range");
//    return FAIL;
//  }
//  if ((str[0] == 'Z' || str[0] == 'z') && str[1] == '\0') {
//    /* utc time, no millisecond, return directly*/
//    *time = seconds * factor;
//  } else if (str[0] == '.') {
//    str += 1;
//    if ((fraction = parseFraction(str, &str)) < 0) {
//      return FAIL;
//    }
//    fraction /= (1000000 / scale);
//
//    *time = seconds * factor + fraction;
//
//    char seg = str[0];
//    if (seg != 'Z' && seg != 'z' && seg != '+' && seg != '-') {
//      return FAIL;
//    } else if ((seg == 'Z' || seg == 'z') && str[1] != '\0') {
//      return FAIL;
//    } else if (seg == '+' || seg == '-') {
//      // parse the timezone
//      if (parseTimezone(str, &tzOffset) == -1) {
//        return FAIL;
//      }
//
//      *time += tzOffset * factor;
//    }
//
//  } else if (str[0] == '+' || str[0] == '-') {
//    *time = seconds * factor + fraction;
//
//    // parse the timezone
//    if (parseTimezone(str, &tzOffset) == -1) {
//      return FAIL;
//    }
//
//    *time += tzOffset * factor;
//  } else {
//    return FAIL;
//  }
//
//  return SUCCESS;
//}
//
//time_t kwdbMktime(struct tm *timep) { return mktime(timep); }
//
//#define LEAP_YEAR_MONTH_DAY 29
//static inline bool validateTm(struct tm *pTm) {
//  if (pTm == NULL) {
//    return false;
//  }
//
//  int32_t dayOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
//
//  int32_t year = pTm->tm_year + 1900;
//  bool isLeapYear =
//      ((year % 100) == 0) ? ((year % 400) == 0) : ((year % 4) == 0);
//
//  if (isLeapYear && (pTm->tm_mon == 1)) {
//    if (pTm->tm_mday > LEAP_YEAR_MONTH_DAY) {
//      return false;
//    }
//  } else {
//    if (pTm->tm_mday > dayOfMonth[pTm->tm_mon]) {
//      return false;
//    }
//  }
//
//  return true;
//}
//
//KStatus parseLocaltimeDst(KString timestr, k_int64 scale, k_int64 *utime,
//                          char delim) {
//  *utime = 0;
//  struct tm tm;
//  memset(&tm, 0, sizeof(tm));
//
//  tm.tm_isdst = -1;
//
//  char *str;
//  int32_t len = timestr.length();
//  if (delim == 'T') {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
//  } else if (delim == 0) {
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
//  } else {
//    str = NULL;
//  }
//
//  if (str == NULL || (((str - timestr.data()) < len) && (*str != '.')) ||
//      !validateTm(&tm)) {
//    // if parse failed, try "%Y-%m-%d" format
//    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d", &tm);
//    if (str == NULL || (((str - timestr.data()) < len) && (*str != '.')) ||
//        !validateTm(&tm)) {
//      return FAIL;
//    }
//  }
//  /* mktime will be affected by TZ, set by using kwdb_options */
//  int64_t seconds = kwdbMktime(&tm);
//
//  int64_t fraction = 0;
//  if (*str == '.') {
//    /* parse the second fraction part */
//    if ((fraction = parseFraction(str + 1, &str)) < 0) {
//      return FAIL;
//    }
//  }
//  fraction /= (1000000 / scale);
//
//  if (!I64_SAFE_MUL_CHECK(seconds, scale)) {
//    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
//                                  "Timestamp/TimestampTZ out of range");
//  }
//  // *utime = TSDB_TICK_PER_SECOND(timePrec) * seconds + fraction;
//  *utime = scale * seconds + fraction;
//  return SUCCESS;
//}
//
//KStatus kwdbParseTime(KString &timestr, k_int64 scale, k_int64 *utime) {
//  /* parse datatime string in with tz */
//  if (timestr.find('T') != std::string::npos) {
//    if (checkTzPresent(timestr)) {
//      return parseTimeWithTz(timestr, scale, utime, 'T');
//    } else {
//      return parseLocaltimeDst(timestr, scale, utime, 'T');
//    }
//  } else {
//    if (checkTzPresent(timestr)) {
//      return parseTimeWithTz(timestr, scale, utime, 0);
//    } else {
//      return parseLocaltimeDst(timestr, scale, utime, 0);
//    }
//  }
//}
//
//KStatus convertStringToTimestamp(KString inputData, k_int64 scale,
//                                 k_int64 *timeVal) {
//  // int32_t charLen = varDataLen(inputData);
//  if (inputData.empty()) {
//    return FAIL;
//  }
//  KStatus ret = kwdbParseTime(inputData, scale, timeVal);
//  if (ret != SUCCESS) {
//    EEPgErrorInfo::SetPgErrorInfo(
//        ERRCODE_INVALID_DATETIME_FORMAT,
//        "parsing as type timestamp: missing required date fields");
//  }
//  return ret;
//}

KStatus makeBewteenArgs(unique_ptr<duckdb::Expression> *left_node,
                        unique_ptr<duckdb::Expression> *right_node,
                        unique_ptr<duckdb::Expression> *lower_ptr,
                        unique_ptr<duckdb::Expression> *upper_ptr,
                        unique_ptr<duckdb::Expression> *input_col) {
  auto &lower = left_node->get()->Cast<BoundComparisonExpression>();
  if (lower.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
    *lower_ptr = std::move(lower.left);
    *input_col = std::move(lower.right);
  } else {
    *lower_ptr = std::move(lower.right);
    *input_col = std::move(lower.left);
  }
  auto &upper = right_node->get()->Cast<BoundComparisonExpression>();
  if (upper.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
    *upper_ptr = std::move(upper.left);
    if (nullptr == (*input_col).get() || duckdb::Expression::Equals(*input_col, upper.right)) {
      return FAIL;
    }
  } else {
    *upper_ptr = std::move(upper.right);
    if (nullptr == (*input_col).get() || !duckdb::Expression::Equals(*input_col, upper.left)) {
      return FAIL;
    }
  }
  return SUCCESS;
}

void makeConstantInt(LogicalType &col_type, k_int64 &int_value,
                     unique_ptr<BoundConstantExpression> &int_expr) {
  auto is_int16 = false;
  auto is_int32 = false;
  if (int_value >= INT16_MIN && int_value <= INT16_MAX) {
    is_int16 = true;
  } else if (int_value >= INT32_MIN && int_value <= INT32_MAX) {
    is_int32 = true;
  }
  
  switch (col_type.id()) {
    case LogicalType::SMALLINT:
      break;
    case LogicalType::INTEGER: {
      if (is_int16) {
        is_int16 = false;
        is_int32 = true;
      }
      break;
    }
    case LogicalType::BIGINT: {
      is_int16 = false;
      is_int32 = false;
      break;
    }
    default:
      return;
  }

  if (is_int16) {
    int_expr = make_uniq<BoundConstantExpression>(Value::SMALLINT(static_cast<int16_t>(int_value)));
  } else if (is_int32) {
    int_expr = make_uniq<BoundConstantExpression>(Value::INTEGER(static_cast<int32_t>(int_value)));
  } else {
    int_expr = make_uniq<BoundConstantExpression>(Value::BIGINT(int_value));
  }
}

KStatus APParseQuery::ConstructTree(std::size_t &i, void *head_node_ptr, void* user_data) {
  auto param = reinterpret_cast<ParseExprParam*>(user_data);
  auto *head_node = reinterpret_cast<unique_ptr<duckdb::Expression>*>(head_node_ptr);
  unique_ptr<duckdb::Expression> current_node;
  unique_ptr<duckdb::Expression> expr_ptr = nullptr;
  KStatus ret = FAIL;
  if (i >= node_list_.size()) {
    return SUCCESS;
  }
  if (node_list_[i]->is_operator) {
    switch (node_list_[i]->operators) {
      case CLOSING_BRACKET: {
        (i)++;
        return FAIL;
      }
      case OPENING_BRACKET: {
        (i)++;
        while (i < node_list_.size() && node_list_[i]->operators != CLOSING_BRACKET) {
          ret = ConstructTree(i, &current_node, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        }
        (i)++;  // Skip CLOSING_BRACKET
        *head_node = std::move(current_node);
        return SUCCESS;
      }
      case AND: {
        (i)++;
        ret = ConstructTree(i, &current_node, user_data);
        if (ret != SUCCESS) {
          return ret;
        }
        auto head_expr = head_node->get();
        bool lower_inclusive = false;
        bool upper_inclusive = false;
        bool head_lower = false;
        bool head_upper = false;
        bool current_lower = false;
        bool current_upper = false;
        if (nullptr != head_expr) {
          auto head_type = head_expr->GetExpressionType();
          auto current_type = current_node->GetExpressionType();
          if (head_type == ExpressionType::COMPARE_GREATERTHAN ||
              head_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
            head_lower = true;
            lower_inclusive =
                head_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          } else if (head_type == ExpressionType::COMPARE_LESSTHAN ||
                     head_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
            head_upper = true;
            upper_inclusive =
                head_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
          }
          if (current_type == ExpressionType::COMPARE_GREATERTHAN ||
              current_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
            current_lower = true;
            lower_inclusive =
                current_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          } else if (current_type == ExpressionType::COMPARE_LESSTHAN ||
                     current_type ==
                         ExpressionType::COMPARE_LESSTHANOREQUALTO) {
            current_upper = true;
            upper_inclusive =
                current_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
          }
          unique_ptr<duckdb::Expression> lower_ptr;
          unique_ptr<duckdb::Expression> upper_ptr;
          unique_ptr<duckdb::Expression> input_col;
          if (current_lower && head_upper) {
            if (makeBewteenArgs(&current_node, head_node, &lower_ptr,
                                &upper_ptr, &input_col) != SUCCESS) {
              return FAIL;
            }
          } else if (current_upper && head_lower) {
            if (makeBewteenArgs(head_node, &current_node, &lower_ptr,
                                &upper_ptr, &input_col) != SUCCESS) {
              return FAIL;
            }
          }
          if (nullptr != lower_ptr && nullptr != upper_ptr &&
              nullptr != input_col) {
            auto between = make_uniq<BoundBetweenExpression>(
                std::move(input_col), std::move(lower_ptr),
                std::move(upper_ptr), lower_inclusive, upper_inclusive);
            *head_node = std::move(between);
          } else {
            auto conjunction = make_uniq<BoundConjunctionExpression>(
                ExpressionType::CONJUNCTION_AND, std::move(*head_node),
                std::move(current_node));
            *head_node = std::move(conjunction);
          }
        } else {
          *head_node = std::move(current_node);
        }
        return SUCCESS;
      }
      case LESS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_LESSTHAN, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case GREATER: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_GREATERTHAN, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_EQUAL, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case NOT_EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_NOTEQUAL, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case LESS_OR_EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_LESSTHANOREQUALTO, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case GREATER_OR_EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case In: {
        if (node_list_[i]->is_negative) {
          auto tmp_expr = make_uniq<BoundComparisonExpression>(
              ExpressionType::COMPARE_NOT_IN, nullptr, nullptr);
          tmp_expr->left = std::move(*head_node);
          *head_node = std::move(tmp_expr);
          (i)++;
        } else {
          auto tmp_expr = make_uniq<BoundComparisonExpression>(
              ExpressionType::COMPARE_IN, nullptr, nullptr);
          tmp_expr->left = std::move(*head_node);
          *head_node = std::move(tmp_expr);
          (i)++;
        }
        return SUCCESS;
      }
      case PLUS: {
        std::string plus = "+";
        EntryLookupInfo lookup_info(CatalogType::SCALAR_FUNCTION_ENTRY, plus);
        auto entry_retry = CatalogEntryRetriever(*param->context_);
        auto func_entry = entry_retry.GetEntry("", "", lookup_info,
                                               OnEntryNotFound::RETURN_NULL);
        auto &func = func_entry->Cast<ScalarFunctionCatalogEntry>();
        (i)++;
        if (ConstructTree(i, &current_node, user_data) != SUCCESS) {
          return FAIL;
        }
        vector<LogicalType> in_type;
        in_type.push_back(head_node->get()->return_type);
        in_type.push_back(current_node->return_type);
        auto type_func =
            func.functions.GetFunctionByArguments(*param->context_, in_type);
        auto return_type = head_node->get()->return_type;
        if (head_node->get()->return_type != type_func.return_type) {
          *head_node = BoundCastExpression::AddCastToType(
              *param->context_, std::move(*head_node), type_func.return_type,
              head_node->get()->return_type.id() == LogicalTypeId::ENUM);
        }
        if (current_node->return_type != type_func.return_type) {
          current_node = BoundCastExpression::AddCastToType(
              *param->context_, std::move(current_node), type_func.return_type,
              current_node->return_type.id() == LogicalTypeId::ENUM);
        }
        vector<unique_ptr<duckdb::Expression>> type_args;
        type_args.push_back(std::move(*head_node));
        type_args.push_back(std::move(current_node));

        auto tmp_expr = make_uniq<BoundFunctionExpression>(
            type_func.return_type, type_func, std::move(type_args), nullptr);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      default: {
        return FAIL;
      }
    }
  } else if (node_list_[i]->is_func) {
    (i)++;
    if (node_list_[i]->operators != OPENING_BRACKET) {
      return FAIL;
    }
    (i)++;
    while (node_list_[i]->operators != CLOSING_BRACKET) {
      if (ConstructTree(i, &expr_ptr, user_data) != SUCCESS) {
        return FAIL;
      }
      if (node_list_[i]->operators == COMMA) {
        // current_node->args.push_back(expr_ptr);
        (i)++;
      }
    }
    if (expr_ptr != nullptr) {
      // current_node->args.push_back(expr_ptr);
    }
    (i)++;
    *head_node = std::move(current_node);
    return SUCCESS;
  } else {
    switch (node_list_[i]->operators) {
      case CAST:
        (i)++;
        *head_node = std::move(current_node);
        return SUCCESS;
      case COLUMN_TYPE: {
        auto origin_node = node_list_[i];
        auto index =
            LogicalIndex(origin_node.get()->value.number.column_id - 1);
        auto &col = param->table_.get().GetColumn(index);
        current_node = make_uniq<BoundReferenceExpression>(
            "", col.Type(), param->col_map_[index.index]);
        (i)++;
        while (i < node_list_.size() &&
               node_list_[i]->operators != CLOSING_BRACKET &&
               node_list_[i]->operators != AND) {
          if (ConstructTree(i, &current_node, user_data) != SUCCESS) {
            return FAIL;
          }
        }
        *head_node = std::move(current_node);
        return SUCCESS;
      }
      case AstEleType::INT_TYPE: {
        auto int_value = node_list_[i]->value.number.int_type;
        unique_ptr<BoundConstantExpression> int_expr;
        auto &comparsion_expr =
            head_node->get()->Cast<BoundComparisonExpression>();
        makeConstantInt(comparsion_expr.left->return_type, int_value, int_expr);
        if (nullptr == int_expr) {
          return FAIL;
        }
        if (int_expr->return_type != comparsion_expr.left->return_type) {
          comparsion_expr.left = BoundCastExpression::AddCastToType(
              *param->context_, std::move(comparsion_expr.left), int_expr->return_type,
              int_expr->return_type.id() == LogicalTypeId::ENUM);
        }
        comparsion_expr.right = std::move(int_expr);
        (i)++;
        return SUCCESS;
      }
      default:
        break;
    }
  }
  return FAIL;
}

//Elements APParseQuery::APParseImpl() {
//  k_int64 bracket = 0;
//
//  while (true) {
//    if (this->pos_->type == TokenType::Error) {
//      node_list_.clear();
//      return node_list_;
//    }
//    if (this->pos_->isEnd()) {
//      if (bracket != 0) {
//        node_list_.clear();
//        return node_list_;
//      }
//      return node_list_;
//    }
//    if (this->pos_->type == TokenType::OpeningRoundBracket) {
//      node_list_.push_back(std::make_shared<Element>(OPENING_BRACKET, true));
//      ++bracket;
//      ++this->pos_;
//      if (this->pos_->type == TokenType::Minus) {
//        k_int64 factor = -1;
//        ++this->pos_;
//
//        node_list_.pop_back();
//        --bracket;
//        k_bool flag = ParseNumber(factor);
//        if (!flag) {
//          node_list_.clear();
//          return node_list_;
//        }
//      } else {
//        --this->pos_;
//      }
//    } else {
//      if (this->pos_->type == TokenType::ClosingRoundBracket) {
//        node_list_.push_back(std::make_shared<Element>(CLOSING_BRACKET, true));
//        --bracket;
//      } else {
//        k_bool flag = ParseSingleExpr();
//        if (!flag) {
//          node_list_.clear();
//          return node_list_;
//        }
//      }
//    }
//    ++this->pos_;
//  }
//}
//
//k_bool APParseQuery::ParseNumber(k_int64 factor) {
//  auto raw_sql = this->raw_sql_;
//  std::basic_string<k_char> read_buffer;
//  auto current_type = this->pos_->type;
//  if (current_type == TokenType::Number ||
//      current_type == TokenType::StringLiteral ||
//      current_type == TokenType::BareWord) {
//    auto size = read_buffer.size();
//    while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//      read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//      size = read_buffer.size();
//    }
//    if (current_type == TokenType::BareWord) {
//      k_int64 value = 1;
//      if (read_buffer.find("TRUE") != std::string::npos ||
//          read_buffer.find("true") != std::string::npos) {
//        auto ele = Element(value);
//        ele.SetType(INT_TYPE);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        return true;
//      } else if (read_buffer.find("FALSE") != std::string::npos ||
//                 read_buffer.find("false") != std::string::npos) {
//        value = 0;
//        auto ele = Element(value);
//        ele.SetType(INT_TYPE);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        return true;
//      } else {
//        return false;
//      }
//    }
//  } else {
//    return false;
//  }
//  ++this->pos_;
//
//  /*
//   * dispose -1
//   */
//  if (this->pos_->type == TokenType::ClosingRoundBracket) {
//    ++this->pos_;
//  }
//
//  if (this->pos_->type == TokenType::TypeAnotation) {
//    ++this->pos_;
//    std::string data_str =
//        raw_sql.substr(this->pos_->current_depth_,
//                       this->pos_->end_depth_ - this->pos_->current_depth_);
//    if (data_str.find("INTERVAL") != std::string::npos) {
//      read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//      auto ele = Element(read_buffer);
//      ele.SetType(INTERVAL_TYPE);
//      node_list_.push_back(std::make_shared<Element>(ele));
//      return true;
//    } else if (data_str.find("INT") != std::string::npos) {
//      k_char *stop_string;
//      auto node_value = std::strtoull(read_buffer.c_str(), &stop_string, 10);
//      auto ele = Element(static_cast<k_int64>(node_value * factor));
//      ele.SetType(INT_TYPE);
//      node_list_.push_back(std::make_shared<Element>(ele));
//      return true;
//    } else {
//      if (data_str.find("DECIMAL") != std::string::npos) {
//        auto node_value = std::stod(read_buffer);
//        auto ele = Element(node_value * factor);
//        ele.SetType(DECIMAL);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        return true;
//      } else {
//        if (data_str.find("FLOAT8") != std::string::npos) {
//          k_float64 node_value;
//          if (read_buffer.compare("'NaN'") == 0) {
//            node_value = NAN;
//          } else {
//            node_value = std::stod(read_buffer);
//          }
//          auto ele = Element(node_value * factor);
//          ele.SetType(FLOAT_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (data_str.find("FLOAT") != std::string::npos) {
//          k_float32 node_value;
//          if (read_buffer.compare("'NaN'") == 0) {
//            node_value = NAN;
//          } else {
//            node_value = std::stof(read_buffer);
//          }
//          auto ele = Element(node_value * factor);
//          ele.SetType(FLOAT_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (data_str.find("TIMESTAMPTZ") != std::string::npos) {
//          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//          // k_int64 tz = getGMT(&read_buffer);
//          k_int64 tz = 0;
//          k_int64 scale = 0;
//          AstEleType ele_type = TIMESTAMPTZ_TYPE;
//          char type = data_str[data_str.size() - 2];
//          if (type == '6') {
//            scale = 1000;
//            ele_type = TIMESTAMPTZ_MICRO_TYPE;
//          } else if (type == '9') {
//            scale = 1000000;
//            ele_type = TIMESTAMPTZ_NANO_TYPE;
//          } else {  // default or 3
//            scale = 1;
//          }
//          if (convertStringToTimestamp(read_buffer, scale, &tz) != SUCCESS)
//            return false;
//          auto ele = Element(tz);
//          ele.SetType(ele_type);
//          node_list_.push_back(std::make_shared<Element>(ele));
//
//          return true;
//        } else if (data_str.find("TIMESTAMP") != std::string::npos) {
//          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//          // k_int64 tz = getGMT(&read_buffer);
//          k_int64 tz = 0;
//          k_int64 scale = 0;
//          AstEleType ele_type = TIMESTAMP_TYPE;
//          char type = data_str[data_str.size() - 2];
//          if (type == '6') {
//            scale = 1000;
//            ele_type = TIMESTAMP_MICRO_TYPE;
//          } else if (type == '9') {
//            scale = 1000000;
//            ele_type = TIMESTAMP_NANO_TYPE;
//          } else {  // default or 3
//            scale = 1;
//          }
//          if (convertStringToTimestamp(read_buffer, scale, &tz) != SUCCESS)
//            return false;
//          auto ele = Element(tz);
//          ele.SetType(ele_type);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (data_str.find("DATE") != std::string::npos) {
//          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//          auto ele = Element(read_buffer);
//          ele.SetType(DATE_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (current_type == TokenType::StringLiteral &&
//                   data_str.find("STRING") != std::string::npos) {
//          read_buffer =
//              parseUnicode2Utf8(read_buffer.substr(1, read_buffer.size() - 2));
//          auto ele = Element(read_buffer);
//          ele.SetType(STRING_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (current_type == TokenType::StringLiteral &&
//                   data_str.find("BYTES") != std::string::npos) {
//          read_buffer =
//              parseHex2String(read_buffer.substr(1, read_buffer.size() - 2));
//          auto ele = Element(read_buffer);
//          ele.SetType(BYTES_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else {
//          return false;
//        }
//      }
//    }
//  }
//  return false;
//}
//
//k_bool APParseQuery::ParseSingleExpr() {
//  if (this->pos_->type == TokenType::Number ||
//      this->pos_->type == TokenType::StringLiteral ||
//      this->pos_->type == TokenType::BareWord) {
//    k_bool flag = ParseNumber(1);
//    return flag;
//  } else if (this->pos_->type == TokenType::At) {
//    std::string raw_sql = this->raw_sql_;
//    std::basic_string<k_char> read_buffer;
//    ++this->pos_;
//    if (this->pos_->type == TokenType::Number) {
//      auto size = read_buffer.size();
//      while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//        read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//        size = read_buffer.size();
//      }
//    } else {
//      return false;
//    }
//    k_uint32 node_value = atoi(read_buffer.c_str());
//    auto ele = Element(node_value);
//    ele.SetType(COLUMN_TYPE);
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::In) {
//    auto ele = Element(In, true);
//    --this->pos_;
//    if (this->pos_->type == TokenType::Not) {
//      ele.SetNegative(KTRUE);
//    }
//    ++this->pos_;
//    node_list_.push_back(std::make_shared<Element>(ele));
//    std::string raw_sql = this->raw_sql_;
//    std::basic_string<k_char> read_buffer;
//    auto size = read_buffer.size();
//    while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//      read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//      size = read_buffer.size();
//    }
//    ele = Element(read_buffer);
//    ele.SetType(STRING_TYPE);
//    node_list_.push_back(std::make_shared<Element>(ele));
//    return true;
//  } else if (this->pos_->type == TokenType::Not) {
//    ++this->pos_;
//    if (this->pos_->type == TokenType::Like ||
//        this->pos_->type == TokenType::ILike ||
//        this->pos_->type == TokenType::Is ||
//        this->pos_->type == TokenType::In) {
//      --this->pos_;
//      return true;
//    }
//    --this->pos_;
//    auto ele = Element(NOT, true);
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::Like) {
//    auto ele = Element(Like, true);
//    --this->pos_;
//    if (this->pos_->type == TokenType::Not) {
//      ele.SetNegative(KTRUE);
//    }
//    ++this->pos_;
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::ILike) {
//    auto ele = Element(ILike, true);
//    --this->pos_;
//    if (this->pos_->type == TokenType::Not) {
//      ele.SetNegative(KTRUE);
//    }
//    ++this->pos_;
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::Is) {
//    ++this->pos_;
//    bool localNegative = KFALSE;
//    if (this->pos_->type == TokenType::Not) {
//      localNegative = KTRUE;
//      ++this->pos_;
//    }
//    if (this->pos_->type == TokenType::Null) {
//      auto ele = Element(IS_NULL, true);
//      if (localNegative) {
//        ele.SetNegative(localNegative);
//      }
//      node_list_.push_back(std::make_shared<Element>(ele));
//    } else if (this->pos_->type == TokenType::Nan) {
//      auto ele = Element(IS_NAN, true);
//      if (localNegative) {
//        ele.SetNegative(localNegative);
//      }
//      node_list_.push_back(std::make_shared<Element>(ele));
//    } else if (this->pos_->type == TokenType::Unknown) {
//      auto ele = Element(IS_UNKNOWN, true);
//      if (localNegative) {
//        ele.SetNegative(localNegative);
//      }
//      node_list_.push_back(std::make_shared<Element>(ele));
//    }
//  } else if (this->pos_->type == TokenType::Null) {
//    auto ele = Element(NULL_TYPE, false);
//    ++this->pos_;
//    if (this->pos_->type == TokenType::DoubleColon) {
//      // ship null
//      ++this->pos_;
//      // bareWords
//      ++this->pos_;
//      if (this->pos_->type == TokenType::ClosingRoundBracket) {
//        --this->pos_;
//      }
//      if (this->pos_->type == TokenType::OpeningRoundBracket) {
//        // (
//        ++this->pos_;
//        // number
//        ++this->pos_;
//      }
//    } else {
//      --this->pos_;
//    }
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else {
//    switch (this->pos_->type) {
//      case TokenType::Plus: {
//        node_list_.push_back(std::make_shared<Element>(PLUS, true));
//        break;
//      }
//      case TokenType::Minus: {
//        node_list_.push_back(std::make_shared<Element>(MINUS, true));
//        break;
//      }
//      case TokenType::Multiple: {
//        node_list_.push_back(std::make_shared<Element>(MULTIPLE, true));
//        break;
//      }
//      case TokenType::Divide: {
//        node_list_.push_back(std::make_shared<Element>(DIVIDE, true));
//        break;
//      }
//      case TokenType::Dividez: {
//        node_list_.push_back(std::make_shared<Element>(DIVIDEZ, true));
//        break;
//      }
//      case TokenType::Remainder: {
//        node_list_.push_back(std::make_shared<Element>(REMAINDER, true));
//        break;
//      }
//      case TokenType::Percent: {
//        node_list_.push_back(std::make_shared<Element>(PERCENT, true));
//        break;
//      }
//      case TokenType::Power: {
//        node_list_.push_back(std::make_shared<Element>(POWER, true));
//        break;
//      }
//      case TokenType::ANDCAL: {
//        node_list_.push_back(std::make_shared<Element>(ANDCAL, true));
//        break;
//      }
//      case TokenType::ORCAL: {
//        node_list_.push_back(std::make_shared<Element>(ORCAL, true));
//        break;
//      }
//      case TokenType::Tilde: {
//        node_list_.push_back(std::make_shared<Element>(TILDE, true));
//        break;
//      }
//      case TokenType::ITilde: {
//        node_list_.push_back(std::make_shared<Element>(IREGEX, true));
//        break;
//      }
//      case TokenType::NotRegex: {
//        node_list_.push_back(std::make_shared<Element>(NOTREGEX, true));
//        break;
//      }
//      case TokenType::NotIRegex: {
//        node_list_.push_back(std::make_shared<Element>(NOTIREGEX, true));
//        break;
//      }
//      case TokenType::Equals: {
//        node_list_.push_back(std::make_shared<Element>(EQUALS, true));
//        break;
//      }
//      case TokenType::NotEquals: {
//        node_list_.push_back(std::make_shared<Element>(NOT_EQUALS, true));
//        break;
//      }
//      case TokenType::LessOrEquals: {
//        node_list_.push_back(std::make_shared<Element>(LESS_OR_EQUALS, true));
//        break;
//      }
//      case TokenType::GreaterOrEquals: {
//        node_list_.push_back(
//            std::make_shared<Element>(GREATER_OR_EQUALS, true));
//        break;
//      }
//      case TokenType::LeftShift: {
//        node_list_.push_back(std::make_shared<Element>(LEFTSHIFT, true));
//        break;
//      }
//      case TokenType::RightShift: {
//        node_list_.push_back(std::make_shared<Element>(RIGHTSHIFT, true));
//        break;
//      }
//      case TokenType::Greater: {
//        node_list_.push_back(std::make_shared<Element>(GREATER, true));
//        break;
//      }
//      case TokenType::Comma: {
//        node_list_.push_back(std::make_shared<Element>(COMMA, false));
//        break;
//      }
//      case TokenType::Less: {
//        node_list_.push_back(std::make_shared<Element>(LESS, true));
//        break;
//      }
//      case TokenType::AND: {
//        node_list_.push_back(std::make_shared<Element>(AND, true));
//        break;
//      }
//      case TokenType::OR: {
//        node_list_.push_back(std::make_shared<Element>(OR, true));
//        break;
//      }
//      case TokenType::In: {
//        node_list_.push_back(std::make_shared<Element>(In, true));
//        break;
//      }
//      case TokenType::Case: {
//        node_list_.push_back(std::make_shared<Element>(CASE, true));
//        break;
//      }
//      case TokenType::When: {
//        node_list_.push_back(std::make_shared<Element>(WHEN, true));
//        break;
//      }
//      case TokenType::Then: {
//        node_list_.push_back(std::make_shared<Element>(THEN, true));
//        break;
//      }
//      case TokenType::Else: {
//        node_list_.push_back(std::make_shared<Element>(ELSE, true));
//        break;
//      }
//      case TokenType::End: {
//        node_list_.push_back(std::make_shared<Element>(CASE_END, true));
//        break;
//      }
//      case TokenType::COALESCE: {
//        node_list_.push_back(std::make_shared<Element>(COALESCE, true));
//        break;
//      }
//      case TokenType::DoubleColon: {
//        // CAST
//        ++this->pos_;
//        // bareWords
//        // ++this->pos_;
//        std::string raw_sql = this->raw_sql_;
//        std::basic_string<k_char> read_buffer;
//        auto size = read_buffer.size();
//        while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//          read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//          size = read_buffer.size();
//        }
//        auto ele = Element(read_buffer);
//        ele.SetType(CAST);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        // if (this->pos_->type == TokenType::OpeningRoundBracket) {
//        //   // (
//        //   ++this->pos_;
//        //   // number
//        //   ++this->pos_;
//        // }
//        break;
//      }
//      case TokenType::TypeAnotation: {
//        ++this->pos_;
//        break;
//      }
//      case TokenType::Function: {
//        auto ele = Element(kwdb::Function, true);
//        std::string raw_sql = this->raw_sql_;
//        std::basic_string<k_char> read_buffer;
//        auto size = read_buffer.size();
//        while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//          read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//          size = read_buffer.size();
//        }
//
//        ele = Element(read_buffer);
//        ele.SetType(kwdb::Function);
//        ele.SetFunc(KTRUE);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        break;
//      }
//      case TokenType::Cast: {
//        // Skip cast token.
//        // ++this->pos_;
//        break;
//      }
//      case TokenType::Any: {
//        node_list_.push_back(std::make_shared<Element>(ANY, true));
//        break;
//      }
//      case TokenType::All: {
//        node_list_.push_back(std::make_shared<Element>(ALL, true));
//        break;
//      }
//      default:
//        return false;
//    }
//  }
//  return true;
//}
}  // namespace kwdbts