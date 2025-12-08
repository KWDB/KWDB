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
#include "ee_cast_utils.h"

#include <string.h>

#include <limits>
#include <string>

#include "ee_global.h"
#include "kwdb_type.h"
#include "pgcode.h"
#include "ts_common.h"

namespace kwdbts {

bool isNumber(char *n) {
  if (*n >= 48 && *n <= 57) return true;
  return false;
}

int fetchNum0(char *&ptr, int &positive, char *&realStart) {
  realStart = nullptr;
  positive = 1;
  if (ptr == nullptr || *ptr == '\0') {
    return 0;
  }
  // skip sign
  if (*ptr == '+' || *ptr == '-') {
    if (*ptr == '-') {
      positive = -1;
    }
    ptr++;
  }

  if (!isNumber(ptr)) {  // not a number
    return 0;
  }

  while (*ptr == '0') {  // skip 0's
    ptr++;
  }

  // point at where number should start
  if (!isNumber(ptr)) {   // not number, should be '.'
    realStart = ptr - 1;  // previous must be '0'
    return 0;
  }
  realStart = ptr;
  // skip all numbers
  do {
    ptr++;
  } while (isNumber(ptr));

  return ptr - realStart;
}
KStatus fetchNum2(char *ptr, int64_t &num) {
  if (ptr == nullptr || *ptr == '\0') {  // fetch nothing
    const KString &msg =
        "could not parse \"" + std::string(ptr) + "\" as type int";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  msg.c_str());
    return FAIL;
  }
  char *endPtr;
  num = strtol(ptr, &endPtr, 10);
  if (*endPtr != '\0') {
    const KString &msg =
        "could not parse \"" + std::string(ptr) + "\" as type int";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  msg.c_str());
    return FAIL;
  }
  if (num == std::numeric_limits<int64_t>::max() &&
      strcmp(ptr, "9223372036854775807") != 0 &&
      strcmp(ptr, "+9223372036854775807") != 0) {  // overflow
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                  "out of range");

    return FAIL;
  }
  if (num == std::numeric_limits<int64_t>::min() &&
      strcmp(ptr, "-9223372036854775808") != 0) {  // overflow
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                  "out of range");
    return FAIL;
  }
  return SUCCESS;
}

bool checkOverflow(int positive, int64_t output, int multiply,
                   int add) {  // add is always positive
  if (positive == 1) {
    if (output > ((std::numeric_limits<int64_t>::max() - add) / multiply)) {
      return true;
    }
  } else if (positive == -1) {
    if (output < ((std::numeric_limits<int64_t>::min() + add) / multiply)) {
      return true;
    }
  } else {
    return true;
  }
  return false;
}

bool boolStrToInt(char *input, int &output) {
  if ((*(input) == 't' || *(input) == 'T') &&
      (*(input + 1) == 'r' || *(input + 1) == 'R') &&
      (*(input + 2) == 'u' || *(input + 2) == 'U') &&
      (*(input + 3) == 'e' || *(input + 3) == 'E') && (*(input + 4) == '\0')) {
    output = 1;
  } else if ((*(input) == 'f' || *(input) == 'F') &&
             (*(input + 1) == 'a' || *(input + 1) == 'A') &&
             (*(input + 2) == 'l' || *(input + 2) == 'L') &&
             (*(input + 3) == 's' || *(input + 3) == 'S') &&
             (*(input + 4) == 'e' || *(input + 4) == 'E') &&
             (*(input + 5) == '\0')) {
    output = 0;
  } else {
    return false;
  }
  return true;
}

KStatus strToDouble(char *str, double &output) {
  // input can be any numerice value, since double is not accurate anyway.
  int boolTmp = 0;
  if (boolStrToInt(str, boolTmp)) {
    output = boolTmp;
  } else {
    char *end_ptr;
    errno = 0;
    double tmp = strtod(str, &end_ptr);
    if (errno == ERANGE) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    "out of range");
      return FAIL;
    }
    if (*end_ptr != '\0' || errno != 0) {
      const KString &msg =
          "could not parse \"" + std::string(str) + "\" as type int";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    msg.c_str());
      return FAIL;
    }
    output = tmp;
  }
  return SUCCESS;
}

KStatus strToInt64(char *input, int64_t &output) {
  int boolTmp = 0;
  if (boolStrToInt(input, boolTmp)) {
    output = boolTmp;
  } else {
    /*
        naming:
            num0: 1st part of digits until '.', 'e', or '\0'.
            num1: 2nd part of digits after '.', until 'e' or '\0'.
            num2: 3rd part of digits after e, until '\0'.

        example:
            123.456e789
                "123" -> num0
                "456" -> num1
                "789" -> num2

            123e45
                "123" -> num0
                ""    -> num1
                "45"  -> num2
            -------------------

        syntax rules:
            1. Unexpected symbol detected.
                Any symbols other than '.', 'e', '+', '-' are not allowed.
                '.' (if exist) must shows before 'e' (if exist).
            2. num0 or num1 should have number(s).
                ex:
                    "5."    valid
                    ".5"    valid
                    ".e5"   invalid
            3. num0 can only have one leading sign.
            4. num1 should only be number(s) or blank.
            5. if 'e' exist, num2 must be numbers.
    */

    char *endPtr = input + strlen(input);
    char *ptr = input;  // moving pointer

    char *decimalPtr = nullptr;  // remeber where '.' is

    char *num0Start = nullptr;  // pointing at first non-zero number
    int num0Len = 0;  // length from num0Start to '.' or 'e' or '\0', ex:
                      // "000e1" = 0, "0100" = 3, "100.1" = 3, "0.0" = 0
    int num1Len = 0;  // length from '.' to 'e' or '\0', ex: "123.000456" = 6

    int positive = 1;  // should only be 1 or -1

    int64_t eNum = 0;  // number after 'e', ex: "123e456" = 456

    // fetch and validate string ========================

    // fetch num0 ----------------
    // skip spaces
    while (*ptr == ' ') {
      ptr++;
    }
    num0Len = fetchNum0(ptr, positive, num0Start);
    if (ptr != endPtr && *ptr != '.' && *ptr != 'e') {  // against rule 1
      const KString &msg =
          "could not parse \"" + std::string(input) + "\" as type int";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    msg.c_str());
      return FAIL;
    }
    // fetch num1 -----------
    if (*ptr == '.') {
      decimalPtr = ptr;
      ptr++;

      while (isNumber(ptr)) {  // skip all 0~9
        ptr++;
      }
      if (ptr != endPtr && *ptr != 'e') {  // against rule 1
        const KString &msg =
            "could not parse \"" + std::string(input) + "\" as type int";
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      msg.c_str());
        return FAIL;
      }
      num1Len = ptr - (decimalPtr + 1);
    }

    if (num0Start == nullptr && num1Len == 0) {  // against rule 2
      const KString &msg =
          "could not parse \"" + std::string(input) + "\" as type int";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    msg.c_str());
      return FAIL;
    }
    // fetch num2 -------------
    if (*ptr == 'e') {
      ptr++;
      auto ret = fetchNum2(ptr, eNum);
      if (ret != SUCCESS) {  // against rule 5
        return ret;
      }
    }

    // start calculation ====================

    if (checkOverflow(true, eNum, 1, num0Len) || eNum + num0Len > 19) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    "out of range");

      return FAIL;
    }

    int i = 0;
    int outputLen = 0;
    output = 0;
    if (eNum >= 0) {  // decimal point should move right, or stay
      if (num0Start && num0Len > 0) {
        ptr = num0Start;
        for (i = 1; i <= num0Len; i++) {
          outputLen++;
          if (outputLen >= 18) {
            if (checkOverflow(positive, output, 10, *ptr - 48)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                            "out of range");

              return FAIL;
            }
          }
          output = output * 10 + (*ptr - 48) * positive;
          ptr++;
        }
      }
      int move =
          eNum;  // times that decimal point should move foward or backward
      if (decimalPtr && num1Len > 0) {
        ptr = decimalPtr + 1;
        for (i = 1; i <= eNum && i <= num1Len; i++) {
          if (outputLen > 0 ||
              (*ptr - 48) > 0) {  // handle "0.00001e5", before reaching "1",
                                  // outputLen is always 0
            outputLen++;
          }
          if (outputLen >= 18) {
            // check overflow before calculate
            if (checkOverflow(positive, output, 10, *ptr - 48)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                            "out of range");

              return FAIL;
            }
          }
          output = output * 10 + (*ptr - 48) * positive;
          ptr++;
        }
        move -= (i - 1);  // (i-1) is either eNum or num1Len
      }

      // rounding ------------
      if (move > 0) {  // means eNum > num1Len, no rounding
        for (; move > 0; move--) {
          outputLen++;
          if (outputLen >= 18) {
            // check overflow before calculate
            if (checkOverflow(positive, output, 10, 0)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                            "out of range");

              return FAIL;
            }
          }
          output = output * 10;
        }
      } else {                 // move == 0, eNum <= num1Len
        if (eNum < num1Len) {  // rounding, "12.04567e2"
          if ((*ptr - 48 >= 5)) {
            if (outputLen == 19) {
              // check overflow before calculate
              if (checkOverflow(positive, output, 1, 1)) {
                EEPgErrorInfo::SetPgErrorInfo(
                    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "out of range");

                return FAIL;
              }
            }
            output += positive;
          }
        }
      }

    } else {                     // decimal point should move left
      if (eNum + num0Len < 0) {  // means (-1*eNum) > num0Len, "0.0e-1"
        return SUCCESS;
      } else if (num0Start && num0Len > 0) {
        ptr = num0Start;
        if (eNum + num0Len > 0) {  // means (-1*eNum) < num0Len
          for (i = 1; i <= num0Len + eNum; i++) {
            outputLen++;
            if (outputLen >= 18) {
              // check overflow before calculate
              if (checkOverflow(positive, output, 10, *ptr - 48)) {
                EEPgErrorInfo::SetPgErrorInfo(
                    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "out of range");

                return FAIL;
              }
            }
            output = output * 10 + (*ptr - 48) * positive;
            ptr++;
          }
        }
        if ((*ptr - 48 >= 5)) {  // rounding
          if (outputLen == 19) {
            // check overflow before calculate
            if (checkOverflow(positive, output, 1, 1)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                            "out of range");

              return FAIL;
            }
          }
          output += positive;
        }

      } else {
      }  // impossible, since num0Len must > 0
    }
  }
  return SUCCESS;
}

void deleteZero(double input, char *str, int32_t length) {
  for (int32_t i = length - 1; i > 0; i--) {
    if (str[i] == '\0') {
      continue;
    } else if (str[i] == '0') {
      str[i] = '\0';
    } else if (str[i] == '.') {
      str[i] = '\0';
      break;
    } else {
      break;
    }
  }
}

#define MAX_DOUBLE_STR_LENGTH 18
#define DEFAULT_PRECISION 16
KStatus doubleToStr(double input, char *str, int32_t length) {
  int d = numDigit(input);
  if (d <= MAX_DOUBLE_STR_LENGTH - 2) {
    int len = MAX_DOUBLE_STR_LENGTH < length? MAX_DOUBLE_STR_LENGTH : length;
    snprintf(str, len, "%.*f", DEFAULT_PRECISION, input);
    deleteZero(input, str, len);
  } else {
    int len = 0;
    if (d > 100)
      len = MAX_DOUBLE_STR_LENGTH - 7;  // e+100 ~ e+307
    else if (d > 10)  // means (d > MAX_DOUBLE_STR_LENGTH - 2 && d <= 100)
      len = MAX_DOUBLE_STR_LENGTH - 6;  // <e+99
    else  // means d <= 10, will never enter here, since this case was handled
          // by "if (d <= MAX_DOUBLE_STR_LENGTH - 2)"
      len = MAX_DOUBLE_STR_LENGTH - 5;  // <- this may be meaningless

    len = len < length? len : length;
    snprintf(str, len , "%.*e", DEFAULT_PRECISION, input);
    // return strlen(str);
  }
  return KStatus::SUCCESS;
}

int64_t strnatoi(char *num, int32_t len) {
  int64_t ret = 0, i, dig, base = 1;

  if (len > (int32_t)strlen(num)) {
    len = (int32_t)strlen(num);
  }

  if ((len > 2) && (num[0] == '0') && ((num[1] == 'x') || (num[1] == 'X'))) {
    for (i = len - 1; i >= 2; --i, base *= 16) {
      if (num[i] >= '0' && num[i] <= '9') {
        dig = (num[i] - '0');
      } else if (num[i] >= 'a' && num[i] <= 'f') {
        dig = num[i] - 'a' + 10;
      } else if (num[i] >= 'A' && num[i] <= 'F') {
        dig = num[i] - 'A' + 10;
      } else {
        return 0;
      }
      ret += dig * base;
    }
  } else {
    for (i = len - 1; i >= 0; --i, base *= 10) {
      if (num[i] >= '0' && num[i] <= '9') {
        dig = (num[i] - '0');
      } else {
        return 0;
      }
      ret += dig * base;
    }
  }

  return ret;
}

char *forward2TimeStringEnd(char *str) {
  int32_t i = 0;
  int32_t numOfSep = 0;

  while (str[i] != 0 && numOfSep < 2) {
    if (str[i++] == ':') {
      numOfSep++;
    }
  }

  while (str[i] >= '0' && str[i] <= '9') {
    i++;
  }

  return &str[i];
}

int32_t parseFraction(char *str, char **end) {
  int32_t i = 0;
  int64_t fraction = 0;

  const int32_t NANO_SEC_FRACTION_LEN = 9;

  int32_t factor[9] = {1,      10,      100,      1000,     10000,
                       100000, 1000000, 10000000, 100000000};
  int32_t times = 1;

  while (str[i] >= '0' && str[i] <= '9') {
    i++;
  }

  int32_t totalLen = i;
  if (totalLen <= 0) {
    return -1;
  }

  /* parse the fraction */
  /* only use the initial 3 bits */
  if (i >= NANO_SEC_FRACTION_LEN) {
    i = NANO_SEC_FRACTION_LEN;
  }
  times = NANO_SEC_FRACTION_LEN - i;

  fraction = strnatoi(str, i) * factor[times];
  *end = str + totalLen;

  return fraction;
}
char *kwdbStrpTime(const char *buf, const char *fmt, struct tm *tm) {
  return strptime(buf, fmt, tm);
}

KStatus parseTimezone(char *str, int64_t *tzOffset) {
  int64_t hour = 0;

  int32_t i = 0;
  if (str[i] != '+' && str[i] != '-') {
    return FAIL;
  }

  i++;

  int32_t j = i;
  while (str[j]) {
    if ((str[j] >= '0' && str[j] <= '9') || str[j] == ':') {
      ++j;
      continue;
    }

    return FAIL;
  }

  char *sep = strchr(&str[i], ':');
  if (sep != NULL) {
    int32_t len = (int32_t)(sep - &str[i]);

    hour = strnatoi(&str[i], len);
    i += len + 1;
  } else {
    hour = strnatoi(&str[i], 2);
    i += 2;
  }

  if (hour > 12 || hour < 0) {
    return FAIL;
  }

  // return error if there're illegal charaters after min(2 Digits)
  char *minStr = &str[i];
  if (minStr[1] != '\0' && minStr[2] != '\0') {
    return FAIL;
  }

  int64_t minute = strnatoi(&str[i], 2);
  if (minute > 59 || (hour == 12 && minute > 0)) {
    return FAIL;
  }

  if (str[0] == '+') {
    *tzOffset = -(hour * 3600 + minute * 60);
  } else {
    *tzOffset = hour * 3600 + minute * 60;
  }

  return SUCCESS;
}
/*
 * rfc3339 format:
 * 2013-04-12T15:52:01+08:00
 * 2013-04-12T15:52:01.123+08:00
 *
 * 2013-04-12T15:52:01Z
 * 2013-04-12T15:52:01.123Z
 *
 * iso-8601 format:
 * 2013-04-12T15:52:01+0800
 * 2013-04-12T15:52:01.123+0800
 */
KStatus parseTimeWithTz(KString timestr, k_int64 scale, k_int64 *time, char delim) {
  // int64_t factor = TSDB_TICK_PER_SECOND(timePrec);
  int64_t factor = 1000 * scale;
  int64_t tzOffset = 0;

  struct tm tm = {0};

  char *str;
  if (delim == 'T') {
    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL) {
    size_t pos = timestr.find('-');
    if (pos == 0) {
        pos = timestr.find('-', 1);
    }
    if (pos!= std::string::npos) {
        std::string num_str = timestr.substr(0, pos);
        int year = std::stoi(num_str);
        if (year > 2970 || year < 0) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                        "Timestamp/TimestampTZ out of range");
        }
    }
    return FAIL;
  }

  int64_t seconds = timegm(&tm);

  int64_t fraction = 0;
  str = forward2TimeStringEnd(timestr.data());

  if (!I64_SAFE_MUL_CHECK(seconds, factor)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "Timestamp/TimestampTZ out of range");
    return FAIL;
  }
  if ((str[0] == 'Z' || str[0] == 'z') && str[1] == '\0') {
    /* utc time, no millisecond, return directly*/
    *time = seconds * factor;
  } else if (str[0] == '.') {
    str += 1;
    if ((fraction = parseFraction(str, &str)) < 0) {
      return FAIL;
    }
    fraction /= (1000000 / scale);

    *time = seconds * factor + fraction;

    char seg = str[0];
    if (seg != 'Z' && seg != 'z' && seg != '+' && seg != '-') {
      return FAIL;
    } else if ((seg == 'Z' || seg == 'z') && str[1] != '\0') {
      return FAIL;
    } else if (seg == '+' || seg == '-') {
      // parse the timezone
      if (parseTimezone(str, &tzOffset) == -1) {
        return FAIL;
      }

      *time += tzOffset * factor;
    }

  } else if (str[0] == '+' || str[0] == '-') {
    *time = seconds * factor + fraction;

    // parse the timezone
    if (parseTimezone(str, &tzOffset) == -1) {
      return FAIL;
    }

    *time += tzOffset * factor;
  } else {
    return FAIL;
  }

  return SUCCESS;
}

bool checkTzPresent(KString &str) {
  int32_t len = str.length();
  char *seg = forward2TimeStringEnd(str.data());
  int32_t seg_len = len - (int32_t)(seg - str.c_str());

  char *c = &seg[seg_len - 1];
  for (int32_t i = 0; i < seg_len; ++i) {
    if (*c == 'Z' || *c == 'z' || *c == '+' || *c == '-') {
      return true;
    }
    c--;
  }
  return false;
}
time_t kwdbMktime(struct tm *timep) { return mktime(timep) + timep->tm_gmtoff; }

#define LEAP_YEAR_MONTH_DAY 29
static inline bool validateTm(struct tm *pTm) {
  if (pTm == NULL) {
    return false;
  }

  int32_t dayOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  int32_t year = pTm->tm_year + 1900;
  bool isLeapYear =
      ((year % 100) == 0) ? ((year % 400) == 0) : ((year % 4) == 0);

  if (isLeapYear && (pTm->tm_mon == 1)) {
    if (pTm->tm_mday > LEAP_YEAR_MONTH_DAY) {
      return false;
    }
  } else {
    if (pTm->tm_mday > dayOfMonth[pTm->tm_mon]) {
      return false;
    }
  }

  return true;
}

KStatus parseLocaltimeDst(KString timestr, k_int64 scale, k_int64 *utime, char delim) {
  *utime = 0;
  struct tm tm = {0};
  tm.tm_isdst = -1;

  char *str;
  int32_t len = timestr.length();
  if (delim == 'T') {
    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL || (((str - timestr.data()) < len) && (*str != '.')) ||
      !validateTm(&tm)) {
    // if parse failed, try "%Y-%m-%d" format
    str = kwdbStrpTime(timestr.c_str(), "%Y-%m-%d", &tm);
    if (str == NULL || (((str - timestr.data()) < len) && (*str != '.')) ||
        !validateTm(&tm)) {
      return FAIL;
    }
  }
  /* mktime will be affected by TZ, set by using kwdb_options */
  int64_t seconds = kwdbMktime(&tm);

  int64_t fraction = 0;
  if (*str == '.') {
    /* parse the second fraction part */
    if ((fraction = parseFraction(str + 1, &str)) < 0) {
      return FAIL;
    }
  }
  fraction /= (1000000 / scale);

  if (!I64_SAFE_MUL_CHECK(seconds, scale)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "Timestamp/TimestampTZ out of range");
  }
  // *utime = TSDB_TICK_PER_SECOND(timePrec) * seconds + fraction;
  *utime = scale * seconds + fraction;
  return SUCCESS;
}

KStatus kwdbParseTime(KString &timestr, k_int64 scale, k_int64 *utime) {
  /* parse datatime string in with tz */
  if (timestr.find('T') != std::string::npos) {
    if (checkTzPresent(timestr)) {
      return parseTimeWithTz(timestr, scale, utime, 'T');
    } else {
      return parseLocaltimeDst(timestr, scale, utime, 'T');
    }
  } else {
    if (checkTzPresent(timestr)) {
      return parseTimeWithTz(timestr, scale, utime, 0);
    } else {
      return parseLocaltimeDst(timestr, scale, utime, 0);
    }
  }
}

KStatus convertStringToTimestamp(KString inputData, k_int64 scale, k_int64 *timeVal) {
  // int32_t charLen = varDataLen(inputData);
  if (inputData.empty()) {
    return FAIL;
  }
  KStatus ret = kwdbParseTime(inputData, scale, timeVal);
  if (ret != SUCCESS) {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_INVALID_DATETIME_FORMAT,
        "parsing as type timestamp: missing required date fields");
  }
  return ret;
}
}  // namespace kwdbts
