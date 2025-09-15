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

#include <arpa/inet.h>
#include <cctype>
#include <cfloat>
#include <cmath>
#include <cstring>
#include "utils/string_utils.h"
#include "utils/big_table_utils.h"
#include "cm_func.h"

#if __cplusplus > 199711L
#define register      // Deprecated in C++11.
#endif  // #if __cplusplus > 199711L

size_t mmap_strlcpy(char *dst, const char *src, size_t siz) {
  char *d = dst;
  const char *s = src;
  size_t n = 0;

  while (n < siz) {
    if ((*d++ = *s++) == 0)
      break;
    n++;
  }
  /* Not enough room in dst, add NUL */
  if (n == siz) {
    *d = '\0'; /* NUL-terminate dst */
  }
  return n;
}

size_t CHARcpy(char *dst, const char *src, size_t siz) {
  char *d = dst;
  const char *s = src;
  size_t n = 0;

  while (n < siz) {
    if ((*d++ = *s++) == 0)
      break;
    n++;
  }
  return n;
}

int dataTypeMaxTextLen(const AttributeInfo &col) {
  switch (col.type) {
    case STRING:
      if (col.max_len == 0)
        return STRING_MAX_LEN;
      return col.max_len;
//    case RAWSTRING:
    case STRING_CONST:
    case CHAR:
      return col.size;
    case INT32:
    case TIMESTAMP:
      return MAX_INT32_STR_LEN;
    case INT64:
    case ROWID:
    case TIMESTAMP64:
      return MAX_INT64_STR_LEN;
    case FLOAT:
      return MAX_DOUBLE_STR_LEN;
    case DOUBLE:
      return MAX_DOUBLE_STR_LEN;
    case VARSTRING:
    case BINARY:
    case VARBINARY:
      if (col.max_len == 0)
        return VARSTRING_MAX_LEN;
      return col.max_len;
    case DATE32:            // yyyy-mm-dd
      return 10;
    case DATETIMEDOS:
    case DATETIME32:        // yyyy-mm-dd hh:mm:ss
    case DATETIME64:
      return 19;
    case INT16:
      return MAX_INT16_STR_LEN;
    case BOOL:
    case BYTE:
    case INT8:
      return MAX_INT8_STR_LEN;
    case TIME:              // -????00:00:00 (2^31/3600 = 596523)
      return MAX_TIME_STR_LEN;
    case TIME64:
      return MAX_TIME64_STR_LEN;
    }
    return STRING_MAX_LEN;
}

bool isNULL(char *s) {
  //to-do(allison&thomas): distinguish  and ''
  if (s == NULL || s == nullptr || s == 0x0) {
    return true;
  }
  char c = *s;
  if (c == 'n' || c == 'N') {
    c = s[1];
    if ((c == 'u') || (c == 'U')) {
      c = s[2];
      if ((c == 'l') || (c == 'L')) {
        c = s[3];
        if (((c == 'l') || (c == 'L')) && s[4] == 0x0) {
          return true;
        }
      }
    }
  }
  return false;
}

int hexToInt(char ch) {
  if (ch >= '0' && ch <= '9')
    return ch - '0';
  if (ch >= 'A' && ch <= 'F')
    return ch - 'A' + 10;
  if (ch >= 'a' && ch <= 'f')
    return ch - 'a' + 10;
  return -1;
}

char intToHex(uint32_t i) {
  if (i < 10)
    return (char )(i + '0');
  return (char )((i - 10) + 'A');
}

// Return length of binary string.
int unHex(const char *s, unsigned char *data, int max_len) {
  int data_len = 0;
  int b;
  int v;
  const char *os = s; // original string
  char c = *s++;
  int is_even = false;

  if (c == '0') {                        // 0x01AF | 0x01af
    if (*s == 'x') {
      s++;
    } else {
      v = 0;
      is_even = true;
    }

    while ((c = *s++) != '\0') {
      if ((b = hexToInt(c)) < 0)
        return -1;  // hex error
      if (is_even) {
        data[data_len++] = (unsigned char)(v | b);
        if (data_len >= max_len) {
          if (*s != '\0')
            return -1;
          return data_len;
        }
        is_even = false;
      } else {
        v = b << 4;
        is_even = true;
      }
    }
  } else if (c == 'x' || c == 'X') {    // X'01AF' | x'01af'
    if (*s++ != '\'')
      return -1;  // hex error
    while ((c = *s++) != '\0' && c != '\'') {
       if ((b = hexToInt(c)) < 0)
         return -1;  // hex error
       if (is_even) {
         data[data_len++] = (unsigned char)(v | b);
         if (data_len >= max_len)
           return data_len;
         is_even = false;
       } else {
         v = b << 4;
         is_even = true;
       }
     }
  } else if (c == '\0') {
    return 0;
  } else {
    // assume null-terminated string input
    int len = strlen(os);
    if (len > max_len)
      return -1;
    memcpy((char *)data, os, len);
    return len;
  }
  if (is_even)
    data[data_len++] = (unsigned char)v;
  return data_len;
}


// the hex result will be reversed. ex: 1234 will be "3930", but actually it should be "3039"
int binaryToHexString(unsigned char *input, char *s, int32_t len) {
  char *hs = s;
  if (len > 0) {
    for (int32_t i = 0; i < len; ++i) {
      uint32_t v = (uint32_t)*input++;
      *hs++ = intToHex((v >> 4));
      *hs++ = intToHex((v & 0x0F));
    }
  }
  return hs - s;
}

namespace kwdbts {

void DoubleFormatToString::setMinMax() {
  max_v_ = (pow(10, MAX_DOUBLE_STR_LEN - 1 - precision_));
  min_v_ = -(pow(10, MAX_DOUBLE_STR_LEN - 2 - precision_));
}

DoubleFormatToString::DoubleFormatToString(int precision, bool is_rm_tz) {
  if (precision >  MAX_PRECISION)
    precision_ = MAX_PRECISION;
  else if (precision < 0)
    precision_ = kwdbts::EngineOptions::precision();
  else
    precision_ = precision;
  setMinMax();
  is_remove_trailing_zeros_ = is_rm_tz;
}

DoubleFormatToString::DoubleFormatToString(const DoubleFormatToString &rhs):
  precision_(rhs.precision_) {
  setMinMax();
  is_remove_trailing_zeros_ = rhs.is_remove_trailing_zeros_;
}

DoubleFormatToString::~DoubleFormatToString() {}

char double_fmt[] = "%.*f";

// return string length after removing trailing zeros
int removeTrailingZeros(char *s) {
  char *p = strchr(s, '.');
  if (p != NULL) {
    while (*p != '\0')    // go to string end NULL.
      p++;
    p--;
    while (*p == '0') // remove trailing zeros.
      *p-- = '\0';
    if (*p == '.') {  // if all decimals were zeros, remove ".".
      *p = '\0';
    }
    return  (*p == '\0') ? (int)(p - s) : (int)(p - s + 1);
  }
  return strlen(s); // s without '.'
}


int DoubleFormatToString::toString(char *str, double v) {
  if (v < max_v_ && v > min_v_) {
    sprintf(str, double_fmt, precision_, v);
    if (is_remove_trailing_zeros_)
      return removeTrailingZeros(str);
    return strlen(str);
  } else {
    if (isfinite(v)) {
      int d = numDigit(v);
      if (d <= MAX_DOUBLE_STR_LEN - 2) {
        sprintf(str, double_fmt, MAX_DOUBLE_STR_LEN - d - 1, v);
        return removeTrailingZeros(str);
      } else {
    	/*
    	 * if MAX_DOUBLE_STR_LEN is 22, it means it allow string like "1.824679000000000e+100", which has length 22
    	 * its d should be 101
    	 * len means the length between '.' and 'e', ex: "824679000000000" of "1.824679000000000e+100" has length 15
    	*/
        int len = 0;
        if (d > 100)
          len = MAX_DOUBLE_STR_LEN - 7; // e+100 ~ e+307
        else if (d > 10) // means (d > MAX_DOUBLE_STR_LEN - 2 && d <= 100)
          len = MAX_DOUBLE_STR_LEN - 6; // <e+99
        else // means d <= 10, will never enter here, since this case was handled by "if (d <= MAX_DOUBLE_STR_LEN - 2)"
          len = MAX_DOUBLE_STR_LEN - 5; // <- this may be meaningless

        sprintf(str, "%.*e", len, v);
        return strlen(str);
      }
    } else if (isinf(v)) {
      if (v >= 0) {
        mmap_strlcpy(str, kwdbts::s_inf.c_str(), 3);
        return 3;
      } else {
        mmap_strlcpy(str, kwdbts::s_n_inf.c_str(), 4);
        return 4;
      }
    } else {
      mmap_strlcpy(str, kwdbts::s_NULL.c_str(), 4);
      return 4;
    }
  }
}

} // namespace kwdbts
