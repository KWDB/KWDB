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

#include "cm_func.h"
#include <bits/stl_algo.h>

namespace kwdbts {

#ifdef K_DEBUG
// caller to make sure ctx is a valid pointer
void ReturnIntl(kwdbContext_p ctx) {
  char k_stack_var;
  k_uint32 stack_size = reinterpret_cast<char*>(&(k_stack_var)) - reinterpret_cast<char*>(ctx);
  // computing stack size
  if (ctx->max_stack_size < stack_size) {
    ctx->max_stack_size = stack_size;
  }
  Assert(ctx->frame_level > 0);
  ctx->frame_level--;
  ctx->func_end_time[ctx->frame_level] = kwdbts::getCurrentTime();
  ctx->file_name[ctx->frame_level][0] = '\0';
  ctx->func_name[ctx->frame_level][0] = '\0';
  return;
}
#endif

// Get timestamp from KWDB routine thread.
KTimestamp getCurrentTime() {
  return 0;  // ctx->conn->timer;
}

// Get file name only from file path
const char* GetFileName(const char* path) {
  if (nullptr == path) {
    return nullptr;
  }
  for (const char* p = path + strlen(path) - 1; p > path; p--) {
    if (*p == '/') {
      p++;
      return p;
    }
  }
  return path;
}

void SetObjectColNotNull(char *bitmap, k_uint32 col_index) {
  const k_uint32 bit = col_index & static_cast<k_uint32>(0x07);
  const k_uint32 idx = col_index >> 3;
  bitmap[idx] &= ~(1 << bit);
}

void SetObjectColNull(char *bitmap, k_uint32 col_index) {
  const k_uint32 bit = col_index & static_cast<k_uint32>(0x07);
  const k_uint32 idx = col_index >> 3;
  bitmap[idx] |= (1 << bit);
}

static const k_uint32 DAY_SECONDS = 86400;
static const k_uint32 YEAR_DAYS = 365;
static const k_uint8  MINUTE_SECONDS = 60;
static const k_uint8  HOUR_MINUTES = 60;
static const k_uint8  WEEK_DAYS = 7;
static const k_uint32 MAGIC_NUMBER_1 = 102032;
static const k_uint32 MAGIC_NUMBER_2 = 102035;
static const k_uint32 MAGIC_NUMBER_3 = 25568;
static const k_uint32 MAGIC_NUMBER_4 = 146097;
static const k_uint32 MAGIC_NUMBER_5 = 153;
static const k_uint32 MAGIC_NUMBER_6 = 1461;

void ToGMT(time_t ts, tm &tm) {
  tm.tm_isdst = 0;
  time_t days = ts / DAY_SECONDS;
  k_int32 seconds = ts % DAY_SECONDS;

  if ((int)seconds < 0) {
    --days;
    seconds += 86400;
  }

  tm.tm_sec = seconds % MINUTE_SECONDS;
  seconds /= MINUTE_SECONDS;
  tm.tm_min = seconds % HOUR_MINUTES;
  tm.tm_hour = seconds / HOUR_MINUTES;

  time_t t1, t2;
  t2 = (days + 4) % WEEK_DAYS;
  if (t2 < 0) {
    t2 += WEEK_DAYS;
  }
  tm.tm_wday = t2;
  t1 = (days << 2) + MAGIC_NUMBER_1;
  t2 = t1 / MAGIC_NUMBER_4;
  if (t1 % MAGIC_NUMBER_4 < 0) {
    --t2;
  }
  --t2;
  days += t2;
  t2 >>= 2;
  days -= t2;
  t2 = (days << 2) + MAGIC_NUMBER_2;
  t1 = t2 / MAGIC_NUMBER_6;
  if (t2 % MAGIC_NUMBER_6 < 0) {
    --t1;
  }
  k_uint32 year_days = days - YEAR_DAYS * t1 - (t1 >> 2) + MAGIC_NUMBER_3;

  k_uint32 num;
  num = year_days * 5 + 8;
  tm.tm_mon = num / MAGIC_NUMBER_5;
  num %= MAGIC_NUMBER_5;

  tm.tm_mday = 1 + num / 5;
  if (tm.tm_mon >= 12) {
    tm.tm_mon -= 12;
    ++t1;
    year_days -= YEAR_DAYS + 1;
  } else {
    if (!((t1 & 3) == 0 && (sizeof(time_t) <= 4 || t1 % 100 != 0 || (t1 + 300) % 400 == 0))) {
      --year_days;
    }
  }
  tm.tm_yday = year_days;
  tm.tm_year = t1;
  return;
}

std::string parseUnicode2Utf8(const std::string &str) {
  std::string utf8str;
  for (size_t i = 0; i < str.size(); i++) {
    if (str[i] == '\\') {
      if (i + 10 <= str.size() && str[i + 1] == 'U') {
        // Parse unicode escape sequences
        int codepoint = std::stoi(str.substr(i + 2, 8), nullptr, 16);
        i += 9;
        if (codepoint <= 0x7F) {
          utf8str += static_cast<char>(codepoint);
        } else if (codepoint <= 0x7FF) {
          utf8str += static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F));
          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
        } else if (codepoint <= 0xFFFF) {
          utf8str += static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F));
          utf8str += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
        } else if (codepoint <= 0x10FFFF) {
          utf8str += static_cast<char>(0xF0 | ((codepoint >> 18) & 0x07));
          utf8str += static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F));
          utf8str += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
        } else {
          // error
          return str;
        }
      } else if (i + 4 < str.size() && str[i + 1] == 'u') {
        // Parse unicode escape symbols
        int codepoint = std::stoi(str.substr(i + 2, 4), nullptr, 16);
        i += 5;
        if (codepoint <= 0x7F) {
          utf8str += static_cast<char>(codepoint);
        } else if (codepoint <= 0x7FF) {
          utf8str += static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F));
          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
        } else if (codepoint <= 0xFFFF) {
          utf8str += static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F));
          utf8str += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
          utf8str += static_cast<char>(0x80 | (codepoint & 0x3F));
        } else {
          // error
          return str;
        }
      } else if (i + 1 < str.size()) {
        if (str[i + 1] != '\'') {
          utf8str += str[i];
        }
        i++;
        if (str[i] != '\\') {
          utf8str += str[i];
        }
      } else {
        utf8str += str[i];
      }
    } else {
      utf8str += str[i];
    }
  }
  utf8str.erase(std::remove(utf8str.begin(), utf8str.end(), '\0'), utf8str.end());
  return utf8str;
}

std::string parseHex2String(const std::string &hexStr) {
  std::string asciiStr;
  if ((hexStr.substr(0, 2) != "\\x")) {
    return hexStr;
  }
  for (size_t i = 2; i < hexStr.length(); i += 2) {
    // extract two characters (hexadecimal numbers)
    std::string hexByte = hexStr.substr(i, 2);
    // convert hexadecimal numbers to integers
    int value = std::stoi(hexByte, nullptr, 16);
    // Convert integers to characters and add them to the result string
    asciiStr += static_cast<char>(value);
  }
  return asciiStr;
}

}  //  namespace kwdbts
