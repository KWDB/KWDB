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
#include "ee_pg_result.h"

#include<cmath>
#include <bitset>

#include "er_api.h"
#include "string"
#include "ee_field.h"
#include "ee_encoding.h"
#include "ee_string_info.h"

namespace kwdbts {

KStatus pg_result_data(kwdbContext_p ctx, EE_StringInfo strinfo,
                       k_uint16 cols, Field **fields) {
  EnterFunc();
  k_uint32 temp_len = strinfo->len;
  char ts_format_buf[32] = {0};
  char *temp_addr = nullptr;

  if (ee_appendBinaryStringInfo(strinfo, "D0000", 5) != SUCCESS) {
    Return(FAIL);
  }

  // write column count
  if (ee_sendint(strinfo, cols, 2) != SUCCESS) {
    Return(FAIL);
  }
  std::string val_str;
  for (k_uint16 i = 0; i < cols; ++i) {
    // binary\varbinary
    std::string bytes_f;
    Field *f = fields[i];
    if (f->is_nullable()) {
      // < 0 indicates the value is NULL
      if (ee_sendint(strinfo, -1, 4) != SUCCESS) {
        Return(FAIL);
      }
      continue;
    }
    //    // get col value
//    val_str = f->ValStr();
    KWDBTypeFamily return_type = f->get_return_type();
    switch (return_type) {
      case KWDBTypeFamily::BoolFamily: {
        // get col value
        k_int64 val = f->ValInt();
        // get col value
        if (val == 0) {
          val_str = "f";
        } else {
          val_str = "t";
        }
        // write size
        if (ee_sendint(strinfo, val_str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write value
        if (ee_appendBinaryStringInfo(strinfo, val_str.data(), val_str.length()) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::StringFamily: {
        String val = f->ValStr();
        // write size
        if (ee_sendint(strinfo, val.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write value
        if (ee_appendBinaryStringInfo(strinfo, val.c_str(), val.length()) !=
            SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::BytesFamily: {
        // get col
        String val = f->ValStr();
        val_str = std::string(val.ptr_, val.length_);
        bytes_f.append("\\x");
        char tmp[3] = {0};
        for (u_char c : val_str) {
          snprintf(tmp, sizeof(tmp), "%02x", c);
          bytes_f.append(tmp, 2);
        }
        if (ee_sendint(strinfo, bytes_f.size(), 4) != SUCCESS) {
          Return(FAIL);
        }
        if (ee_appendBinaryStringInfo(strinfo, bytes_f.c_str(), bytes_f.size()) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::TimestampFamily:
      case KWDBTypeFamily::TimestampTZFamily: {
       // format timestamps as strings
        time_t ms = f->ValInt();
        time_t sec;
        if (ms < 0) {
          sec = floor(ms / 1000.0);
          ms = (ms % 1000) ? (ms % 1000) + 1000 : 0;
        } else {
          sec = ms / 1000;
          ms = ms % 1000;
        }

        if (return_type == KWDBTypeFamily::TimestampTZFamily) {
          sec += ctx->timezone * 3600;
        }
        tm ts{};
        ToGMT(sec, ts);
        strftime(ts_format_buf, 32, "%F %T", &ts);
        k_uint8 format_len = strlen(ts_format_buf);
        if (ms != 0) {
          snprintf(&ts_format_buf[format_len], sizeof(char[5]), ".%03ld", ms);
        }
        format_len = strlen(ts_format_buf);
        // encode zone info
        if (return_type == KWDBTypeFamily::TimestampTZFamily) {
          const char *timezoneFormat;
          auto timezoneAbs = std::abs(ctx->timezone);
          if (ctx->timezone >= 0) {
            timezoneFormat = "+%02d:00";
            timezoneAbs = ctx->timezone;
          } else {
            timezoneFormat = "-%02d:00";
          }
          snprintf(&ts_format_buf[format_len], sizeof(char[7]), timezoneFormat, timezoneAbs);
        }
        format_len = strlen(ts_format_buf);
        // write size
        if (ee_sendint(strinfo, format_len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write value
        if (ee_appendBinaryStringInfo(strinfo, ts_format_buf, format_len) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::FloatFamily: {
        if (roachpb::FLOAT == f->get_storage_type()) {
          k_char buf[30] = {0};
          double d = f->ValReal();
          k_int32 n = snprintf(buf, sizeof(buf), "%.6f", d);
          // write size
          if (ee_sendint(strinfo, n, 4) != SUCCESS) {
            Return(FAIL);
          }
          if (std::isnan(d)) {
            buf[0] = 'N';
            buf[1] = 'a';
            buf[2] = 'N';
            n = 3;
          }
          // write value
          if (ee_appendBinaryStringInfo(strinfo, buf, n) != SUCCESS) {
            Return(FAIL);
          }
        } else {
          k_char buf[30] = {0};
          double d = f->ValReal();
          k_int32 n = snprintf(buf, sizeof(buf), "%.17g", d);
          if (std::isnan(d)) {
            buf[0] = 'N';
            buf[1] = 'a';
            buf[2] = 'N';
            n = 3;
          }
          // write size
          if (ee_sendint(strinfo, n, 4) != SUCCESS) {
            Return(FAIL);
          }
          // write value
          if (ee_appendBinaryStringInfo(strinfo, buf, n) != SUCCESS) {
            Return(FAIL);
          }
        }
      }
        break;
      case KWDBTypeFamily::DecimalFamily: {
        if (f->get_storage_type() != roachpb::BIGINT) {
          k_char buf[30] = {0};
          double d = f->ValReal();
          k_int32 n = snprintf(buf, sizeof(buf), "%.8g", d);
          // write size
          if (ee_sendint(strinfo, n, 4) != SUCCESS) {
            Return(FAIL);
          }
          // write value
          if (ee_appendBinaryStringInfo(strinfo, buf, n) != SUCCESS) {
            Return(FAIL);
          }
        } else {
          // write size
          k_int64 val = f->ValInt();
          char val_char[32];
          snprintf(val_char, sizeof(val_char), "%ld", val);
          if (ee_sendint(strinfo, strlen(val_char), 4) != SUCCESS) {
            Return(FAIL);
          }
          // write value
          if (ee_appendBinaryStringInfo(strinfo, val_char, strlen(val_char)) != SUCCESS) {
            Return(FAIL);
          }
        }
      }
        break;
      case KWDBTypeFamily::IntervalFamily: {
        time_t ms = f->ValInt();
        char buf[32] = {0};
        struct KWDuration duration;
        size_t n;
        switch (f->get_storage_type()) {
          case roachpb::TIMESTAMP_MICRO:
          case roachpb::TIMESTAMPTZ_MICRO:
            n = duration.format_pg_result(ms, buf, 32, 1000);
            break;
          case roachpb::TIMESTAMP_NANO:
          case roachpb::TIMESTAMPTZ_NANO:
            n = duration.format_pg_result(ms, buf, 32, 1);
            break;
          default:
            n = duration.format_pg_result(ms, buf, 32, 1000000);
            break;
        }

        // write size
        if (ee_sendint(strinfo, n, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write value
        if (ee_appendBinaryStringInfo(strinfo, buf, n) != SUCCESS) {
          Return(FAIL);
        }

        break;
      }
      case KWDBTypeFamily::DateFamily: {
        String str = f->ValStr();
        // write size
        if (ee_sendint(strinfo, str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write value
        if (ee_appendBinaryStringInfo(strinfo, str.c_str(), str.length()) != SUCCESS) {
          Return(FAIL);
        }
        break;
      }
      default: {
        // write size
        k_int64 val = f->ValInt();
        char val_char[32];
        snprintf(val_char, sizeof(val_char), "%ld", val);
        if (ee_sendint(strinfo, strlen(val_char), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write value
        if (ee_appendBinaryStringInfo(strinfo, val_char, strlen(val_char)) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
    }
  }

  temp_addr = &strinfo->data[temp_len + 1];
  k_uint32 n32 = be32toh(strinfo->len - temp_len - 1);
  memcpy(temp_addr, &n32, 4);
  Return(SUCCESS);
}

}  // namespace kwdbts
