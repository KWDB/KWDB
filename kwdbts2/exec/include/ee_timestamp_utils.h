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
#pragma once

#include <regex>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>

#include "kwdb_type.h"

namespace kwdbts {

#define GET_HIGHER_PRECISION_TIME_TYPE(a, b) (a > b ? a : b)

inline std::string replaceTimeUnit(KString timestring) {
    static const std::vector<std::pair<KString, KString>> replacements = {
        {"nanoseconds", "ns"}, {"nanosecond", "ns"}, {"nsecs", "ns"}, {"nsec", "ns"},
        {"microseconds", "us"}, {"microsecond", "us"}, {"usecs", "us"}, {"usec", "us"},
        {"milliseconds", "ms"}, {"millisecond", "ms"}, {"msecs", "ms"}, {"msec", "ms"},
        {"seconds", "s"}, {"second", "s"}, {"secs", "s"}, {"sec", "s"},
        {"minutes", "m"}, {"minute", "m"}, {"mins", "m"}, {"min", "m"},
        {"hours", "h"}, {"hour", "h"}, {"hrs", "h"}, {"hr", "h"},
        {"days", "d"}, {"day", "d"},
        {"weeks", "w"}, {"week", "w"},
        {"months", "n"}, {"month", "n"}, {"mons", "n"}, {"mon", "n"},
        {"years", "y"}, {"year", "y"}, {"yrs", "y"}, {"yr", "y"},
        {"millennia", "millennium"}, {"millenniums", "millennium"},
        {"decades", "decade"},
        {"centuries", "century"}
    };

    std::transform(timestring.begin(), timestring.end(), timestring.begin(),
                  [](unsigned char c) { return std::tolower(c); });
    std::string result = timestring;
    for (const auto& pair : replacements) {
      if (timestring.find(pair.first) != std::string::npos) {
        result = std::regex_replace(result, std::regex(pair.first), pair.second);
        break;
      }
    }
    return result;
}

inline k_int64 getIntervalSeconds(KString timestring, k_bool& var_interval,
                                  k_bool& year_bucket, KString* in_unit,
                                  std::string& error_info, bool max_to_week) {
  // std::string timestring = {args_[1]->ValStr().getptr(),
  //                           args_[1]->ValStr().length_};
  k_uint32 code = ERRCODE_INVALID_DATETIME_FORMAT;
  KString intervalStr;
  KString unit;
  do {
    if (timestring.length() < 2) {
      error_info =
          "invalid input interval time.";
      code = ERRCODE_INVALID_PARAMETER_VALUE;
      break;
    }
    unit = timestring.back();
    if (unit != "y" && unit != "n" && unit != "w" && unit != "d" &&
        unit != "h" && unit != "m" && unit != "s") {
      error_info = "interval: invalid input syntax: " + timestring + ".";
      break;
    }
    char first_unit = timestring[timestring.length() - 2];
    if (unit == "s" &&
        (first_unit == 'm' || first_unit == 'u' || first_unit == 'n')) {
      intervalStr = timestring.substr(0, timestring.size() - 2);
      unit = first_unit + unit;
    } else {
      intervalStr = timestring.substr(0, timestring.size() - 1);
    }
    if (max_to_week && (unit == "y" || unit == "n")) {
      error_info = "invalid input syntax: " + timestring + ".";
      break;
    }
    try {
      if (std::stol(intervalStr) <= 0) {
        error_info = "second arg should be a positive interval.";
        code = ERRCODE_INVALID_PARAMETER_VALUE;
        break;
      }
    } catch (...) {
      error_info = "interval: invalid input syntax: " + timestring + ".";
      break;
    }
    for (char c : intervalStr) {
      if (c > '9' || c < '0') {
        error_info =
          "invalid input interval time.";
        code = ERRCODE_INVALID_PARAMETER_VALUE;
        break;
      }
    }
  } while (false);
  *in_unit = unit;
  if (error_info != "") {
    EEPgErrorInfo::SetPgErrorInfo(code, error_info.c_str());
    return 0;
  }
  k_int64 interval_seconds = stoll(intervalStr);

  // Convert interval to milliseconds based on unit
  if (unit == "y") {
    year_bucket = true;
    var_interval = true;
    // Do not convert interval_seconds_ as it is a variable interval
  } else if (unit == "n") {
    var_interval = true;
    // Do not convert interval_seconds_ as it is a variable interval
  } else if (unit == "s") {
    interval_seconds *= MILLISECOND_PER_SECOND;
  } else if (unit == "m") {
    interval_seconds *= MILLISECOND_PER_MINUTE;
  } else if (unit == "h") {
    interval_seconds *= MILLISECOND_PER_HOUR;
  } else if (unit == "d") {
    interval_seconds *= MILLISECOND_PER_DAY;
  } else if (unit == "w") {
    interval_seconds *= MILLISECOND_PER_WEEK;
  }

  return interval_seconds;
}

inline roachpb::DataType getTimeFieldType(roachpb::DataType var_type,
                                          KString unit, k_int64* time_diff,
                                          k_int64* type_scale,
                                          k_bool* type_scale_multi_or_divde) {
  roachpb::DataType type;
  *(time_diff) *= 3600000;
  if (unit == "ns") {
    *(time_diff) *= 1000000;
    if (var_type == roachpb::DataType::TIMESTAMP ||
        var_type == roachpb::DataType::TIMESTAMP_MICRO ||
        var_type == roachpb::DataType::TIMESTAMP_NANO) {
      type = roachpb::DataType::TIMESTAMP_NANO;
    } else {
      type = roachpb::DataType::TIMESTAMPTZ_NANO;
    }
    if (var_type == roachpb::DataType::TIMESTAMP_MICRO ||
        var_type == roachpb::DataType::TIMESTAMPTZ_MICRO) {
      *(type_scale) = 1000;
      *(type_scale_multi_or_divde) = KTRUE;  // multi
    } else if (var_type != roachpb::DataType::TIMESTAMP_NANO &&
               var_type != roachpb::DataType::TIMESTAMPTZ_NANO) {
      *(type_scale) = 1000000;
      *(type_scale_multi_or_divde) = KTRUE;  // multi
    }
  } else if (unit == "us") {
    *(time_diff) *= 1000;
    if (var_type == roachpb::DataType::TIMESTAMP ||
        var_type == roachpb::DataType::TIMESTAMP_MICRO ||
        var_type == roachpb::DataType::TIMESTAMP_NANO) {
      type = roachpb::DataType::TIMESTAMP_MICRO;
    } else {
      type = roachpb::DataType::TIMESTAMPTZ_MICRO;
    }
    if (var_type == roachpb::DataType::TIMESTAMP_NANO ||
        var_type == roachpb::DataType::TIMESTAMPTZ_NANO) {
      *(type_scale) = 1000;
      *(type_scale_multi_or_divde) = KFALSE;  // devide
    } else if (var_type != roachpb::DataType::TIMESTAMP_MICRO &&
               var_type != roachpb::DataType::TIMESTAMPTZ_MICRO) {
      *(type_scale) = 1000;
      *(type_scale_multi_or_divde) = KTRUE;  // multi
    }
    // Convert first arg to milliseconds
  } else {
    if (var_type == roachpb::DataType::TIMESTAMP ||
        var_type == roachpb::DataType::TIMESTAMP_MICRO ||
        var_type == roachpb::DataType::TIMESTAMP_NANO) {
      type = roachpb::DataType::TIMESTAMP;
    } else {
      type = roachpb::DataType::TIMESTAMPTZ;
    }
    if (var_type == roachpb::DataType::TIMESTAMP_NANO ||
        var_type == roachpb::DataType::TIMESTAMPTZ_NANO) {
      *(type_scale) = 1000000;
      *(type_scale_multi_or_divde) = KFALSE;  // devide
    } else if (var_type == roachpb::DataType::TIMESTAMP_MICRO ||
               var_type == roachpb::DataType::TIMESTAMPTZ_MICRO) {
      *(type_scale) = 1000;
      *(type_scale_multi_or_divde) = KFALSE;  // devide
    }
  }
  return type;
}
inline CKTime getCKTime(k_int64 val, roachpb::DataType type, k_int8 timezone) {
  CKTime ck_time;
  k_int64 val_interval;
  switch (type) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
      val_interval = 1000;
      break;
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
      val_interval = 1000000;
      break;
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      val_interval = 1000000000;
      break;
    default:
      val_interval = 1000;
      break;
  }
  if (val < 0 && val % val_interval) {
    ck_time.t_timespec.tv_sec = val / val_interval - 1;
    ck_time.t_timespec.tv_nsec =
        ((val % val_interval) + val_interval) * (1000000000 / val_interval);
  } else {
    ck_time.t_timespec.tv_sec = (val / val_interval);
    ck_time.t_timespec.tv_nsec =
        val % val_interval * (1000000000 / val_interval);
  }
  ck_time.UpdateSecWithTZ(timezone);
  return ck_time;
}

inline bool convertTimePrecision(k_int64* val, roachpb::DataType in_type,
                                 roachpb::DataType out_type) {
  if (in_type == out_type) {
    return KTRUE;
  }
  switch (in_type) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
      switch (out_type) {
        case roachpb::TIMESTAMP:
        case roachpb::TIMESTAMPTZ:
          return KTRUE;
        case roachpb::TIMESTAMP_MICRO:
        case roachpb::TIMESTAMPTZ_MICRO:
          if (I64_SAFE_MUL_CHECK(*val, 1000)) {
            *val *= 1000;
            return KTRUE;
          } else {
            return KFALSE;
          }
        case roachpb::TIMESTAMP_NANO:
        case roachpb::TIMESTAMPTZ_NANO:
          if (I64_SAFE_MUL_CHECK(*val, 1000000)) {
            *val *= 1000000;
            return KTRUE;
          } else {
            return KFALSE;
          }
        default:
          return KFALSE;
      }
      break;
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
      switch (out_type) {
        case roachpb::TIMESTAMP:
        case roachpb::TIMESTAMPTZ:
          if (*val != 0) {
            *val /= 1000;
          }
          return KTRUE;
        case roachpb::TIMESTAMP_MICRO:
        case roachpb::TIMESTAMPTZ_MICRO:
          return KTRUE;
        case roachpb::TIMESTAMP_NANO:
        case roachpb::TIMESTAMPTZ_NANO:
          if (I64_SAFE_MUL_CHECK(*val, 1000)) {
            *val *= 1000;
            return KTRUE;
          } else {
            return KFALSE;
          }
        default:
          return KFALSE;
      }
      break;
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      switch (out_type) {
        case roachpb::TIMESTAMP:
        case roachpb::TIMESTAMPTZ:
          if (*val != 0) {
            *val /= 1000000;
          }
          return KTRUE;
        case roachpb::TIMESTAMP_MICRO:
        case roachpb::TIMESTAMPTZ_MICRO:
          if (*val != 0) {
            *val /= 1000;
          }
          return KTRUE;
        case roachpb::TIMESTAMP_NANO:
        case roachpb::TIMESTAMPTZ_NANO:
          return KTRUE;
        default:
          return KFALSE;
      }
      break;
    default:
      break;
  }
  return KFALSE;
}

inline k_double64 getExtract(k_int64 intvalue, String strvalue, roachpb::DataType datetype, KWDBTypeFamily returntype,
                             k_int8 timezone) {
  CKTime ckTime = getCKTime(intvalue, datetype, timezone);
  k_int64 res = 0;
  struct tm ltm;
  if (returntype == KWDBTypeFamily::TimestampTZFamily) {
    ckTime.t_timespec.tv_sec += ckTime.t_abbv;
  }
  ToGMT(ckTime.t_timespec.tv_sec, ltm);
  KString item = replaceTimeUnit({strvalue.getptr(), strvalue.length_});
  if (item == "ns") {
    return ckTime.t_timespec.tv_nsec;
  } else if (item == "us") {
    return static_cast<double>(ckTime.t_timespec.tv_nsec) / 1000;
  } else if (item == "ms") {  //  second field, in milliseconds, containing decimals
    return static_cast<double>(ckTime.t_timespec.tv_nsec) / 1000000;
  } else if (item == "s") {  // containing decimals，in
                             // secondes，containing decimals
    return static_cast<double>(ltm.tm_sec) + ckTime.t_timespec.tv_nsec / 1000000000;
  } else if (item == "epoch") {  // seconds since 1970-01-01 00:00:00 UTC
    res = ckTime.t_timespec.tv_sec;
  } else if (item == "m") {  // minute (0-59)
    res = ltm.tm_min;
  } else if (item == "h") {  // hour (0-23)
    res = ltm.tm_hour;
  } else if (item == "d") {  // on the day of the month (1-31)
    res = ltm.tm_mday;
  } else if (item == "dow" || item == "dofweek") {  // what day of the week, Sunday (0) to
                                                    // Saturday (6)
    res = ltm.tm_wday;
  } else if (item == "isodow") {  // Day of the week based on ISO 8601, Monday
                                  // (1) to Sunday (7)
    if (ltm.tm_wday == 0) {
      res = 7;
    } else {
      res = ltm.tm_wday;
    }
  } else if (item == "doy" || item == "dofyear") {  // on which day of the year, ranging from
                                                    // 1 to 366
    res = ltm.tm_yday + 1;
  } else if (item == "julian") {
    if (ltm.tm_mon > 2) {
      ltm.tm_mon++;
      ltm.tm_year += 4800;
    } else {
      ltm.tm_mon += 13;
      ltm.tm_year += 4799;
    }

    k_int64 century = ltm.tm_year / 100;
    res = ltm.tm_year * 365 - 32167;
    res += ltm.tm_year / 4 - century + century / 4;
    res += 7834 * ltm.tm_mon / 256 + ltm.tm_yday;
    res += ltm.tm_hour * 3600 + ltm.tm_min * 60 + ltm.tm_sec;
    return static_cast<double>(res) + ckTime.t_timespec.tv_nsec / 1000000000;
  } else if (item == "w") {  // ISO 8601
    k_int32 weekday = 0;
    if (ltm.tm_wday == 0) {
      weekday = 7;
    } else {
      weekday = ltm.tm_wday;
    }
    k_int64 wd = ltm.tm_yday + 4 - weekday;
    res = (wd / 7) + 1;
  } else if (item == "n") {  // month，1-12
    res = ltm.tm_mon + 1;
  } else if (item == "quarter") {  // quarter
    if (ltm.tm_mon <= 2) {
      res = 1;
    } else if (ltm.tm_mon <= 5) {
      res = 2;
    } else if (ltm.tm_mon <= 8) {
      res = 3;
    } else {
      res = 4;
    }
  } else if (item == "y") {  // yes
    res = ltm.tm_year + 1900;
  } else if (item == "isoy") {  // year based on ISO 8601 "isoyear"
    res = ltm.tm_year + 1900;
    // The last week of each year is the week of the last Thursday of the year
    k_int32 weekday = 0;
    if (ltm.tm_wday == 0) {
      weekday = 7;
    } else {
      weekday = ltm.tm_wday;
    }
    if (ltm.tm_yday < 7 && weekday > 4) {
      res -= 1;
    }
    if (ltm.tm_yday > 362 && weekday < 4) {
      res += 1;
    }
  } else if (item == "decade") {  // year/10
    res = (ltm.tm_year + 1900) / 10;
  } else if (item == "century") {  // century
    res = (ltm.tm_year + 1900) / 100 + 1;
  } else if (item == "millennium") {  // millennium
    res = (ltm.tm_year + 1900) / 1000 + 1;
  } else if (item == "timezone") {  // time zone offset from UTC, in seconds
    res = ltm.tm_gmtoff;
  } else if (item == "timezone_m") {  // The minute portion of the time
                                      // zone offset "timezone_minute"
    res = ltm.tm_gmtoff / 60;
  } else if (item == "timezone_h") {  // The hourly portion of the time zone
                                      // offset "timezone_hour"
    res = ltm.tm_gmtoff / 60 * 60;
  }

  return static_cast<double>(res);
}

}  // namespace kwdbts
