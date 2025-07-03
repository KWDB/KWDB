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
#pragma once

#include <sys/time.h>

#include <cmath>
#include <string>
#include <ctime>

#include "ee_decimal.h"
#include "kwdb_type.h"

namespace kwdbts {

enum VEncodeType : unsigned char {
  VT_Unknown = 0,
  VT_Null = 1,
  VT_NotNull = 2,
  VT_Int = 3,
  VT_Float = 4,
  VT_Decimal = 5,
  VT_Bytes = 6,
  VT_BytesDesc = 7,  // Bytes encoded descendingly
  VT_Time = 8,
  VT_Duration = 9,
  VT_True = 10,
  VT_False = 11,
  VT_UUID = 12,
  VT_Array = 13,
  VT_IPAddr = 14,
  // SentinelType is used for bit manipulation to check if the encoded type
  // value requires more than 4 bits, and thus will be encoded in two bytes. It
  // is not used as a type value, and thus intentionally overlaps with the
  // subsequent type value. The 'Type' annotation is intentionally omitted here.
  VT_SentinelType = 15,
  VT_JSON = 15,
  //    Tuple = 16,
  VT_BitArray = 17,
  VT_BitArrayDesc = 18,
};

// A CKSlice contains read-only data that does not need to be freed.
struct CKSlice {
  char *data;
  k_int32 len;
};

// The gap between floatNaNDesc and bytesMarker was left for
// compatibility reasons.
const k_uint8 bytesMarker = 0x12;
const k_uint8 bytesDescMarker = bytesMarker + 1;
const k_uint8 timeMarker = bytesDescMarker + 1;
// Only used for durations < MinInt64 nanos.
const k_uint8 durationBigNegMarker = timeMarker + 1;
const k_uint8 durationMarker = durationBigNegMarker + 1;
// Only used for durations > MaxInt64 nanos.
const k_uint8 durationBigPosMarker = durationMarker + 1;

const k_int32 decimalNaN = durationBigPosMarker + 1;  // 24
const k_int32 decimalNegativeInfinity = decimalNaN + 1;
const k_int32 decimalNegLarge = decimalNegativeInfinity + 1;
const k_int32 decimalNegMedium = decimalNegLarge + 11;
const k_int32 decimalNegSmall = decimalNegMedium + 1;
const k_int32 decimalZero = decimalNegSmall + 1;
const k_int32 decimalPosSmall = decimalZero + 1;
const k_int32 decimalPosMedium = decimalPosSmall + 1;
const k_int32 decimalPosLarge = decimalPosMedium + 11;
const k_int32 decimalInfinity = decimalPosLarge + 1;
const k_int32 decimalNaNDesc = decimalInfinity + 1;  // NaN encoded descendingly
const k_int32 decimalTerminator = 0x00;

const k_int32 bigWordSize = sizeof(k_uint64);

// These constants must be in the following order. CmpTotal assumes that
// the order of these constants reflects the total order on decimals.
// Finite is the finite form.
const k_int32 FormFinite = 0;
// Infinite is the infinite form.
const k_int32 FormInfinite = FormFinite + 1;
// NaNSignaling is the signaling NaN form. It will always raise the
// InvalidOperation condition during an operation.
const k_int32 FormNaNSignaling = FormInfinite + 1;
// NaN is the NaN form.
const k_int32 FormNaN = FormNaNSignaling + 1;

const int IntMin = 128;
const int IntMaxWidth = 8;
const int IntZero = IntMin + IntMaxWidth;             // 136
const int IntMax = 0xfd;                              // 253
const int IntSmall = IntMax - IntZero - IntMaxWidth;  // 109

// timestamp
struct CKTime {
  timespec t_timespec;  // t_sec:The number of seconds that have elapsed since
                        // UTC on January 1, 1970
  // t_Nsec:Nanosecond returns the nanosecond offset within the second specified
  // by t ,
  // OffsetSecs is the offset of the zone, with the sign reversed. e.g.
  // -0800 (PDT) would have OffsetSecs of +8*60*60. This is in line with the
  // postgres implementation. This means timeofday.Secs() + OffsetSecs = UTC
  // secs.
  k_int32 t_abbv = 0;  // The offset of the t_abby in seconds east of UTC
  void UpdateSecWithTZ(k_int8 timezone) {
    t_abbv = timezone * 3600;
  }
};

const k_int64 Nanosecond = 1;
const k_int64 Microsecond = 1000 * Nanosecond;
const k_int64 Millisecond = 1000 * Microsecond;
const k_int64 Second = 1000 * Millisecond;
const k_int64 Minute = 60 * Second;
const k_int64 Hour = 60 * Minute;
const k_int64 Day = 24 * Hour;

struct KWDuration {
  k_int64 months = 0;
  k_int64 days = 0;
  k_int64 nanos = 0;

  void format(k_int64 ms, k_int64 scale) {
    k_int64 extraDays = ms / (Day / scale);
    days += extraDays;
    ms -= extraDays * (Day / scale);
    nanos = ms * scale;
  }

  size_t format_pg_result(k_int64 ms, char *buf, size_t sz, k_int64 scale) {
    format(ms, scale);
    size_t n = 0;
    if (days) {
      n += snprintf(buf + n, sz - n, "%ld %s", days,
                    days > 1 || days < -1 ? "days " : "day ");
    }

    k_int64 total_sec = nanos / 1000000000;

    k_int64 seconds_after_decimal = std::abs(nanos % 1000000000);

    if (nanos < 0) {
      n += snprintf(buf + n, sz - n, "-");
    }
    k_int64 abs_sec = std::abs(total_sec);
    k_int64 hours = abs_sec / 3600;
    k_int64 minutes = (abs_sec % 3600) / 60;
    k_int64 seconds = abs_sec % 60;
    n +=
        snprintf(buf + n, sz - n, "%02ld:%02ld:%02ld", hours, minutes, seconds);

    if (seconds_after_decimal != 0) {
      int digits = 9;
      while (seconds_after_decimal % 10 == 0) {
        seconds_after_decimal /= 10;
        digits--;
      }

      n += snprintf(buf + n, sz - n, ".%0*ld", digits, seconds_after_decimal);
    }

    return n;
  }
};

class ValueEncoding {
 public:
  // Calculate the encoding length
  static k_int32 EncodeComputeLenBool(k_uint64 colID, bool invert);

  static k_int32 EncodeComputeLenInt(k_uint32 colID, k_int64 value);

  static k_int32 EncodeComputeLenString(k_uint32 colID, k_int32 data_size);

  static k_int32 EncodeComputeLenDecimal(k_uint32 colID, const CKDecimal &data);

  static k_int32 EncodeComputeLenFloat(k_uint32 colID);

  static k_int32 EncodeComputeLenTime(k_uint32 colID, CKTime data);

  static k_int32 EncodeComputeLenDuration(k_uint32 colID, KWDuration data);

  static k_int32 EncodeComputeLenNull(k_uint32 colID);

  // Value encoding function interface
  // ========Value encoding function interface=========
  // encode value---Bool
  static void EncodeBoolValue(CKSlice *slice, k_uint32 colID, bool invert);
  // encode value---Int
  static void EncodeIntValue(CKSlice *slice, k_uint32 colID, k_int64 value);
  static k_int32 EncodeUntaggedIntValue(CKSlice *slice, k_int32 offset,
                                        k_int64 value);
  // encode value---Bytes
  static void EncodeBytesValue(CKSlice *slice, k_uint32 colID,
                               const std::string &data);
  static k_int32 EncodeUntaggedBytesValue(CKSlice *slice, k_int32 offset,
                                          const std::string &data);
  // encode value---Decimal
  static void EncodeDecimalValue(CKSlice *slice, k_uint32 colID,
                                 CKDecimal& data);
  static k_int32 EncodeUntaggedDecimalValue(CKSlice *slice, k_int32 offset,
                                            const CKDecimal &data);
  // encode value---Float
  static void EncodeFloatValue(CKSlice *slice, k_uint32 colID, k_double64 data);
  static k_int32 EncodeUntaggedFloatValue(CKSlice *slice, k_int32 offset,
                                          const k_double64 &data);
  // encode value---Time
  static void EncodeTimeValue(CKSlice *slice, k_uint32 colID, CKTime data);
  static void EncodeUntaggedTimeValue(CKSlice *slice, k_int32 offset,
                                      CKTime data);

  static void EncodeDurationValue(CKSlice *slice, k_uint32 colID,
                                  KWDuration data);
  static void EncodeUntaggedDurationValue(CKSlice *slice, k_int32 offset,
                                          KWDuration data);

  // encode Null---Null
  static void EncodeNullValue(CKSlice *slice, k_uint32 colID);
};
};  // namespace kwdbts

