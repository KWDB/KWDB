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

#include "ee_encoding.h"

#include <limits.h>

#include <bitset>
#include <cstring>

namespace kwdbts {

// ===============Encode Value=================================
// Get 64-bit IEEE 754 format of the decimal value
k_uint64 GetBinary64(k_double64 value) {
  // union should be replaced with std::variant
  union {
    k_double64 input;  // assumes sizeof(float) == sizeof(int)
    k_uint64 output;
  } data;
  data.input = value;
  // std::bitset<sizeof(k_double64) * CHAR_BIT> bits(data.output);
  return data.output;
}

k_int32 ComputeX(k_uint64 x) {
  const k_uint64 one = 1;
  if (x < (one << 7))  // 128
    return 1;
  else if (x < (one << 14))
    return 2;
  else if (x < (one << 21))
    return 3;
  else if (x < (one << 28))
    return 4;
  else if (x < (one << 35))
    return 5;
  else if (x < (one << 42))
    return 6;
  else if (x < (one << 49))
    return 7;
  else if (x < (one << 56))
    return 8;
  else if (x < (one << 63))
    return 9;
  else
    return 10;
}

k_int32 ComputeDecimal(k_uint64 x) {
  if (x <= 109)
    return 1;
  else if (x <= 0xff)
    return 2;
  else if (x <= 0xffff)
    return 3;
  else if (x <= 0xffffff)
    return 4;
  else if (x <= 0xffffffff)
    return 5;
  else if (x <= 0xffffffffff)
    return 6;
  else if (x <= 0xffffffffffff)
    return 7;
  else if (x <= 0xffffffffffffff)
    return 8;
  else
    return 9;
}

k_int32 ComputeCopyWords(k_uint64 *nat, k_int32 nat_size) {
  k_int32 res = 0;
  bool leading = true;
  for (k_int32 w = nat_size - 1; w >= 0; w--) {
    k_uint64 d = nat[w];
    for (k_int64 j = bigWordSize - 1; j >= 0; j--) {
      k_uint8 by = k_uint8(d >> (8 * k_uint64(j)));
      if (by == 0 && leading) {
        continue;
      }
      leading = false;
      res = res + 1;
    }
  }
  return res;
}

k_int32 ComputeLenString(k_uint64 colID, VEncodeType typ, k_int32 data_size) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  len += ComputeX(k_uint64(data_size)) + data_size;
  return len;
}

k_int32 ComputeLenInt(k_uint64 colID, VEncodeType typ, k_int64 value) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  k_uint64 ux = k_uint64(value) << 1;
  if (value < 0) {
    ux = ~ux;
  }
  len += ComputeX(ux);
  return len;
}

k_int32 ComputeLenBool(k_uint64 colID, VEncodeType typ) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  return len;
}

k_int32 ComputeLenNull(k_uint64 colID, VEncodeType typ) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  return len;
}

k_int32 ComputeLenTime(k_uint64 colID, VEncodeType typ, CKTime value) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  k_uint64 ux1 = k_uint64(value.t_timespec.tv_sec) << 1;
  if (value.t_timespec.tv_sec < 0) {
    ux1 = ~ux1;
  }
  len += ComputeX(ux1);
  k_uint64 ux2 = k_uint64(value.t_timespec.tv_nsec) << 1;
  if (value.t_timespec.tv_nsec < 0) {
    ux2 = ~ux2;
  }
  len += ComputeX(ux2);
  /*
  k_uint64 ux3 = k_uint64(value.t_abbv / 3600) << 1;
  if (value.t_timespec.tv_sec < 0) {
    ux3 = ~ux3;
  }
  len += ComputeX(ux3);
  */
  return len;
}

k_int32 ComputeLenDuration(k_uint64 colID, VEncodeType typ, KWDuration value) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  k_uint64 ux1 = k_uint64(value.months) << 1;
  if (value.months < 0) {
    ux1 = ~ux1;
  }
  len += ComputeX(ux1);
  k_uint64 ux2 = k_uint64(value.days) << 1;
  if (value.days < 0) {
    ux2 = ~ux2;
  }
  len += ComputeX(ux2);
  k_uint64 ux3 = k_uint64(value.nanos) << 1;
  if (value.nanos < 0) {
    ux3 = ~ux3;
  }
  len += ComputeX(ux3);

  return len;
}

k_int32 ComputeLenNonsortingDecimal(const CKDecimal& value) {
  k_int32 len = 0;
  bool neg = value.negative;
  switch (value.my_form) {
    case FormFinite: {
      break;
    }
    case FormInfinite:
      if (neg) {
        len += ComputeX(decimalNegativeInfinity);
      } else {
        len += ComputeX(decimalInfinity);
      }
      return len;
    case FormNaN: {
      len += ComputeX(decimalNaN);
      return len;
    }
    default:
      // std::cerr << "EncodeNonsortingDecimal unknown form" << std::endl;
      // exit(1);
      return -1;
  }

  // We only encode "0" as decimalZero. All others ("0.0", "-0", etc) are
  // encoded like normal values.
  if (value.IsZero() && !neg && value.Exponent == 0) {
    len += ComputeX(decimalZero);
    return len;
  }
  k_uint32 count = 0;
  k_int32 nDigits = 1;
  if (value.my_coeff.abs_size != 0) {
    k_int64 n = value.my_coeff.abs[0];
    while (n != 0) {
      n = n / 10;
      count++;
    }
    nDigits = count;
  }
  k_int32 e = nDigits + k_int32(value.Exponent);
  if (neg && e > 0) {
    len += ComputeDecimal(decimalNegLarge);
    len += ComputeDecimal(k_uint64(e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (neg && e == 0) {
    len += ComputeDecimal(decimalNegMedium);
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (neg && e < 0) {
    len += ComputeDecimal(decimalNegSmall);
    len += ComputeDecimal(k_uint64(-e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (!neg && e < 0) {
    len += ComputeX(decimalPosSmall);
    len += ComputeDecimal(k_uint64(-e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (!neg && e == 0) {
    len += ComputeX(decimalPosMedium);
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (!neg && e > 0) {
    len += ComputeX(decimalPosLarge);
    len += ComputeDecimal(k_uint64(e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else {
    // std::cerr << "computeLenDecimal unreachable" << std::endl;
    // exit(1);
    return -1;
  }
  return len;
}

k_int32 ComputeLenDecimal(k_uint64 colID, VEncodeType typ, const CKDecimal& value) {
  k_int32 len = 0;
  k_int32 head_len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  head_len = len;
  bool neg = value.negative;
  switch (value.my_form) {
    case FormFinite: {
      break;
    }
    case FormInfinite:
      if (neg) {
        len += ComputeX(decimalNegativeInfinity);
      } else {
        len += ComputeX(decimalInfinity);
      }
      return len;
    case FormNaN: {
      len += ComputeX(decimalNaN);
      return len;
    }
    default:
      // std::cerr << "EncodeNonsortingDecimal unknown form" << std::endl;
      // exit(1);
      return -1;
  }

  // We only encode "0" as decimalZero. All others ("0.0", "-0", etc) are
  // encoded like normal values.
  if (value.IsZero() && !neg && value.Exponent == 0) {
    len += ComputeX(decimalZero);
    len += ComputeX(len - head_len);
    return len;
  }
  k_uint32 count = 0;
  k_int32 nDigits = 1;
  if (value.my_coeff.abs_size != 0) {
    k_int64 n = value.my_coeff.abs[0];
    while (n != 0) {
      n = n / 10;
      count++;
    }
    nDigits = count;
  }
  k_int32 e = nDigits + k_int32(value.Exponent);
  if (neg && e > 0) {
    len += ComputeDecimal(decimalNegLarge);
    len += ComputeDecimal(k_uint64(e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (neg && e == 0) {
    len += ComputeDecimal(decimalNegMedium);
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (neg && e < 0) {
    len += ComputeDecimal(decimalNegSmall);
    len += ComputeDecimal(k_uint64(-e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (!neg && e < 0) {
    len += ComputeDecimal(decimalPosSmall);
    len += ComputeDecimal(k_uint64(-e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (!neg && e == 0) {
    len += ComputeDecimal(decimalPosMedium);
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else if (!neg && e > 0) {
    len += ComputeDecimal(decimalPosLarge);
    len += ComputeDecimal(k_uint64(e));
    len += ComputeCopyWords(value.my_coeff.abs, value.my_coeff.abs_size);
  } else {
    // std::cerr << "computeLenDecimal unreachable" << std::endl;
    // exit(1);
    return -1;
  }
  len += ComputeX(len - head_len);
  return len;
}

k_int32 ComputeLenFloat(k_uint64 colID, VEncodeType typ) {
  k_int32 len = 0;
  if (typ > VEncodeType::VT_SentinelType) {
    len += ComputeX(colID << 4 | k_uint64(VEncodeType::VT_SentinelType));
    len += ComputeX(k_uint64(typ));
  }
  if (colID == 0) {
    len += sizeof(typ);
  } else {
    len += ComputeX(colID << 4 | k_uint64(typ));
  }
  len += 8;
  return len;
}

k_int32 EncodeNonsortingUvarint(CKSlice *slice, k_int32 offset, k_uint64 x) {
  const k_uint64 one = 1;
  if (x < (one << 7)) {  // 128
    slice->data[offset++] = static_cast<k_uint8>(k_int32(x));
  } else if (x < (one << 14)) {  // 16384
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 21)) {  // 2097152
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 28)) {  // 268435456
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 35)) {  // 34359738368
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 42)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 49)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 56)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 49));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else if (x < (one << 63)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 56));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 49));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  } else {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 63));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 56));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 49));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x7f & k_uint8(x));
  }
  return offset;
}

k_int32 PutUvarint(CKSlice *slice, k_int32 offset, k_uint64 x) {
  const k_uint64 one = 1;
  if (x < (one << 7)) {  // 128
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x));
  } else if (x < (one << 14)) {  // 16384
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 7));
  } else if (x < (one << 21)) {  // 2097152
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 14));
  } else if (x < (one << 28)) {  // 268435456
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 21));

  } else if (x < (one << 35)) {  // 34359738368
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 28));
  } else if (x < (one << 42)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 35));
  } else if (x < (one << 49)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 42));
  } else if (x < (one << 56)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 49));
  } else if (x < (one << 63)) {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 49));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 56));
  } else {
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 7));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 14));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 21));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 28));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 35));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 42));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 49));
    slice->data[offset++] = static_cast<k_uint8>(0x80 | k_uint8(x >> 56));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(x >> 63));
  }
  return offset;
}

k_int32 PutVarint(CKSlice *slice, k_int32 offset, const k_int64 &value) {
  k_uint64 ux = k_uint64(value) << 1;
  if (value < 0) {
    ux = ~ux;
  }
  return PutUvarint(slice, offset, ux);
}

// encode float key Asc
k_int32 EncodeUint64Ascending(CKSlice *slice, k_int32 offset, k_uint64 v) {
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 56));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 48));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 40));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 32));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 24));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
  slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  return offset;
}

k_int32 EncodeValueTag(CKSlice *slice, k_int32 offset, uint32_t colID,
                       VEncodeType typ) {
  if (typ >= VEncodeType::VT_SentinelType) {
    offset = EncodeNonsortingUvarint(
        slice, offset,
        k_uint64(colID) << 4 | k_uint64(VEncodeType::VT_SentinelType));
    offset = EncodeNonsortingUvarint(slice, offset, k_uint64(typ));
  }
  if (colID == 0) {
    slice->data[offset++] = k_uint8(typ);
    offset += sizeof(typ) - 1;
    return offset;
  }
  offset = EncodeNonsortingUvarint(slice, offset,
                                   k_uint64(colID) << 4 | k_uint64(typ));
  return offset;
}

k_int32 EncodeUvarintAscending(CKSlice *slice, k_int32 offset, k_uint64 v) {
  if (v <= IntSmall) {  // 109
    slice->data[offset++] = static_cast<k_uint8>(IntZero + k_uint8(v));
  } else if (v <= 0xff) {  // 255
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 7);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  } else if (v <= 0xffff) {  // 65535
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 6);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  } else if (v <= 0xffffff) {  // 16777215
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 5);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  } else if (v <= 0xffffffff) {  // 4294967295
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 4);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 24));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));

  } else if (v <= 0xffffffffff) {
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 3);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 32));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 24));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  } else if (v <= 0xffffffffffff) {
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 2);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 40));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 32));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 24));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  } else if (v <= 0xffffffffffffff) {
    slice->data[offset++] = static_cast<k_uint8>(IntMax - 1);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 48));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 40));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 32));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 24));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  } else {
    slice->data[offset++] = static_cast<k_uint8>(IntMax);
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 56));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 48));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 40));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 32));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 24));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 16));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v >> 8));
    slice->data[offset++] = static_cast<k_uint8>(k_uint8(v));
  }
  return offset;
}

k_int32 CopyWords(CKSlice *slice, k_int32 offset, k_uint64 *nat,
                  k_int32 nat_size) {
  // Omit leading zeros from the resulting byte slice, which is both
  // safe and exactly what big.Int.Bytes() does. See big.nat.setBytes()
  // and big.nat.norm() for how this is normalized on decoding.
  bool leading = true;
  for (k_int32 w = nat_size - 1; w >= 0; w--) {
    k_uint64 d = nat[w];
    for (k_int64 j = bigWordSize - 1; j >= 0; j--) {
      k_uint8 by = k_uint8(d >> (8 * k_uint64(j)));
      if (by == 0 && leading) {
        continue;
      }
      leading = false;
      slice->data[offset++] = static_cast<k_uint8>(by);
    }
  }
  return offset;
}

k_int32 EncodeNonsortingDecimalValue(CKSlice *slice, k_int32 offset,
                                     k_uint64 exp, k_uint64 *digits,
                                     k_int32 digits_size) {
  offset = EncodeUvarintAscending(slice, offset, exp);
  return CopyWords(slice, offset, digits, digits_size);
}

k_int32 EncodeNonsortingDecimalValueWithoutExp(CKSlice *slice, k_int32 offset,
                                               k_uint64 *digits,
                                               k_int32 digits_size) {
  return CopyWords(slice, offset, digits, digits_size);
}

k_int32 EncodeNonsortingDecimal(CKSlice *slice, k_int32 offset, const CKDecimal& d) {
  bool neg = d.negative;
  switch (d.my_form) {
    case FormFinite: {
      break;
    }
    case FormInfinite:
      if (neg) {
        slice->data[offset++] = k_uint8(decimalNegativeInfinity);
        return offset;
      } else {
        slice->data[offset++] = k_uint8(decimalInfinity);
        return offset;
      }
    case FormNaN: {
      slice->data[offset++] = k_uint8(decimalNaN);
      return offset;
    }
    default:
      // std::cerr << "EncodeNonsortingDecimal unknown form" << std::endl;
      // exit(1);
      return -1;
  }

  // We only encode "0" as decimalZero. All others ("0.0", "-0", etc) are
  // encoded like normal values.
  if (d.IsZero() && !neg && d.Exponent == 0) {
    slice->data[offset++] = k_uint8(decimalZero);
    return offset;
  }
  // Determine the exponent of the decimal, with the
  // exponent defined as .xyz * 10^exp.
  uint count = 0;
  k_int32 nDigits = 1;
  if (d.my_coeff.abs_size != 0) {
    k_int64 n = d.my_coeff.abs[0];
    while (n != 0) {
      n = n / 10;
      count++;
    }
    nDigits = count;
  }
  k_int32 e = nDigits + k_int32(d.Exponent);
  k_uint64 *bNat = d.my_coeff.abs;
  k_int32 bNat_size = d.my_coeff.abs_size;
  std::string buf;
  if (neg && e > 0) {
    slice->data[offset++] = k_uint8(decimalNegLarge);
    EncodeNonsortingDecimalValue(slice, offset, k_uint64(e), bNat, bNat_size);
  } else if (neg && e == 0) {
    slice->data[offset++] = k_uint8(decimalNegMedium);
    EncodeNonsortingDecimalValueWithoutExp(slice, offset, bNat, bNat_size);
  } else if (neg && e < 0) {
    slice->data[offset++] = k_uint8(decimalNegSmall);
    EncodeNonsortingDecimalValue(slice, offset, k_uint64(-e), bNat, bNat_size);
  } else if (!neg && e < 0) {
    slice->data[offset++] = k_uint8(decimalPosSmall);
    EncodeNonsortingDecimalValue(slice, offset, k_uint64(-e), bNat, bNat_size);
  } else if (!neg && e == 0) {
    slice->data[offset++] = k_uint8(decimalPosMedium);
    EncodeNonsortingDecimalValueWithoutExp(slice, offset, bNat, bNat_size);
  } else if (!neg && e > 0) {
    slice->data[offset++] = k_uint8(decimalPosLarge);
    EncodeNonsortingDecimalValue(slice, offset, k_uint64(e), bNat, bNat_size);
  } else {
    // std::cerr << "EncodeNonsortingDecimal unreachable" << std::endl;
    // exit(1);
    return -1;
  }
  return offset;
}

k_int32 ValueEncoding::EncodeComputeLenBool(k_uint64 colID, bool invert) {
  if (invert) {
    return ComputeLenBool(k_uint64(colID), VEncodeType::VT_True);
  } else {
    return ComputeLenBool(k_uint64(colID), VEncodeType::VT_False);
  }
}

k_int32 ValueEncoding::EncodeComputeLenInt(k_uint32 colID, k_int64 value) {
  return ComputeLenInt(k_uint64(colID), VEncodeType::VT_Int, value);
}

k_int32 ValueEncoding::EncodeComputeLenString(k_uint32 colID,
                                              k_int32 data_size) {
  return ComputeLenString(k_uint64(colID), VEncodeType::VT_Bytes, data_size);
}

k_int32 ValueEncoding::EncodeComputeLenDecimal(k_uint32 colID,
                                               const CKDecimal &data) {
  return ComputeLenDecimal(k_uint64(colID), VEncodeType::VT_Decimal, data);
}

k_int32 ValueEncoding::EncodeComputeLenFloat(k_uint32 colID) {
  return ComputeLenFloat(k_uint64(colID), VEncodeType::VT_Float);
}

k_int32 ValueEncoding::EncodeComputeLenTime(k_uint32 colID, CKTime data) {
  return ComputeLenTime(k_uint64(colID), VEncodeType::VT_Time, data);
}

k_int32 ValueEncoding::EncodeComputeLenDuration(k_uint32 colID, KWDuration data) {
  return ComputeLenDuration(k_uint64(colID), VEncodeType::VT_Duration, data);
}

k_int32 ValueEncoding::EncodeComputeLenNull(k_uint32 colID) {
  return ComputeLenNull(k_uint64(colID), VEncodeType::VT_Null);
}

void ValueEncoding::EncodeBoolValue(CKSlice *slice, k_uint32 colID,
                                    bool invert) {
  if (invert) {
    EncodeValueTag(slice, 0, colID, VEncodeType::VT_True);
  } else {
    EncodeValueTag(slice, 0, colID, VEncodeType::VT_False);
  }
}

k_int32 ValueEncoding::EncodeUntaggedIntValue(CKSlice *slice, k_int32 offset,
                                              k_int64 value) {
  offset = PutVarint(slice, offset, value);
  return offset;
}

void ValueEncoding::EncodeIntValue(CKSlice *slice, k_uint32 colID,
                                   k_int64 value) {
  k_int32 offset = EncodeValueTag(slice, 0, colID, VEncodeType::VT_Int);
  EncodeUntaggedIntValue(slice, offset, value);
}

k_int32 ValueEncoding::EncodeUntaggedBytesValue(CKSlice *slice, k_int32 offset,
                                                const std::string &data) {
  offset = EncodeNonsortingUvarint(slice, offset, k_uint64(data.size()));
  std::memcpy(slice->data + offset, data.data(), data.size());
  offset += data.size();
  return offset;
}

void ValueEncoding::EncodeBytesValue(CKSlice *slice, k_uint32 colID,
                                     const std::string &data) {
  k_int32 offset = EncodeValueTag(slice, 0, colID, VEncodeType::VT_Bytes);
  EncodeUntaggedBytesValue(slice, offset, data);
}

k_int32 ValueEncoding::EncodeUntaggedDecimalValue(CKSlice *slice,
                                                  k_int32 offset,
                                                  const CKDecimal &data) {
  k_int32 res = offset;
  k_int32 len = ComputeLenNonsortingDecimal(data);
  offset = PutUvarint(slice, res, len);
  offset = EncodeNonsortingDecimal(slice, offset, data);
  return offset;
}

void ValueEncoding::EncodeDecimalValue(CKSlice *slice, k_uint32 colID,
                                       CKDecimal& data) {
  k_int32 offset = EncodeValueTag(slice, 0, colID, VEncodeType::VT_Decimal);
  EncodeUntaggedDecimalValue(slice, offset, data);
}

k_int32 ValueEncoding::EncodeUntaggedFloatValue(CKSlice *slice, k_int32 offset,
                                                const k_double64 &data) {
  return EncodeUint64Ascending(slice, offset, GetBinary64(data));
}

void ValueEncoding::EncodeFloatValue(CKSlice *slice, k_uint32 colID,
                                     k_double64 data) {
  k_int32 offset = EncodeValueTag(slice, 0, colID, VEncodeType::VT_Float);
  EncodeUntaggedFloatValue(slice, offset, data);
}

// encode Time
void ValueEncoding::EncodeUntaggedTimeValue(CKSlice *slice, k_int32 offset,
                                            CKTime data) {
  offset =
      EncodeUntaggedIntValue(slice, offset, k_int64(data.t_timespec.tv_sec));
  offset =
      EncodeUntaggedIntValue(slice, offset, k_int64(data.t_timespec.tv_nsec));
  // no zone timestamp
  // EncodeUntaggedIntValue(slice, offset, k_int64(data.t_abbv / 3600));
}

void ValueEncoding::EncodeTimeValue(CKSlice *slice, k_uint32 colID,
                                    CKTime data) {
  k_int32 offset = EncodeValueTag(slice, 0, colID, VEncodeType::VT_Time);
  EncodeUntaggedTimeValue(slice, offset, data);
}

void ValueEncoding::EncodeDurationValue(CKSlice *slice, k_uint32 colID, KWDuration data) {
  k_int32 offset = EncodeValueTag(slice, 0, colID, VEncodeType::VT_Duration);
  EncodeUntaggedDurationValue(slice, offset, data);
}

void ValueEncoding::EncodeUntaggedDurationValue(CKSlice *slice, k_int32 offset,
                                      KWDuration data) {
  offset = EncodeUntaggedIntValue(slice, offset, k_int64(data.months));
  offset = EncodeUntaggedIntValue(slice, offset, k_int64(data.days));
  offset = EncodeUntaggedIntValue(slice, offset, k_int64(data.nanos));
}

void ValueEncoding::EncodeNullValue(CKSlice *slice, k_uint32 colID) {
  EncodeValueTag(slice, 0, colID, VEncodeType::VT_Null);
}
}  // namespace kwdbts
