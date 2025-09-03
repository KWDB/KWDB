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
#include "ee_decimal.h"

#include <cmath>

#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

CKBigInt::CKBigInt() {
  abs = new k_uint64(0);
  abs_size = 1;
}

CKBigInt::CKBigInt(const CKBigInt &big) {
  neg = big.neg;
  abs_size = big.abs_size;
  abs = new k_uint64(0);
  memcpy(abs, big.abs, sizeof(k_uint64));
}

CKBigInt::~CKBigInt() {
  if (nullptr != abs) {
    delete abs;
    abs = nullptr;
  }

  abs_size = 0;
}

k_int32 CKBigInt::Sign() const {
  if (abs_size == 0) {
    return 0;
  }
  if (neg) {
    return -1;
  }
  return 1;
}

k_double64 CKDecimal::DecimalToDouble() {
  k_double64 de = 0;
  k_double64 power = pow(10, k_double64(Exponent));
  if (my_coeff.abs_size == 0) {
    de = 0;
    return de;
  }

  if (negative) {
    de = -k_double64(my_coeff.abs[0]) * power;
  } else {
    de = k_double64(my_coeff.abs[0]) * power;
  }
  return de;
}

k_int32 CKDecimal::Sign() const {
  if (my_form == 0 && (my_coeff.Sign() == 0)) {
    return 0;
  }
  if (negative) {
    return -1;
  }
  return 1;
}

struct CKDecimal IntToDecimal(k_uint64 val, k_bool unsigned_flag) {
  struct CKDecimal decimal;
  decimal.my_form = 0;
  decimal.Exponent = 0;
  decimal.my_coeff.neg = 0;
  if (!unsigned_flag) {
    decimal.negative = 1;
  } else {
    decimal.negative = 0;
  }

  decimal.my_coeff.neg = 0;
  if (val != 0) {
    memcpy(decimal.my_coeff.abs, &val, sizeof(k_uint64));
    decimal.my_coeff.abs_size = 1;
  } else {
    decimal.my_coeff.abs_size = 0;
  }
  return decimal;
}

struct CKDecimal DoubleToDecimal(k_double64 val, k_bool unsigned_flag) {
  CKDecimal decimal;
  k_uint64 u = 0;
  decimal.my_form = 0;
  if (unsigned_flag) {
    decimal.negative = 0;
  } else {
    decimal.negative = 1;
  }

  k_char buf[30] = {0};
  k_int32 n = snprintf(buf, sizeof(buf), "%.15g", val);
  std::string str = buf;
  std::string::size_type sz = str.length();
  std::string::size_type pos = str.find('.');
  if (pos != std::string::npos) {
    k_uint32 exp = sz - pos - 1;
    decimal.Exponent = -exp;
    k_double64 power = pow(10, k_double64(exp));
    val = val * power;
    if (val > UINT64_MAX) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    "decimal out of range");
    }
    u = static_cast<k_uint64>(val);
  } else {
    decimal.Exponent = 0;
  }

  decimal.my_coeff.neg = 0;
  memcpy(decimal.my_coeff.abs, &u, sizeof(k_uint64));
  decimal.my_coeff.abs_size = 1;

  return decimal;
}

CKBigInt2::~CKBigInt2() { abs_size = 0; }

k_int32 CKBigInt2::Sign() {
  if (abs_size == 0) {
    return 0;
  }
  if (neg) {
    return -1;
  }
  return 1;
}

void CKBigInt2::SetString(std::string s) {
  k_uint64 bn = 10000000000000000000ULL;
  k_uint16 n = 19;
  k_int32 count = 0;
  k_uint64 di = 0;
  k_uint32 base = 10;
  k_uint16 i = 0;
  for (char ch : s) {
    k_uint32 d1 = 0;
    d1 = ch - '0';
    ++count;
    di = di * base + d1;
    ++i;
    if (i == n) {
      mulAddWW(bn, di);
      di = 0;
      i = 0;
    }
  }

  if (i > 0) {
    mulAddWW(pow(base, i), di);
  }
}

void CKBigInt2::SetInt64(k_int64 x) {
  if (0 == x) {
    return;
  }

  neg = false;
  if (x < 0) {
    neg = true;
    x = -x;
  }

  memcpy(&(abs[0]), &x, sizeof(k_int64));
  abs_size = 1;
}

void CKBigInt2::mulAddWW(k_uint64 y, k_uint64 r) {
  if (0 == abs_size || 0 == y) {
    abs[0] = y;
    return;
  }
}

k_uint64 CKBigInt2::pow(k_uint32 b, k_uint32 n) {
  k_uint64 p = 1;
  while (n > 0) {
    if ((n & 1) != 0) {
      p *= b;
    }
    b *= b;
    n >>= 1;
  }

  return p;
}

k_double64 CKDecimal2::DecimalToDouble() {
  k_double64 de = 0;
  k_double64 power = pow(10, k_double64(Exponent));
  if (my_coeff.abs_size == 0) {
    de = 0;
    return de;
  }

  if (negative) {
    de = -k_double64(my_coeff.abs[0]) * power;
  } else {
    de = k_double64(my_coeff.abs[0]) * power;
  }
  return de;
}

k_int32 CKDecimal2::Sign() {
  if (my_form == 0 && (my_coeff.Sign() == 0)) {
    return 0;
  }
  if (negative) {
    return -1;
  }
  return 1;
}

struct CKDecimal2 StringToDecimal(std::string str) {
  CKDecimal2 d;
  // std::string orig = str;
  if (str[0] == '-') {
    d.negative = 1;
    str.erase(0, 1);
  } else {
    d.negative = 0;
  }
  d.Exponent = 0;
  d.my_coeff.SetInt64(0);
  // d.my_form = NAN;
  std::string::size_type sz = str.length();
  std::string::size_type pos = str.find('.');
  if (pos != std::string::npos) {
    k_uint32 exp = sz - pos - 1;
    d.Exponent = -exp;
    str.erase(pos, 1);
    d.my_coeff.SetString(str);
  } else {
    d.Exponent = 0;
  }

  return d;
}

}  // namespace kwdbts
