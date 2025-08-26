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

#include <string>

#include "kwdb_type.h"

namespace kwdbts {

struct CKBigInt {
  CKBigInt();
  CKBigInt(const CKBigInt &big);
  ~CKBigInt();
  bool neg;  // sign
             // The flag bit, which represents positive or negative, is always
             // false in decimal, and the flag is meaningless in decimal
  k_uint64 *abs = nullptr;  // absolute value of the integer
  k_int32 abs_size = 0;
  k_int32 Sign() const;
};

// Decimal is an arbitrary-precision decimal. Its value is:
//
//     negative × my_coeff × 10**Exponent
//
// my_coeff must be positive. If it is negative results may be incorrect and
// apd may panic.
struct CKDecimal {
  k_int32 my_form;        // Form specifies the form of a Decimal.
  bool negative = false;  // The initial default value of the structure must be
                          // false, false: positive
  k_int32 Exponent;
  struct CKBigInt my_coeff;

  k_double64 DecimalToDouble();
  k_int32 Sign() const;
  bool IsZero() const { return Sign() == 0; }
};

extern struct CKDecimal IntToDecimal(k_uint64 val, k_bool unsigned_flag);
extern struct CKDecimal DoubleToDecimal(k_double64 val, k_bool unsigned_flag);

struct CKBigInt2 {
  ~CKBigInt2();
  bool neg;  // sign
             // The flag bit, which represents positive or negative, is always
             // false in decimal, and the flag is meaningless in decimal
  k_uint64 abs[4] = {0};  // absolute value of the integer
  k_int32 abs_size = 0;
  k_int32 Sign();

  void SetString(std::string s);
  void SetInt64(k_int64 x);
  void make(k_uint8 sz);
  void mulAddWW(k_uint64 y, k_uint64 r);
  k_uint64 pow(k_uint32 b, k_uint32 n);
};

struct CKDecimal2 {
  k_int32 my_form;        // Form specifies the form of a Decimal.
  bool negative = false;  // The initial default value of the structure must be
                          // false, false: positive
  k_int32 Exponent;
  struct CKBigInt2 my_coeff;

  k_double64 DecimalToDouble();
  k_int32 Sign();
  bool IsZero() { return Sign() == 0; }
};

extern struct CKDecimal2 StringToDecimal(std::string str);

}  // namespace kwdbts
