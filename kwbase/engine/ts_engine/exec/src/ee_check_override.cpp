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
// Created by liguoliang on 2022/07/18.

#include <climits>
#include <cmath>

#include "ee_check_overflow.h"

namespace kwdbts {

void raise_numeric_overflow(const char *type_name) {}

k_int64 raise_integer_overflow(k_bool unsigned_flag) {
  raise_numeric_overflow(unsigned_flag ? "BIGINT UNSIGNED" : "BIGINT");
  return 0;
}

k_double64 raise_float_overflow() {
  raise_numeric_overflow("DOUBLE");
  return 0.0;
}

k_double64 check_float_overflow(k_double64 value) {
  return std::isfinite(value) ? value : raise_float_overflow();
}

k_int64 check_integer_overflow(k_int64 value, k_bool unsigned_flag,
                               bool val_unsigned) {
  if ((unsigned_flag && !val_unsigned && value < 0) ||
      (!unsigned_flag && val_unsigned && (k_uint64)value > (k_uint64)LLONG_MAX))
    return raise_integer_overflow(unsigned_flag);
  return value;
}

}  // namespace kwdbts
