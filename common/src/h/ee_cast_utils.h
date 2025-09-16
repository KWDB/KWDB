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
#include <cstdint>
#include "kwdb_type.h"

namespace kwdbts {

// TODO(exec)
// After the error reporting logic is executed, the ErrorCode should be removed.
// This is only used for concatenated logic
enum TypeCastErrorCode {
  TYPE_CAST_SUCCESS = 0,
  TYPE_CAST_EDATA = -1,
  TYPE_CAST_ERANGE = -2,
};

bool isNumber(char *n);

int fetchNum0(char *&ptr, int &positive, char *&realStart);
// check over flow func
bool checkOverflow(int positive, int64_t output, int multiply, int add);
// type convert
bool boolStrToInt(char *input, int &output);
KStatus strToDouble(char *input, double &output);

KStatus strToInt64(char *input, int64_t &output);
KStatus doubleToStr(double input, char *str, int32_t length);

KStatus convertStringToTimestamp(KString inputData, k_int64 scale, k_int64* timeVal);
}  // namespace kwdbts
