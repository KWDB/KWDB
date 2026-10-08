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
// Created by jerry on 2022/5/13.
//

#ifndef COMMON_SRC_INCLUDE_KWDB_CONSTS_H_
#define COMMON_SRC_INCLUDE_KWDB_CONSTS_H_

#include <string>
#include "kwdb_type.h"

using namespace std;

namespace kwdbts {
/************************ KWDB widely useful constants ************************/
// similar to NULL for a pointer
const KId kNullID = 0;
const KId kErrorID = -1;
const KId kInvalidID = -2;
const k_int8 kInt8Max = 127;
const k_int8 kInt8Min = -128;
const k_uint8 kUInt8Max = 255;
const k_uint8 kUInt8Min = 0;
const k_int16 kInt16Max = 32767;
const k_int16 kInt16Min = -32768;
const k_uint16 kUint16Max = 65535;
const k_uint16 kUint16Min = 0;
const k_int32 kInt32Max = 2147483647;
const k_int32 kInt32Min = -2147483648;
const k_uint32 kUint32max = 4294967295;
const k_uint32 kUint32Min = 0;
const k_int64 kInt64Max = 9223372036854775807;
const k_int64 kInt64Min = -9223372036854775807 - 1;
const k_uint64 kUint64Max = 18446744073709551615U;
const k_uint64 kUint64Min = 0;
const k_float32 kFloat32Max = 3.402823466e+38F;
const k_float32 kFloat32Min = 1.175494351e-38F;
const k_double64 kDouble64Max = 1.7976931348623158e+308;
const k_double64 kDouble64Min = 2.2250738585072014e-308;
const k_float64 kFloat64Max = 1.7976931348623158e+308;
const k_float64 kFloat64Min = 2.2250738585072014e-308;


const static string& s_emptyString = "";
const static string& s_NULL = "NULL";
const static string& s_true = "true";
const static string& s_false = "false";
const static string& s_deletable = "_d";
const static string& s_bt = ".bt";
const static string& s_kaiwudb = "kaiwudb";
const static string& s_row = "row";
const static string& s_inf = "inf";
const static string& s_n_inf = "-inf";
const static string& s_default = "default";

}  // namespace kwdbts

#endif  // COMMON_SRC_INCLUDE_KWDB_CONSTS_H_
