// Copyright 2016-2024 ClickHouse, Inc.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

#include <vector>

#include "kwdb_type.h"
/*
 * ToDo: Add interval Inet json type
 * There are only int float decimal
 */
namespace kwdbts {
union ScalarValue {
  k_uint32 column_id;
  k_int64 int_type;
  k_float64 float_type;
  k_float64 decimal;
  k_bool comparison;
  k_int64 unknown_type;
};

enum AstEleType {
  OPENING_BRACKET,
  CLOSING_BRACKET,
  INT_TYPE,
  FLOAT_TYPE,
  DECIMAL,
  TIMESTAMP_TYPE,
  TIMESTAMPTZ_TYPE,
  TIMESTAMP_MICRO_TYPE,
  TIMESTAMPTZ_MICRO_TYPE,
  TIMESTAMP_NANO_TYPE,
  TIMESTAMPTZ_NANO_TYPE,
  DATE_TYPE,
  TIME_TYPE,
  COLUMN_TYPE,
  STRING_TYPE,
  BYTES_TYPE,
  UNKNOWN_TYPE,
  COMMA,
  INTERVAL_TYPE,

  PLUS,
  MINUS,
  MULTIPLE,
  DIVIDE,
  DIVIDEZ,
  REMAINDER,
  PERCENT,
  POWER,
  ANDCAL,
  ORCAL,
  TILDE,
  REGEX,
  IREGEX,
  NOTREGEX,
  NOTIREGEX,

  EQUALS,
  LESS,
  GREATER,
  LESS_OR_EQUALS,
  GREATER_OR_EQUALS,
  RIGHTSHIFT,
  LEFTSHIFT,
  NOT_EQUALS,
  AND,
  OR,
  In,
  Like,
  ILike,
  IS_NULL,
  IS_NAN,
  IS_UNKNOWN,
  CASE,
  WHEN,
  THEN,
  ELSE,
  CASE_END,
  CAST,
  COALESCE,
  ANY,
  ALL,
  NOT,

  Function,

  COMPARE,

  NULL_TYPE
};

struct AstEleValue {
  k_bool unknown_value;
  KString string_type;
  ScalarValue number;
  std::vector<ScalarValue> numbervec;
  std::vector<KString> stringvec;
};

class Element {
 public:
  Element(AstEleType operators_, k_bool is_operator_)
      : operators(operators_), is_operator(is_operator_) {}
  explicit Element(k_int64 int_type_) : is_operator(KFALSE) {
    value.number.int_type = int_type_;
  }
  explicit Element(k_uint32 column_id_) : is_operator(KFALSE) {
    value.number.column_id = column_id_;
  }
  explicit Element(k_double64 decimal_) : is_operator(KFALSE) {
    value.number.decimal = decimal_;
  }
  explicit Element(k_float32 float_type_) : is_operator(KFALSE) {
    value.number.float_type = float_type_;
  }
  explicit Element(KString string_) : is_operator(KFALSE) {
    value.string_type = string_;
  }
  explicit Element(std::vector<KString> newstring_) : is_operator(KFALSE) {
    value.stringvec = newstring_;
  }
  void SetType(AstEleType operators_) { this->operators = operators_; }
  void SetNegative(k_bool negative_) { this->is_negative = negative_; }
  void SetFunc(k_bool isfunc_) { this->is_func = isfunc_; }
  AstEleType operators;
  k_bool is_operator{KFALSE};
  AstEleValue value{};
  k_bool is_func{KFALSE};
  k_bool is_negative{KFALSE};
};

}  // namespace kwdbts
