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
#include <list>
#include <string>

#include "ee_field.h"
#include "ee_field_common.h"
#include "ee_base_op.h"
#include "ts_common.h"

namespace kwdbts {

// the field in the bucket
class FieldAggNum : public Field {
  typedef Field super;

 public:
  FieldAggNum() { type_ = FIELD_AGG; }
  explicit FieldAggNum(k_uint32 num, roachpb::DataType type, k_uint32 length, BaseOperator *op)
      : super(num, type, length, FIELD_AGG), op_(op) {}
  virtual ~FieldAggNum() {}
  k_bool is_nullable() override;
  char *get_ptr() override;

  Field *field_to_copy() override { return nullptr; }
  k_bool fill_template_field(char *ptr) override { return 0; }

 protected:
  BaseOperator *op_;   // agg operator
};

// bool
class FieldAggBool : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// smallint
class FieldAggShort : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// int
class FieldAggInt : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// bigint
class FieldAggLonglong : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// float
class FieldAggFloat : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// double
class FieldAggDouble : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// handle decimal column of intermediate result in aggregation operator
class FieldAggDecimal : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
};

// string
class FieldAggString : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
};

// avg
class FieldAggAvg : public FieldAggNum {
 public:
  using FieldAggNum::FieldAggNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
};

}   // namespace kwdbts
