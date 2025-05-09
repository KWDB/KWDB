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
#include <regex>
#include <algorithm>

#include "ee_field.h"
#include "ee_field_const.h"
#include "ee_timestamp_utils.h"

namespace kwdbts {

const k_int64 BASE_CONSTANT = 62135596800000;

// typedef KString (*_str_fn)(Field **);
// Base class for operations like '+', '-', '*', '/', '%'
class FieldFuncOp : public FieldFunc {
 public:
  explicit FieldFuncOp(Field *a) : FieldFunc(a) {
    type_ = FIELD_ARITHMETIC;
    CalcStorageType();
  }
  explicit FieldFuncOp(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_ARITHMETIC;
    CalcStorageType();
  }

  explicit FieldFuncOp(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    CalcStorageType();
  }

  void CalcStorageType();
 protected:
  // Only used in FieldFuncMinus::ValInt, Initialize in CalcStorgeType().
  // Convert different timestamp precision to the same.
  std::list<k_int64> time_scales_;
};

class FieldFuncPlus : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return PLUS_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncMinus : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return MINUS_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncMult : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return MULT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncDivide : public FieldFuncOp {
 public:
  explicit FieldFuncDivide(Field *a, Field *b) : FieldFuncOp(a, b) {
    storage_type_ = roachpb::DataType::DOUBLE;
    storage_len_ = sizeof(k_double64);
  }

  enum Functype functype() override { return DIV_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;

 protected:
  k_bool field_is_nullable() override;
};

class FieldFuncDividez : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return DIVZ_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;

 protected:
  k_bool field_is_nullable() override;
};

class FieldFuncRemainder : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return REMAINDER_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncPercent : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return PERCENT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;

 protected:
  k_bool field_is_nullable() override;
};

class FieldFuncPower : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return POWER_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncMod : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return MOD_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncAndCal : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return ANDCAL_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncOrCal : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return ORCAL_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncNotCal : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return NOTCAL_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncLeftShift : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return ORCAL_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncRightShift : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;

  enum Functype functype() override { return ORCAL_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class  FieldFuncTimeBucket : public FieldFuncOp {
 public:
  using FieldFuncOp::FieldFuncOp;
  explicit FieldFuncTimeBucket(std::list<Field *> fields, k_int8 tz) : FieldFuncOp(fields) {
    time_zone_ = tz;
    // getIntervalSeconds(var_interval_, year_bucket_, error_info_);
    KString unit = "";
    KString interval = replaceTimeUnit({args_[1]->ValStr().getptr(),
                            args_[1]->ValStr().length_});
    interval_seconds_ = getIntervalSeconds(interval, var_interval_, year_bucket_, &unit, error_info_, false);
    k_int64 time_diff = time_zone_;
    sql_type_ = getTimeFieldType(args_[0]->get_storage_type(), unit, &time_diff, &type_scale_, &type_scale_multi_or_divde_);
    storage_type_ = sql_type_;
    storage_len_ = sizeof(k_int64);
    k_int64 mo = 0;
    if (interval_seconds_ != 0) {
      if (storage_type_ == roachpb::DataType::TIMESTAMP_NANO || storage_type_ == roachpb::DataType::TIMESTAMPTZ_NANO) {
        mo = ((BASE_CONSTANT % interval_seconds_) * (1000000 % interval_seconds_)) % interval_seconds_;
      } else if (storage_type_ == roachpb::DataType::TIMESTAMP_MICRO ||
                 storage_type_ == roachpb::DataType::TIMESTAMPTZ_MICRO) {
        mo = (BASE_CONSTANT * 1000) % interval_seconds_;
      } else {
        mo = BASE_CONSTANT % interval_seconds_;
      }
      time_diff_mo_ = (time_diff % interval_seconds_ + mo) % interval_seconds_;
    }

    allow_null_ = false;
  }
  enum Functype functype() override { return TIME_BUCKET_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;

  // Replace keywords in the timestring
  // std::string replaceKeywords(const std::string& timestring);

  KTimestampTz getOriginalTimestamp() {
    auto val_ptr = args_[0]->get_ptr();
    return *reinterpret_cast<KTimestampTz *>(val_ptr);
  }
  k_int8 time_zone_{0};
  k_int64 interval_seconds_{0};
  k_int64 time_diff_mo_{0};
  // for the first arg, type_scale_multi_or_divde means muiltply or divide the scale. true is muiltply and false is divide.
  k_int64 type_scale_{1};
  k_bool type_scale_multi_or_divde_{true};
  k_bool var_interval_{false};
  k_bool year_bucket_{false};
  std::string error_info_{""};
  k_int64 last_time_bucket_value_{INT64_MIN};
  k_int64 time_diff_{0};
};

class FieldFuncCastCheckTs : public FieldFunc {
 public:
  FieldFuncCastCheckTs(Field *left, Field *right);

  enum Functype functype() override { return Functype::CAST_CHECK_TS; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;

 private:
  void CalcDataType();

 private:
  DATATYPE datatype_{DATATYPE::NO_TYPE};
};

class FieldFuncCurrentDate : public FieldFunc {
 public:
  using FieldFunc::FieldFunc;
  FieldFuncCurrentDate() {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::TIMESTAMP;
    storage_type_ = roachpb::DataType::TIMESTAMP;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return CURRENT_DATE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
};

class FieldFuncCurrentTimeStamp : public FieldFunc {
 public:
  using FieldFunc::FieldFunc;
  explicit FieldFuncCurrentTimeStamp(Field *a, k_int8 tz) {
    if (a != nullptr) {
      args_[0] = a;
      arg_count_ = 1;
    }
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::TIMESTAMP_NANO;
    storage_type_ = roachpb::DataType::TIMESTAMP_NANO;
    storage_len_ = sizeof(k_int64);
    time_zone_ = tz;
  }
  enum Functype functype() override { return CURRENT_TIMESTAMP_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;

  k_int8 time_zone_{0};
};

class FieldFuncDateTrunc : public FieldFunc {
 public:
  FieldFuncDateTrunc(Field *a, Field *b, k_int8 tz) : FieldFunc(a, b) {
    type_ = FIELD_ARITHMETIC;
    time_diff_ = tz;
    if (a->get_field_type() == FIELD_CONSTANT) {
      if (a->is_nullable()) {
        is_null_value_ = true;
      } else {
        is_unit_const_ = true;
        unit_ = replaceTimeUnit({a->ValStr().getptr(), a->ValStr().length_});
        sql_type_ = getTimeFieldType(b->get_storage_type(), unit_, &time_diff_,
                                     &type_scale_, &type_scale_multi_or_divde_);
      }
    } else {
      sql_type_ = b->get_storage_type();
    }

    storage_type_ = sql_type_;

    storage_len_ = b->get_storage_length();
    time_zone_ = tz;
  }
  enum Functype functype() override { return DATE_TRUNC_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  k_bool is_nullable() override;
  Field *field_to_copy() override;
  KString unit_;
  // for the first arg, type_scale_multi_or_divde means muiltply or divide the scale. true is muiltply and false is divide.
  k_int64 type_scale_{1};
  k_bool type_scale_multi_or_divde_{true};
  k_int64 time_diff_;
  k_int8 time_zone_;
  k_bool is_unit_const_{false};
  k_bool is_null_value_{false};
};

class FieldFuncExtract : public FieldFunc {
 public:
  FieldFuncExtract(Field *a, Field *b, k_int8 tz) : FieldFunc(a, b) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::DOUBLE;
    storage_type_ = roachpb::DataType::DOUBLE;
    storage_len_ = sizeof(double);
    time_zone_ = tz;
  }
  enum Functype functype() override { return EXTRACT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;

  k_int8 time_zone_;
};

class FieldFuncExpStrftime : public FieldFunc {
 public:
  FieldFuncExpStrftime(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::CHAR;
    storage_type_ = roachpb::DataType::CHAR;
    if (b->get_field_type() != FIELD_CONSTANT) {
      storage_len_ = b->get_storage_length() * 4;
    } else {
      std::string value(b->ValStr().c_str());
      for (int i = 0; i < value.length(); ++i) {
        while (value[i] != '-' && i < value.length()) {
          if (value[i] == '%') {
            ++i;
            ++storage_len_;
            if (value[i] == '%') {
              ++i;
              ++storage_len_;
            } else if (value[i] == 'y' || value[i] == 'm' || value[i] == 'd' ||
                       value[i] == 'H' || value[i] == 'I' || value[i] == 'p' ||
                       value[i] == 'M' || value[i] == 'S' || value[i] == 'U' ||
                       value[i] == 'W' || value[i] == 'e' || value[i] == 'g' ||
                       value[i] == 'V') {
              ++i;
              storage_len_ += 2;
            } else if (value[i] == 'a' || value[i] == 'b' || value[i] == 'Z' ||
                       value[i] == 'j' || value[i] == 'h') {
              ++i;
              storage_len_ += 3;
            } else if (value[i] == 't' || value[i] == 'Y' || value[i] == 'G') {
              ++i;
              storage_len_ += 4;
            } else if (value[i] == 'R' || value[i] == 'z') {
              ++i;
              storage_len_ += 5;
            } else if (value[i] == 'f') {
              ++i;
              storage_len_ += 6;
            } else if (value[i] == 'D' || value[i] == 'T' || value[i] == 'x' ||
                       value[i] == 'X') {
              ++i;
              storage_len_ += 8;
            } else if (value[i] == 'A' || value[i] == 'B') {
              ++i;
              storage_len_ += 9;
            } else if (value[i] == 'F' || value[i] == 's') {
              ++i;
              storage_len_ += 10;
            } else if (value[i] == 'r') {
              ++i;
              storage_len_ += 11;
            } else if (value[i] == 'c') {
              ++i;
              storage_len_ += 24;
            } else {
              ++i;
              storage_len_ += 1;
            }
          } else {
            ++i;
            storage_len_ += 1;
          }
        }
        storage_len_ += 1;
      }
    }
  }
  enum Functype functype() override { return EXTRACT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncTimeOfDay : public FieldFunc {
 public:
  explicit FieldFuncTimeOfDay(k_int8 tz) : FieldFunc() {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::CHAR;
    storage_type_ = roachpb::DataType::CHAR;
    storage_len_ = 48;
    time_zone = tz;
  }
  enum Functype functype() override { return TIME_OF_DAY_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;

  k_int8 time_zone;
};

class FieldFuncNow : public FieldFunc {
 public:
  using FieldFunc::FieldFunc;
  FieldFuncNow() {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::TIMESTAMP_NANO;
    storage_type_ = roachpb::DataType::TIMESTAMP_NANO;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return NOW_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncRandom : public FieldFunc {
 public:
  FieldFuncRandom() {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::DOUBLE;
    storage_type_ = roachpb::DataType::DOUBLE;
    storage_len_ = sizeof(k_double64);
  }
  enum Functype functype() override { return RANDOM_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncWidthBucket : public FieldFunc {
 public:
  explicit FieldFuncWidthBucket(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return WIDTHBUCKET_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncAge : public FieldFunc {
 public:
  FieldFuncAge(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = GET_HIGHER_PRECISION_TIME_TYPE(a->get_storage_type(), b->get_storage_type());
    storage_type_ = GET_HIGHER_PRECISION_TIME_TYPE(a->get_storage_type(), b->get_storage_type());
    storage_len_ = sizeof(k_int64);
  }
  explicit FieldFuncAge(Field *a) : FieldFunc(a) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = a->get_sql_type();
    storage_type_ = a->get_storage_type();
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return AGE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncCase : public FieldFunc {
 public:
  explicit FieldFuncCase(Field *a) : FieldFunc(a) {
    type_ = FIELD_CMP;
    sql_type_ = a->get_sql_type();
    storage_type_ = a->get_storage_type();
    storage_len_ = a->get_storage_length();
  }
  explicit FieldFuncCase(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_CMP;
    sql_type_ = b->get_sql_type();
    storage_type_ = b->get_storage_type();
    storage_len_ = b->get_storage_length();
  }

  explicit FieldFuncCase(const std::list<Field *> &fields) : FieldFunc(fields) {
    type_ = FIELD_CMP;
    sql_type_ = fields.front()->get_sql_type();
    storage_type_ = fields.front()->get_storage_type();
    storage_len_ = 0;
    for (auto &elem : fields) {
      k_int32 len = elem->get_storage_length();
      if (len > storage_len_) {
        storage_len_ = len;
      }
    }
  }
  enum Functype functype() override { return CASE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  k_bool is_condition_met() override;
  Field *field_to_copy() override;
  k_bool is_nullable() override;
  bool condition_met{0};
};

class FieldFuncThen : public FieldFuncCase {
 public:
  using FieldFuncCase::FieldFuncCase;

  enum Functype functype() override { return THEN_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  k_bool is_condition_met() override;
  Field *field_to_copy() override;
  k_bool is_nullable() override;
};

class FieldFuncElse : public FieldFunc {
 public:
  explicit FieldFuncElse(Field *a) : FieldFunc(a) {
    type_ = FIELD_CMP;
    sql_type_ = a->get_sql_type();
    storage_type_ = a->get_storage_type();
    storage_len_ = a->get_storage_length();
  }

  enum Functype functype() override { return ELSE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  k_bool is_condition_met() override;
  Field *field_to_copy() override;
};

class FieldFuncCrc32C : public FieldFunc {
 public:
  explicit FieldFuncCrc32C(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return CRC32C_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncCrc32I : public FieldFunc {
 public:
  explicit FieldFuncCrc32I(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return CRC32I_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncFnv32 : public FieldFunc {
 public:
  explicit FieldFuncFnv32(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return FNV32_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncFnv32a : public FieldFunc {
 public:
  explicit FieldFuncFnv32a(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return FNV32A_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncFnv64 : public FieldFunc {
 public:
  explicit FieldFuncFnv64(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return FNV64_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncFnv64a : public FieldFunc {
 public:
  explicit FieldFuncFnv64a(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
  }
  enum Functype functype() override { return FNV64A_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncCoalesce : public FieldFunc {
 public:
  explicit FieldFuncCoalesce(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_CMP;
    sql_type_ = a->get_sql_type();

    switch (a->get_storage_type()) {
      case roachpb::DataType::SMALLINT:
      case roachpb::DataType::INT:
      case roachpb::DataType::BIGINT:
        storage_type_ = roachpb::DataType::BIGINT;
        storage_len_ = sizeof(k_int64);
        break;
      case roachpb::DataType::FLOAT:
      case roachpb::DataType::DOUBLE:
        storage_type_ = roachpb::DataType::DOUBLE;
        storage_len_ = sizeof(k_double64);
        break;
      case roachpb::DataType::NULLVAL:
        storage_type_ = b->get_storage_type();
        storage_len_ = b->get_storage_length();
      default:
        storage_type_ = a->get_storage_type();
        storage_len_ = a->get_storage_length();
        break;
    }
  }

  enum Functype functype() override { return COALESCE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  k_bool is_nullable() override;
  Field *field_to_copy() override;
};

struct DiffInfo {
  k_bool hasPrev;
  k_bool includeNull;
  k_bool ignoreNegative;
  k_bool firstOutput;
  union {
    k_int64 i64_value;
    k_double64  d64_value;
  } prev;

  k_int64 prevTs;
};

class FieldFuncDiff : public FieldFunc {
 public:
  explicit FieldFuncDiff(Field *a) : FieldFunc(a) {
    type_ = FIELD_FUNC;
    if (roachpb::DataType::FLOAT == a->get_storage_type() ||
        roachpb::DataType::DOUBLE == a->get_storage_type()) {
      sql_type_ = roachpb::DataType::DOUBLE;
      storage_type_ = roachpb::DataType::DOUBLE;
      storage_len_ = sizeof(k_double64);
    } else {
      sql_type_ = roachpb::DataType::BIGINT;
      storage_type_ = roachpb::DataType::BIGINT;
      storage_len_ = sizeof(k_int64);
    }

    diff_info_.hasPrev = false;
    diff_info_.prevTs = -1;
  }

  enum Functype functype() override { return DIFF_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;
  k_bool is_nullable() override;

 public:
  DiffInfo diff_info_;
};

}  // namespace kwdbts
