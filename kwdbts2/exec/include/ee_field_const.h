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

#include "ee_field.h"
#include "ee_field_common.h"
#include "ee_cast_utils.h"

namespace kwdbts {

// base const class
class FieldConst : public Field {
 public:
  using Field::get_ptr;
  FieldConst(roachpb::DataType datatype, k_uint32 length)
      : Field(-1, datatype, length, FIELD_CONSTANT) {}

  char *get_ptr() override { return nullptr; }
  k_bool is_nullable() override { return false; }
};

class FieldConstInt : public FieldConst {
 public:
  FieldConstInt(roachpb::DataType datatype, k_int64 data, k_uint32 length)
      : FieldConst(datatype, length) {
    value_ = data;
    storage_type_ = datatype;
    storage_len_ = length;
    return_type_ = KWDBTypeFamily::IntFamily;
  }

  char *get_ptr(RowBatch *batch) override;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;

  Field *field_to_copy() override;
  k_bool fill_template_field(char *ptr) override;

 protected:
  k_int64 value_;
};

class FieldConstInterval : public FieldConst {
 public:
  FieldConstInterval(roachpb::DataType datatype, const KString &str)
  : FieldConst(datatype, str.size()) {
    value_ = str;
    // storage_type_ = roachpb::DataType::TIMESTAMP;
    storage_len_ = sizeof(k_int64);
    return_type_ = KWDBTypeFamily::IntervalFamily;
    type_ = FIELD_INTERVAL;
  }

  char *get_ptr(RowBatch *batch) override;
  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) { return 0; }
  k_int64 ValInt(k_int64 *val, k_bool negative) override;
  k_double64 ValReal() { return 0.0; }
  k_double64 ValReal(k_char *ptr) { return 0.0; }
  String ValStr() { return String(""); }
  String ValStr(k_char *ptr) { return String(""); }

  Field *field_to_copy() override;
  k_bool fill_template_field(char *ptr) override { return 0; }

 protected:
  KString value_;
};

class FieldConstDouble : public FieldConst {
 public:
  FieldConstDouble(roachpb::DataType datatype, k_double64 data)
      : FieldConst(datatype, sizeof(double)) {
    value_ = data;
    storage_type_ = datatype;
    storage_len_ = sizeof(double);
    return_type_ = KWDBTypeFamily::FloatFamily;
  }

  char *get_ptr(RowBatch *batch) override;
  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;

  Field *field_to_copy() override;
  k_bool fill_template_field(char *ptr) override;

 protected:
  k_double64 value_;
};

class FieldConstString : public FieldConst {
 public:
  FieldConstString(roachpb::DataType datatype, const KString &str)
      : FieldConst(datatype, str.size()) {
    value_ = str;
    storage_type_ = datatype;
    if (IsStorageString(datatype)) {
      storage_len_ = str.size() + 1;
    } else {
      storage_len_ = sizeof(k_int64);
    }
    return_type_ = KWDBTypeFamily::StringFamily;
  }

  char *get_ptr(RowBatch *batch) override;
  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;

  Field *field_to_copy() override;
  k_bool fill_template_field(char *ptr) override;

 protected:
  KString value_;
};

class FieldConstDate : public FieldConstString {
 public:
  FieldConstDate(roachpb::DataType datatype, const KString &str, k_int8 timezone)
    : FieldConstString(datatype, str) {
      timezone_ = timezone;
    }
  k_int64 ValInt() override;
 protected:
  k_int8 timezone_;
};

class FieldConstNull : public FieldConst {
 public:
  FieldConstNull(roachpb::DataType datatype, k_uint32 length)
      : FieldConst(datatype, length) {}
  k_int64 ValInt() { return 0; }
  k_int64 ValInt(k_char *ptr) { return 0; }
  k_double64 ValReal() { return 0; }
  k_double64 ValReal(k_char *ptr) { return 0; }
  String ValStr() { return String(""); }
  String ValStr(k_char *ptr) { return String(""); }
  Field *field_to_copy() override;
  k_bool fill_template_field(char *ptr) { return 0; }
  char *get_ptr() override { return nullptr; }
  k_bool is_nullable() override { return true; }
};

}  // namespace kwdbts
