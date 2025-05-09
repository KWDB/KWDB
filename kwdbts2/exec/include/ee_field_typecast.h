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

#include <cmath>
#include <string>

#include "ee_cast_utils.h"
#include "ee_field.h"
#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

typedef KStatus (*_to_signed_func)(Field *, k_int64 &);
typedef KStatus (*_to_real_func)(Field *, k_double64 &);
typedef KStatus (*_to_string_func)(Field *, char *, k_uint32);
typedef KStatus (*_to_timestamp_func)(Field *, k_int64 &, k_int64, k_int64, roachpb::DataType);
typedef KStatus (*_to_bool_func)(Field *, k_bool &);

// inline KStatus integerToString(Field *field, )
class FieldTypeCast : public Field {
 public:
  explicit FieldTypeCast(Field *field) : field_{field} {
    type_ = Field::Type::FIELD_TYPECAST;
    // storage_type_ = field->get_storage_type();
    // storage_len_ = field->get_storage_length();
  }
  ~FieldTypeCast();
  k_int64 ValInt() override { return 0; }
  k_double64 ValReal() override { return 0.0; }
  String ValStr() override { return String(""); }
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal(char *ptr) override;
  String ValStr(char *ptr) override;
  char *get_ptr() override;
  k_bool is_nullable() override;
  k_bool fill_template_field(char *ptr) override;

 protected:
  Field *field_{nullptr};
};
template <typename T>
class FieldTypeCastSigned : public FieldTypeCast {
 public:
  explicit FieldTypeCastSigned(Field *field);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastSigned<T>(*this); }
  _to_signed_func func_;
};
template <typename T>
class FieldTypeCastReal : public FieldTypeCast {
 public:
  explicit FieldTypeCastReal(Field *field);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() { return new FieldTypeCastReal<T>(*this); }
  _to_real_func func_;
};

class FieldTypeCastString : public FieldTypeCast {
 public:
  explicit FieldTypeCastString(Field *field, k_uint32 field_length,
                               KString &output_type);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastString(*this); }
  _to_string_func func_{nullptr};

 protected:
  k_uint32 letter_len_ = 0;
};
class FieldTypeCastTimestamptz2String : public FieldTypeCast {
 public:
  explicit FieldTypeCastTimestamptz2String(Field *field, k_uint32 field_length,
                                           KString &output_type,
                                           k_int8 time_zone);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastTimestamptz2String(*this); }
  k_int8 time_zone_;
  k_int64 type_scale_{1};
};
class FieldTypeCastTimestampTz : public FieldTypeCast {
 public:
  explicit FieldTypeCastTimestampTz(Field *field, k_int8 type_num, k_int8 timezone);

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastTimestampTz(*this); }
  _to_timestamp_func func_;
  k_int64 timezone_diff_;
  k_int64 type_scale_{1};
};

class FieldTypeCastBool : public FieldTypeCast {
 public:
  explicit FieldTypeCastBool(Field *field);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastBool(*this); }
  _to_bool_func func_;
};
class FieldTypeCastBytes : public FieldTypeCast {
 public:
  explicit FieldTypeCastBytes(Field *field, k_uint32 field_length,
                              k_uint32 bytes_length);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastBytes(*this); }
  _to_bool_func func_;

 protected:
  k_uint32 bytes_len_ = 0;
};

class FieldTypeCastDecimal : public FieldTypeCast {
 public:
  explicit FieldTypeCastDecimal(Field *field);
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() { return new FieldTypeCastDecimal(*this); }
};

}  // namespace kwdbts
