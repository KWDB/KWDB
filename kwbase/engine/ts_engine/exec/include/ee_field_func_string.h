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
#include <codecvt>
#include <iostream>
#include <list>
#include <locale>
#include <regex>
#include <sstream>
#include <string>

#include "ee_field.h"

namespace kwdbts {

typedef String (*_str_fn)(Field **, k_int32);
typedef KStatus (*_str_func_field_type_fn)(const std::list<Field *> &,
                                       roachpb::DataType *, roachpb::DataType *,
                                       k_uint32 *);

typedef struct FieldStringFuncion {
  KString name;
  FieldFunc::Functype func_type;
  _str_fn func1;
  _str_fn func2;
  _str_fn func3;
  _str_fn funcn;
  _str_func_field_type_fn field_type_func;
} FieldStringFuncion;

class FieldFuncString : public FieldFunc {
 public:
  FieldFuncString(const KString &name, const std::list<Field *> &fields);
  enum Functype functype() override { return func_type_; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

  Functype func_type_;
  _str_fn func_;

 protected:
  String strvalue_{""};
};

class FieldFuncLength : public FieldFunc {
 public:
  explicit FieldFuncLength(Field *a) : FieldFunc(a) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
  }
  enum Functype functype() override { return LENGTH_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncChr : public FieldFunc {
 public:
  explicit FieldFuncChr(Field *a) : FieldFunc(a) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::CHAR;
    storage_type_ = roachpb::DataType::CHAR;
    storage_len_ = sizeof(k_char) * 3;
  }
  enum Functype functype() override { return ENCODE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  String strvalue_{""};
};

class FieldFuncDecode : public FieldFunc {
 public:
  explicit FieldFuncDecode(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::BINARY;
    storage_type_ = roachpb::DataType::BINARY;
    storage_len_ = a->get_storage_length();
  }
  enum Functype functype() override { return ENCODE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  String strvalue_{""};
};

class FieldFuncEncode : public FieldFunc {
 public:
  explicit FieldFuncEncode(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::CHAR;
    storage_type_ = roachpb::DataType::CHAR;
    storage_len_ = a->get_storage_length() * 4;
  }
  enum Functype functype() override { return ENCODE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  String strvalue_{""};
};

class FieldFuncGetBit : public FieldFunc {
 public:
  explicit FieldFuncGetBit(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_ARITHMETIC;
  }
  enum Functype functype() override { return GETBIT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncInitCap : public FieldFunc {
 public:
  explicit FieldFuncInitCap(Field *a) : FieldFunc(a) {
    type_ = FIELD_ARITHMETIC;

    sql_type_ = a->get_sql_type();
    storage_type_ = a->get_storage_type();
    storage_len_ = a->get_storage_length();
  }
  enum Functype functype() override { return INITCAP_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  String strvalue_{""};
};

class FieldFuncBitLength : public FieldFunc {
 public:
  explicit FieldFuncBitLength(Field *a) : FieldFunc(a) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
  }
  enum Functype functype() override { return LENGTH_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncOctetLength : public FieldFunc {
 public:
  explicit FieldFuncOctetLength(Field *a) : FieldFunc(a) {
    type_ = FIELD_ARITHMETIC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
  }
  enum Functype functype() override { return LENGTH_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

}  // namespace kwdbts
