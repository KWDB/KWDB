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
#include<utility>

#include "ee_field.h"

namespace kwdbts {

typedef k_int64 (*_int_fn)(Field **, k_int32);
typedef k_double64 (*_double_fn)(Field **, k_int32);

typedef float (*_float_val_fn)(float);
typedef double (*_double_val_fn)(double);
typedef double (*_double_val_fn_2)(double, double);
typedef k_int64 (*_int_val_fn)(k_int64);
typedef k_int64 (*_int_val_fn_2)(k_int64, k_int64);

typedef std::pair<roachpb::DataType, k_uint32> (*_func_field_type_fn)(roachpb::DataType, k_uint32);

typedef k_bool (*_precheck_fn)(Field **, k_int32);
typedef struct FieldMathFuncion {
  KString name;
  FieldFunc::Functype func_type;
  _int_fn int_func;
  _double_fn double_func;
  _precheck_fn precheck_func;
  _func_field_type_fn field_type_func;
  KString error_tag;
} FieldMathFuncion;

extern const FieldMathFuncion mathFuncBuiltins1[];
extern const k_int32 mathFuncBuiltinsNum1;
extern const FieldMathFuncion mathFuncBuiltins2[];
extern const k_int32 mathFuncBuiltinsNum2;
class FieldFuncMath : public FieldFunc {
 public:
  FieldFuncMath(const std::list<Field *> &fields, const FieldMathFuncion &func);
  FieldFuncMath(Field *left,  Field *right, const FieldMathFuncion &func);

  enum Functype functype() override { return func_type_; }


  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  Field *field_to_copy() override;

 protected:
  KStatus set_storage_info();
  k_bool field_is_nullable() override;

  Functype func_type_;
  _int_fn int_func_;
  _double_fn double_func_;
  _precheck_fn precheck_func;
  KString error_tag;
};
}  // namespace kwdbts
