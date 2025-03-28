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
#include "ee_field_func_math.h"

#include <cmath>
#include <iomanip>

#include "ee_field_common.h"
#include "ee_global.h"
#include "pgcode.h"
#include "ee_string.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

k_double64 doubleMathFunc1(Field **args, _double_val_fn valFn) {
  return valFn(args[0]->ValReal());
}
k_double64 doubleMathFunc2(Field **args, _double_val_fn_2 valFn) {
  return valFn(args[0]->ValReal(), args[1]->ValReal());
}
k_int64 intMathFunc1(Field **args, _int_val_fn valFn) {
  return valFn(args[0]->ValInt());
}
k_int64 intMathFunc2(Field **args, _int_val_fn_2 valFn) {
  return valFn(args[0]->ValInt(), args[1]->ValInt());
}

std::pair<roachpb::DataType, k_uint32> enlargeNumType(
    roachpb::DataType input_type, k_uint32 input_len) {
  switch (input_type) {
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
      return std::make_pair(roachpb::DataType::BIGINT, sizeof(k_int64));
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      return std::make_pair(roachpb::DataType::DOUBLE, sizeof(k_double64));

    default:
      return std::make_pair(input_type, input_len);
  }
}
std::pair<roachpb::DataType, k_uint32> getDoubleType(
    roachpb::DataType input_type, k_uint32 input_len) {
  return std::make_pair(roachpb::DataType::DOUBLE, sizeof(k_double64));
}
template <typename T>
bool checkOverflow(T input, roachpb::DataType out_type) {
  switch (out_type) {
    case roachpb::DataType::SMALLINT:
      if (input > (std::numeric_limits<k_int16>::max()) ||
          input < (std::numeric_limits<k_int16>::min())) {
        return false;
      }
      return true;
    case roachpb::DataType::INT:
      if (input > (std::numeric_limits<k_int32>::max()) ||
          input < (std::numeric_limits<k_int32>::min())) {
        return false;
      }
      return true;
    case roachpb::DataType::BIGINT:
      if (input > (std::numeric_limits<k_int64>::max()) ||
          input < (std::numeric_limits<k_int64>::min())) {
        return false;
      }
      return true;
    default:
      break;
  }
  return true;
}

k_int64 kmod(k_int64 x, k_int64 y) {
  x %= y;
  return x;
}
k_bool powPreCheck(Field **args, k_int32 arg_count) {
  if (arg_count == 2 && FLT_EQUAL(args[0]->ValReal(), 0.0) &&
      FLT_EQUAL(args[1]->ValReal(), 0.0)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                                  "invalid argument for power function");
    return FAIL;
  }
  return SUCCESS;
}

k_bool modPerCheck(Field **args, k_int32 arg_count) {
  for (k_uint32 i = 0; i < arg_count; ++i) {
    if (roachpb::DataType::FLOAT == args[i]->get_storage_type() ||
        roachpb::DataType::DOUBLE == args[i]->get_storage_type()) {
      return SUCCESS;
    }

    if (i != 0 && args[i]->ValInt() == 0) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO, "zero modulus");
      return FAIL;
    }
  }
  return SUCCESS;
}

k_bool logPreCheck(Field **args, k_int32 arg_count) {
  for (k_uint32 i = 0; i < arg_count; ++i) {
    if (FLT_EQUAL(args[i]->ValReal(), 0.0)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                                    "cannot take logarithm of zero");
      return FAIL;
    }

    if (args[i]->ValReal() < 0) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_INVALID_ARGUMENT_FOR_LOG,
          "cannot take logarithm of a negative number");
      return FAIL;
    }
  }

  return SUCCESS;
}

k_bool sqrtPreCheck(Field **args, k_int32 arg_count) {
  if (arg_count == 1 && args[0]->ValReal() < 0) {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
        "cannot take square root of a negative number");
    return FAIL;
  }

  return SUCCESS;
}

k_bool divPerCheck(Field **args, k_int32 arg_count) {
  for (k_uint32 i = 0; i < arg_count; i++) {
    if (roachpb::DataType::FLOAT == args[i]->get_storage_type() ||
        roachpb::DataType::DOUBLE == args[i]->get_storage_type()) {
      return SUCCESS;
    }

    if (i != 0 && args[i]->ValInt() == 0) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                    "div(): division by zero");
      return FAIL;
    }
  }
  return SUCCESS;
}

k_double64 klog2(k_double64 v, k_double64 base) {
  k_double64 a = log(v);
  k_double64 b = log(base);
  if (isnan(b) || isinf(b)) {
    return b;
  } else if (isnan(a) || isinf(a)) {
    return a;
  } else if (a == 0) {
    return INFINITY;
  } else {
    return b / a;
  }
}
k_double64 kround(k_double64 v, k_double64 bits) {
  k_double64 number = v;
  stringstream ss;
  ss << fixed << setprecision(bits) << number;
  ss >> number;
  return number;
}

k_double64 doubleMod(k_double64 x, k_double64 y) {
  if (FLT_EQUAL(y, 0.0)) {
    return NAN;
  }

  return fmod(x, y);
}

k_double64 kcot(k_double64 val) {
  k_double64 a = cos(val);
  k_double64 b = sin(val);

  return a / b;
}

k_double64 kdegrees(k_double64 val) { return (val * (180.0 / M_PI)); }

k_double64 katan2(k_double64 x, k_double64 y) { return atan(x / y); }

k_double64 kdivdouble(k_double64 x, k_double64 y) {
  k_double64 val = x / y;
  if ((val != INFINITY) && (val != -INFINITY)) {
    val = k_int64(val);
  }
  return val;
}

k_int64 kdivint(k_int64 x, k_int64 y) { return x / y; }

k_double64 sinFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, sin);
}
k_double64 cosFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, cos);
}
k_double64 tanFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, tan);
}
k_double64 asinFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, asin);
}
k_double64 acosFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, acos);
}
k_double64 atanFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, atan);
}
k_double64 powFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc2(args, pow);
}
k_double64 sqrtFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, sqrt);
}
k_double64 roundFunc1(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, round);
}
k_double64 roundFunc2(Field **args, k_int32 arg_count) {
  return doubleMathFunc2(args, kround);
}
k_double64 logFunc1(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, log10);
}
k_double64 logFunc2(Field **args, k_int32 arg_count) {
  return doubleMathFunc2(args, klog2);
}
k_double64 atanFunc2(Field **args, k_int32 arg_count) {
  return doubleMathFunc2(args, katan2);
}
k_int64 modFunc(Field **args, k_int32 arg_count) {
  return intMathFunc2(args, kmod);
}
k_double64 modDoubleFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc2(args, doubleMod);
}
k_int64 absIntFunc(Field **args, k_int32 arg_count) {
  if (args[0]->get_storage_type() == roachpb::DataType::BIGINT) {
    k_int64 val = args[0]->ValInt();
    if (val == std::numeric_limits<int64_t>::min()) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
          "abs(): abs of min integer value (-9223372036854775808) not defined");
      return 0;
    }
    return abs(val);
  }
  return intMathFunc1(args, abs);
}
k_double64 absDoubleFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, abs);
}
k_double64 ceilFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, ceil);
}
k_double64 floorFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, floor);
}

// add start
k_double64 kisnan(k_double64 val) { return isnan(val); }

k_double64 kradians(k_double64 val) { return val * (M_PI / 180.0); }

k_double64 ksign(k_double64 val) {
  if (val > 0) {
    return 1;
  } else if (val < 0) {
    return -1;
  } else {
    return 0;
  }
}

k_double64 isnanFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, kisnan);
}

k_double64 lnFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, log);
}

k_double64 radiansFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, kradians);
}

k_double64 signFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, ksign);
}

k_double64 truncFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, trunc);
}
// add end

k_double64 cotFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, kcot);
}
k_double64 cbrtFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, cbrt);
}
k_double64 expFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, exp);
}
k_double64 degreesFunc(Field **args, k_int32 arg_count) {
  return doubleMathFunc1(args, kdegrees);
}

k_double64 divDoubleFunc2(Field **args, k_int32 arg_count) {
  return doubleMathFunc2(args, kdivdouble);
}

k_int64 divFunc2(Field **args, k_int32 arg_count) {
  return intMathFunc2(args, kdivint);
}

const FieldMathFuncion mathFuncBuiltins1[] = {
    {
        .name = "sin",
        .func_type = FieldFunc::Functype::SIN_FUNC,
        .int_func = nullptr,
        .double_func = sinFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "cos",
        .func_type = FieldFunc::Functype::COS_FUNC,
        .int_func = nullptr,
        .double_func = cosFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "tan",
        .func_type = FieldFunc::Functype::TAN_FUNC,
        .int_func = nullptr,
        .double_func = tanFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "asin",
        .func_type = FieldFunc::Functype::ASIN_FUNC,
        .int_func = nullptr,
        .double_func = asinFunc,
    },
    {
        .name = "acos",
        .func_type = FieldFunc::Functype::ACOS_FUNC,
        .int_func = nullptr,
        .double_func = acosFunc,
    },
    {
        .name = "atan",
        .func_type = FieldFunc::Functype::ATAN_FUNC,
        .int_func = nullptr,
        .double_func = atanFunc,
        .precheck_func = nullptr,
        .field_type_func = enlargeNumType,
    },
    {
        .name = "sqrt",
        .func_type = FieldFunc::Functype::SQRT_FUNC,
        .int_func = nullptr,
        .double_func = sqrtFunc,
        .precheck_func = sqrtPreCheck,
        .field_type_func = getDoubleType,
    },
    {
        .name = "round",
        .func_type = FieldFunc::Functype::ROUND_FUNC,
        .int_func = nullptr,
        .double_func = roundFunc1,
        .precheck_func = nullptr,
        .field_type_func = enlargeNumType,
    },
    {
        .name = "log",
        .func_type = FieldFunc::Functype::LOG_FUNC,
        .int_func = nullptr,
        .double_func = logFunc1,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "abs",
        .func_type = FieldFunc::Functype::ABS_FUNC,
        .int_func = absIntFunc,
        .double_func = absDoubleFunc,
    },
    {
        .name = "ceil",
        .func_type = FieldFunc::Functype::CEIL_FUNC,
        .int_func = nullptr,
        .double_func = ceilFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "ceiling",
        .func_type = FieldFunc::Functype::CEIL_FUNC,
        .int_func = nullptr,
        .double_func = ceilFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "floor",
        .func_type = FieldFunc::Functype::FLOOR_FUNC,
        .int_func = nullptr,
        .double_func = floorFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "isnan",
        .func_type = FieldFunc::Functype::IS_NAN_FUNC,
        .int_func = nullptr,
        .double_func = isnanFunc,
    },
    {
        .name = "ln",
        .func_type = FieldFunc::Functype::LN_FUNC,
        .int_func = nullptr,
        .double_func = lnFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "radians",
        .func_type = FieldFunc::Functype::RADIANS_FUNC,
        .int_func = nullptr,
        .double_func = radiansFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "sign",
        .func_type = FieldFunc::Functype::SIGN_FUNC,
        .int_func = nullptr,
        .double_func = signFunc,
        .precheck_func = nullptr,
        .field_type_func = enlargeNumType,
    },
    {
        .name = "trunc",
        .func_type = FieldFunc::Functype::TRUNC_FUNC,
        .int_func = nullptr,
        .double_func = truncFunc,
    },
    {
        .name = "cot",
        .func_type = FieldFunc::Functype::COT_FUNC,
        .int_func = nullptr,
        .double_func = cotFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "cbrt",
        .func_type = FieldFunc::Functype::CBRT_FUNC,
        .int_func = nullptr,
        .double_func = cbrtFunc,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "exp",
        .func_type = FieldFunc::Functype::EXP_FUNC,
        .int_func = nullptr,
        .double_func = expFunc,
        .precheck_func = nullptr,
        .field_type_func = enlargeNumType,
    },
    {
        .name = "degrees",
        .func_type = FieldFunc::Functype::DEGRESS_FUNC,
        .int_func = nullptr,
        .double_func = degreesFunc,
    }};

const FieldMathFuncion mathFuncBuiltins2[] = {
    {
        .name = "log",
        .func_type = FieldFunc::Functype::LOG_FUNC,
        .int_func = nullptr,
        .double_func = logFunc2,
        .precheck_func = logPreCheck,
        .field_type_func = enlargeNumType,
    },
    {
        .name = "round",
        .func_type = FieldFunc::Functype::ROUND_FUNC,
        .int_func = nullptr,
        .double_func = roundFunc2,
        .precheck_func = nullptr,
        .field_type_func = enlargeNumType,
    },
    {
        .name = "pow",
        .func_type = FieldFunc::Functype::POWER_FUNC,
        .int_func = nullptr,
        .double_func = powFunc,
        .precheck_func = powPreCheck,
        .field_type_func = enlargeNumType,
        .error_tag = "exponent",
    },
    {
        .name = "power",
        .func_type = FieldFunc::Functype::POWER_FUNC,
        .int_func = nullptr,
        .double_func = powFunc,
        .precheck_func = powPreCheck,
        .field_type_func = enlargeNumType,
        .error_tag = "exponent",
    },
    {
        .name = "mod",
        .func_type = FieldFunc::Functype::MOD_FUNC,
        .int_func = modFunc,
        .double_func = modDoubleFunc,
        .precheck_func = modPerCheck,
    },
    {
        .name = "atan2",
        .func_type = FieldFunc::Functype::ATAN2_FUNC,
        .int_func = nullptr,
        .double_func = atanFunc2,
        .precheck_func = nullptr,
        .field_type_func = getDoubleType,
    },
    {
        .name = "div",
        .func_type = FieldFunc::Functype::DIVMATH_FUNC,
        .int_func = divFunc2,
        .double_func = divDoubleFunc2,
        .precheck_func = divPerCheck,
    }};
const k_int32 mathFuncBuiltinsNum1 =
    (sizeof(mathFuncBuiltins1) / sizeof(FieldMathFuncion));
const k_int32 mathFuncBuiltinsNum2 =
    (sizeof(mathFuncBuiltins2) / sizeof(FieldMathFuncion));

FieldFuncMath::FieldFuncMath(const std::list<Field *> &fields,
                             const FieldMathFuncion &func)
    : FieldFunc(fields) {
  type_ = FIELD_ARITHMETIC;
  if (func.field_type_func != nullptr) {
    std::pair<roachpb::DataType, k_uint32> field_info =
        func.field_type_func(fields.front()->get_storage_type(),
                             fields.front()->get_storage_length());
    sql_type_ = field_info.first;
    storage_type_ = field_info.first;
    storage_len_ = field_info.second;
  } else {
    set_storage_info();
  }
  func_type_ = func.func_type;
  int_func_ = func.int_func;
  double_func_ = func.double_func;
  precheck_func = func.precheck_func;
}
FieldFuncMath::FieldFuncMath(Field *left, Field *right,
                             const FieldMathFuncion &func)
    : FieldFunc(left, right) {
  type_ = FIELD_ARITHMETIC;
  if (func.field_type_func != nullptr) {
    std::pair<roachpb::DataType, k_uint32> field_info = func.field_type_func(
        left->get_storage_type(), left->get_storage_length());
    sql_type_ = field_info.first;
    storage_type_ = field_info.first;
    storage_len_ = field_info.second;
  } else {
    set_storage_info();
  }
  func_type_ = func.func_type;
  int_func_ = func.int_func;
  double_func_ = func.double_func;
  precheck_func = func.precheck_func;
  error_tag = func.error_tag;
}
k_int64 FieldFuncMath::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  }
  KString et = error_tag.empty() ? "integer" : error_tag;
  if (precheck_func != nullptr && precheck_func(args_, arg_count_) != SUCCESS) {
    return 0;
  }
  if (int_func_ != nullptr) {
    k_int64 val = int_func_(args_, arg_count_);
    if (!checkOverflow<k_int64>(val, storage_type_)) {
      et += " out of range";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    et.data());
      return 0;
    }
    return val;
  } else {
    k_double64 val = double_func_(args_, arg_count_);
    if (!checkOverflow<k_double64>(val, storage_type_)) {
      et += " out of range";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    et.data());
      return 0;
    }
    return val;
  }
}

k_double64 FieldFuncMath::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValReal(ptr);
  }
  KString et = error_tag.empty() ? "float" : error_tag;
  if (precheck_func != nullptr && precheck_func(args_, arg_count_) != SUCCESS) {
    return 0;
  }
  if (double_func_ != nullptr) {
    k_double64 val = double_func_(args_, arg_count_);
    if (!checkOverflow<k_double64>(val, storage_type_)) {
      et += " out of range";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    et.data());

      return 0.0;
    }
    return val;
  } else {
    k_int64 val = int_func_(args_, arg_count_);
    if (!checkOverflow<k_int64>(val, storage_type_)) {
      et += " out of range";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    et.data());
      return 0.0;
    }
    return val;
  }
}

String FieldFuncMath::ValStr() {
  if (double_func_ != nullptr) {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
    s.length_ = strlen(s.ptr_);
    return s;
  } else {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
    s.length_ = strlen(s.ptr_);
    return s;
  }
}

k_bool FieldFuncMath::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->is_nullable()) {
      return true;
    }
  }
  return false;
}

Field *FieldFuncMath::field_to_copy() {
  FieldFuncMath *field = new FieldFuncMath(*this);
  return field;
}

KStatus FieldFuncMath::set_storage_info() {
  if (arg_count_ == 0) {
    storage_type_ = roachpb::DataType::DOUBLE;
    storage_len_ = sizeof(k_float64);
    return SUCCESS;
  }
  sql_type_ = args_[0]->get_sql_type();
  storage_type_ = args_[0]->get_storage_type();
  storage_len_ = args_[0]->get_storage_length();
  for (k_uint32 i = 1; i < arg_count_; ++i) {
    if (args_[i]->get_storage_type() > storage_type_) {
      storage_type_ = args_[i]->get_storage_type();
      storage_len_ = args_[i]->get_storage_length();
    }
    if (args_[i]->get_sql_type() > sql_type_) {
      sql_type_ = args_[i]->get_sql_type();
    }
  }
  return SUCCESS;
}
}  // namespace kwdbts
