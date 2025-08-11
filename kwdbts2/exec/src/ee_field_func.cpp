// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_field_func.h"

#include <time.h>

#include <cmath>
#include <exception>
#include <iomanip>
#include <random>
#include <sstream>

#include "ee_common.h"
#include "ee_crc32.h"
#include "ee_field_common.h"
#include "ee_fnv.h"
#include "ee_global.h"
#include "ee_timestamp_utils.h"
#include "pgcode.h"
#include "ts_time_partition.h"

namespace kwdbts {

void FieldFuncOp::CalcStorageType() {
  k_bool is_all_timestamp = KTRUE;
  sql_type_ = roachpb::DataType::TIMESTAMP;
  storage_type_ = roachpb::DataType::TIMESTAMP;
  for (k_int32 i = 0; i < arg_count_; ++i) {
    auto arg_storage_type = args_[i]->get_storage_type();
    if (roachpb::DataType::FLOAT == arg_storage_type ||
        roachpb::DataType::DOUBLE == arg_storage_type) {
      sql_type_ = roachpb::DataType::DOUBLE;
      storage_type_ = roachpb::DataType::DOUBLE;
      storage_len_ = sizeof(k_double64);
      return;
    } else if (is_all_timestamp &&
               (arg_storage_type == roachpb::DataType::TIMESTAMP ||
                arg_storage_type == roachpb::DataType::TIMESTAMPTZ ||
                arg_storage_type == roachpb::DataType::TIMESTAMP_MICRO ||
                arg_storage_type == roachpb::DataType::TIMESTAMPTZ_MICRO ||
                arg_storage_type == roachpb::DataType::TIMESTAMP_NANO ||
                arg_storage_type == roachpb::DataType::TIMESTAMPTZ_NANO)) {
      if (sql_type_ < arg_storage_type) {
        sql_type_ = arg_storage_type;
        storage_type_ = arg_storage_type;
      }
    } else {
      is_all_timestamp = KFALSE;
    }
  }

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
      for (k_int32 i = 0; i < arg_count_; ++i) {
        switch (args_[i]->get_storage_type()) {
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
            time_scales_.push_back(1000);
            break;
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
            time_scales_.push_back(1);
            break;
          default:
            break;
        }
      }
      break;
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      for (k_int32 i = 0; i < arg_count_; ++i) {
        switch (args_[i]->get_storage_type()) {
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
            time_scales_.push_back(1000000);
            break;
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
            time_scales_.push_back(1000);
            break;
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
            time_scales_.push_back(1);
            break;
          default:
            break;
        }
      }
      break;
    default:
      for (k_int32 i = 0; i < arg_count_; ++i) {
        time_scales_.push_back(1);
      }
      break;
  }

  if (!is_all_timestamp) {
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
  }

  storage_len_ = sizeof(k_int64);
}

static k_int64 getDateTrunc(k_bool is_unit_const, k_bool type_scale_multi_or_divde, k_int8 time_zone,
                            k_int64 type_scale, k_int64 time_diff, KWDBTypeFamily return_type,
                            roachpb::DataType date_type, KString &unit, String strvalue, k_int64 intvalue) {
  if (!is_unit_const) {
    unit = replaceTimeUnit({strvalue.getptr(), strvalue.length_});
    getTimeFieldType(date_type, unit, &time_diff, &type_scale, &type_scale_multi_or_divde);
  }
  k_int64 original_timestamp = intvalue;
  CKTime ck_time = getCKTime(original_timestamp, date_type, time_zone);
  struct tm ltm;
  if (return_type == KWDBTypeFamily::TimestampTZFamily) {
    ck_time.t_timespec.tv_sec += ck_time.t_abbv;
    original_timestamp += time_diff;
  }
  gmtime_r(&ck_time.t_timespec.tv_sec, &ltm);
  if (type_scale != 1) {
    // multi
    if (type_scale_multi_or_divde) {
      if (!I64_SAFE_MUL_CHECK(original_timestamp, type_scale)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Timestamp/TimestampTZ out of range");
        return 0;
      }
      original_timestamp *= type_scale;
      // divide
    } else {
      original_timestamp /= type_scale;
    }
  }
  if (unit == "ns" || unit == "us" || unit == "ms") {
    return original_timestamp;
  } else if (unit == "s") {
    return original_timestamp / 1000 * 1000;
  } else if (unit == "m") {
    return original_timestamp / 60000 * 60000;
  }
  ltm.tm_sec = 0;
  ltm.tm_min = 0;
  while (KTRUE) {
    if (unit == "h") {
      break;
    }
    ltm.tm_hour = 0;
    if (unit == "d") {
      break;
    }
    if (unit == "w") {
      if (ltm.tm_wday == 0) {
        // Sunday
        ltm.tm_mday -= 6;
      } else {
        ltm.tm_mday -= ltm.tm_wday - 1;
      }
      break;
    }
    ltm.tm_mday = 1;
    if (unit == "n") {
      break;
    }
    if (unit == "quarter") {
      if (ltm.tm_mon <= 2) {
        ltm.tm_mon = 0;
      } else if (ltm.tm_mon <= 5) {
        ltm.tm_mon = 3;
      } else if (ltm.tm_mon <= 8) {
        ltm.tm_mon = 6;
      } else {
        ltm.tm_mon = 9;
      }
      break;
    }
    ltm.tm_mon = 0;
    if (unit == "y") {
      break;
    }
    if (unit == "decade") {
      ltm.tm_year -= ltm.tm_year % 12;
    } else if (unit == "century") {
      ltm.tm_year -= ltm.tm_year % 100;
    } else if (unit == "millennium") {
      ltm.tm_year -= ltm.tm_year % 1000;
    } else {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "unsupported timespan");
      return 0;
    }
    break;
  }
  if (return_type == KWDBTypeFamily::TimestampTZFamily) {
    return (mktime(&ltm) + ltm.tm_gmtoff - time_zone * 3600) * 1000;
  }
  return (mktime(&ltm) + ltm.tm_gmtoff) * 1000;
}

char *FieldFuncPlus::get_ptr(RowBatch *batch) {
  k_bool is_double = false;
  intvalue_ = 0;
  doublevalue_ = 0.0;

  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field plus, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          if (args_[i]->get_field_type() == FIELD_INTERVAL) {
            if (i == 0) {
              intvalue_ = args_[1]->ValInt(ptr);
              intvalue_ = args_[i]->ValInt(&intvalue_, KFALSE);
              ++i;
            } else {
              intvalue_ = args_[i]->ValInt(&intvalue_, KFALSE);
            }
          } else {
            k_int64 arg_val = args_[i]->ValInt(ptr);
            if (I64_SAFE_ADD_CHECK(intvalue_, arg_val)) {
              intvalue_ += arg_val;
            } else {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range");
              return const_cast<char *>("");
            }
          }
          break;
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          is_double = true;
          doublevalue_ += args_[i]->ValReal(ptr);
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field plus");
          return const_cast<char *>("");
          break;
      }
    }
  }

  if (is_double) {
    doublevalue_ += intvalue_;
    return reinterpret_cast<char *>(&doublevalue_);
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncPlus::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = 0;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (!args_[i]->CheckNull()) {
        if (args_[i]->get_field_type() == FIELD_INTERVAL) {
          if (i == 0) {
            val = args_[1]->ValInt();
            val = args_[i]->ValInt(&val, KFALSE);
            ++i;
          } else {
            val = args_[i]->ValInt(&val, KFALSE);
          }
        } else {
          k_int64 arg_val = args_[i]->ValInt();
          if (I64_SAFE_ADD_CHECK(val, arg_val)) {
            val += arg_val;
          } else {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                          "integer out of range");
            return 0;
          }
        }
      }
    }

    return val;
  }
}

k_double64 FieldFuncPlus::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else if (storage_type_ == roachpb::DataType::BIGINT) {
    return ValInt();
  } else {
    k_double64 val = 0;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (!args_[i]->CheckNull()) val += args_[i]->ValReal();
    }

    return val;
  }
}

String FieldFuncPlus::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValStr(ptr);
  } else {
    std::string val = "";
    for (size_t i = 0; i < arg_count_; ++i) {
      if (!args_[i]->CheckNull()) {
        String s1 = args_[i]->ValStr();
        val += std::string(s1.getptr(), s1.length_);
      }
    }

    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%s", val.c_str());
    s.length_ = strlen(s.ptr_);
    return s;
    // return val;
  }
}

Field *FieldFuncPlus::field_to_copy() {
  FieldFuncPlus *field = new FieldFuncPlus(*this);

  return field;
}

char *FieldFuncMinus::get_ptr(RowBatch *batch) {
  k_bool is_double = false;
  KStatus err = SUCCESS;
  char *ptr = nullptr;
  std::list<k_int64>::iterator it = time_scales_.begin();
  intvalue_ = 0;
  doublevalue_ = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field minus, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          if (i != 0) {
            ++it;
            if (args_[i]->get_field_type() == FIELD_INTERVAL) {
              intvalue_ = args_[i]->ValInt(&intvalue_, KTRUE);
            } else {
              k_int64 val2 = args_[i]->ValInt(ptr);
              if (!I64_SAFE_MUL_CHECK(val2, (*it))) {
                EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Timestamp/TimestampTZ out of range");
                err = FAIL;
                return const_cast<char *>("");
              }
              val2 *= (*it);
              if (!I64_SAFE_SUB_CHECK(intvalue_, val2)) {
                EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range");
                return 0;
              }
              intvalue_ -= val2;
            }
          } else {
            intvalue_ = args_[0]->ValInt(ptr);
            if (!I64_SAFE_MUL_CHECK(intvalue_, (*it))) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Timestamp/TimestampTZ out of range");
              err = FAIL;
              return const_cast<char *>("");
            }
            intvalue_ *= (*it);
          }
          break;
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
          if (i != 0) {
            doublevalue_ -= args_[i]->ValReal(ptr);
          } else {
            doublevalue_ = args_[i]->ValReal(ptr);
          }
          is_double == true;
          break;
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field minus");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (is_double) {
    doublevalue_ += intvalue_;
    return reinterpret_cast<char *>(&doublevalue_);
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncMinus::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = 0;
    std::list<k_int64>::iterator it = time_scales_.begin();
    val = args_[0]->ValInt();
    if (!I64_SAFE_MUL_CHECK(val, (*it))) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                    "Timestamp/TimestampTZ out of range");
      return 0;
    }
    val *= (*it);
    for (size_t i = 1; i < arg_count_; ++i) {
      ++it;
      if (args_[i]->get_field_type() == FIELD_INTERVAL) {
        val = args_[i]->ValInt(&val, KTRUE);
      } else {
        k_int64 val2 = args_[i]->ValInt();
        if (!I64_SAFE_MUL_CHECK(val2, (*it))) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                        "Timestamp/TimestampTZ out of range");
          return 0;
        }
        val2 *= (*it);
        if (!I64_SAFE_SUB_CHECK(val, val2)) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                        "integer out of range");
          return 0;
        }
        val -= val2;
      }
    }
    return val;
  }
}

k_double64 FieldFuncMinus::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = 0;
    val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      val -= args_[i]->ValReal();
    }
    return val;
  }
}

String FieldFuncMinus::ValStr() { return String(""); }

Field *FieldFuncMinus::field_to_copy() {
  FieldFuncMinus *field = new FieldFuncMinus(*this);

  return field;
}

char *FieldFuncMult::get_ptr(RowBatch *batch) {
  k_bool is_double = false;
  KStatus err = SUCCESS;
  char *ptr = nullptr;
  intvalue_ = 1;
  doublevalue_ = 1;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field mult, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
          intvalue_ *= args_[i]->ValInt(ptr);
          break;
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
          doublevalue_ *= args_[i]->ValReal(ptr);
          is_double = true;
          break;
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field mult");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (is_double) {
    doublevalue_ *= intvalue_;
    return reinterpret_cast<char *>(&doublevalue_);
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncMult::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = 1;
    for (size_t i = 0; i < arg_count_; ++i) {
      val *= args_[i]->ValInt();
    }

    return val;
  }
}

k_double64 FieldFuncMult::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = 1;
    for (size_t i = 0; i < arg_count_; ++i) {
      val *= args_[i]->ValReal();
    }

    return val;
  }
}

String FieldFuncMult::ValStr() { return String(""); }

Field *FieldFuncMult::field_to_copy() {
  FieldFuncMult *field = new FieldFuncMult(*this);

  return field;
}

char *FieldFuncDivide::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field divide, get null value");
    return const_cast<char *>("");
  }

  doublevalue_ = args_[0]->ValReal(ptr);

  for (size_t i = 1; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field divide, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          k_double64 newval = args_[i]->ValReal(ptr);
          if (newval == 0) {
            doublevalue_ /= newval;
            return reinterpret_cast<char *>(&doublevalue_);
          }
          doublevalue_ /= newval;
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field mult");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::BIGINT) {
    intvalue_ = doublevalue_;
    return reinterpret_cast<char *>(&intvalue_);
  }
  return reinterpret_cast<char *>(&doublevalue_);
}

k_int64 FieldFuncDivide::ValInt() { return ValReal(); }

k_double64 FieldFuncDivide::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 newval = args_[i]->ValReal();
      if (newval == 0) {
        return val /= newval;
      }
      val /= newval;
    }

    return val;
  }
}

String FieldFuncDivide::ValStr() { return String(""); }

Field *FieldFuncDivide::field_to_copy() {
  FieldFuncDivide *field = new FieldFuncDivide(*this);

  return field;
}

k_bool FieldFuncDivide::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->CheckNull()) {
      return true;
    } else if (i != 0 && FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
      if (FLT_EQUAL(args_[0]->ValReal(), 0.0)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                      "division undefined");
        return false;
      }
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                    "division by zero");
      return false;
    }
  }

  return false;
}

char *FieldFuncDividez::get_ptr(RowBatch *batch) {
  k_bool is_double = false;
  KStatus err = SUCCESS;

  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field dividez, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          if (i != 0) {
            k_int64 val = args_[i]->ValInt(ptr);
            if (val == 0) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO, "division by zero");
              return const_cast<char *>("");
            }
            if (!is_double) {
              intvalue_ /= val;
            } else {
              doublevalue_ /= val;
            }
          } else {
            intvalue_ = args_[i]->ValInt(ptr);
          }
          break;
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          if (!is_double) {
            is_double = true;
            doublevalue_ = intvalue_;
          }
          if (i != 0) {
            k_double64 val = args_[i]->ValReal(ptr);
            if (FLT_EQUAL(val, 0.0)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO, "division by zero");
              return const_cast<char *>("");
            }
            doublevalue_ /= val;
          } else {
            doublevalue_ = args_[i]->ValReal(ptr);
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field dividez");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (is_double) {
    doublevalue_ = std::floor(doublevalue_);
    return reinterpret_cast<char *>(&doublevalue_);
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncDividez::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_int64 v = args_[i]->ValInt();
      if (v == 0) {
        return 0;
      }
      val /= args_[i]->ValInt();
    }
    return val;
  }
}

k_double64 FieldFuncDividez::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = args_[0]->ValReal();

    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 v = args_[i]->ValInt();
      if (FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
        return 0.0;
      }
      val /= args_[i]->ValReal();
    }
    val = std::floor(val);
    return val;
  }
}

String FieldFuncDividez::ValStr() { return String(""); }

Field *FieldFuncDividez::field_to_copy() {
  FieldFuncDividez *field = new FieldFuncDividez(*this);

  return field;
}

k_bool FieldFuncDividez::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->CheckNull()) {
      return true;
    } else if (i != 0 && FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                    "division by zero");
      return false;
    }
  }

  return false;
}

char *FieldFuncRemainder::get_ptr(RowBatch *batch) {
  k_int64 val[2] = {0};
  KStatus err = SUCCESS;
  k_double64 doubleValue = 0;

  for (size_t i = 0; i < 2; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field remainder, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
          val[i] = args_[i]->ValInt(ptr);
          break;
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
          val[i] = args_[i]->ValReal(ptr);
          break;
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field remainder");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (val[1] != 0) {
    intvalue_ = val[0] ^ val[1];
  } else {
    intvalue_ = val[0];
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = intvalue_;
    return reinterpret_cast<char *>(&doublevalue_);
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncRemainder::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val1 = args_[0]->ValInt();
    k_int64 val2 = args_[1]->ValInt();
    if (val2 == 0) {
      return val1;
    }
    k_int64 val = val1 ^ val2;

    return val;
  }
}

k_double64 FieldFuncRemainder::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_int64 val1 = args_[0]->ValReal();
    k_int64 val2 = args_[1]->ValReal();
    if (val2 == 0) {
      return val1;
    }
    k_int64 val = val1 ^ val2;

    return val;
  }
}

String FieldFuncRemainder::ValStr() { return String(""); }

Field *FieldFuncRemainder::field_to_copy() {
  FieldFuncRemainder *field = new FieldFuncRemainder(*this);

  return field;
}

char *FieldFuncPercent::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_double64 newval = 0;
  k_bool is_double = false;

  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field percent, get null value");
        return const_cast<char *>("");
      }

      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          if (i != 0) {
            newval = args_[i]->ValReal(ptr);
            if (!is_double) {
              if (newval == 0) {
                intvalue_ /= newval;
                return reinterpret_cast<char *>(&intvalue_);
              }
              intvalue_ %= args_[i]->ValInt(ptr);
            } else {
              doublevalue_ = std::fmod(doublevalue_, newval);
            }
          } else {
            intvalue_ = args_[0]->ValInt(ptr);
          }
          break;
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          if (!is_double) {
            is_double = true;
            doublevalue_ = intvalue_;
          }
          if (i != 0) {
            newval = args_[i]->ValReal(ptr);
            doublevalue_ = std::fmod(doublevalue_, newval);
          } else {
            doublevalue_ = args_[0]->ValReal(ptr);
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field percent");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (is_double) {
    return reinterpret_cast<char *>(&doublevalue_);
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncPercent::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 newval = args_[i]->ValReal();
      if (newval == 0) {
        return val /= newval;
      }
      val %= args_[i]->ValInt();
    }

    return val;
  }
}

k_double64 FieldFuncPercent::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 newval = args_[i]->ValReal();
      val = std::fmod(val, newval);
    }

    return val;
  }
  // char *ptr = get_ptr();
  // if (ptr) {
  //   return FieldFuncOp::ValInt(ptr);
  // } else {
  //   k_double64 x = args_[0]->ValReal();
  //   for (size_t i = 1; i < arg_count_; ++i) {
  //     k_double64 y = args_[i]->ValReal();
  //     y = std::abs(y);
  //     k_double64 yfr = 0.0f;
  //     k_int32 yexp = 0;
  //     yfr = std::frexp(newval, &yexp);
  //     k_double64 r = x;
  //     if (x < 0) {
  //       r = -x;
  //     }

  //     while(r >= y) {
  //       k_double64 rfr = 0.0f;
  //       k_int32 rexp = 0;
  //       rfr = std::frexp(newval, &rexp);
  //       if (rfr < yfr) {
  //         rexp = rexp - 1;
  //       }
  //     }
  //   }

  //   return val;
  // }
}

String FieldFuncPercent::ValStr() { return String(""); }

Field *FieldFuncPercent::field_to_copy() {
  FieldFuncPercent *field = new FieldFuncPercent(*this);

  return field;
}

k_bool FieldFuncPercent::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->CheckNull()) {
      return true;
    } else if (i != 0 && FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO, "zero modulus");
      return false;
    }
  }

  return false;
}

char *FieldFuncPower::get_ptr(RowBatch * batch) {
  KStatus err = SUCCESS;
  k_int64 tmp = 0;
  k_int64 res = 0;

  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field power, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          if (i != 0) {
            tmp = args_[i]->ValInt(ptr);
            res = pow(intvalue_, tmp);
          } else {
            intvalue_ = args_[0]->ValInt(ptr);
          }
          break;
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          if (i != 0) {
            k_int64 tmp = args_[i]->ValReal(ptr);
            res = pow(doublevalue_, tmp);
          } else {
            doublevalue_ = args_[0]->ValReal(ptr);
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field power");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::BIGINT) {
    intvalue_ = res;
    return reinterpret_cast<char *>(&intvalue_);
  } else {
    doublevalue_ = res;
    return reinterpret_cast<char *>(&doublevalue_);
  }
}

k_int64 FieldFuncPower::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 res = 0;
    k_int64 val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_int64 tmp = args_[i]->ValInt();
      res = pow(val, tmp);
    }

    return res;
  }
}

k_double64 FieldFuncPower::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_int64 res = 0;
    k_int64 val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_int64 tmp = args_[i]->ValReal();
      res = pow(val, tmp);
    }

    return res;
  }
}

String FieldFuncPower::ValStr() { return String(""); }

Field *FieldFuncPower::field_to_copy() {
  FieldFuncPower *field = new FieldFuncPower(*this);

  return field;
}

char *FieldFuncMod::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_bool first = true;
  k_int64 val = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field mod, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          if (first) {
            val = args_[i]->ValInt(ptr);
            first = false;
          } else if (!first) {
            val %= args_[i]->ValInt(ptr);
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field mod");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = val;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = val;
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncMod::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_bool first = true;
    k_int64 val = 0;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (first) {
        val = args_[i]->ValInt();
        first = false;
      } else if (!first) {
        val %= args_[i]->ValInt();
      }
    }

    return val;
  }
}

k_double64 FieldFuncMod::ValReal() { return ValInt(); }

String FieldFuncMod::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncMod::field_to_copy() {
  FieldFuncMod *field = new FieldFuncMod(*this);

  return field;
}

char *FieldFuncAndCal::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_int64 result = 1;
  k_int64 val = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field and_cal, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          if (i == 0) {
            result = args_[i]->ValInt(ptr);
          } else {
            k_int64 val = args_[i]->ValInt(ptr);
            result &= val;
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field and_cal");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = result;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = result;
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncAndCal::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        result &= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncAndCal::ValReal() { return ValInt(); }

String FieldFuncAndCal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncAndCal::field_to_copy() {
  FieldFuncAndCal *field = new FieldFuncAndCal(*this);

  return field;
}

char *FieldFuncOrCal::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_int64 result = 1;
  k_int64 val = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field or_cal, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          if (i == 0) {
            result = args_[i]->ValInt(ptr);
          } else {
            val = args_[i]->ValInt(ptr);
            result |= val;
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field or_cal");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = result;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = result;
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncOrCal::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        result |= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncOrCal::ValReal() { return ValInt(); }

String FieldFuncOrCal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncOrCal::field_to_copy() {
  FieldFuncOrCal *field = new FieldFuncOrCal(*this);

  return field;
}

char *FieldFuncNotCal::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_int64 result = 1;
  k_int64 val = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field not_cal, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          val = args_[i]->ValInt(ptr);
          result = ~val;
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field not_cal");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = result;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = result;
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncNotCal::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      k_int64 val = args_[i]->ValInt();
      result = ~val;
    }

    return result;
  }
}

k_double64 FieldFuncNotCal::ValReal() { return ValInt(); }

String FieldFuncNotCal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncNotCal::field_to_copy() {
  FieldFuncNotCal *field = new FieldFuncNotCal(*this);

  return field;
}

char *FieldFuncLeftShift::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_int64 result = 1;
  k_int64 val = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field left shift, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          if (i == 0) {
            result = args_[i]->ValInt(ptr);
          } else {
            k_int64 val = args_[i]->ValInt(ptr);
            if (val < 0 || val >= 64) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "shift argument out of range");
              return const_cast<char *>("");
            }
            result <<= val;
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field left shift");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = result;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = result;
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncLeftShift::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        if (val < 0 || val >= 64) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                        "shift argument out of range");
          return 0;
        }
        result <<= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncLeftShift::ValReal() { return ValInt(); }

String FieldFuncLeftShift::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncLeftShift::field_to_copy() {
  FieldFuncLeftShift *field = new FieldFuncLeftShift(*this);

  return field;
}

char *FieldFuncRightShift::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_int64 result = 1;
  k_int64 val = 0;
  for (size_t i = 0; i < arg_count_; ++i) {
    if (!args_[i]->CheckNull()) {
      char *ptr = args_[i]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field right_shift, get null value");
        return const_cast<char *>("");
      }
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          if (i == 0) {
            result = args_[i]->ValInt(ptr);
          } else {
            k_int64 val = args_[i]->ValInt(ptr);
            if (val < 0 || val >= 64) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "shift argument out of range");
              return const_cast<char *>("");
            }
            result >>= val;
          }
          break;
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field right_shift");
          err = FAIL;
          break;
      }
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = result;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = result;
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncRightShift::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        if (val < 0 || val >= 64) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                        "shift argument out of range");
          return 0;
        }
        result >>= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncRightShift::ValReal() { return ValInt(); }

String FieldFuncRightShift::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncRightShift::field_to_copy() {
  FieldFuncRightShift *field = new FieldFuncRightShift(*this);

  return field;
}

k_int64 FieldFuncTimeBucket::ValInt() {
  if (args_[0]->CheckNull()) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "time_bucket(): first arg can not be null.");
    return 0;
  }
  auto original_timestamp = args_[0]->ValInt();
  if (type_scale_ != 1) {
    // multi
    if (type_scale_multi_or_divde_) {
      if (!I64_SAFE_MUL_CHECK(original_timestamp, type_scale_)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                      "Timestamp/TimestampTZ out of range");
        return 0;
      }
      original_timestamp *= type_scale_;
      // divide
    } else {
      original_timestamp /= type_scale_;
    }
  }
  if (!var_interval_) {
    if (last_time_bucket_value_ != INT64_MIN &&
        original_timestamp > last_time_bucket_value_ &&
        original_timestamp < (last_time_bucket_value_ + interval_seconds_)) {
      return last_time_bucket_value_;
    } else {
      // use 0000-01-01 00:00:00 as start
      // -62135596800000 is the timestamp of 0000-01-01 00:00:00
      // Negative timestamp needs to be rounded down
      last_time_bucket_value_ = CALCULATE_TIME_BUCKET_VALUE(original_timestamp, time_diff_mo_, interval_seconds_);
      return last_time_bucket_value_;
    }
  } else {
    // construct_variable calculate the start of time_bucket for year and month
    // use 0000-01-01 00:00:00 as start
    std::time_t tt = (std::time_t)original_timestamp / 1000;
    struct std::tm tm;
    gmtime_r(&tt, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;
    if (year_bucket_) {
      tm.tm_mon = 0;
      tm.tm_year =
          (int32_t)((tm.tm_year + 1899) / static_cast<int>(interval_seconds_) *
                        static_cast<int>(interval_seconds_) -
                    1899);
      tm.tm_hour -= time_zone_;
    } else {
      int32_t mon = (tm.tm_year + 1899) * 12 + tm.tm_mon;
      mon = (int32_t)(mon / static_cast<int>(interval_seconds_) *
                      static_cast<int>(interval_seconds_));
      tm.tm_year = (mon / 12) - 1899;
      tm.tm_mon = mon % 12;
      tm.tm_hour -= time_zone_;
    }
    return (KTimestampTz)(timegm(&tm) * 1000);
  }
}

k_double64 FieldFuncTimeBucket::ValReal() { return ValInt(); }

String FieldFuncTimeBucket::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncTimeBucket::field_to_copy() {
  FieldFuncTimeBucket *field = new FieldFuncTimeBucket(*this);

  return field;
}

FieldFuncCastCheckTs::FieldFuncCastCheckTs(Field *left, Field *right)
    : FieldFunc(left, right) {
  sql_type_ = roachpb::DataType::BOOL;
  storage_type_ = roachpb::DataType::BOOL;
  storage_len_ = sizeof(k_bool);
  CalcDataType();
}

char *FieldFuncCastCheckTs::get_ptr(RowBatch *batch) {
  k_int64 val = 0;
  KStatus err = SUCCESS;

  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field cast_checkTs, get null value");
    return const_cast<char *>("");
  }
  switch (args_[0]->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      String str = args_[0]->ValStr(ptr);
      ErrorInfo err;
      int ret = TsTimePartition::tryAlterType({str.ptr_, str.length_}, datatype_, err);
      val = ret == 0 ? 1 : 0;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field cast_checkTs");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = val;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = val;
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncCastCheckTs::ValInt() {
  String str = args_[0]->ValStr();
  ErrorInfo err;
  int ret =
      TsTimePartition::tryAlterType({str.ptr_, str.length_}, datatype_, err);
  return 0 == ret ? 1 : 0;
}

k_double64 FieldFuncCastCheckTs::ValReal() { return ValInt(); }

String FieldFuncCastCheckTs::ValStr() { return String(); }

Field *FieldFuncCastCheckTs::field_to_copy() {
  FieldFuncCastCheckTs *field = new FieldFuncCastCheckTs(*this);

  return field;
}

void FieldFuncCastCheckTs::CalcDataType() {
  String str = args_[1]->ValStr();
  if (0 == str.compare("int2", strlen("int2")) ||
      0 == str.compare("smallint", strlen("smallint"))) {
    datatype_ = DATATYPE::INT16;
  } else if (0 == str.compare("int4", strlen("int4")) ||
             0 == str.compare("int", strlen("int")) ||
             0 == str.compare("integer", strlen("integer"))) {
    datatype_ = DATATYPE::INT32;
  } else if (0 == str.compare("int8", strlen("int8")) ||
             0 == str.compare("bigint", strlen("bigint")) ||
             0 == str.compare("int64", strlen("int64"))) {
    datatype_ = DATATYPE::INT64;
  } else if (0 == str.compare("real", strlen("real")) ||
             0 == str.compare("float4", strlen("float4"))) {
    datatype_ = DATATYPE::FLOAT;
  } else {
    datatype_ = DATATYPE::DOUBLE;
  }
}

char *FieldFuncCurrentDate::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncCurrentDate::ValInt() {
  time_t t = time(nullptr);
  struct tm ltm;
  gmtime_r(&t, &ltm);
  ltm.tm_hour = 0;
  ltm.tm_min = 0;
  ltm.tm_sec = 0;
  return k_int64(mktime(&ltm) * 1000);
}

k_double64 FieldFuncCurrentDate::ValReal() { return ValInt(); }

String FieldFuncCurrentDate::ValStr() {
  char buffer[80];
  time_t t = time(nullptr);
  struct tm ltm;
  gmtime_r(&t, &ltm);
  strftime(buffer, 80, "%Y-%m-%d", &ltm);
  String s(80);
  snprintf(s.ptr_, 80 + 1, "%s", buffer);
  s.length_ = strlen(buffer);
  return s;
}

Field *FieldFuncCurrentDate::field_to_copy() {
  FieldFuncCurrentDate *field = new FieldFuncCurrentDate(*this);

  return field;
}

char *FieldFuncCurrentTimeStamp::get_ptr(RowBatch *batch) {
  auto duration_since_epoch = std::chrono::high_resolution_clock::now().time_since_epoch();
  auto ns_since_epoch = std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch).count();
  time_t t = time(nullptr);
  struct tm ltm;
  gmtime_r(&t, &ltm);
  ns_since_epoch += (ltm.tm_gmtoff - time_zone_ * 3600) * 1000000000;
  // add precision handle
  if (arg_count_ > 0) {
    char *ptr = args_[0]->get_ptr(batch);
    if (ptr == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    "could not parse \"\" field func_current_ts, get null value");
      return const_cast<char *>("");
    }
    k_int64 prec = args_[0]->ValInt(ptr);
    if (prec < 0 || prec > 9) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
          ("current_timestamp(): precision " + std::to_string(prec) + " out of range").c_str());
      return const_cast<char *>("");
    }
    prec = 9 - prec;
    int64_t mask = pow(10, prec);
    ns_since_epoch = ns_since_epoch / mask * mask;
  }
  intvalue_ = ns_since_epoch;
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncCurrentTimeStamp::ValInt() {
  auto duration_since_epoch =
      std::chrono::high_resolution_clock::now().time_since_epoch();
  auto ns_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch)
          .count();
  time_t t = time(nullptr);
  struct tm ltm;
  gmtime_r(&t, &ltm);
  ns_since_epoch += (ltm.tm_gmtoff - time_zone_ * 3600) * 1000000000;
  // add precision handle
  if (arg_count_ > 0) {
    k_int64 prec = args_[0]->ValInt();
    if (prec < 0 || prec > 9) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
          ("current_timestamp(): precision " + std::to_string(prec) + " out of range").c_str());
      return 0;
    }
    prec = 9 - prec;
    int64_t mask = pow(10, prec);
    ns_since_epoch = ns_since_epoch / mask * mask;
  }
  return ns_since_epoch;
}

k_double64 FieldFuncCurrentTimeStamp::ValReal() { return ValInt(); }

String FieldFuncCurrentTimeStamp::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncCurrentTimeStamp::field_to_copy() {
  FieldFuncCurrentTimeStamp *field = new FieldFuncCurrentTimeStamp(*this);

  return field;
}

char *FieldFuncNow::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncNow::ValInt() {
  auto duration_since_epoch =
      std::chrono::high_resolution_clock::now().time_since_epoch();
  auto ns_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch)
          .count();
  return ns_since_epoch;
}

k_double64 FieldFuncNow::ValReal() { return ValInt(); }

String FieldFuncNow::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncNow::field_to_copy() {
  FieldFuncNow *field = new FieldFuncNow(*this);

  return field;
}

char *FieldFuncDateTrunc::get_ptr(RowBatch *batch) {
  char *ptr0 = args_[0]->get_ptr(batch);
  char *ptr1 = args_[1]->get_ptr(batch);

  if ((ptr0 == nullptr) || (ptr1 == nullptr)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field date_trunc, get null value");
    return const_cast<char *>("");
  }

  String strvalue = args_[0]->ValStr(ptr0);
  k_int64 intvalue = args_[1]->ValInt(ptr1);
  intvalue_ = getDateTrunc(is_unit_const_, type_scale_multi_or_divde_, time_zone_, type_scale_,  time_diff_,
                           this->return_type_, args_[1]->get_storage_type(), unit_, strvalue, intvalue);
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncDateTrunc::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    String strvalue = args_[0]->ValStr();
    k_int64 intvalue = args_[1]->ValInt();
    k_int64 value = getDateTrunc(is_unit_const_, type_scale_multi_or_divde_, time_zone_, type_scale_, time_diff_,
                                 this->return_type_, args_[1]->get_storage_type(), unit_, strvalue, intvalue);
    return value;
  }
}

k_double64 FieldFuncDateTrunc::ValReal() { return ValInt(); }

String FieldFuncDateTrunc::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncDateTrunc::field_to_copy() {
  FieldFuncDateTrunc *field = new FieldFuncDateTrunc(*this);

  return field;
}

k_bool FieldFuncDateTrunc::is_nullable() {
  if (is_null_value_) {
    return true;
  }
  if (!is_unit_const_ && args_[0]->CheckNull()) {
    return true;
  }
  if (args_[1]->CheckNull()) {
    return true;
  }
  return false;
}

char *FieldFuncExtract::get_ptr(RowBatch *batch) {
  char *ptr0 = args_[0]->get_ptr(batch);
  char *ptr1 = args_[1]->get_ptr(batch);
  if ((ptr0 == nullptr) || (ptr1 == nullptr)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field extract, get null value");
    return const_cast<char *>("");
  }

  k_int64 res = getExtract(args_[1]->ValInt(ptr1), args_[0]->ValStr(ptr0), args_[1]->get_storage_type(),
                           args_[1]->get_return_type(), time_zone_);

  if (storage_type_ == roachpb::DataType::DOUBLE) {
    doublevalue_ = res;
    return reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = res;
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncExtract::ValInt() { return ValReal(); }

k_double64 FieldFuncExtract::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValReal(ptr);
  } else {
    k_int64 res = getExtract(args_[1]->ValInt(), args_[0]->ValStr(), args_[1]->get_storage_type(),
                             args_[1]->get_return_type(), time_zone_);

    return static_cast<double>(res);
  }
}

String FieldFuncExtract::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncExtract::field_to_copy() {
  FieldFuncExtract *field = new FieldFuncExtract(*this);

  return field;
}

char *FieldFuncTimeOfDay::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_int64 val = 0;
  char *ptrResult = nullptr;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      intvalue_ = ValInt();
      ptrResult = reinterpret_cast<char *>(&intvalue_);
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      strvalue_ = ValStr();
      ptrResult = const_cast<char *>(strvalue_.getptr());
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func time_of_day");
      ptrResult = const_cast<char *>("");
      break;
  }
  return ptrResult;
}

k_int64 FieldFuncTimeOfDay::ValInt() {
  auto duration_since_epoch =
      std::chrono::high_resolution_clock::now().time_since_epoch();
  auto ns_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch)
          .count();
  return ns_since_epoch;
}

k_double64 FieldFuncTimeOfDay::ValReal() { return ValInt(); }

String FieldFuncTimeOfDay::ValStr() {
  time_t timestamp = std::time(nullptr);
  timestamp += time_zone * 3600;
  const int kArraySize = this->storage_len_;
  String s(kArraySize);
  struct tm t;
  memset(&t, 0, sizeof(t));
  gmtime_r(&timestamp, &t);
  t.tm_gmtoff = time_zone * 3600;
  std::strftime(s.ptr_, kArraySize, "%a %b %d %H:%M:%S.xxxxxxxxx %Y %z", &t);
  std::string formattedTime(s.ptr_);
  auto pos = formattedTime.find("xxxxxxxxx");
  if (pos != std::string::npos) {
    formattedTime.replace(pos, 3, "%ld");
  }
  auto duration_since_epoch =
      std::chrono::high_resolution_clock::now().time_since_epoch();
  auto ns_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch)
          .count();
  std::snprintf(s.ptr_, kArraySize, formattedTime.c_str(),
                ns_since_epoch % 1000000000);

  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncTimeOfDay::field_to_copy() {
  FieldFuncTimeOfDay *field = new FieldFuncTimeOfDay(*this);

  return field;
}

char *FieldFuncExpStrftime::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      char *ptr = args_[0]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field expstr_time, get null value");
        return const_cast<char *>("");
      }

      CKTime ck_time = getCKTime(args_[0]->ValInt(ptr), args_[0]->get_storage_type(), 0);
      struct tm ltm;
      gmtime_r(&ck_time.t_timespec.tv_sec, &ltm);
      const int kArraySize = this->storage_len_;
      String s(kArraySize);
      try {
        ptr = args_[1]->get_ptr(batch);
        if (ptr == nullptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                        "could not parse \"\" field expstr_time, get null value");
          return const_cast<char *>("");
        }
        s.length_ = std::strftime(s.ptr_, kArraySize, args_[1]->ValStr(ptr).getptr(), &ltm);
        strvalue_ = s;
      } catch (const std::exception &e) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_DATETIME_FORMAT, "Strftime transfer error");
        return const_cast<char *>("");
      }
      break;
    }
    default:
      err = FAIL;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE,
                                    "unsupported data type for field func exp_strf_time");
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return const_cast<char *>(strvalue_.getptr());
}

k_int64 FieldFuncExpStrftime::ValInt() { return 0; }

k_double64 FieldFuncExpStrftime::ValReal() { return ValInt(); }

Field *FieldFuncExpStrftime::field_to_copy() {
  FieldFuncExpStrftime *field = new FieldFuncExpStrftime(*this);

  return field;
}
String FieldFuncExpStrftime::ValStr() {
  CKTime ck_time =
      getCKTime(args_[0]->ValInt(), args_[0]->get_storage_type(), 0);
  struct tm ltm;
  gmtime_r(&ck_time.t_timespec.tv_sec, &ltm);
  const int kArraySize = this->storage_len_;
  String s(kArraySize);
  try {
    s.length_ =
        std::strftime(s.ptr_, kArraySize, args_[1]->ValStr().getptr(), &ltm);
  } catch (const std::exception &e) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_DATETIME_FORMAT,
                                  "Strftime transfer error");
  }
  // s.length_ = strlen(s.ptr_);
  return s;
}

char *FieldFuncRandom::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_real_distribution<k_double64> distribution(0, 1);
      doublevalue_ = distribution(gen);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field random");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&doublevalue_);
}

k_int64 FieldFuncRandom::ValInt() { return ValReal(); }

k_double64 FieldFuncRandom::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValReal(ptr);
  } else {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<k_double64> distribution(0, 1);

    return distribution(gen);
  }
}

String FieldFuncRandom::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncRandom::field_to_copy() {
  FieldFuncRandom *field = new FieldFuncRandom(*this);

  return field;
}

char *FieldFuncWidthBucket::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  char *ptr0 = args_[0]->get_ptr(batch);
  char *ptr1 = args_[1]->get_ptr(batch);
  char *ptr2 = args_[2]->get_ptr(batch);
  char *ptr3 = args_[3]->get_ptr(batch);
  if ((ptr0 == nullptr) || (ptr1 == nullptr) || (ptr2 == nullptr) || (ptr3 == nullptr)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field width_bucket, get null value");
    return const_cast<char *>("");
  }

  switch (args_[1]->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      k_int64 expr = args_[0]->ValInt(ptr0);
      k_int64 min = args_[1]->ValInt(ptr1);
      k_int64 max = args_[2]->ValInt(ptr2);
      k_int64 buckets_num = args_[3]->ValInt(ptr3);

      if ((min < max && expr > max) || (min > max && expr < max)) {
        intvalue_ = buckets_num + 1;
      } else if ((min < max && expr < min) || (min > max && expr > min)) {
        intvalue_ = 0;
      } else {
        k_double64 width = static_cast<k_double64>((max - min) / buckets_num);
        k_int64 difference = expr - min;
        intvalue_ = floor(difference / width) + 1;
      }
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field width_bucket.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncWidthBucket::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    k_int64 expr = args_[0]->ValInt();
    k_int64 min = args_[1]->ValInt();
    k_int64 max = args_[2]->ValInt();
    k_int64 buckets_num = args_[3]->ValInt();

    if ((min < max && expr > max) || (min > max && expr < max)) {
      return buckets_num + 1;
    }

    if ((min < max && expr < min) || (min > max && expr > min)) {
      return 0;
    }

    k_double64 width = static_cast<k_double64>((max - min) / buckets_num);
    k_int64 difference = expr - min;
    return floor(difference / width) + 1;
  }
}

k_double64 FieldFuncWidthBucket::ValReal() { return ValInt(); }

String FieldFuncWidthBucket::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncWidthBucket::field_to_copy() {
  FieldFuncWidthBucket *field = new FieldFuncWidthBucket(*this);

  return field;
}

char *FieldFuncAge::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  char *ptrResult = nullptr;

  char *ptr0 = args_[0]->get_ptr(batch);
  char *ptr1 = args_[1]->get_ptr(batch);
  if ((ptr0 == nullptr) || (ptr1 == nullptr)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field age, get null value");
    return const_cast<char *>("");
  }

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      if (arg_count_ == 1) {
        auto duration_since_epoch = std::chrono::high_resolution_clock::now().time_since_epoch();
        auto ns_since_epoch = std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch).count();
        convertTimePrecision(&ns_since_epoch, roachpb::DataType::TIMESTAMP_NANO, storage_type_);
        intvalue_ = ns_since_epoch - args_[0]->ValInt(ptr0);
        ptrResult = reinterpret_cast<char *>(&intvalue_);
      } else {
        k_int64 val_a = args_[0]->ValInt(ptr0);
        k_int64 val_b = args_[1]->ValInt(ptr1);
        if (convertTimePrecision(&val_a, args_[0]->get_storage_type(), storage_type_) &&
            convertTimePrecision(&val_b, args_[1]->get_storage_type(), storage_type_)) {
          intvalue_ = val_a - val_b;
          ptrResult = reinterpret_cast<char *>(&intvalue_);
        } else {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Timestamp/TimestampTZ out of range");
          err = FAIL;
        }
      }
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      std::string str;
      if (arg_count_ == 1) {
        auto duration_since_epoch = std::chrono::system_clock::now().time_since_epoch();
        auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count();
        str = std::to_string(ms_since_epoch / 1000 - args_[0]->ValReal(ptr0));
      }
      str = std::to_string(args_[0]->ValInt(ptr0) - args_[1]->ValInt(ptr1));
      String s(str.length());
      snprintf(s.ptr_, str.length() + 1, "%s", str.c_str());
      s.length_ = str.length();
      strvalue_ = s;
      ptrResult = reinterpret_cast<char *>(strvalue_.ptr_);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field fun age");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return ptrResult;
}

k_int64 FieldFuncAge::ValInt() {
  if (arg_count_ == 1) {
    auto duration_since_epoch =
        std::chrono::high_resolution_clock::now().time_since_epoch();
    auto ns_since_epoch = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              duration_since_epoch)
                              .count();
    convertTimePrecision(&ns_since_epoch, roachpb::DataType::TIMESTAMP_NANO,
                         storage_type_);
    return ns_since_epoch - args_[0]->ValInt();
  }
  k_int64 val_a = args_[0]->ValInt();
  k_int64 val_b = args_[1]->ValInt();
  if (convertTimePrecision(&val_a, args_[0]->get_storage_type(),
                           storage_type_) &&
      convertTimePrecision(&val_b, args_[1]->get_storage_type(),
                           storage_type_)) {
    return val_a - val_b;
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "Timestamp/TimestampTZ out of range");
    return 0;
  }
}

k_double64 FieldFuncAge::ValReal() { return ValInt(); }

Field *FieldFuncAge::field_to_copy() {
  FieldFuncAge *field = new FieldFuncAge(*this);

  return field;
}
String FieldFuncAge::ValStr() {
  std::string str;
  if (arg_count_ == 1) {
    auto duration_since_epoch =
        std::chrono::system_clock::now().time_since_epoch();
    auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(
                              duration_since_epoch)
                              .count();
    str = std::to_string(ms_since_epoch / 1000 - args_[0]->ValReal());
  }
  str = std::to_string(args_[0]->ValInt() - args_[1]->ValInt());
  String s(str.length());
  snprintf(s.ptr_, str.length() + 1, "%s", str.c_str());
  s.length_ = str.length();
  return s;
}

char *FieldFuncCase::get_ptr(RowBatch *batch) {
  condition_met = KTRUE;

  for (size_t i = 0; i < arg_count_; ++i) {
    char *ptr = args_[i]->get_ptr(batch);
    if (ptr == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    "could not parse \"\" field func case, get null value");
      return const_cast<char *>("");
    }
    if (args_[i]->is_condition_met()) {
      switch (args_[i]->get_storage_type()) {
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BOOL:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          intvalue_ = args_[i]->ValInt(ptr);
          return reinterpret_cast<char *>(&intvalue_);
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          doublevalue_ = args_[i]->ValReal(ptr);
          return reinterpret_cast<char *>(&doublevalue_);
        }
        case roachpb::DataType::CHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::VARBINARY: {
          String s1 = args_[i]->ValStr(ptr);
          strvalue_ = s1;
          return reinterpret_cast<char *>(strvalue_.ptr_);
        }
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func case.");
          return const_cast<char *>("");
      }
    }
  }
  condition_met = KFALSE;

  if (storage_type_ == roachpb::DataType::BIGINT) {
    intvalue_ = 0;
    return reinterpret_cast<char *>(&intvalue_);
  } else if (storage_type_ == roachpb::DataType::BIGINT) {
    doublevalue_ = 0;
    return reinterpret_cast<char *>(&doublevalue_);
  }

  return const_cast<char *>("");
}

k_int64 FieldFuncCase::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    condition_met = KTRUE;
    for (size_t i = 0; i < arg_count_; i++) {
      k_int64 val = args_[i]->ValInt();
      if (args_[i]->is_condition_met()) {
        return val;
      }
    }
    condition_met = KFALSE;
    return 0;
  }
}

k_double64 FieldFuncCase::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  }
  condition_met = KTRUE;
  for (size_t i = 0; i < arg_count_; i++) {
    k_double64 val = args_[i]->ValReal();
    if (args_[i]->is_condition_met()) {
      return val;
    }
  }
  condition_met = KFALSE;
  return 0;
}

String FieldFuncCase::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    condition_met = KTRUE;
    for (size_t i = 0; i < arg_count_; i++) {
      String val = args_[i]->ValStr();
      if (args_[i]->is_condition_met()) {
        return val;
      }
    }
    condition_met = KFALSE;
    return String("");
  }
}

Field *FieldFuncCase::field_to_copy() {
  FieldFuncCase *field = new FieldFuncCase(*this);

  return field;
}

k_bool FieldFuncCase::is_nullable() {
  for (k_int32 i = 0; i < arg_count_; i++) {
    if (!args_[i]->CheckNull()) {
      return KFALSE;
    }
  }
  return KTRUE;
}

k_bool FieldFuncCase::is_condition_met() { return condition_met; }

char *FieldFuncThen::get_ptr(RowBatch *batch) {
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field func then, get null value");
    return const_cast<char *>("");
  }
  if (args_[0]->ValInt(ptr)) {
    ptr = args_[1]->get_ptr(batch);
    if (ptr == nullptr) {
      return const_cast<char *>("");
    }

    switch (args_[1]->get_storage_type()) {
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BOOL:
      case roachpb::DataType::SMALLINT:
      case roachpb::DataType::INT:
      case roachpb::DataType::BIGINT: {
        condition_met = KTRUE;
        intvalue_ = args_[1]->ValInt(ptr);
        return reinterpret_cast<char *>(&intvalue_);
      }
      case roachpb::DataType::FLOAT:
      case roachpb::DataType::DOUBLE: {
        condition_met = KTRUE;
        doublevalue_ = args_[1]->ValReal(ptr);
        return reinterpret_cast<char *>(&doublevalue_);
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARBINARY: {
        condition_met = KTRUE;
        strvalue_ = args_[1]->ValStr(ptr);
        return const_cast<char *>(strvalue_.c_str());
      }
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func then.");
        return const_cast<char *>("");
        break;
    }
  } else {
    condition_met = KFALSE;
  }

  if (storage_type_ == roachpb::DataType::BIGINT) {
    intvalue_ = 0;
    return reinterpret_cast<char *>(&intvalue_);
  } else if (storage_type_ == roachpb::DataType::BIGINT) {
    doublevalue_ = 0;
    return reinterpret_cast<char *>(&doublevalue_);
  }

  return const_cast<char *>("");
}

k_int64 FieldFuncThen::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    if (args_[0]->ValInt()) {
      condition_met = KTRUE;
      return args_[1]->ValInt();
    }
    condition_met = KFALSE;
    return 0;
  }
}

k_double64 FieldFuncThen::ValReal() {
  if (args_[0]->ValInt()) {
    condition_met = KTRUE;
    return args_[1]->ValReal();
  }
  condition_met = KFALSE;
  return 0;
}

String FieldFuncThen::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    if (args_[0]->ValInt()) {
      condition_met = KTRUE;
      return args_[1]->ValStr();
    }
    condition_met = KFALSE;
    return String("");
  }
}

k_bool FieldFuncThen::is_condition_met() { return condition_met; }

Field *FieldFuncThen::field_to_copy() {
  FieldFuncThen *field = new FieldFuncThen(*this);

  return field;
}

k_bool FieldFuncThen::is_nullable() {
  if (arg_count_ > 1) {
    if (args_[0]->ValInt()) {
      return args_[1]->CheckNull();
    }
  }
  return KTRUE;
}

char *FieldFuncElse::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  char * ptrResult = nullptr;

  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field func else, get null value");
    return const_cast<char *>("");
  }

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT: {
      intvalue_ = args_[0]->ValInt(ptr);
      ptrResult = reinterpret_cast<char *>(&intvalue_);
      break;
    }
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      doublevalue_ = args_[0]->ValReal(ptr);
      ptrResult = reinterpret_cast<char *>(&doublevalue_);
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      String s1 = args_[0]->ValStr(ptr);
      strvalue_ = s1;
      ptrResult = reinterpret_cast<char *>(strvalue_.ptr_);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field else");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return ptrResult;
}

k_int64 FieldFuncElse::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    return args_[0]->ValInt();
  }
}

k_double64 FieldFuncElse::ValReal() { return args_[0]->ValReal(); }

String FieldFuncElse::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    return args_[0]->ValStr();
  }
}

k_bool FieldFuncElse::is_condition_met() { return KTRUE; }

Field *FieldFuncElse::field_to_copy() {
  FieldFuncElse *field = new FieldFuncElse(*this);

  return field;
}

char *FieldFuncCrc32C::get_ptr(RowBatch *batch) {
  string valstr = "";
  k_uint32 result = 0;
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (k_int32 i = 0; i < arg_count_; i++) {
        char *ptr = args_[i]->get_ptr(batch);
        String s = args_[i]->ValStr(ptr);
        valstr += std::string(s.ptr_, s.length_);
      }
      result = kwdb_crc32_castagnoli(valstr.data(), valstr.length());
      intvalue_ = result;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field crc32c");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncCrc32C::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = kwdb_crc32_castagnoli(valstr.data(), valstr.length());
  }

  return (k_int64)val;
}

k_double64 FieldFuncCrc32C::ValReal() { return ValInt(); }

String FieldFuncCrc32C::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncCrc32C::field_to_copy() {
  FieldFuncCrc32C *field = new FieldFuncCrc32C(*this);

  return field;
}

char *FieldFuncCrc32I::get_ptr(RowBatch *batch) {
  string valstr = "";
  k_uint32 result = 0;
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (k_int32 i = 0; i < arg_count_; i++) {
        char *ptr = args_[i]->get_ptr(batch);
        String s = args_[i]->ValStr(ptr);
        valstr += std::string(s.ptr_, s.length_);
      }
      result = kwdb_crc32_ieee(valstr.data(), valstr.length());
      intvalue_ = result;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field crc32i");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncCrc32I::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = kwdb_crc32_ieee(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}
k_double64 FieldFuncCrc32I::ValReal() { return ValInt(); }

String FieldFuncCrc32I::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncCrc32I::field_to_copy() {
  FieldFuncCrc32I *field = new FieldFuncCrc32I(*this);

  return field;
}

char *FieldFuncFnv32::get_ptr(RowBatch *batch) {
  string valstr = "";
  k_uint32 result = 0;
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (k_int32 i = 0; i < arg_count_; i++) {
        char *ptr = args_[i]->get_ptr(batch);
        String s = args_[i]->ValStr(ptr);
        valstr += std::string(s.ptr_, s.length_);
      }
      result = fnv1_hash32(valstr.data(), valstr.length());
      intvalue_ = result;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field fnv32");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncFnv32::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1_hash32(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}
k_double64 FieldFuncFnv32::ValReal() { return ValInt(); }

String FieldFuncFnv32::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv32::field_to_copy() {
  FieldFuncFnv32 *field = new FieldFuncFnv32(*this);
  return field;
}

char *FieldFuncFnv32a::get_ptr(RowBatch *batch) {
  string valstr = "";
  k_uint32 result = 0;
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (k_int32 i = 0; i < arg_count_; i++) {
        char *ptr = args_[i]->get_ptr(batch);
        String s = args_[i]->ValStr(ptr);
        valstr += std::string(s.ptr_, s.length_);
      }
      result = fnv1a_hash32(valstr.data(), valstr.length());
      intvalue_ = result;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field fnv32a");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncFnv32a::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1a_hash32(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}
k_double64 FieldFuncFnv32a::ValReal() { return ValInt(); }

String FieldFuncFnv32a::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv32a::field_to_copy() {
  FieldFuncFnv32a *field = new FieldFuncFnv32a(*this);
  return field;
}

char *FieldFuncFnv64::get_ptr(RowBatch *batch) {
  string valstr = "";
  k_uint64 result = 0;
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (k_int32 i = 0; i < arg_count_; i++) {
        char *ptr = args_[i]->get_ptr(batch);
        String s = args_[i]->ValStr(ptr);
        valstr += std::string(s.ptr_, s.length_);
      }
      result = fnv1_hash64(valstr.data(), valstr.length());
      intvalue_ = result;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field fnv64");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncFnv64::ValInt() {
  char *ptr = get_ptr();
  k_uint64 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1_hash64(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}

k_double64 FieldFuncFnv64::ValReal() { return ValInt(); }

String FieldFuncFnv64::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv64::field_to_copy() {
  FieldFuncFnv64 *field = new FieldFuncFnv64(*this);
  return field;
}

char *FieldFuncFnv64a::get_ptr(RowBatch *batch) {
  string valstr = "";
  k_uint64 result = 0;
  KStatus err = SUCCESS;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (k_int32 i = 0; i < arg_count_; i++) {
        char *ptr = args_[i]->get_ptr(batch);
        String s = args_[i]->ValStr(ptr);
        valstr += std::string(s.ptr_, s.length_);
      }
      result = fnv1a_hash64(valstr.data(), valstr.length());
      intvalue_ = result;
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field fnv64a");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncFnv64a::ValInt() {
  char *ptr = get_ptr();
  k_uint64 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1a_hash64(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}

k_double64 FieldFuncFnv64a::ValReal() { return ValInt(); }

String FieldFuncFnv64a::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv64a::field_to_copy() {
  FieldFuncFnv64a *field = new FieldFuncFnv64a(*this);
  return field;
}

char *FieldFuncCoalesce::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_bool is_nullable = args_[0]->CheckNull();
  char *ptr = nullptr;
  char *ptrResult = nullptr;

  if (is_nullable) {
    ptr = args_[1]->get_ptr(batch);
  } else {
    ptr = args_[0]->get_ptr(batch);
  }

  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field func coalesce, get null value");
    return const_cast<char *>("");
  }

  switch (args_[1]->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
      if (is_nullable) {
        intvalue_ = args_[1]->ValInt(ptr);
      } else {
        intvalue_ = args_[0]->ValInt(ptr);
      }
      ptrResult = reinterpret_cast<char *>(&intvalue_);
      break;
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      if (is_nullable) {
        doublevalue_ = args_[1]->ValReal(ptr);
      } else {
        doublevalue_ = args_[0]->ValReal(ptr);
      }
      ptrResult = reinterpret_cast<char *>(&doublevalue_);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
    case roachpb::DataType::NULLVAL: {
      if (is_nullable) {
        String s1 = args_[1]->ValStr(ptr);
        std::string destStr = std::string(s1.ptr_, s1.length_);
        if (this->storage_type_ == roachpb::DataType::BINARY) {
          ptr = args_[0]->get_ptr(batch);
          String s0 = args_[0]->ValStr(ptr);
          std::string orgStr = std::string(s0.ptr_, s0.length_);
          k_int32 storageLen = this->storage_len_;
          k_int32 destStrLen = destStr.length();
          if (destStrLen < storageLen) {
            orgStr.replace(0, destStrLen, destStr);
            String s(orgStr.length());
            snprintf(s.getptr(), storage_len_, "%s", orgStr.c_str());
            s.length_ = orgStr.length();
            strvalue_ = s;
            return const_cast<char *>(strvalue_.ptr_);
          }
          if (destStrLen > storageLen) {
            destStr = destStr.substr(0, storageLen);
          }
        }
        String s(destStr.length());
        snprintf(s.getptr(), storage_len_, "%s", destStr.c_str());
        s.length_ = destStr.length();
        strvalue_ = s;
      } else {
        strvalue_ = args_[0]->ValStr(ptr);
      }
      ptrResult = const_cast<char *>(strvalue_.ptr_);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func coalesce.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return ptrResult;
}

k_int64 FieldFuncCoalesce::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  }
  if (args_[0]->CheckNull()) {
    return args_[1]->ValInt();
  }
  return args_[0]->ValInt();
}

k_double64 FieldFuncCoalesce::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValReal(ptr);
  }
  if (args_[0]->CheckNull()) {
    return args_[1]->ValReal();
  }
  return args_[0]->ValReal();
}

String FieldFuncCoalesce::ValStr() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  }
  if (args_[0]->CheckNull()) {
    String s1 = args_[1]->ValStr();
    std::string destStr = std::string(s1.ptr_, s1.length_);
    if (this->storage_type_ == roachpb::DataType::BINARY) {
      String s0 = args_[0]->ValStr();
      std::string orgStr = std::string(s0.ptr_, s0.length_);
      k_int32 storageLen = this->storage_len_;
      k_int32 destStrLen = destStr.length();
      if (destStrLen < storageLen) {
        orgStr.replace(0, destStrLen, destStr);
        String s(orgStr.length());
        snprintf(s.getptr(), storage_len_, "%s", orgStr.c_str());
        s.length_ = orgStr.length();
        return s;
      }
      if (destStrLen > storageLen) {
        destStr = destStr.substr(0, storageLen);
      }
    }
    String s(destStr.length());
    snprintf(s.getptr(), storage_len_, "%s", destStr.c_str());
    s.length_ = destStr.length();
    return s;
  }
  return args_[0]->ValStr();
}

Field *FieldFuncCoalesce::field_to_copy() {
  FieldFuncCoalesce *field = new FieldFuncCoalesce(*this);

  return field;
}

k_bool FieldFuncCoalesce::is_nullable() {
  if (args_[0]->CheckNull() && args_[1]->CheckNull()) {
    return true;
  }
  return false;
}

char *FieldFuncDiff::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  k_bool is_double = false;
  k_int64 val = 0;
  k_double64 doubleValue = 0.0;
  char *ptrResult = nullptr;

  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field func diff, get null value");
    return const_cast<char *>("");
  }

  switch (storage_type_) {
    case roachpb::DataType::BIGINT:
      val = args_[0]->ValInt(ptr);
      break;
    case roachpb::DataType::DOUBLE:
      doubleValue = args_[0]->ValReal(ptr);
      is_double = true;
      break;
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func diff.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  if (is_clear_) {
    diff_info_.hasPrev = false;
  }
  is_clear_ = false;

  if (is_double) {
    doublevalue_ = 0.0;
    if (diff_info_.hasPrev) {
      doublevalue_ = doubleValue - diff_info_.prev.d64_value;
      diff_info_.prev.d64_value = doubleValue;
    } else {
      diff_info_.prev.d64_value = doubleValue;
      diff_info_.hasPrev = true;
      return const_cast<char *>("");
    }
    ptrResult = reinterpret_cast<char *>(&doublevalue_);
  } else {
    intvalue_ = 0;
    if (diff_info_.hasPrev) {
      if (I64_SAFE_SUB_CHECK(val, diff_info_.prev.i64_value)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range");
        return const_cast<char *>("");
      }
      intvalue_ = val - diff_info_.prev.i64_value;
      diff_info_.prev.i64_value = val;
    } else {
      diff_info_.prev.i64_value = val;
      diff_info_.hasPrev = true;
      return const_cast<char *>("");
    }
    ptrResult = reinterpret_cast<char *>(&intvalue_);
  }

  return ptrResult;
}

k_int64 FieldFuncDiff::ValInt() {
  k_int64 val;
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    val = FieldFunc::ValInt(ptr);
  } else {
    val = args_[0]->ValInt();
  }

  if (is_clear_) {
    diff_info_.hasPrev = false;
  }
  is_clear_ = false;

  if (diff_info_.hasPrev) {
    if (!I64_SAFE_SUB_CHECK(val, diff_info_.prev.i64_value)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    "integer out of range");
      return 0;
    }
    k_int64 tmp = val - diff_info_.prev.i64_value;
    diff_info_.prev.i64_value = val;
    return tmp;
  } else {
    diff_info_.prev.i64_value = val;
    diff_info_.hasPrev = true;
    return 0;
  }
}

k_double64 FieldFuncDiff::ValReal() {
  k_double64 val;
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    val = FieldFunc::ValReal(ptr);
  } else {
    val = args_[0]->ValReal();
  }

  if (is_clear_) {
    diff_info_.hasPrev = false;
  }
  is_clear_ = false;

  if (diff_info_.hasPrev) {
    k_double64 tmp = val - diff_info_.prev.d64_value;
    diff_info_.prev.d64_value = val;
    return tmp;
  } else {
    diff_info_.prev.d64_value = val;
    diff_info_.hasPrev = true;
    return 0.0;
  }
  return 0.0;
}

String FieldFuncDiff::ValStr() { return String(""); }

Field *FieldFuncDiff::field_to_copy() {
  FieldFuncDiff *field = new FieldFuncDiff(*this);

  return field;
}

k_bool FieldFuncDiff::is_nullable() {
  if (args_[0]->CheckNull()) {
    return true;
  }
  return false;
}

}  // namespace kwdbts
