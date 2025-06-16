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

#pragma once

#include <list>
#include <string>

#include "ee_common.h"
#include "ee_field.h"
#include "ee_field_common.h"

namespace kwdbts {

class FieldFuncStateWindow : public FieldFunc {
 public:
  // STATE_WINDOW(COLUMN_NAME | CASE WHEN EXPR)
  explicit FieldFuncStateWindow(Field *a) : FieldFunc(a) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
    if (a->get_storage_type() == roachpb::DataType::FLOAT ||
        a->get_storage_type() == roachpb::DataType::DOUBLE) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_INVALID_PARAMETER_VALUE,
          "Only support STATE_WINDOW on integer/bool/char/varchar column.");
    }
    if (a->get_storage_type() > roachpb::DataType::BOOL) {
      val_tp_ = roachpb::DataType::CHAR;
    }
  }
  enum Functype functype() override { return WINDOW_GROUP_FUNC; }
  k_bool is_nullable() override;
  k_int64 ValInt() override;
  Field *field_to_copy() override { return new FieldFuncStateWindow(*this); }
  void backup() override {
    old_group_id_ = group_id_;
    old_int_val_ = int_val_;
    old_str_val_ = str_val_;
  }
  void restore() override {
    group_id_ = old_group_id_;
    int_val_ = old_int_val_;
    str_val_ = old_str_val_;
  }

 private:
  roachpb::DataType val_tp_{roachpb::DataType::INT};
  k_int64 int_val_{0};
  String str_val_;
  k_int32 group_id_{0};
  k_int32 old_group_id_{0};
  k_int64 old_int_val_{0};
  String old_str_val_;
};

class FieldFuncEventWindow : public FieldFunc {
 public:
  // EVENT_WINDOW(start_trigger_condition,end_trigger_condition)
  FieldFuncEventWindow(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
  }
  enum Functype functype() override { return WINDOW_GROUP_FUNC; }

  k_int64 ValInt() override;
  Field *field_to_copy() override { return new FieldFuncEventWindow(*this); }
  k_bool is_nullable() override;
  void backup() override {
    old_group_id_ = group_id_;
    old_begin_ = begin_;
  }
  void restore() override {
    group_id_ = old_group_id_;
    begin_ = old_begin_;
  }
  void reset() {
    begin_ = false;
    old_begin_ = false;
  }

 private:
  k_int32 group_id_{0};
  bool begin_{false};
  k_int32 old_group_id_{0};
  bool old_begin_{false};
};

class FieldFuncSessionWindow : public FieldFunc {
 public:
  // SESSION_WINDOW(ts, tol_val)
  FieldFuncSessionWindow(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
    // resolve tol_val
    ResolveTolVal();
    set_allow_null(false);
  }

  enum Functype functype() override { return WINDOW_GROUP_FUNC; }

  k_int64 ValInt() override;
  Field *field_to_copy() override { return new FieldFuncSessionWindow(*this); }

 private:
  void ResolveTolVal();

 private:
  k_int64 val_{0};
  k_int64 tol_val_{0};
  k_int32 group_id_{0};
  k_int64 type_scale_{1};
  k_bool type_scale_multi_or_divde_{true};
  std::string error_info_;
};

class FieldFuncCountWindow : public FieldFunc {
 public:
  // COUNT_WINDOW(count_val,  sliding_val)
  explicit FieldFuncCountWindow(std::list<Field *> fields) : FieldFunc(fields) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
    count_val_ = args_[0]->ValInt();
    sliding_val_ = count_val_;
    set_allow_null(false);
    if (count_val_ <= 0) {
      std::string error_info = "invalid count value.";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                    error_info.c_str());
      return;
    }
    if (arg_count_ > 1) {
      sliding_val_ = args_[1]->ValInt();
      if (sliding_val_ > count_val_ || sliding_val_ <= 0) {
        std::string error_info = "invalid sliding value.";
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                      error_info.c_str());
        return;
      }
      if (sliding_val_ != count_val_) {
        is_sliding_ = true;
      }
    }
  }
  enum Functype functype() override { return WINDOW_GROUP_FUNC; }

  k_int64 ValInt() override;
  Field *field_to_copy() override { return new FieldFuncCountWindow(*this); }

  k_int32 *GetCurrentCount() { return &current_count_; }
  k_int32 GetParams(k_int32 *count_val, k_int32 *sliding_val) {
    *count_val = count_val_;
    *sliding_val = sliding_val_;
    return count_val_;
  }
  bool IsSilding() { return is_sliding_; }

 private:
  k_int32 count_val_{0};
  k_int32 sliding_val_{0};
  k_int32 current_count_{0};
  k_int32 group_id_{0};
  bool    is_sliding_{false};
};

class FieldFuncTimeWindowStart : public FieldFunc {
 public:
  // TIME_WINDOW_START(ts)
  explicit FieldFuncTimeWindowStart(Field *a) : FieldFunc(a) {
    type_ = FIELD_FUNC;
    sql_type_ = a->get_sql_type();
    storage_type_ = a->get_storage_type();
    storage_len_ = a->get_storage_length();
    set_allow_null(false);
  }

  k_int64 ValInt() override { return val_; }
  enum Functype functype() override { return WINDOW_GROUP_FUNC; }

  Field *field_to_copy() override {
    return new FieldFuncTimeWindowStart(*this);
  }

 public:
  k_int64 val_{0};
};


class FieldFuncTimeWindowEnd : public FieldFunc {
 public:
  // TIME_WINDOW_END(ts)
  explicit FieldFuncTimeWindowEnd(Field *a) : FieldFunc(a) {
    type_ = FIELD_FUNC;
    sql_type_ = a->get_sql_type();
    storage_type_ = a->get_storage_type();
    storage_len_ = a->get_storage_length();
    set_allow_null(false);
  }

  k_int64 ValInt() override { return val_; }

  enum Functype functype() override { return WINDOW_GROUP_FUNC; }

  Field *field_to_copy() override { return new FieldFuncTimeWindowEnd(*this); }

 public:
  k_int64 val_{0};
};

class FieldFuncTimeWindow : public FieldFunc {
 public:
  // TIME_WINDOW(ts, duration, sliding_time)
  FieldFuncTimeWindow(std::list<Field *> fields, k_int8 tz)
      : FieldFunc(fields) {
    type_ = FIELD_FUNC;
    sql_type_ = roachpb::DataType::INT;
    storage_type_ = roachpb::DataType::INT;
    storage_len_ = sizeof(k_int32);
    time_zone_ = tz;
    set_allow_null(false);
    ResolveParams();
    if (arg_count_ > 2) {
      if (interval_seconds_ != sliding_interval_seconds_ ||
          var_interval_ != sliding_var_interval_ ||
          year_bucket_ != sliding_year_bucket_) {
        is_sliding_ = true;
      }
    }
  }

  k_int32 GetParams(k_int64 *interval_seconds, bool *var_interval,
                    bool *year_bucket, k_int64 *s_interval_seconds,
                    bool *s_var_interval, bool *s_year_bucket,
                    bool *sliding, k_int64 *type_scale) {
    *interval_seconds = interval_seconds_;
    *var_interval = var_interval_;
    *year_bucket = year_bucket_;
    *s_interval_seconds = sliding_interval_seconds_;
    *s_var_interval = sliding_var_interval_;
    *s_year_bucket = sliding_year_bucket_;
    *sliding = is_sliding_;
    *type_scale = type_scale_;
    return interval_seconds_;
  }
  enum Functype functype() override { return WINDOW_GROUP_FUNC; }

  k_int64 ValInt() override;
  Field *field_to_copy() override { return new FieldFuncTimeWindow(*this); }
  bool IsSilding() { return is_sliding_; }

 private:
  void ResolveParams();

 private:
  k_int8 time_zone_{0};
  k_int64 interval_seconds_{0};
  k_bool var_interval_{false};
  k_bool year_bucket_{false};
  k_int64 sliding_interval_seconds_{0};
  k_bool sliding_var_interval_{false};
  k_bool sliding_year_bucket_{false};
  std::string error_info_{""};
  k_bool is_sliding_{false};
  k_int64 type_scale_{1};
};

};  // namespace kwdbts
