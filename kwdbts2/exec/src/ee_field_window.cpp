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

#include "ee_field_window.h"

#include <time.h>

#include "pgcode.h"

namespace kwdbts {

k_bool FieldFuncStateWindow::is_nullable() {
  if (args_[0]->is_nullable()) {
    return true;
  }
  return false;
}

k_int64 FieldFuncStateWindow::ValInt() {
  if (val_tp_ == roachpb::DataType::INT) {
    if (group_id_ == 0) {
      int_val_ = args_[0]->ValInt();
      group_id_++;
    } else {
      k_int64 new_val = args_[0]->ValInt();
      if (new_val != int_val_) {
        int_val_ = new_val;
        group_id_++;
      }
    }
  } else {
    if (group_id_ == 0) {
      str_val_ = args_[0]->ValStr();
      String val(str_val_.length_);
      memcpy(val.ptr_, str_val_.ptr_, str_val_.length_);
      val.length_ = str_val_.length_;
      str_val_ = val;
      group_id_++;
    } else {
      String new_val = args_[0]->ValStr();
      if (new_val.compare(str_val_) != 0) {
        String val(new_val.length_);
        memcpy(val.ptr_, new_val.ptr_, new_val.length_);
        val.length_ = new_val.length_;
        str_val_ = val;
        group_id_++;
      }
    }
  }
  return group_id_;
}

k_bool FieldFuncEventWindow::is_nullable() {
  if (begin_) {
    if (args_[1]->is_nullable()) {
      return true;
    }
  } else {
    if (args_[0]->is_nullable() || args_[1]->is_nullable()) {
      return true;
    }
    if (args_[0]->ValInt() <= 0) {
      return true;
    }
  }
  return false;
}

k_int64 FieldFuncEventWindow::ValInt() {
  if (!begin_) {
    if (args_[0]->ValInt() > 0) {
      begin_ = true;
      group_id_++;
      if (args_[1]->ValInt() > 0) {
        begin_ = false;
      }
    }
  } else {
    if (args_[1]->ValInt() > 0) {
      begin_ = false;
    }
  }
  return group_id_;
}

// Accurate to milliseconds
void FieldFuncSessionWindow::ResolveTolVal() {
  std::string timestring = {args_[1]->ValStr().getptr(),
                            args_[1]->ValStr().length_};
  k_bool var_interval = false;
  k_bool year_bucket = false;
  KString unit = "";
  k_int64 time_diff = 0;
  timestring = replaceTimeUnit(timestring);
  tol_val_ = getIntervalSeconds(timestring, var_interval, year_bucket,
                                &unit, error_info_, true);
  if (error_info_ != "") {
    return;
  }
  sql_type_ = getTimeFieldType(args_[0]->get_storage_type(), unit, &time_diff, &type_scale_, &type_scale_multi_or_divde_);
  storage_type_ = sql_type_;
  storage_len_ = sizeof(k_int64);
  if (storage_type_ == roachpb::DataType::TIMESTAMP_NANO ||
      storage_type_ == roachpb::DataType::TIMESTAMPTZ_NANO) {
    tol_val_ *= 1000000;
  } else if (storage_type_ == roachpb::DataType::TIMESTAMP_MICRO ||
             storage_type_ == roachpb::DataType::TIMESTAMPTZ_MICRO) {
    tol_val_ *= 1000;
  }
  if (var_interval && year_bucket) {
    tol_val_ = tol_val_ * 365LL * MILLISECOND_PER_DAY;
  } else if (var_interval) {
    tol_val_ = tol_val_ * 30LL * MILLISECOND_PER_DAY;
  }
}

k_int64 FieldFuncSessionWindow::ValInt() {
  k_int64 new_val = args_[0]->ValInt();
    if (type_scale_ != 1) {
    // multi
    if (type_scale_multi_or_divde_) {
      if (!I64_SAFE_MUL_CHECK(new_val, type_scale_)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                      "Timestamp/TimestampTZ out of range");
        return 0;
      }
      new_val *= type_scale_;
      // divide
    } else {
      new_val /= type_scale_;
    }
  }
  if (new_val - val_ > tol_val_) {
    group_id_++;
  }
  val_ = new_val;
  return group_id_;
}

k_int64 FieldFuncCountWindow::ValInt() {
  current_count_++;
  if (current_count_ == 1) {
    group_id_++;
  } else if (current_count_ == count_val_) {
    current_count_ = 0;
  }
  return group_id_;
}

void FieldFuncTimeWindow::ResolveParams() {
  std::string timestring = {args_[1]->ValStr().getptr(),
                            args_[1]->ValStr().length_};
  KString unit = "";
  k_int64 time_diff = 0;
  timestring = replaceTimeUnit(timestring);
  interval_seconds_ = getIntervalSeconds(timestring, var_interval_,
                                         year_bucket_, &unit, error_info_, false);
  if (error_info_ != "") {
    return;
  }
  if (unit == "ns" || unit == "us") {
    error_info_ = "duration time does not support us/ns";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  error_info_.c_str());
    return;
  }
  roachpb::DataType storage_type = args_[0]->get_storage_type();
  if (storage_type_ == roachpb::DataType::TIMESTAMP_NANO ||
      storage_type == roachpb::DataType::TIMESTAMPTZ_NANO) {
    type_scale_ = 1000000;
  } else if (storage_type == roachpb::DataType::TIMESTAMP_MICRO ||
             storage_type == roachpb::DataType::TIMESTAMPTZ_MICRO) {
    type_scale_ = 1000;
  }
  if (!var_interval_ && !year_bucket_ && interval_seconds_ < TIME_WINDOW_MIN_DURATION_MS) {
    error_info_ = "duration time must exceed 10ms.";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  error_info_.c_str());
    return;
  }
  if (arg_count_ > 2) {
    timestring = {args_[2]->ValStr().getptr(), args_[2]->ValStr().length_};
    timestring = replaceTimeUnit(timestring);
    sliding_interval_seconds_ =
        getIntervalSeconds(timestring, sliding_var_interval_,
                           sliding_year_bucket_, &unit, error_info_, false);
    if (error_info_ != "") {
      return;
    }
    if (unit == "ns" || unit == "us") {
      error_info_ = "sliding time does not support us/ns";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  error_info_.c_str());
      return;
    }
    // check sliding time > duration time ?
    k_int64 duration, sliding;
    duration = interval_seconds_;
    sliding = sliding_interval_seconds_;
    if (var_interval_ && year_bucket_) {
      duration = duration * 365LL * MILLISECOND_PER_DAY;
    } else if (var_interval_) {
      duration = duration * 30LL * MILLISECOND_PER_DAY;
    }
    if (sliding_var_interval_) {
      error_info_ = "sliding time does not support month/year";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                    error_info_.c_str());
      return;
    }
    if (sliding > duration) {
      error_info_ = "sliding value no larger than the duration value";
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                    error_info_.c_str());
    }
  }
}

k_int64 FieldFuncTimeWindow::ValInt() {
  return 0;
}


}  // namespace kwdbts
