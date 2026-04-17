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

#include "ts_agg.h"

#include <algorithm>

#include "engine.h"

namespace kwdbts {

bool AggCalculatorV2::isnull(size_t row) const {
  const auto* bitmap = bitmap_;
  if (bitmap == nullptr) {
    return false;
  }
  return (*bitmap)[row] == DataFlags::kNull;
}

KStatus AggCalculatorV2::CalcAggForFlush(bool is_not_null, bool& is_overflow, uint16_t& count,
                                         void* max_addr, void* min_addr, void* sum_addr) {
  count = 0;
  is_overflow = false;
  void* min = nullptr;
  void* max = nullptr;
  bool overflow = false;
  const bool need_sum = isSumType(type_) && sum_addr != nullptr;
  const auto row_stride = static_cast<intptr_t>(size_);
  for (int i = 0; i < count_; ++i) {
    if (!is_not_null && isnull(i)) {
      continue;
    }
    ++count;
    auto current = static_cast<void*>(mem_ + static_cast<intptr_t>(i) * row_stride);

    if (!max || cmp(current, max, type_, size_) > 0) {
      max = current;
    }
    if (!min || cmp(current, min, type_, size_) < 0) {
      min = current;
    }
    if (need_sum) {
      auto* int_sum_addr = static_cast<int64_t*>(sum_addr);
      auto* double_sum_addr = static_cast<double*>(sum_addr);
      if (!overflow) {
        switch (type_) {
          case DATATYPE::INT8:
            overflow = AddAggInteger<int64_t>(
                *int_sum_addr,
                *static_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            overflow = AddAggInteger<int64_t>(
                *int_sum_addr,
                *static_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            overflow = AddAggInteger<int64_t>(
                *int_sum_addr,
                *static_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            overflow = AddAggInteger<int64_t>(
                *int_sum_addr,
                *static_cast<int64_t*>(current));
            break;
          case DATATYPE::FLOAT:
            AddAggFloat<double>(
                *double_sum_addr,
                *static_cast<float*>(current));
            break;
          case DATATYPE::DOUBLE:
            AddAggFloat<double>(
                *double_sum_addr,
                *static_cast<double*>(current));
            break;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d", type_);
            return KStatus::FAIL;
        }
        if (overflow) {
          *double_sum_addr = static_cast<double>(*int_sum_addr);
        }
      }
      if (overflow) {
        switch (type_) {
          case DATATYPE::INT8:
            AddAggFloat<double, int64_t>(
                *double_sum_addr,
                *static_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            AddAggFloat<double, int64_t>(
                *double_sum_addr,
                *static_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            AddAggFloat<double, int64_t>(
                *double_sum_addr,
                *static_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            AddAggFloat<double, int64_t>(
                *double_sum_addr,
                *static_cast<int64_t*>(current));
            break;
          case DATATYPE::FLOAT:
          case DATATYPE::DOUBLE:
            LOG_ERROR("Overflow not supported for sum, datatype: %d",
                type_);
            return KStatus::FAIL;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d",
                type_);
            return KStatus::FAIL;
        }
      }
    }
  }

  if (min != nullptr && min_addr != nullptr && min != min_addr) {
    memcpy(min_addr, min, size_);
  }

  if (max != nullptr && max_addr != nullptr && max != max_addr) {
    memcpy(max_addr, max, size_);
  }
  is_overflow = overflow;
  return KStatus::SUCCESS;
}

void VarColAggCalculatorV2::CalcAggForFlush(string& max, string& min, uint64_t& count) {
  if (var_rows_.empty()) {
    count = 0;
    return;
  }

  count = var_rows_.size();

  const auto [min_it, max_it] = std::minmax_element(var_rows_.begin(), var_rows_.end());
  min = *min_it;
  max = *max_it;
}
}  // namespace kwdbts
