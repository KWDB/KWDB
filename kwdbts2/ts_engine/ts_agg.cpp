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
#include "engine.h"

namespace kwdbts {

bool AggCalculatorV2::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  return (*bitmap_)[row] == DataFlags::kNull;
}

bool AggCalculatorV2::CalcAggForFlush(int is_not_null, uint16_t& count, void* max_addr, void* min_addr,
                                      void* sum_addr) {
  count = 0;
  void* min = nullptr;
  void* max = nullptr;
  bool is_overflow = false;
  for (int i = 0; i < count_; ++i) {
    if (!is_not_null && isnull(i)) {
      continue;
    }
    ++count;
    auto current = reinterpret_cast<void*>(reinterpret_cast<intptr_t>(mem_) + i * size_);

    if (!max || cmp(current, max, type_, size_) > 0) {
      max = current;
    }
    if (!min || cmp(current, min, type_, size_) < 0) {
      min = current;
    }
    if (isSumType(type_) && sum_addr != nullptr) {
      if (!is_overflow) {
        switch (type_) {
          case DATATYPE::INT8:
            is_overflow = AddAggInteger<int64_t>(
                *static_cast<int64_t*>(sum_addr),
                *static_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            is_overflow = AddAggInteger<int64_t>(
                *static_cast<int64_t*>(sum_addr),
                *static_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            is_overflow = AddAggInteger<int64_t>(
                *static_cast<int64_t*>(sum_addr),
                *static_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            is_overflow = AddAggInteger<int64_t>(
                *static_cast<int64_t*>(sum_addr),
                *static_cast<int64_t*>(current));
            break;
          case DATATYPE::FLOAT:
            AddAggFloat<double>(
                *static_cast<double*>(sum_addr),
                *static_cast<float*>(current));
            break;
          case DATATYPE::DOUBLE:
            AddAggFloat<double>(
                *static_cast<double*>(sum_addr),
                *static_cast<double*>(current));
            break;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d", type_);
            return KStatus::FAIL;
        }
        if (is_overflow) {
          *static_cast<double*>(sum_addr) = *static_cast<int64_t*>(sum_addr);
        }
      }
      if (is_overflow) {
        switch (type_) {
          case DATATYPE::INT8:
            AddAggFloat<double, int64_t>(
                *static_cast<double*>(sum_addr),
                *static_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            AddAggFloat<double, int64_t>(
                *static_cast<double*>(sum_addr),
                *static_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            AddAggFloat<double, int64_t>(
                *static_cast<double*>(sum_addr),
                *static_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            AddAggFloat<double, int64_t>(
                *static_cast<double*>(sum_addr),
                *static_cast<int64_t*>(current));
            break;
          case DATATYPE::FLOAT:
          case DATATYPE::DOUBLE:
            LOG_ERROR("Overflow not supported for sum, datatype: %d",
                type_);
            return KStatus::FAIL;
            break;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d",
                type_);
            return KStatus::FAIL;
            break;
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
  return is_overflow;
}

void VarColAggCalculatorV2::CalcAggForFlush(string& max, string& min, uint64_t& count) {
  if (var_rows_.empty()) {
    count = 0;
    return;
  }

  count = var_rows_.size();

  auto max_it = std::max_element(var_rows_.begin(), var_rows_.end());
  max = *max_it;

  auto min_it = std::min_element(var_rows_.begin(), var_rows_.end());
  min = *min_it;
}
}  // namespace kwdbts
