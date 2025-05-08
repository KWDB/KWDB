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
int AggCalculatorV2::cmp(void* l, void* r) {
  switch (type_) {
    case DATATYPE::INT8:
    case DATATYPE::BYTE:
    case DATATYPE::CHAR:
    case DATATYPE::BOOL:
    case DATATYPE::BINARY: {
      k_int32 ret = memcmp(l, r, size_);
      return ret;
    }
    case DATATYPE::INT16: {
      k_int32 ret = (*(static_cast<k_int16*>(l))) - (*(static_cast<k_int16*>(r)));
      return ret;
    }
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP: {
      k_int64 diff = (*(static_cast<k_int32*>(l))) - (*(static_cast<k_int32*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO: {
      double diff = (*(static_cast<k_int64*>(l))) - (*(static_cast<k_int64*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::TIMESTAMP64_LSN:
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
    case DATATYPE::TIMESTAMP64_LSN_NANO: {
      double diff = (*(static_cast<TimeStamp64LSN*>(l))).ts64 - (*(static_cast<TimeStamp64LSN*>(r))).ts64;
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::FLOAT: {
      double diff = (*(static_cast<float*>(l))) - (*(static_cast<float*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::DOUBLE: {
      double diff = (*(static_cast<double*>(l))) - (*(static_cast<double*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::STRING: {
      k_int32 ret = strncmp(static_cast<char*>(l), static_cast<char*>(r), size_);
      return ret;
    }
      break;
    default:
      break;
  }
  return false;
}

bool AggCalculatorV2::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  return (*bitmap_)[row] == DataFlags::kNull;
}

bool AggCalculatorV2::CalcAllAgg(uint16_t& count, void* max_addr, void* min_addr, void* sum_addr) {
  count = 0;
  void* min = nullptr;
  void* max = nullptr;
  bool is_overflow = false;
  for (int i = 0; i < count_; ++i) {
    if (isnull(i)) {
      continue;
    }
    ++count;
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);

    if (!max || cmp(current, max) > 0) {
      max = current;
    }
    if (!min || cmp(current, min) < 0) {
      min = current;
    }
    if (isSumType(type_) && sum_addr != nullptr) {
      // When a memory overflow has occurred, there is no need to calculate the sum result again
      if (is_overflow) {
        continue;
      }
      // sum
      switch (type_) {
        case DATATYPE::INT8:
          is_overflow = AddAggInteger<int8_t>((*(static_cast<int8_t*>(sum_addr))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          is_overflow = AddAggInteger<int16_t>((*(static_cast<int16_t*>(sum_addr))),
                                               (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          is_overflow = AddAggInteger<int32_t>((*(static_cast<int32_t*>(sum_addr))),
                                               (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          is_overflow = AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_addr))),
                                               (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          AddAggFloat<double>((*(static_cast<double*>(sum_addr))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          AddAggFloat<float>((*(static_cast<float*>(sum_addr))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::TIMESTAMP64_LSN:
          break;
        default:
          break;
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

void VarColAggCalculatorV2::CalcAllAgg(string& max, string& min, uint16_t& count) {
  // for (int i = 0; i < count_; ++i) {
  //   if (isnull(i)) {
  //     continue;
  //   }
  //   if (count_addr) {
  //     *reinterpret_cast<uint16_t*>(count_addr) += 1;
  //   }
  // }
  // TODO(zqh): calc count by isnull
  if (var_rows_.empty()) {
    count = 0;
    return;
  }
  count = count_;

  auto max_it = std::max_element(var_rows_.begin(), var_rows_.end());
  max = *max_it;

  auto min_it = std::min_element(var_rows_.begin(), var_rows_.end());
  min = *min_it;
}

}  // namespace kwdbts
