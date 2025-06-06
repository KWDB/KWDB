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
      // k_int32 ret = (*(static_cast<k_int16*>(l))) - (*(static_cast<k_int16*>(r)));
      k_int16 lv = *(static_cast<k_int16*>(l));
      k_int16 rv = *(static_cast<k_int16*>(r));
      k_int32 diff = static_cast<k_int32>(lv) - static_cast<k_int32>(rv);
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP: {
      // k_int64 diff = (*(static_cast<k_int32*>(l))) - (*(static_cast<k_int32*>(r)));
      k_int32 lv = *(static_cast<k_int32*>(l));
      k_int32 rv = *(static_cast<k_int32*>(r));
      k_int64 diff = static_cast<k_int64>(lv) - static_cast<k_int64>(rv);
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

bool AggCalculatorV2::CalcAggForFlush(uint16_t& count, void* max_addr, void* min_addr,
                                      void* sum_addr) {
  count = 0;
  void* min = nullptr;
  void* max = nullptr;
  bool is_overflow = false;
  for (int i = 0; i < count_; ++i) {
    if (isnull(i)) {
      continue;
    }
    ++count;
    auto current = reinterpret_cast<void*>(reinterpret_cast<intptr_t>(mem_) + i * size_);

    if (!max || cmp(current, max) > 0) {
      max = current;
    }
    if (!min || cmp(current, min) < 0) {
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

inline void AggCalculatorV2::InitSumValue(void* ptr) {
  switch (type_) {
    case DATATYPE::INT8:
    case DATATYPE::INT16:
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP:
    case DATATYPE::INT64:
      *static_cast<int64_t*>(ptr) = 0;
      break;
    case DATATYPE::FLOAT:
    case DATATYPE::DOUBLE:
      *static_cast<double*>(ptr) = 0.0;
      break;
    default:
      break;
  }
}


inline void AggCalculatorV2::InitAggData(TSSlice& agg_data) {
  agg_data.data = static_cast<char*>(malloc(agg_data.len));
  memset(agg_data.data, 0, agg_data.len);
}

// MergeAggResultFromBlock
//
// This function performs aggregation (COUNT, MIN, MAX, SUM) over a single data block
// and updates the provided aggregation result buffers in-place.
//
// Unlike `CalcAllAgg`, which initializes aggregation state internally and returns a local result,
// this function **accumulates into pre-initialized global results** (`max_addr`, `min_addr`, `sum_addr`, and `count`).
//
// Preconditions:
// - `max_addr`, `min_addr`, `sum_addr` must point to valid memory initialized to extreme values.
// - `count` is an accumulated counter that will be incremented.
// - This function is designed to be called repeatedly across multiple blocks.
KStatus AggCalculatorV2::MergeAggResultFromBlock(TSSlice& agg_data, Sumfunctype agg_type,
                                                  uint32_t col_idx, bool& is_overflow) {
  if (mem_ == nullptr) {
    return SUCCESS;  // ddl add column, the old block does not have the column
  }

  for (int i = 0; i < count_; ++i) {
    if (isnull(i)) {
      continue;
    }

    if (agg_type == Sumfunctype::COUNT) {
      ++(KUint64(agg_data.data));
    }

    // Currently the size of first column which must be timestamp is 16 bytes after the conversion.
    void* current = reinterpret_cast<void*>((intptr_t)(mem_) + i * (col_idx == 0 ? 16 : size_));

    // === MAX ===
    if (agg_type == Sumfunctype::MAX) {
      if (agg_data.data == nullptr) {
        agg_data.len = size_;
        InitAggData(agg_data);
        memcpy(agg_data.data, current, size_);
      } else if (cmp(current, agg_data.data) > 0) {
        memcpy(agg_data.data, current, size_);
      }
    }

    // === MIN ===
    if (agg_type == Sumfunctype::MIN) {
      if (agg_data.data == nullptr) {
        agg_data.len = size_;
        InitAggData(agg_data);
        memcpy(agg_data.data, current, size_);
      } else if (cmp(current, agg_data.data) < 0) {
        memcpy(agg_data.data, current, size_);
      }
    }

    // === SUM ===
    if (agg_type == Sumfunctype::SUM && isSumType(type_)) {
      if (agg_data.data == nullptr) {
        agg_data.len = sizeof(int64_t);
        InitAggData(agg_data);
        InitSumValue(agg_data.data);
      }

      if (!is_overflow) {
        switch (type_) {
          case DATATYPE::INT8:
            is_overflow = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            is_overflow = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            is_overflow = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            is_overflow = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int64_t*>(current));
            break;
          case DATATYPE::FLOAT:
            AddAggFloat<double>(
                *reinterpret_cast<double*>(agg_data.data),
                *reinterpret_cast<float*>(current));
            break;
          case DATATYPE::DOUBLE:
            AddAggFloat<double>(
                *reinterpret_cast<double*>(agg_data.data),
                *reinterpret_cast<double*>(current));
            break;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d",
                type_);
            return KStatus::FAIL;
            break;
        }
        if (is_overflow) {
          *reinterpret_cast<double*>(agg_data.data) = *reinterpret_cast<int64_t*>(agg_data.data);
        }
      }
      if (is_overflow) {
        switch (type_) {
          case DATATYPE::INT8:
            AddAggFloat<double, int64_t>(
                *reinterpret_cast<double*>(agg_data.data),
                *reinterpret_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            AddAggFloat<double, int64_t>(
                *reinterpret_cast<double*>(agg_data.data),
                *reinterpret_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            AddAggFloat<double, int64_t>(
                *reinterpret_cast<double*>(agg_data.data),
                *reinterpret_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            AddAggFloat<double, int64_t>(
                *reinterpret_cast<double*>(agg_data.data),
                *reinterpret_cast<int64_t*>(current));
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

  return KStatus::SUCCESS;
}

KStatus AggCalculatorV2::MergeAggResultFromPreAgg(TSSlice& agg_data, Sumfunctype agg_type, bool& is_overflow) {
  switch (agg_type) {
    case COUNT: {
      KUint64(agg_data.data) += count_;
      break;
    }
    case MAX: {
      void* max = nullptr;
      max = pre_agg_ + sizeof(uint16_t);
      if (agg_data.data == nullptr) {
        agg_data.len = size_;
        InitAggData(agg_data);
        memcpy(agg_data.data, max, size_);
      } else if (cmp(max, agg_data.data) > 0) {
        memcpy(agg_data.data, max, size_);
      }
      break;
    }
    case MIN: {
      void* min = nullptr;
      min = pre_agg_ + sizeof(uint16_t) + size_;
      if (agg_data.data == nullptr) {
        agg_data.len = size_;
        InitAggData(agg_data);
        memcpy(agg_data.data, min, size_);
      } else if (cmp(min, agg_data.data) < 0) {
        memcpy(agg_data.data, min, size_);
      }
      break;
    }
    case SUM: {
      bool pre_agg_overflow = *static_cast<bool *>(pre_agg_ + sizeof(uint16_t) + size_ * 2);
      if (pre_agg_overflow) {
        is_overflow = true;
      }
      void* cur_sum = pre_agg_ + sizeof(uint16_t) + size_ * 2 + 1;
      if (agg_data.data == nullptr) {
        agg_data.len = sizeof(int64_t);
        InitAggData(agg_data);
        InitSumValue(agg_data.data);
      }
      if (!is_overflow) {
        switch (type_) {
          case DATATYPE::INT8:
          case DATATYPE::INT16:
          case DATATYPE::INT32:
          case DATATYPE::INT64:
            is_overflow = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *static_cast<int64_t*>(cur_sum));
            break;
          case DATATYPE::FLOAT:
          case DATATYPE::DOUBLE:
            AddAggFloat<double>(
                *reinterpret_cast<double*>(agg_data.data),
                *static_cast<double*>(cur_sum));
            break;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d",
                type_);
            return KStatus::FAIL;
            break;
        }
        if (is_overflow) {
          *reinterpret_cast<double*>(agg_data.data) = *reinterpret_cast<int64_t*>(agg_data.data);
        }
      }
      if (is_overflow) {
        switch (type_) {
          case DATATYPE::INT8:
          case DATATYPE::INT16:
          case DATATYPE::INT32:
          case DATATYPE::INT64:
            if (!pre_agg_overflow) {
              AddAggFloat<double, int64_t>(
                *reinterpret_cast<double*>(agg_data.data),
                *static_cast<int64_t*>(cur_sum));
            } else {
              AddAggFloat<double, double>(
                *reinterpret_cast<double*>(agg_data.data),
                *static_cast<double*>(cur_sum));
            }
            break;
          case DATATYPE::FLOAT:
          case DATATYPE::DOUBLE:
            LOG_ERROR("Overflow not supported for sum, datatype: %d", type_);
            return KStatus::FAIL;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d", type_);
            return KStatus::FAIL;
        }
      }
      break;
    }
    default:
      LOG_ERROR("Not supported functype %d for pre agg", agg_type);
      break;
  }
  return SUCCESS;
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

void VarColAggCalculatorV2::MergeAggResultFromBlock(TSSlice& agg_data, Sumfunctype agg_type) {
  if (var_rows_.empty()) {
    return;
  }

  if (agg_type == Sumfunctype::COUNT) {
    KUint64(agg_data.data) += var_rows_.size();
  }

  if (agg_type == Sumfunctype::MAX) {
    auto max_it = std::max_element(var_rows_.begin(), var_rows_.end());
    if (agg_data.data) {
      string current_max({agg_data.data + kStringLenLen, agg_data.len});
      if (current_max < *max_it) {
        free(agg_data.data);
        agg_data.data = nullptr;
      }
    }
    if (agg_data.data == nullptr) {
      // Can we use the memory in var_rows?
      agg_data.len = max_it->length() + kStringLenLen;
      agg_data.data = static_cast<char*>(malloc(agg_data.len));
      KUint16(agg_data.data) = max_it->length();
      memcpy(agg_data.data + kStringLenLen, max_it->c_str(), max_it->length());
    }
  }

  if (agg_type == Sumfunctype::MIN) {
    auto min_it = std::min_element(var_rows_.begin(), var_rows_.end());
    if (agg_data.data) {
      string current_min({agg_data.data + kStringLenLen, agg_data.len});
      if (current_min > *min_it) {
        free(agg_data.data);
        agg_data.data = nullptr;
      }
    }
    if (agg_data.data == nullptr) {
      // Can we use the memory in var_rows?
      agg_data.len = min_it->length() + kStringLenLen;
      agg_data.data = static_cast<char*>(malloc(agg_data.len));
      KUint16(agg_data.data) = min_it->length();
      memcpy(agg_data.data + kStringLenLen, min_it->c_str(), min_it->length());
    }
  }
}

KStatus VarColAggCalculatorV2::MergeAggResultFromPreAgg(TSSlice &agg_data, Sumfunctype agg_type) {
  uint32_t max_len{0};
  uint32_t min_len{0};
  switch (agg_type) {
    case COUNT: {
      KUint64(agg_data.data) += count_;
      break;
    }
    case MAX: {
      void* max = nullptr;
      max_len = *static_cast<uint32_t *>(pre_agg_ + sizeof(uint16_t));
      max = pre_agg_ + sizeof(uint16_t) + sizeof(uint32_t) * 2;
      string max_str(static_cast<const char *>(max), max_len);
      if (agg_data.data) {
        string current_max({agg_data.data + kStringLenLen, agg_data.len});
        if (current_max < max_str) {
          free(agg_data.data);
          agg_data.data = nullptr;
        }
      }
      if (agg_data.data == nullptr) {
        // Can we use the memory in var_rows?
        agg_data.len = max_len + kStringLenLen;
        agg_data.data = static_cast<char*>(malloc(agg_data.len));
        KUint16(agg_data.data) = max_len;
        memcpy(agg_data.data + kStringLenLen, max, max_len);
      }
      break;
    }
    case MIN: {
      void* min = nullptr;
      max_len = *static_cast<uint32_t *>(pre_agg_ + sizeof(uint16_t));
      min_len = *static_cast<uint32_t *>(pre_agg_+ sizeof(uint16_t) + sizeof(uint32_t));
      min = pre_agg_ + sizeof(uint16_t) + sizeof(uint32_t) * 2 + max_len;
      string min_str(static_cast<const char *>(min), min_len);
      if (agg_data.data) {
        string current_min(agg_data.data + kStringLenLen);
        if (current_min > min_str) {
          free(agg_data.data);
          agg_data.data = nullptr;
        }
      }
      if (agg_data.data == nullptr) {
        // Can we use the memory in var_rows?
        agg_data.len = min_len + kStringLenLen;
        agg_data.data = static_cast<char*>(malloc(agg_data.len));
        KUint16(agg_data.data) = min_len;
        memcpy(agg_data.data + kStringLenLen, min, min_len);
      }
      break;
    }
    default:
      break;
  }
  return SUCCESS;
}
}  // namespace kwdbts
