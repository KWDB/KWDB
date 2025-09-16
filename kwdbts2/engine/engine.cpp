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

#include "engine.h"

#include <dirent.h>
#include <iostream>
#include <utility>
#include <shared_mutex>

#include "ee_dml_exec.h"
#include "sys_utils.h"
#include "ts_table.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "ee_exec_pool.h"
#include "st_tier.h"

#ifndef KWBASE_OSS
#include "ts_config_autonomy.h"
#endif

extern std::map<std::string, std::string> g_cluster_settings;
extern DedupRule g_dedup_rule;
extern std::shared_mutex g_settings_mutex;
extern bool g_go_start_service;

KStatus TSEngine::Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp) {
  ctx->ts_engine = this;
  KStatus ret = DmlExec::ExecQuery(ctx, req, resp);
  return ret;
}

namespace kwdbts {
int32_t EngineOptions::iot_interval  = 864000;
string EngineOptions::home_;  // NOLINT
size_t EngineOptions::ps_ = sysconf(_SC_PAGESIZE);

#define DEFAULT_NS_ALIGN_SIZE       2  // 16GB name service

int EngineOptions::ns_align_size_ = DEFAULT_NS_ALIGN_SIZE;
int EngineOptions::table_type_ = ROW_TABLE;
int EngineOptions::double_precision_ = 12;
int EngineOptions::float_precision_ = 6;
int64_t EngineOptions::max_anon_memory_size_ = 1*1024*1024*1024;  // 1G
int EngineOptions::dt32_base_year_ = 2000;
bool EngineOptions::zero_if_null_ = false;
#if defined(_WINDOWS_)
const char BigObjectConfig::slash_ = '\\';
#else
const char EngineOptions::slash_ = '/';
#endif
bool EngineOptions::is_single_node_ = false;
int EngineOptions::table_cache_capacity_ = 1000;
std::atomic<int64_t> kw_used_anon_memory_size;

void EngineOptions::init() {
  char * env_var = getenv(ENV_KW_HOME);
  if (env_var) {
    home_ = string(env_var);
  } else {
    home_ =  getenv(ENV_CLUSTER_CONFIG_HOME);
  }

  env_var = getenv(ENV_KW_IOT_INTERVAL);
  if (env_var) {
    setInteger(iot_interval, string(env_var), 30);
  }
}

bool AggCalculator::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(bitmap_)[byte] & bit;
}

bool AggCalculator::isDeleted(char* delete_flags, size_t row) {
  if (!delete_flags) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(delete_flags)[byte] & bit;
}

void* AggCalculator::GetMax(void* base, bool need_to_new) {
  void* max = nullptr;
  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!max || cmp(current, max, type_, size_) > 0) {
      max = current;
    }
  }
  if (base && cmp(base, max, type_, size_) > 0) {
    max = base;
  }
  if (need_to_new && max) {
    void* new_max = malloc(size_);
    memcpy(new_max, max, size_);
    max = new_max;
  }
  return max;
}

void* AggCalculator::GetMin(void* base, bool need_to_new) {
  void* min = nullptr;
  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!min || cmp(current, min, type_, size_) < 0) {
      min = current;
    }
  }
  if (base && cmp(base, min, type_, size_) < 0) {
    min = base;
  }
  if (need_to_new && min) {
    void* new_min = malloc(size_);
    memcpy(new_min, min, size_);
    min = new_min;
  }
  return min;
}

void* AggCalculator::changeBaseType(void* base) {
  void* sum_base = malloc(sizeof(double));
  memset(sum_base, 0, sizeof(double));
  switch (type_) {
    case DATATYPE::INT8:
      *(static_cast<double*>(sum_base)) = *(static_cast<int8_t*>(base));
      break;
    case DATATYPE::INT16:
      *(static_cast<double*>(sum_base)) = *(static_cast<int16_t*>(base));
      break;
    case DATATYPE::INT32:
      *(static_cast<double*>(sum_base)) = *(static_cast<int32_t*>(base));
      break;
    case DATATYPE::INT64:
      *(static_cast<double*>(sum_base)) = *(static_cast<int64_t*>(base));
      break;
    case DATATYPE::FLOAT:
      *(static_cast<double*>(sum_base)) = *(static_cast<float*>(base));
    default:
      break;
  }
  free(base);
  base = nullptr;
  return sum_base;
}

bool AggCalculator::GetSum(void** sum_res, void* base, bool is_overflow) {
  if (!isSumType(type_)) {
    *sum_res = nullptr;
    return false;
  }
  void* sum_base = nullptr, *new_sum_base = nullptr;
  if (base) {
    if (!is_overflow && is_overflow_) {
      sum_base = changeBaseType(base);
    } else if (is_overflow && !is_overflow_) {
      new_sum_base = base;
    } else {
      sum_base = base;
    }
  } else {
    sum_base = malloc(sum_size_);
    memset(sum_base, 0, sum_size_);
  }

  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    switch (sum_type_) {
      case DATATYPE::INT8:
        if (sum_base) {
          if (AddAggInteger<int64_t, int8_t>((*(static_cast<int64_t*>(sum_base))),
                                             (*(static_cast<int8_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int8_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int8_t*>(current))));
        }
        break;
      case DATATYPE::INT16:
        if (sum_base) {
          if (AddAggInteger<int64_t, int16_t>((*(static_cast<int64_t*>(sum_base))),
                                              (*(static_cast<int16_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int16_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int16_t*>(current))));
        }
        break;
      case DATATYPE::INT32:
        if (sum_base) {
          if (AddAggInteger<int64_t, int32_t>((*(static_cast<int64_t*>(sum_base))),
                                              (*(static_cast<int32_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int32_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int32_t*>(current))));
        }
        break;
      case DATATYPE::INT64:
        if (sum_base) {
          if (AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_base))), (*(static_cast<int64_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int64_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int64_t*>(current))));
        }
        break;
      case DATATYPE::DOUBLE:
        AddAggFloat<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
        break;
      case DATATYPE::FLOAT:
        AddAggFloat<double, float>((*(static_cast<double*>(sum_base))), (*(static_cast<float*>(current))));
        break;
      default:
        break;
    }
  }
  *sum_res = sum_base ? sum_base : new_sum_base;
  return (new_sum_base != nullptr) || is_overflow_;
}

bool AggCalculator::CalAllAgg(void* min_base, void* max_base, void* sum_base, void* count_base,
                              bool block_first_line, const BlockSpan& span) {
  void* min = block_first_line ? nullptr : min_base;
  void* max = block_first_line ? nullptr : max_base;

  if (block_first_line && sum_base) {
    memset(sum_base, 0, size_);
  }

  bool is_overflow = false;
  bool hasDeleted = span.block_item->getDeletedCount() > 0;
  for (int i = 0; i < count_; ++i) {
    if (hasDeleted && isDeleted(span.block_item->rows_delete_flags, first_row_ + i)) {
      continue;
    }
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) += 1;
    }

    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!max || cmp(current, max, type_, size_) > 0) {
      max = current;
    }
    if (!min || cmp(current, min, type_, size_) < 0) {
      min = current;
    }
    if (isSumType(type_)) {
      // When a memory overflow has occurred, there is no need to calculate the sum result again
      if (is_overflow) {
        continue;
      }
      // sum
      switch (type_) {
        case DATATYPE::INT8:
          is_overflow = AddAggInteger<int8_t>((*(static_cast<int8_t*>(sum_base))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          is_overflow = AddAggInteger<int16_t>((*(static_cast<int16_t*>(sum_base))),
                                               (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          is_overflow = AddAggInteger<int32_t>((*(static_cast<int32_t*>(sum_base))),
                                               (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          is_overflow = AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_base))),
                                               (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          AddAggFloat<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          AddAggFloat<float>((*(static_cast<float*>(sum_base))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
          break;
        default:
          break;
      }
    }
  }

  if (min != nullptr && min != min_base) {
    memcpy(min_base, min, size_);
  }

  if (max != nullptr && max != max_base) {
    memcpy(max_base, max, size_);
  }
  return is_overflow;
}

int VarColAggCalculator::cmp(void* l, void* r) {
  uint16_t l_len = *(reinterpret_cast<uint16_t*>(l));
  uint16_t r_len = *(reinterpret_cast<uint16_t*>(r));
  uint16_t len = min(l_len, r_len);
  void* l_data = reinterpret_cast<void*>((intptr_t)(l) + sizeof(uint16_t));
  void* r_data = reinterpret_cast<void*>((intptr_t)(r) + sizeof(uint16_t));
  k_int32 ret = memcmp(l_data, r_data, len);
  return (ret == 0) ? (l_len - r_len) : ret;
}

bool VarColAggCalculator::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(bitmap_)[byte] & bit;
}

bool VarColAggCalculator::isDeleted(char* delete_flags, size_t row) {
  if (!delete_flags) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(delete_flags)[byte] & bit;
}

std::shared_ptr<void> VarColAggCalculator::GetMax(bool& base_changed, std::shared_ptr<void> base) {
  base_changed = true;
  void* max = nullptr;
  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* var_data = var_mem_[i].get();
    if (!max || cmp(var_data, max) > 0) {
      max = var_data;
    }
  }
  if (base && cmp(base.get(), max) > 0) {
    base_changed = false;
    max = base.get();
  }

  uint16_t len = *(reinterpret_cast<uint16_t*>(max));
  void* data = std::malloc(len + kStringLenLen);
  memcpy(data, max, len + kStringLenLen);
  std::shared_ptr<void> ptr(data, free);
  return ptr;
}

std::shared_ptr<void> VarColAggCalculator::GetMin(bool& base_changed, std::shared_ptr<void> base) {
  base_changed = true;
  void* min = nullptr;
  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* var_data = var_mem_[i].get();
    if (!min || cmp(var_data, min) < 0) {
      min = var_data;
    }
  }
  if (base && cmp(base.get(), min) < 0) {
    base_changed = false;
    min = base.get();
  }
  uint16_t len = *(reinterpret_cast<uint16_t*>(min));
  void* data = std::malloc(len + kStringLenLen);
  memcpy(data, min, len + kStringLenLen);
  std::shared_ptr<void> ptr(data, free);
  return ptr;
}

void VarColAggCalculator::CalAllAgg(void* min_base, void* max_base, std::shared_ptr<void> var_min_base,
                                    std::shared_ptr<void> var_max_base, void* count_base,
                                    bool block_first_line, const BlockSpan& span) {
  void* min = block_first_line ? nullptr : min_base;
  void* max = block_first_line ? nullptr : max_base;
  void* var_min = block_first_line ? nullptr : var_min_base.get();
  void* var_max = block_first_line ? nullptr : var_max_base.get();

  bool hasDeleted = span.block_item->getDeletedCount() > 0;
  for (int i = 0; i < count_; ++i) {
    if (hasDeleted && isDeleted(span.block_item->rows_delete_flags, first_row_ + i)) {
      continue;
    }
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) += 1;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    void* var_data = var_mem_[i].get();
    if (!max || cmp(var_data, var_max) > 0) {
      max = current;
      var_max = var_data;
    }
    if (!min || cmp(var_data, var_min) < 0) {
      min = current;
      var_min = var_data;
    }
  }

  if (min != nullptr && min != min_base) {
    memcpy(min_base, min, size_);
  }

  if (max != nullptr && max != max_base) {
    memcpy(max_base, max, size_);
  }
}

void AggCalculator::UndoAgg(void* min_base, void* max_base, void* sum_base, void* count_base) {
//  if (block_first_line && sum_base) {
//    memset(sum_base, 0, size_);
//  }

  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) -= 1;
    }

    if (isSumType(type_)) {
      void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
//      if (!max || (cmp(current, max) > 0)) {
//        max = current;
//      }
//      if (!min_base || (cmp(current, min) < 0)) {
//        min = current;
//      }

      // sum
      switch (type_) {
        case DATATYPE::INT8:
          SubAgg<int8_t>((*(static_cast<int8_t*>(sum_base))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          SubAgg<int16_t>((*(static_cast<int16_t*>(sum_base))), (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          SubAgg<int32_t>((*(static_cast<int32_t*>(sum_base))), (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          SubAgg<int64_t>((*(static_cast<int64_t*>(sum_base))), (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          SubAgg<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          SubAgg<float>((*(static_cast<float*>(sum_base))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
          break;
        default:
          break;
      }
    }
  }
}

}  //  namespace kwdbts
