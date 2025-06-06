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

#include <string>
#include <vector>
#include "data_type.h"
#include "ts_common.h"
#include "ts_bitmap.h"

namespace kwdbts {
class AggCalculatorV2 {
 public:
  AggCalculatorV2(void* mem, TsBitmap* bitmap, DATATYPE type, int32_t size, int32_t count) :
      mem_(mem), bitmap_(bitmap), type_(type), size_(size), count_(count) {
    if (type_ == DATATYPE::TIMESTAMP64_LSN) {
      size_ = 8;
    }
    if (is_overflow_) {
      sum_type_ = (DATATYPE)DOUBLE;
      sum_size_ = sizeof(double);
    } else {
      sum_type_ = type_;
      sum_size_ = getSumSize(type_);
    }
  }

  AggCalculatorV2(void* pre_agg, DATATYPE type, int32_t size, int32_t count) :
      pre_agg_(pre_agg), type_(type), size_(size), count_(count) {
    if (type_ == DATATYPE::TIMESTAMP64_LSN) {
      size_ = 8;
    }
    if (is_overflow_) {
      sum_type_ = (DATATYPE)DOUBLE;
      sum_size_ = sizeof(double);
    } else {
      sum_type_ = type_;
      sum_size_ = getSumSize(type_);
    }
  }

  bool CalcAggForFlush(uint16_t& count, void* max_addr, void* min_addr, void* sum_addr);
  KStatus MergeAggResultFromBlock(TSSlice& agg_data, Sumfunctype agg_type, uint32_t col_idx, bool& is_overflow);

  KStatus MergeAggResultFromPreAgg(TSSlice& agg_data, Sumfunctype agg_type, bool& is_overflow);

 private:
  int cmp(void* l, void* r);
  void InitSumValue(void* ptr);
  void InitAggData(TSSlice& agg_data);
  bool isnull(size_t row);

 private:
  void* mem_;
  void* pre_agg_;
  TsBitmap* bitmap_ = nullptr;
  DATATYPE type_;
  int32_t size_;
  uint16_t count_;
  bool is_overflow_ = false;
  DATATYPE sum_type_;
  int32_t sum_size_;
};

class VarColAggCalculatorV2 {
 public:
  explicit VarColAggCalculatorV2(const std::vector<string>& var_rows) : var_rows_(var_rows) {
  }

  VarColAggCalculatorV2(void* pre_agg, DATATYPE type, int32_t count) :
      pre_agg_(pre_agg), type_(type), count_(count) {  }

  void CalcAggForFlush(string& max, string& min, uint64_t& count);
  void MergeAggResultFromBlock(TSSlice& agg_data, Sumfunctype agg_type);
  KStatus MergeAggResultFromPreAgg(TSSlice& agg_data, Sumfunctype agg_type);

 private:
  std::vector<string> var_rows_;
  DATATYPE type_;
  void* pre_agg_;
  uint16_t count_;
};



}  // namespace kwdbts
