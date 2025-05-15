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

  bool CalcAllAgg(uint16_t& count, void* max_addr, void* min_addr, void* sum_addr);
  bool MergeAggResultFromBlock(uint64_t& count, void* max_addr, void* min_addr, void* sum_addr);

 private:
  int cmp(void* l, void* r);

  bool isnull(size_t row);

 private:
  void* mem_;
  TsBitmap* bitmap_ = nullptr;
  // size_t first_row_;
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
    count_ = var_rows.size();
  }

  VarColAggCalculatorV2(const std::vector<string>& var_mem, unsigned char* bitmap,
                        int32_t size, int32_t count) :
                        var_rows_(var_mem), bitmap_(bitmap), size_(size), count_(count) {
  }

  void CalcAllAgg(string& max, string& min, uint16_t& count);
  bool isnull(size_t row);

 private:
  std::vector<string> var_rows_;
  void* bitmap_ = nullptr;
  int32_t size_;
  uint16_t count_;
};



}  // namespace kwdbts
