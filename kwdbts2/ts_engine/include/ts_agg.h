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
  AggCalculatorV2(char* mem, const TsBitmapBase* bitmap, DATATYPE type, int32_t size, int32_t count) :
      mem_(mem), bitmap_(bitmap), type_(type), size_(size), count_(count) {
  }

  KStatus CalcAggForFlush(bool is_not_null, bool& is_overflow, uint16_t& count,
                          void* max_addr, void* min_addr, void* sum_addr);

 private:
  [[nodiscard]] bool isnull(size_t row) const;

 private:
  char* mem_;
  const TsBitmapBase* bitmap_ = nullptr;
  DATATYPE type_;
  int32_t size_;
  int32_t count_;
};

class VarColAggCalculatorV2 {
 public:
  explicit VarColAggCalculatorV2(const std::vector<string>& var_rows) : var_rows_(var_rows) {
  }

  void CalcAggForFlush(string& max, string& min, uint64_t& count);

 private:
  const std::vector<string>& var_rows_;
};



}  // namespace kwdbts
