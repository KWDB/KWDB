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
#include <atomic>
#include <memory>
#include <vector>

#include "kwdb_type.h"

namespace kwdbts {
struct SortDesc {
  k_int32 sort_order;
  k_int32 null_first;

  SortDesc() = default;
  SortDesc(k_bool is_asc, k_bool inull_first) {
    sort_order = is_asc ? 1 : -1;
    null_first = (inull_first ? -1 : 1) * sort_order;
  }
  SortDesc(k_int32 order, k_int32 null) : sort_order(order), null_first(null) {}

  // Discard sort_order effect on the null_first
  k_int32 NanDirection() const { return null_first * sort_order; }
  k_bool IsNullFirst() const { return (null_first * sort_order) == -1; }
  k_bool IsAsc() const { return sort_order == 1; }
};
struct SortDescs {
  std::vector<SortDesc> descs;

  SortDescs() = default;
  ~SortDescs() = default;

  SortDescs(const std::vector<k_bool>& orders,
            const std::vector<k_bool>& null_firsts) {
    descs.resize(orders.size());
    for (size_t i = 0; i < orders.size(); ++i) {
      descs[i] = SortDesc(orders.at(i), null_firsts.at(i));
    }
  }

  SortDescs(const std::vector<k_int32>& orders,
            const std::vector<k_int32>& nulls) {
    descs.reserve(orders.size());
    for (k_int32 i = 0; i < orders.size(); i++) {
      descs.emplace_back(orders[i], nulls[i]);
    }
  }

  // Create a default desc with asc order and null_first
  static SortDescs asc_null_first(k_int32 columns) {
    SortDescs res;
    for (k_int32 i = 0; i < columns; i++) {
      res.descs.emplace_back(1, -1);
    }
    return res;
  }

  size_t ColumnsNum() const { return descs.size(); }

  SortDesc GetColumnDesc(k_int32 col) const { return descs[col]; }
};
}  // namespace kwdbts
