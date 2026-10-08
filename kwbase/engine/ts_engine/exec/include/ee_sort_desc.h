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
struct SortingRule {
  k_int32 order_direction;
  k_int32 null_first;
  k_int32 sort_priority;

  SortingRule() = default;
  SortingRule(k_int32 direction, k_int32 null_direction)
      : order_direction(direction), null_first(null_direction), sort_priority(0) {
  }
  SortingRule(k_bool is_ascending, k_bool inull_first) {
    order_direction = is_ascending ? 1 : -1;
    null_first = (inull_first ? -1 : 1) * order_direction;
    sort_priority = 0;
  }
  // compare values
  template <typename T>
  int CompareValues(const T& a, const T& b) const {
    if (a < b) return -order_direction;
    if (a > b) return order_direction;
    return 0;
  }

  // null direction: -1 if null first, 1 if null last
  k_int32 NullDirection() const {
    return null_first * order_direction;
  }

  k_bool IsAscending() const {
    return order_direction == 1;
  }
  // set priority
  void SetPriority(k_int32 priority) {
    sort_priority = priority;
  }

  // get priority
  k_int32 GetPriority() const {
    return sort_priority;
  }
  k_bool IsNullFirst() const {
    return (null_first * order_direction) == -1;
  }
};
struct SortingRules {
  std::vector<SortingRule> rules;

  SortingRules() = default;
  ~SortingRules() = default;

  // sort rules
  SortingRules(const std::vector<k_bool>& orders,
            const std::vector<k_bool>& null_firsts) {
    rules.resize(orders.size());
    for (size_t i = 0; i < orders.size(); ++i) {
      rules[i] = SortingRule(orders.at(i), null_firsts.at(i));
    }
  }

  // sort rules
  SortingRules(const std::vector<k_int32>& orders,
            const std::vector<k_int32>& nulls) {
    rules.reserve(orders.size());
    for (k_int32 i = 0; i < orders.size(); i++) {
      rules.emplace_back(orders[i], nulls[i]);
    }
  }

  SortingRule GetColumnRule(k_int32 col) const {
    return rules[col];
  }
  // Check if rules are empty
  bool IsEmpty() const {
    return rules.empty();
  }
  // null fisrt
  static SortingRules AscNullFirst(k_int32 cols) {
    SortingRules res;
    for (k_int32 i = 0; i < cols; i++) {
      res.rules.emplace_back(1, -1);
    }
    return res;
  }
  // Create rules for ascending order with nulls last
  static SortingRules AscNullLast(k_int32 cols) {
    SortingRules res;
    for (k_int32 i = 0; i < cols; i++) {
      res.rules.emplace_back(1, 1);
    }
    return res;
  }
  size_t ColumnsNum() const {
    return rules.size();
  }
  // Add a new sorting rule
  void AddRule(const SortingRule& rule) {
    rules.push_back(rule);
  }

  // Add a new rule with individual parameters
  void AddRule(k_bool order, k_bool null_first) {
    rules.emplace_back(order, null_first);
  }
};
}  // namespace kwdbts
