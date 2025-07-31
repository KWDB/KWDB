
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights
// reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.
//
//
#pragma once

#include <algorithm>
#include <type_traits>

namespace kwdbts {

template <typename T, typename Enable = void>
struct SorterComparator {
  static int compare(const T& lhs, const T& rhs) {
    return (lhs == rhs) ? 0 : ((lhs < rhs) ? -1 : 1);
  }
};

// 浮点类型特化
template <typename T>
struct SorterComparator<T, std::enable_if_t<std::is_floating_point_v<T>>> {
  static int compare(T lhs, T rhs) {
    if (std::isnan(lhs)) lhs = 0;
    if (std::isnan(rhs)) rhs = 0;
    return (lhs == rhs) ? 0 : ((lhs < rhs) ? -1 : 1);
  }
};
}  // namespace kwdbts
