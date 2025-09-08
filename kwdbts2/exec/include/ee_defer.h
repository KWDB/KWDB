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

#include <utility>

#undef DISALLOW_COPY
#define DISALLOW_COPY(TypeName)       \
  TypeName(const TypeName&) = delete; \
  void operator=(const TypeName&) = delete

#undef DISALLOW_MOVE
#define DISALLOW_MOVE(TypeName)  \
  TypeName(TypeName&&) = delete; \
  void operator=(TypeName&&) = delete

#undef DISALLOW_COPY_AND_MOVE
#define DISALLOW_COPY_AND_MOVE(TypeName) \
  DISALLOW_COPY(TypeName);               \
  DISALLOW_MOVE(TypeName)

namespace kwdbts {

template <class DeferFunction>
class DeferOp {
 public:
  explicit DeferOp(DeferFunction func) : _func(std::move(func)) {}

  ~DeferOp() noexcept { (void)_func(); }

  DISALLOW_COPY_AND_MOVE(DeferOp);

 private:
  DeferFunction _func;
};
}  // namespace kwdbts
