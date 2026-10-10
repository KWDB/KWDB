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
// Created by wuchan on 2022/11/10.
#pragma once

#include <memory>
#include <list>

#include "ee_ast_element_type.h"
#include "ee_field_common.h"

namespace kwdbts {
class BinaryExpr {
 public:
  bool is_leaf{false};
  bool is_negative{false};
  AstEleType operator_type{OPENING_BRACKET};
  std::shared_ptr<VirtualField> reference_ptr{nullptr};
  // std::shared_ptr<Field> const_ptr;
  std::shared_ptr<Element> const_ptr{nullptr};
  std::shared_ptr<BinaryExpr> left{nullptr}, right{nullptr};
  std::list<std::shared_ptr<BinaryExpr>> args;  // Only for parameter lists in Func
  explicit BinaryExpr(k_bool is_leaf_)
      : is_leaf(is_leaf_), operator_type(OPENING_BRACKET) {}
  void SetNegative(k_bool negative_) {
    this->is_negative = negative_;
  }
  BinaryExpr() = default;
};
using ExprPtr = std::shared_ptr<BinaryExpr>;
}  // namespace kwdbts
