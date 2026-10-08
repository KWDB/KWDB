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

#include <stack>
#include <string>
#include <utility>
#include <memory>
#include <vector>

#include "ee_binary_expr.h"
#include "ee_iparser.h"
#include "kwdb_type.h"

namespace kwdbts {
class ParseQuery : public IParser {
 public:
  using ElementPtr = std::shared_ptr<Element>;
  using Elements = std::vector<ElementPtr>;
  explicit ParseQuery(std::string sql, kwdbts::IParser::Pos pos)
      : raw_sql_(std::move(sql)), pos_(pos) {}
  k_bool ParseNumber(k_int64 factor);
  k_bool ParseSingleExpr();
  KStatus ConstructTree(std::size_t *i, ExprPtr *head_node);
  ExprPtr ParseImpl();
 private:
  std::string raw_sql_;
  kwdbts::IParser::Pos pos_;
  Elements node_list_;
};
}  // namespace kwdbts
