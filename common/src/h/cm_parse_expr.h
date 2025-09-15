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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ee_ast_element_type.h"
#include "ee_iparser.h"
#include <functional>

using kwdbts::KStatus;
using kwdbts::k_bool;
using kwdbts::k_int64;

namespace kwdb {
class ParseExpr : public IParser {
 public:
  using ElementPtr = std::shared_ptr<Element>;
  using Elements = std::vector<ElementPtr>;
  explicit ParseExpr(std::string sql, IParser::Pos pos)
        : raw_sql_(std::move(sql)), pos_(pos) {}
  bool ParseImpl(void *expr, void* user_data);
  bool ParseNode();
  bool ConstructExpr(void *expr, void *user_data);
  Elements &GetNodeList() { return node_list_; };
  
 protected:
  k_bool ParseNumber(k_int64 factor);
  k_bool ParseSingleExpr();
  virtual KStatus ConstructTree(std::size_t &i, void *head_node, void* user_data) = 0;
  
  std::string raw_sql_;
  IParser::Pos pos_;
  Elements node_list_;
};
}  // namespace kwdb
