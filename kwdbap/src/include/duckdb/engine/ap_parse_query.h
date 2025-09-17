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

#include <utility>
#include <memory>
#include <map>
#include <string>
#include "cm_parse_expr.h"

using namespace duckdb;

namespace kwdbap {

struct ParseExprParam {
 public:
  ParseExprParam(ClientContext* context, std::map<idx_t, idx_t>& col_map, std::map<idx_t, LogicalType>& col_typ_map) :
  context_(context), col_map_(col_map), col_typ_map_(col_typ_map) {}
  ClientContext *context_;
  std::map<idx_t, idx_t>& col_map_;
  std::map<idx_t, LogicalType>& col_typ_map_;
};

class APParseQuery : public kwdb::ParseExpr {
 public:
  explicit APParseQuery(std::string sql, Pos pos)
      : kwdb::ParseExpr(std::move(sql), std::move(pos)) {}


  KStatus ConstructTree(std::size_t &i, void *head_node, void* user_data) override;
};

}  // namespace kwdbap
