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

#include "duckdb/engine/plan_transform.h"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

// using namespace kwdbts;

namespace kwdbap {

vector<unique_ptr<duckdb::Expression>> splitAndExpr(unique_ptr<duckdb::Expression> &expr) {
  vector<unique_ptr<duckdb::Expression>> exprs;
  if (expr->GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
    auto &and_expr = expr->Cast<BoundConjunctionExpression>();
    for (auto & child : and_expr.children) {
      auto child_exprs = splitAndExpr(child);
      exprs.insert(exprs.end(), std::make_move_iterator(child_exprs.begin()),
                   std::make_move_iterator(child_exprs.end()));
    }
  } else {
    exprs.push_back(std::move(expr));
  }
  return exprs;
}

TransFormPlan::TransFormPlan(duckdb::ClientContext& context, duckdb::PhysicalPlan* plan, std::string& db_path) {
  context_ = &context;
  db_path_ = db_path;
  physical_plan_ = plan;
}

PhyOpRef TransFormPlan::TransFormPhysicalPlan(const TSProcessorSpec& procSpec, const PostProcessSpec& post,
                                              const TSProcessorCoreUnion& core, PhyOpRefVec &child) {
  // New operator by type
  if (core.has_aptablereader()) {
    printf("core.has_aptablereader() \n");
    return TransFormTableScan(procSpec, post, core);
  } else if (core.has_aggregator()) {
    printf("core.has_aggregator() \n");
    return TransFormAggregator(post, core, *child[0]);
  } else if (core.has_hashjoiner()) {
    printf("core.has_hashjoiner() \n");
    return TransFormHashJoin(post, core, *child[0], *child[1]);
  } else if (core.has_distinct()) {
    printf("core.has_distinct() \n");
    return TransFormTableScan(procSpec, post, core);
  } else if (core.has_window()) {
    printf("core.has_window() \n");
    return TransFormTableScan(procSpec, post, core);
  }
  EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,"Invalid operator type");
  throw std::runtime_error("Invalid operator type");
}

vector<unique_ptr<duckdb::Expression>> TransFormPlan::BuildAPExpr(
    const std::string& str, ParseExprParam& param) {
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<kwdb::Tokens>(
      str.data(), str.data() + str.size(), max_query_size);
  IParser::Pos pos(tokens_ptr, max_parser_depth);
  APParseQuery parser(str, pos);
  vector<unique_ptr<duckdb::Expression>> expressions;
  if (!parser.ParseNode()) {
    return expressions;
  }
  size_t i = 0;
  auto node_list = parser.GetNodeList();
  while (i < node_list.size()) {
    unique_ptr<duckdb::Expression> expr;
    if (parser.ConstructTree(i, &expr, reinterpret_cast<void*>(&param)) != SUCCESS) {
      expressions.clear();
      return expressions;
    }
    if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
      auto tmp_exprs = splitAndExpr(expr);
      expressions.insert(expressions.end(), std::make_move_iterator(tmp_exprs.begin()),
                         std::make_move_iterator(tmp_exprs.end()));
    } else {
      expressions.push_back(std::move(expr));
    }
  }
  return expressions;
}

vector<column_t> TransFormPlan::GetColsFromRenderExpr(const std::string& str) {
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<kwdb::Tokens>(
      str.data(), str.data() + str.size(), max_query_size);
  IParser::Pos pos(tokens_ptr, max_parser_depth);
  APParseQuery parser(str, pos);
  vector<column_t> column_ids;
  if (!parser.ParseNode()) {
    return column_ids;
  }
  auto node_list = parser.GetNodeList();
  size_t i = 0;
  while (i < node_list.size()) {
    if (node_list[i]->operators == COLUMN_TYPE) {
      column_ids.push_back(node_list[i].get()->value.number.column_id - 1);
    }
    i++;
  }
  return column_ids;
}

}  // namespace kwdbap
