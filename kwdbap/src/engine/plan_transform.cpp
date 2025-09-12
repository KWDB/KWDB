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
using namespace duckdb;

namespace kwdbap {

vector<unique_ptr<duckdb::Expression>> splitAndExpr(unique_ptr<duckdb::Expression> &expr) {
  vector<unique_ptr<duckdb::Expression>> exprs;
  if (expr->GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
    auto &and_expr = expr->Cast<BoundConjunctionExpression>();
    for (auto & child : and_expr.children) {
      auto child_exprs = splitAndExpr(child);
      exprs.insert(exprs.end(), std::make_move_iterator(child_exprs.begin()), std::make_move_iterator(child_exprs.end()));
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

PhyOpRef TransFormPlan::TransFormPhysicalPlan(const ProcessorSpec& procSpec, const PostProcessSpec& post,
                                              const ProcessorCoreUnion& core, PhyOpRefVec &child) {
  // New operator by type
  if (core.has_aptablereader()) {
    return TransFormTableScan(procSpec, post, core);
  } else if (core.has_aggregator()) {
    return TransFormAggregator(post, core, *child[0]);
  }
  EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                "Invalid operator type");
  throw std::runtime_error("Invalid operator type");
}

vector<unique_ptr<duckdb::Expression>> TransFormPlan::BuildAPExpr(
    const std::string& str, TableCatalogEntry& table,
    std::map<idx_t, idx_t>& col_map) {
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<Tokens>(
      str.data(), str.data() + str.size(), max_query_size);
  IParser::Pos pos(tokens_ptr, max_parser_depth);
  APParseQuery parser(str, pos);
  auto node_list = parser.APParseImpl();  // expr tree
  size_t i = 0;
  vector<unique_ptr<duckdb::Expression>> expressions;
  while (i < node_list.size()) {
    unique_ptr<duckdb::Expression> expr;
    auto construct_ret =
        parser.ConstructAPExpr(*context_, table, &i, &expr, col_map);
    if (construct_ret != SUCCESS) {
      expressions.clear();
      return expressions;
    }
    if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
      auto tmp_exprs = splitAndExpr(expr);
      expressions.insert(expressions.end(), std::make_move_iterator(tmp_exprs.begin()), std::make_move_iterator(tmp_exprs.end()));
    } else {
      expressions.push_back(std::move(expr));
    }
  }
  return expressions;
}

vector<column_t> TransFormPlan::GetColsFromRenderExpr(
    const std::string& str, TableCatalogEntry& table) {
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<Tokens>(
      str.data(), str.data() + str.size(), max_query_size);
  IParser::Pos pos(tokens_ptr, max_parser_depth);
  APParseQuery parser(str, pos);
  auto node_list = parser.APParseImpl();
  vector<column_t> column_ids;
  size_t i = 0;
  while (i < node_list.size()) {
    if (node_list[i]->operators == COLUMN_TYPE) {
      auto index = LogicalIndex(node_list[i].get()->value.number.column_id - 1);
      auto& col = table.GetColumn(index);
      column_ids.push_back(col.Oid());
    }
    i++;
  }
  return column_ids;
}

}  // namespace kwdbap
