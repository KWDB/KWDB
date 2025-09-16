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

#include "duckdb/engine/ap_parse_query.h"
#include "duckdb/engine/plan_transform.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;

namespace kwdbap {
PhyOpRef TransFormPlan::AddAPProjection(PhyOpRef plan,
                                        const kwdbts::PostProcessSpec &post,
                                        ParseExprParam &param) {
  // build result projection
  vector<LogicalType> proj_types;
  vector<unique_ptr<duckdb::Expression>> proj_exprs;
  if (post.has_projection() && !post.projection() &&
      post.render_exprs_size() > 0) {
    for (auto &render_expr : post.render_exprs()) {
      const auto &proj_expr = render_expr.expr();
      auto expressions = BuildAPExpr(proj_expr, param);
      if (expressions.empty()) {
        return plan;
      }
      auto &res_expr = expressions.front();
      proj_types.push_back(res_expr->return_type);
      proj_exprs.push_back(std::move(expressions.front()));
    }
  } else if (post.output_columns_size() > 0) {
    vector<string> proj_names;
    vector<ColumnIndex> proj_column_ids;
    for (auto &out_col : post.output_columns()) {
      proj_types.push_back(param.col_typ_map_[out_col]);
      proj_column_ids.emplace_back(out_col);
    }
    proj_exprs.reserve(proj_column_ids.size());
    for (idx_t col_idx = 0; col_idx < proj_column_ids.size(); col_idx++) {
      auto proj_idx =
          param.col_map_[proj_column_ids[col_idx].GetPrimaryIndex()];
      proj_exprs.emplace_back(make_uniq<BoundReferenceExpression>(
          proj_names[col_idx], proj_types[col_idx], proj_idx));
    }
  }

  auto &res_proj = physical_plan_->Make<PhysicalProjection>(
      proj_types, std::move(proj_exprs), 0);
  res_proj.children.push_back(plan);
  return res_proj;
}

}  // namespace kwdbap
