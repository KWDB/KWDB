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

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/engine/ap_parse_query.h"
#include "duckdb/engine/plan_transform.h"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace kwdbap {
  PhyOpRef TransFormPlan::AddAPFilters(PhyOpRef physicalOp, const kwdbts::PostProcessSpec &post,
                                       TableCatalogEntry &table, IdxMap &col_map) {
  vector<LogicalType> proj_types;
  vector<string> proj_names;
  vector<ColumnIndex> proj_column_ids;
  auto &scan = physicalOp.Cast<PhysicalTableScan>();
  if (!post.projection()) {
    if (post.render_exprs_size() <= 0) {
      proj_types = scan.returned_types;
      proj_names = scan.names;
      proj_column_ids = scan.column_ids;
    } else {
      std::unordered_set<column_t> expr_column_ids;
      auto proj_exprs = post.render_exprs();
      for (auto &proj_expr : proj_exprs) {
        auto tmp_cols = GetColsFromRenderExpr(proj_expr.expr(), table);
        expr_column_ids.insert(tmp_cols.begin(), tmp_cols.end());
      }
      for (size_t i = 0; i < scan.column_ids.size(); ++i) {
        auto col_id = scan.column_ids[i];
        if (expr_column_ids.find(col_id.GetPrimaryIndex()) ==
            expr_column_ids.end()) {
          continue;
        }
        proj_types.push_back(scan.types[i]);
        proj_names.push_back(scan.names[i]);
        proj_column_ids.push_back(col_id);
      }
    }
  } else {
    for (auto &out_col : post.output_columns()) {
      auto index = LogicalIndex(out_col);
      auto &col = table.GetColumn(index);
      proj_types.push_back(col.Type());
      proj_names.push_back(col.Name());
      proj_column_ids.emplace_back(col.Oid());
    }
  }

  auto filter_expr = post.filter().expr();
  reference<PhysicalOperator> plan = VerifyProjectionByTableScan(scan, col_map);

  printf("expr: %s", filter_expr.c_str());
  auto expressions = BuildAPExpr(filter_expr, table, col_map);
  if (expressions.empty()) {
    return physicalOp;
  }
  // auto filter_push_down = true;
  // auto combiner = FilterCombiner(*context_);
  // for (auto & expr : expressions) {
  //   if (combiner.AddFilter(std::move(expr)) == FilterResult::UNSATISFIABLE) {
  //     filter_push_down = false;
  //   }
  // }
  // auto &proj = projection.Cast<PhysicalProjection>();
  auto &filter = physical_plan_->Make<PhysicalFilter>(plan.get().types, std::move(expressions),
      plan.get().estimated_cardinality);
  filter.children.push_back(plan.get());
  // build result projection
  vector<unique_ptr<duckdb::Expression>> proj_exprs;
  proj_exprs.reserve(proj_column_ids.size());
  for (idx_t col_idx = 0; col_idx < proj_column_ids.size(); col_idx++) {
    auto proj_idx = col_map[proj_column_ids[col_idx].GetPrimaryIndex()];
    proj_exprs.emplace_back(make_uniq<BoundReferenceExpression>(
        proj_names[col_idx], proj_types[col_idx], proj_idx));
    col_map[proj_column_ids[col_idx].GetPrimaryIndex()] = col_idx;
  }

  auto &res_proj = physical_plan_->Make<PhysicalProjection>(
      proj_types, std::move(proj_exprs), 0);
  res_proj.children.push_back(filter);
  return res_proj;
}

PhyOpRef TransFormPlan::VerifyProjectionByTableScan(PhyOpRef plan, IdxMap &col_map) {
  auto &scan = plan.Cast<PhysicalTableScan>();

  // build child_proj
  const auto child_types = scan.types;
  const auto column_ids = scan.column_ids;
  const auto column_names = scan.names;
  const auto column_count = column_ids.size();

  // If our child has columns [i, j], we will generate a projection like so
  // [NULL, j, NULL, i, NULL]
  const auto projection_column_count = column_count * 2 + 1;
  vector<unique_ptr<duckdb::Expression>> expressions;
  expressions.reserve(projection_column_count);

  vector<LogicalType> proj_types;
  proj_types.reserve(projection_column_count);
  // First fill with all NULLs
  for (idx_t col_idx = 0; col_idx < projection_column_count; col_idx++) {
    expressions.emplace_back(
        make_uniq<BoundConstantExpression>(Value(LogicalType::UTINYINT)));
    proj_types.emplace_back(LogicalType::UTINYINT);
  }

  for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
    const auto new_col_idx = projection_column_count - 2 - col_idx * 2;
    expressions[new_col_idx] = make_uniq<BoundReferenceExpression>(
        column_names[col_idx], child_types[col_idx], col_idx);
    proj_types[new_col_idx] = child_types[col_idx];
    col_map[column_ids[col_idx].GetPrimaryIndex()] = new_col_idx;
  }

  auto &child_proj = physical_plan_->Make<PhysicalProjection>(
      proj_types, std::move(expressions), scan.estimated_cardinality);
  child_proj.children.push_back(scan);

  // build result_proj
  auto &temp_proj = child_proj.Cast<PhysicalProjection>();
  const auto child_proj_types = temp_proj.types;
  const auto column_proj_count = temp_proj.select_list.size();

  // If our child has columns [i, j], we will generate a projection like so
  // [NULL, j, NULL, i, NULL]
  const auto result_proj_column_count = column_proj_count * 2 + 1;
  vector<unique_ptr<duckdb::Expression>> result_expressions;
  result_expressions.reserve(result_proj_column_count);

  vector<LogicalType> result_proj_types;
  result_proj_types.reserve(result_proj_column_count);
  // First fill with all NULLs
  for (idx_t col_idx = 0; col_idx < result_proj_column_count; col_idx++) {
    result_expressions.emplace_back(
        make_uniq<BoundConstantExpression>(Value(LogicalType::UTINYINT)));
    result_proj_types.emplace_back(LogicalType::UTINYINT);
  }

  for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
    auto proj_idx = col_map[column_ids[col_idx].GetPrimaryIndex()];
    const auto new_col_idx = result_proj_column_count - 2 - proj_idx * 2;
    result_expressions[new_col_idx] = make_uniq<BoundReferenceExpression>(
        column_names[col_idx], child_types[col_idx], proj_idx);
    result_proj_types[new_col_idx] = child_types[col_idx];
    col_map[column_ids[col_idx].GetPrimaryIndex()] = new_col_idx;
  }

  auto &result_proj = physical_plan_->Make<PhysicalProjection>(
      result_proj_types, std::move(result_expressions),
      scan.estimated_cardinality);
  result_proj.children.push_back(child_proj);
  return result_proj;
}

}  // namespace kwdbap
