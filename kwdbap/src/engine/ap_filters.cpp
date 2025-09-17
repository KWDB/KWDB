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
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace kwdbts;

namespace kwdbap {
PhyOpRef TransFormPlan::AddAPFilters(
    PhyOpRef physicalOp, const PostProcessSpec &post,
    ParseExprParam &param, std::unordered_set<idx_t> &scan_filter_idx) {
  duckdb::vector<LogicalType> proj_types;
  duckdb::vector<ColumnIndex> proj_column_ids;
  auto &scan = physicalOp.Cast<PhysicalTableScan>();
  if (!post.projection()) {
    if (post.render_exprs_size() <= 0) {
      proj_types = scan.returned_types;
      proj_column_ids = scan.column_ids;
    } else {
      std::unordered_set<column_t> expr_column_ids;
      auto proj_exprs = post.render_exprs();
      for (auto &proj_expr : proj_exprs) {
        auto tmp_cols = GetColsFromRenderExpr(proj_expr.expr());
        expr_column_ids.insert(tmp_cols.begin(), tmp_cols.end());
      }
      for (size_t i = 0; i < scan.column_ids.size(); ++i) {
        auto col_id = scan.column_ids[i];
        if (expr_column_ids.find(col_id.GetPrimaryIndex()) ==
            expr_column_ids.end()) {
          continue;
        }
        proj_types.push_back(scan.types[i]);
        proj_column_ids.push_back(col_id);
      }
    }
  } else {
    for (auto &out_col : post.output_columns()) {
      proj_types.push_back(param.col_typ_map_[out_col]);
      proj_column_ids.emplace_back(out_col);
    }
  }

  auto filter_expr = post.filter().expr();
  reference<PhysicalOperator> plan =
      VerifyProjectionByTableScan(scan, param.col_map_);

  //printf("expr: %s \n", filter_expr.c_str());
  auto expressions = BuildAPExpr(filter_expr, param);
  // if scan_filter_idx is not empty,
  // we need to remove the filters that have been pushed down to the scan from the expressions.
  for (size_t i = 0; i < expressions.size(); ++i) {
    auto key = scan_filter_idx.find(i);
    if (key != scan_filter_idx.end()) {
      expressions.erase_at(i--);
      scan_filter_idx.erase(key);
    }
  }
  D_ASSERT(!expressions.empty());

  // auto &proj = projection.Cast<PhysicalProjection>();
  auto &filter = physical_plan_->Make<PhysicalFilter>(
      plan.get().types, std::move(expressions),
      plan.get().estimated_cardinality);
  filter.children.push_back(plan.get());
  // build result projection
  vector<unique_ptr<duckdb::Expression>> proj_exprs;
  proj_exprs.reserve(proj_column_ids.size());
  for (idx_t col_idx = 0; col_idx < proj_column_ids.size(); col_idx++) {
    auto proj_idx = param.col_map_[proj_column_ids[col_idx].GetPrimaryIndex()];
    proj_exprs.emplace_back(make_uniq<BoundReferenceExpression>(
        "", proj_types[col_idx], proj_idx));
    param.col_map_[proj_column_ids[col_idx].GetPrimaryIndex()] = col_idx;
  }

  auto &res_proj = physical_plan_->Make<PhysicalProjection>(
      proj_types, std::move(proj_exprs), 0);
  res_proj.children.push_back(filter);
  return res_proj;
}

static bool supportedFilterComparison(ExpressionType expression_type) {
  switch (expression_type) {
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_NOTEQUAL:
      return true;
    default:
      return false;
  }
}

bool typeSupportsConstantFilter(const LogicalType &type) {
  if (TypeIsNumeric(type.InternalType())) {
    return true;
  }
  if (type.InternalType() == PhysicalType::VARCHAR ||
      type.InternalType() == PhysicalType::BOOL) {
    return true;
  }
  return false;
}

bool supportedColumn(const vector<ColumnIndex> &column_ids,
                     unique_ptr<duckdb::Expression> col_ptr,
                     ColumnIndex &result) {
  if (col_ptr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
    auto &cast_expr = col_ptr->Cast<BoundCastExpression>();
    return supportedColumn(column_ids, std::move(cast_expr.child), result);
  }
  if (col_ptr->GetExpressionType() != ExpressionType::BOUND_REF) {
    return false;
  }
  auto &col_expr = col_ptr->Cast<BoundReferenceExpression>();
  for (auto &col_id : column_ids) {
    if (col_expr.index == col_id.GetPrimaryIndex()) {
      result = col_id;
      return true;
    }
  }
  return false;
}

KStatus tryPushDownFilter(unique_ptr<TableFilterSet> *table_filters,
                          unique_ptr<duckdb::Expression> &expr,
                          const vector<ColumnIndex> &column_ids) {
  switch (expr->GetExpressionClass()) {
    case ExpressionClass::BOUND_COMPARISON: {
      auto &tmp_expr = expr->Cast<BoundComparisonExpression>();
      unique_ptr<duckdb::Expression> col_ptr;
      unique_ptr<duckdb::Expression> value_ptr;
      if (tmp_expr.left->GetExpressionClass() ==
          ExpressionClass::BOUND_CONSTANT) {
        value_ptr = std::move(tmp_expr.left);
        col_ptr = std::move(tmp_expr.right);
      } else {
        value_ptr = std::move(tmp_expr.right);
        col_ptr = std::move(tmp_expr.left);
      }
      auto &value_expr = value_ptr->Cast<BoundConstantExpression>();
      if (value_expr.value.IsNull()) {
        // no constants - already removed
        return FAIL;
      }
      // check if the type is supported
      if (!typeSupportsConstantFilter(value_expr.value.type())) {
        // not supported
        return FAIL;
      }
      if (!supportedFilterComparison(tmp_expr.GetExpressionType())) {
        return FAIL;
      }
      //! Here we check if these filters are column references
      ColumnIndex column_index;
      if (!supportedColumn(column_ids, std::move(col_ptr), column_index)) {
        return FAIL;
      }
      auto constant_filter = make_uniq<ConstantFilter>(
          tmp_expr.GetExpressionType(), value_expr.value);
      if (nullptr == table_filters->get()) {
        *table_filters = make_uniq<TableFilterSet>();
      }
      table_filters->get()->PushFilter(column_index,
                                       std::move(constant_filter));
      break;
    }
    case ExpressionClass::BOUND_BETWEEN: {
      auto &tmp_expr = expr->Cast<BoundBetweenExpression>();
      //! Here we check if these filters are column references
      ColumnIndex column_index;
      if (!supportedColumn(column_ids, std::move(tmp_expr.input),
                           column_index)) {
        return FAIL;
      }
      auto &lower_expr = tmp_expr.lower->Cast<BoundConstantExpression>();
      if (lower_expr.value.IsNull()) {
        // no constants - already removed
        return FAIL;
      }
      // check if the type is supported
      if (!typeSupportsConstantFilter(lower_expr.value.type())) {
        // not supported
        return FAIL;
      }
      unique_ptr<ConstantFilter> lower_constant;
      if (tmp_expr.lower_inclusive) {
        lower_constant = make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, lower_expr.value);
      } else {
        lower_constant = make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHAN, lower_expr.value);
      }
      auto &upper_expr = tmp_expr.upper->Cast<BoundConstantExpression>();
      if (upper_expr.value.IsNull()) {
        // no constants - already removed
        return FAIL;
      }
      // check if the type is supported
      if (!typeSupportsConstantFilter(upper_expr.value.type())) {
        // not supported
        return FAIL;
      }
      unique_ptr<ConstantFilter> upper_constant;
      if (tmp_expr.upper_inclusive) {
        upper_constant = make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_LESSTHANOREQUALTO, upper_expr.value);
      } else {
        upper_constant = make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_LESSTHAN, upper_expr.value);
      }
      if (nullptr == table_filters->get()) {
        *table_filters = make_uniq<TableFilterSet>();
      }
      table_filters->get()->PushFilter(column_index, std::move(lower_constant));
      table_filters->get()->PushFilter(column_index, std::move(upper_constant));

      break;
    }
    default:
      return FAIL;
  }
  return SUCCESS;
}

unique_ptr<TableFilterSet> TransFormPlan::CreateTableFilters(
    const vector<ColumnIndex> &column_ids, const PostProcessSpec &post,
    ParseExprParam &param, std::unordered_set<idx_t> &scan_filter_idx,
    bool &all_filter_push_scan) {
  auto filter_expr = post.filter().expr();
  //printf("expr: %s", filter_expr.c_str());
  auto expressions = BuildAPExpr(filter_expr, param);
  if (expressions.empty()) {
    return nullptr;
  }
  auto expr_size = expressions.size();
  unique_ptr<TableFilterSet> table_filters;
  idx_t idx = 0;
  for (auto &expr : expressions) {
    if (tryPushDownFilter(&table_filters, expr, column_ids) != SUCCESS) {
      continue;
    }
    scan_filter_idx.insert(idx);
    idx++;
  }
  if (scan_filter_idx.size() == expr_size) {
    all_filter_push_scan = true;
  }

  return table_filters;
}

PhyOpRef TransFormPlan::VerifyProjectionByTableScan(PhyOpRef plan,
                                                    IdxMap &col_map) {
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
