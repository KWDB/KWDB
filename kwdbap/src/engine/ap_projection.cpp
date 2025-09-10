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

#include "cm_func.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/extra_operator_info.hpp"  // 包含 ExtraOperatorInfo 定义
#include "duckdb/engine/ap_parse_query.h"
#include "duckdb/engine/duckdb_exec.h"
#include "duckdb/engine/plan_transform.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"  // 确保包含此类定义
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "ee_comm_def.h"
#include "ee_encoding.h"
#include "ee_iparser.h"
#include "ee_string_info.h"
#include "kwdb_type.h"
#include "lg_api.h"

using namespace duckdb;

namespace kwdbap {

PhyPlanPtr TransFormPlan::AddAPProjection(PhyPlanPtr plan,
                                          const kwdbts::PostProcessSpec &post,
                                          TableCatalogEntry &table,
                                          std::map<idx_t, idx_t> &col_map) {
  // build result projection
  vector<LogicalType> proj_types;
  vector<unique_ptr<duckdb::Expression>> proj_exprs;
  if (post.projection() && post.output_columns_size() > 0) {
    vector<string> proj_names;
    vector<ColumnIndex> proj_column_ids;
    for (auto &out_col : post.output_columns()) {
      auto index = LogicalIndex(out_col);
      auto &col = table.GetColumn(index);
      proj_types.push_back(col.Type());
      proj_names.push_back(col.Name());
      proj_column_ids.emplace_back(col.Oid());
    }
    proj_exprs.reserve(proj_column_ids.size());
    for (idx_t col_idx = 0; col_idx < proj_column_ids.size(); col_idx++) {
      auto proj_idx = col_map[proj_column_ids[col_idx].GetPrimaryIndex()];
      proj_exprs.emplace_back(make_uniq<BoundReferenceExpression>(
          proj_names[col_idx], proj_types[col_idx], proj_idx));
    }
  } else if (post.render_exprs_size() > 0) {
    for (auto &render_expr : post.render_exprs()) {
      auto proj_expr = render_expr.expr();
      auto expressions = BuildAPExpr(proj_expr, table, col_map);
      if (expressions.empty()) {
        return nullptr;
      }
      auto &res_expr = expressions.front();
      proj_types.push_back(res_expr->return_type);
      proj_exprs.push_back(std::move(expressions.front()));
    }
  }

  auto &res_proj =
      plan->Make<PhysicalProjection>(proj_types, std::move(proj_exprs), 0);
  res_proj.children.push_back(plan->Root());
  plan->SetRoot(res_proj);
  return plan;
}
}  // namespace kwdbap