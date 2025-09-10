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

#include "duckdb/engine/plan_transform.h"
#include "duckdb/engine/ap_parse_query.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/extra_operator_info.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace kwdbap {

unique_ptr<PhysicalPlan> TransFormPlan::TransFormTableScan(const kwdbts::ProcessorSpec& procSpec,
                                                           const kwdbts::PostProcessSpec& post,
                                                           const kwdbts::ProcessorCoreUnion& core) {
  auto physical_plan = make_uniq<PhysicalPlan>(Allocator::Get(*context_));
  try {
    if (core.has_aptablereader()) {
      const auto &apReader = core.aptablereader();
      if (post.output_columns_size() > 0 && post.output_columns_size() != post.output_types_size()) {
        return nullptr;
      }
      std::string db_name;
      if (apReader.has_db_name() && !apReader.db_name().empty()) {
        db_name = apReader.db_name();
      }
      auto table_name = "";
      if (apReader.has_table_name() && !apReader.table_name().empty()) {
        table_name = apReader.table_name().c_str();
      } else {
        return nullptr;
      }
      auto schema_name = "";
      if (apReader.has_schema_name() && !apReader.schema_name().empty()) {
        schema_name = apReader.schema_name().c_str();
      } else {
        schema_name = "public";
      }
      context_->AttachDB(db_name, db_path_);
      optional_ptr<Catalog> catalog;
      catalog = Catalog::GetCatalog(*context_, db_name);
      auto &schema = catalog->GetSchema(*context_, schema_name);
      auto entry =
          schema.GetEntry(catalog->GetCatalogTransaction(*context_), CatalogType::TABLE_ENTRY, table_name);
      auto &table = entry->Cast<TableCatalogEntry>();

      if (apReader.scan_columns_size() <= 0) {
        return nullptr;
      }
      vector<LogicalType> scan_types;
      vector<string> scan_names;
      vector<ColumnIndex> column_ids;
      vector<idx_t> projection_ids;
      IdxMap col_map;
      for (auto &out_col : apReader.scan_columns()) {
        auto index = LogicalIndex(out_col);
        auto &col = table.GetColumn(index);
        scan_types.push_back(col.Type());
        scan_names.push_back(col.Name());
        column_ids.emplace_back(col.Oid());
        projection_ids.push_back(column_ids.size() - 1);
        col_map[col.Oid()] = column_ids.size() - 1;
      }

      unique_ptr<FunctionData> bind_data;
      auto scan_function = table.GetScanFunction(*context_, bind_data);

      virtual_column_map_t virtual_columns;
      if (scan_function.get_virtual_columns) {
        virtual_columns = scan_function.get_virtual_columns(*context_, bind_data.get());
      }

      unique_ptr<TableFilterSet> table_filters;
      ExtraOperatorInfo extra_info;
      vector<Value> parameters;

      auto &table_scan = physical_plan->Make<PhysicalTableScan>(
          scan_types, scan_function, std::move(bind_data), scan_types, column_ids, projection_ids, scan_names,
          std::move(table_filters), 0, std::move(extra_info), std::move(parameters), std::move(virtual_columns));

      physical_plan->SetRoot(table_scan);

      // add filters
      if (post.has_filter() && post.filter().has_expr() && !post.filter().expr().empty()) {
        physical_plan = AddAPFilters(std::move(physical_plan), post, table, col_map);
      }
      // add projection
      if (!post.projection() && post.render_exprs_size() > 0) {
        physical_plan = AddAPProjection(std::move(physical_plan), post, table, col_map);
      }
    }
  } catch (const Exception& e) {
    auto error = e.what();
    printf("catch error %s \n", error);
    return nullptr;
  }

  return physical_plan;
}

}  // namespace kwdbap