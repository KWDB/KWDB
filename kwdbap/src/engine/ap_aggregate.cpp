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

#include "duckdb/engine/duckdb_exec.h"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"


using namespace duckdb;

namespace kwdbts {

unique_ptr<PhysicalPlan> DuckdbExec::CreateAPAggregator(int *i) {
    // auto physical_plan = make_uniq<PhysicalPlan>(Allocator::Get(*connect_->context));
    (*i)--;
    auto input_plan = ConvertFlowToPhysicalPlan(i);
    const ProcessorSpec &proc = fspecs_->processors(*i);
    if (proc.has_core() && proc.core().has_apaggregator()) {
        auto apAggregator = proc.core().apaggregator();
        const PostProcessSpec &post = proc.post();
        if (post.output_columns_size() > 0 && post.output_columns_size() != post.output_types_size()) {
            return nullptr;
        }
        if (input_plan->Root().type != PhysicalOperatorType::TABLE_SCAN) {
            throw InvalidInputException("Operator is not a table scan");
        }
        auto &table_scan = input_plan->Root().Cast<PhysicalTableScan>();
        vector<unique_ptr<duckdb::Expression>> children;
        for (auto idx : table_scan.projection_ids) {
            children.push_back(make_uniq<BoundReferenceExpression>(table_scan.returned_types[idx], idx));
        }
        auto &proj = physical_planner_ -> Make<PhysicalProjection>(table_scan.returned_types, std::move(children), 0);
        proj.children.push_back(input_plan->Root());

        switch (apAggregator.aggregations()[0].func()) {
            case TSAggregatorSpec_Func_SUM: {
                vector<unique_ptr<duckdb::Expression>> expressions;

                std::string sum_func = "sum";
                EntryLookupInfo lookup_info(CatalogType::AGGREGATE_FUNCTION_ENTRY, sum_func);
                auto entry_retry = CatalogEntryRetriever(*connect_->context);
                auto func_entry = entry_retry.GetEntry("", "", lookup_info, OnEntryNotFound::RETURN_NULL);
                auto &func = func_entry->Cast<AggregateFunctionCatalogEntry>();
                unique_ptr<duckdb::Expression> filter;
                unique_ptr<FunctionData> bind_info;
                auto aggr_type = AggregateType::NON_DISTINCT;

                auto agg_expr = make_uniq<BoundAggregateExpression>(func.functions.GetFunctionByArguments(*connect_->context, {LogicalType::INTEGER}), std::move(children), std::move(filter), std::move(bind_info), aggr_type);
                expressions.push_back(std::move(agg_expr));
                // todo: current type is not correct
                auto &res = physical_planner_ -> Make<PhysicalUngroupedAggregate>(table_scan.returned_types, std::move(expressions), 0);
                res.children.push_back(proj);
                input_plan->SetRoot(res);
                break;
            }
            default: {
                throw InvalidInputException("unsupported agg function");
            }

        }

        //
        //
        // vector<unique_ptr<duckdb::Expression>> expressions;
        //
        // if (apAggregator.aggregations()[0].func() == TSAggregatorSpec_Func_SUM) {
        //   vector<unique_ptr<duckdb::Expression>> children;
        //   auto expr = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0);
        //   children.push_back(std::move(expr));
        //
        //   std::string sum_func = "sum";
        //   EntryLookupInfo lookup_info(CatalogType::AGGREGATE_FUNCTION_ENTRY, sum_func);
        //   auto entry_retry = CatalogEntryRetriever(*connect_->context);
        //   auto func_entry = entry_retry.GetEntry("", "", lookup_info, OnEntryNotFound::RETURN_NULL);
        //   auto &func = func_entry->Cast<AggregateFunctionCatalogEntry>();
        //
        //   unique_ptr<duckdb::Expression> filter;
        //   unique_ptr<FunctionData> bind_info;
        //   auto aggr_type = AggregateType::NON_DISTINCT;
        //
        //   auto agg_expr = make_uniq<BoundAggregateExpression>(func.functions.GetFunctionByArguments(*connect_->context, {LogicalType::INTEGER}), std::move(children), std::move(filter), std::move(bind_info), aggr_type);
        //   expressions.push_back(std::move(agg_expr));
        //
        //   vector<unique_ptr<duckdb::Expression>> exprs_copy;
        //   for (auto &aggr : expressions) {
        //     auto &bound_aggr = aggr->Cast<BoundAggregateExpression>();
        //     for (auto &child : bound_aggr.children) {
        //       auto ref = make_uniq<BoundReferenceExpression>(child->return_type, 0);
        //       exprs_copy.push_back(std::move(child));
        //       child = std::move(ref);
        //     }
        //   }
        //
        //
        //   auto &proj = physical_planner.Make<PhysicalProjection>(result_types, std::move(exprs_copy), 0);
        //   proj.children.push_back(physical_plan->Root());
        //
        //   auto &res = physical_planner.Make<PhysicalUngroupedAggregate>(result_types, std::move(expressions), 0);
        //   res.children.push_back(proj);
        //   physical_plan = physical_planner.GetPhysicalPlan();
        //   physical_plan->SetRoot(res);
        // }
    }
    return input_plan;
}
}