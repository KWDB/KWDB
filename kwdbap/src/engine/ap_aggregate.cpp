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

    vector<unique_ptr<duckdb::Expression>> CopyChildren(
    const vector<unique_ptr<duckdb::Expression>> &children) {
        vector<unique_ptr<duckdb::Expression>> result;
        result.reserve(children.size());
        for (auto &child : children) {
            result.push_back(child->Copy());  // 每个元素调用 Copy()
        }
        return result;
    }

unique_ptr<PhysicalPlan> DuckdbExec::CreateAPAggregator(int *i) {
    const ProcessorSpec &proc = fspecs_->processors(*i);
    (*i)--;
    auto input_plan = ConvertFlowToPhysicalPlan(i);
    if (proc.has_core() && proc.core().has_apaggregator()) {
        auto apAggregator = proc.core().apaggregator();
        const PostProcessSpec &post = proc.post();
        // if (post.output_columns_size() > 0 && post.output_columns_size() != post.output_types_size()) {
        //     return nullptr;
        // }
        if (input_plan->Root().type != PhysicalOperatorType::TABLE_SCAN) {
            throw InvalidInputException("operator is not a table scan");
        }
        auto &table_scan = input_plan->Root().Cast<PhysicalTableScan>();
        vector<unique_ptr<duckdb::Expression>> children;
        // todo: 需要根据实际情况构建投影，比如下层返回a,b, 本层实际上要返回sum(a),sum(b),a
        for (auto idx : table_scan.projection_ids) {
            children.push_back(make_uniq<BoundReferenceExpression>(table_scan.returned_types[idx], idx));
        }
        // auto ref_copy = CopyChildren(children);
        auto &proj = physical_planner_ -> Make<PhysicalProjection>(table_scan.returned_types, std::move(children), 0);
        proj.children.push_back(input_plan->Root());

        vector<unique_ptr<duckdb::Expression>> expressions;
        vector<LogicalType> agg_returned_types;
        for (auto &agg : apAggregator.aggregations()) {
            std::string func_name = "";
            switch (agg.func()) {
                case TSAggregatorSpec_Func_SUM: {
                    func_name = "sum";
                    break;
                }
                case TSAggregatorSpec_Func_AVG: {
                    func_name = "avg";
                    break;
                }
                case TSAggregatorSpec_Func_COUNT: {
                    func_name = "count";
                    break;
                }
                case TSAggregatorSpec_Func_MAX: {
                    func_name = "max";
                    break;
                }
                case TSAggregatorSpec_Func_MIN: {
                    func_name = "min";
                    break;
                }
                default: {
                    throw InvalidInputException("unsupported agg function");
                }
            }
            EntryLookupInfo lookup_info(CatalogType::AGGREGATE_FUNCTION_ENTRY, func_name);
            auto entry_retry = CatalogEntryRetriever(*connect_->context);
            auto func_entry = entry_retry.GetEntry("", "", lookup_info, OnEntryNotFound::RETURN_NULL);
            if (!func_entry) {
                throw std::runtime_error("Aggregate function not found: " + func_name);
            }
            auto &func = func_entry->Cast<AggregateFunctionCatalogEntry>();
            unique_ptr<duckdb::Expression> filter;
            unique_ptr<FunctionData> bind_info;
            auto aggr_type = AggregateType::NON_DISTINCT;

            vector<unique_ptr<duckdb::Expression>> ref;
            for (auto idx : agg.col_idx()) {
                ref.push_back(make_uniq<BoundReferenceExpression>(table_scan.returned_types[idx], idx));
                auto agg_func = func.functions.GetFunctionByArguments(*connect_->context, {table_scan.returned_types[idx]});
                auto agg_expr = make_uniq<BoundAggregateExpression>(agg_func, std::move(ref), std::move(filter), std::move(bind_info), aggr_type);
                agg_returned_types.push_back(agg_expr->return_type);
                expressions.push_back(std::move(agg_expr));
            }

        }
        auto &res = physical_planner_ -> Make<PhysicalUngroupedAggregate>(agg_returned_types, std::move(expressions), 0);
        res.children.push_back(proj);
        input_plan->SetRoot(res);
    }
    return input_plan;
}
}