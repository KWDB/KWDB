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
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "fmt/printf.h"


using namespace duckdb;

namespace kwdbap {

vector<unique_ptr<duckdb::Expression>> CopyChildren(
const vector<unique_ptr<duckdb::Expression>> &children) {
    vector<unique_ptr<duckdb::Expression>> result;
    result.reserve(children.size());
    for (auto &child : children) {
        result.push_back(child->Copy());  // 每个元素调用 Copy()
    }
    return result;
}

PhyOpRef TransFormPlan::InitAggregateExpressions(PhyOpRef child,
                                                         vector<unique_ptr<duckdb::Expression>> &aggregates,
                                                         vector<unique_ptr<duckdb::Expression>> &groups,
                                                         vector<LogicalType> &agg_returned_types,
                                                         const kwdbts::PostProcessSpec& post,
                                                         const kwdbts::ProcessorCoreUnion& core) {
    vector<unique_ptr<duckdb::Expression>> expressions;
    vector<LogicalType> types;
    auto &agg = core.aggregator();
    // todo: bind sorted aggregates
    // for (auto &aggr : aggregates) {
    //     auto &bound_aggr = aggr->Cast<BoundAggregateExpression>();
    //     if (bound_aggr.order_bys) {
    //         // sorted aggregate!
    //         FunctionBinder::BindSortedAggregate(context, bound_aggr, groups);
    //     }
    // }
    int idx1 = 0;
    for (auto &group : agg.group_cols()) {
        auto ref = make_uniq<BoundReferenceExpression>(child.types[group], group);
        // auto ref_copy = ref->Copy();
        types.push_back(child.types[group]);
        expressions.push_back(std::move(ref));
        agg_returned_types.push_back(child.types[group]);
        auto ref_copy = make_uniq<BoundReferenceExpression>(child.types[group], idx1++);
        groups.push_back(std::move(ref_copy));
    }

    for (auto &aggr : agg.aggregations()) {
        std::string func_name = "";
        switch (aggr.func()) {
            case TSAggregatorSpec_Func_ANY_NOT_NULL: {
                continue;
            }
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
        auto entry_retry = CatalogEntryRetriever(*context_);
        auto func_entry = entry_retry.GetEntry("", "", lookup_info, OnEntryNotFound::RETURN_NULL);
        if (!func_entry) {
            throw std::runtime_error("Aggregate function not found: " + func_name);
        }
        auto &func = func_entry->Cast<AggregateFunctionCatalogEntry>();
        unique_ptr<duckdb::Expression> filter;
        unique_ptr<FunctionData> bind_info;
        auto aggr_type = AggregateType::NON_DISTINCT;

        vector<unique_ptr<duckdb::Expression>> agg_refs;
        for (auto idx : aggr.col_idx()) {
            auto proj_ref = make_uniq<BoundReferenceExpression>(child.types[idx], idx);
            types.push_back(proj_ref->return_type);
            expressions.push_back(std::move(proj_ref));
            agg_refs.push_back(make_uniq<BoundReferenceExpression>(child.types[idx],idx1++));

            auto agg_func = func.functions.GetFunctionByArguments(*context_, {child.types[idx]});
            if (agg_func.bind) {
                bind_info = agg_func.bind(*context_, agg_func, agg_refs);
                // we may have lost some arguments in the bind
                agg_refs.resize(MinValue(agg_func.arguments.size(), agg_refs.size()));
            }
            auto agg_expr = make_uniq<BoundAggregateExpression>(agg_func,
                                                                std::move(agg_refs), std::move(filter),
                                                                std::move(bind_info), aggr_type);
            agg_returned_types.push_back(agg_expr->return_type);
            aggregates.push_back(std::move(agg_expr));

            // todo: filter
            // if (bound_aggr.filter) {
            //     auto &filter = bound_aggr.filter;
            //     auto ref = make_uniq<BoundReferenceExpression>(filter->return_type, expressions.size());
            //     types.push_back(filter->return_type);
            //     expressions.push_back(std::move(filter));
            //     bound_aggr.filter = std::move(ref);
            // }

        }
    }

    if (expressions.empty()) {
        return child;
    }

    auto &proj = physical_plan_->Make<PhysicalProjection>(std::move(types), std::move(expressions), child.estimated_cardinality);
    proj.children.push_back(child);
    return proj;
}

PhyOpRef TransFormPlan::TransFormAggregator(const kwdbts::PostProcessSpec& post, const kwdbts::ProcessorCoreUnion& core,
                                            PhyOpRef child) {
  auto apAggregator = core.aggregator();
    vector<unique_ptr<duckdb::Expression>> aggregates;
    vector<unique_ptr<duckdb::Expression>> groups;
    vector<LogicalType> agg_returned_types;
    auto &proj = InitAggregateExpressions(child, aggregates,groups,agg_returned_types,post,core);

    if (groups.size() == 0) {
        auto &res = physical_plan_->Make<PhysicalUngroupedAggregate>(agg_returned_types,
                                                               std::move(aggregates), 0);
        res.children.push_back(proj);
        return res;
    }

    auto &group_by = physical_plan_->Make<PhysicalHashAggregate>(*context_, agg_returned_types, std::move(aggregates),
                                                            std::move(groups), 0);
    group_by.children.push_back(proj);

    vector<unique_ptr<duckdb::Expression>> expressions;
    vector<LogicalType> types;
    if (post.projection() && post.output_columns_size() > 0) {
        for (auto col : post.output_columns()) {
            expressions.push_back(make_uniq<BoundReferenceExpression>(group_by.types[col], col));
            types.push_back(group_by.types[col]);
        }
        auto &res = physical_plan_->Make<PhysicalProjection>(types, std::move(expressions), group_by.estimated_cardinality);
        res.children.push_back(group_by);
        return res;
    }

    return group_by;
}

}  // namespace kwdbap
