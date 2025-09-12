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
    // // bind sorted aggregates
    // for (auto &aggr : aggregates) {
    //     auto &bound_aggr = aggr->Cast<BoundAggregateExpression>();
    //     if (bound_aggr.order_bys) {
    //         // sorted aggregate!
    //         FunctionBinder::BindSortedAggregate(context, bound_aggr, groups);
    //     }
    // }

    for (auto &group : agg.group_cols()) {
        auto ref = make_uniq<BoundReferenceExpression>(child.types[group], group);
        auto ref_copy = ref->Copy();
        types.push_back(child.types[group]);
        agg_returned_types.push_back(child.types[group]);
        expressions.push_back(std::move(ref));
        groups.push_back(std::move(ref_copy));
    }

    // for (auto &group : groups) {
    //     auto ref = make_uniq<BoundReferenceExpression>(group->return_type, expressions.size());
    //     types.push_back(group->return_type);
    //     expressions.push_back(std::move(group));
    //     group = std::move(ref);
    // }

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

        vector<unique_ptr<duckdb::Expression>> ref;
        for (auto idx : aggr.col_idx()) {
            ref.push_back(make_uniq<BoundReferenceExpression>(child.types[idx], idx));
            auto agg_func = func.functions.GetFunctionByArguments(*context_, {child.types[idx]});
            if (agg_func.bind) {
                bind_info = agg_func.bind(*context_, agg_func, ref);
                // we may have lost some arguments in the bind
                ref.resize(MinValue(agg_func.arguments.size(), ref.size()));
            }
            auto agg_expr = make_uniq<BoundAggregateExpression>(agg_func,
                                                                std::move(ref), std::move(filter),
                                                                std::move(bind_info), aggr_type);
            agg_returned_types.push_back(agg_expr->return_type);
            // auto agg_expr_copy = agg_expr->Copy();
            auto agg_expr_copy = CopyChildren(agg_expr->children);
            aggregates.push_back(std::move(agg_expr));

            for (auto &child1 : agg_expr_copy) {
                auto ref1 = make_uniq<BoundReferenceExpression>(child1->return_type, idx);
                types.push_back(ref1->return_type);
                expressions.push_back(std::move(ref1));
            }
        }
    }



    // for (auto &aggr : aggregates) {
    //     auto &bound_aggr = aggr->Cast<BoundAggregateExpression>();
    //     for (auto &child : bound_aggr.children) {
    //         auto ref = make_uniq<BoundReferenceExpression>(child->return_type, expressions.size());
    //         types.push_back(child->return_type);
    //         expressions.push_back(std::move(child));
    //         child = std::move(ref);
    //     }
    //     if (bound_aggr.filter) {
    //         auto &filter = bound_aggr.filter;
    //         auto ref = make_uniq<BoundReferenceExpression>(filter->return_type, expressions.size());
    //         types.push_back(filter->return_type);
    //         expressions.push_back(std::move(filter));
    //         bound_aggr.filter = std::move(ref);
    //     }
    // }
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

  // auto &table_scan = child.Cast<PhysicalTableScan>();
  // vector<unique_ptr<duckdb::Expression>> children;
  // // todo: 需要根据实际情况构建投影，比如下层返回a,b, 本层实际上要返回sum(a),sum(b),a
  // for (auto idx : table_scan.projection_ids) {
  //     children.push_back(make_uniq<BoundReferenceExpression>(table_scan.returned_types[idx], idx));
  // }
  // // auto ref_copy = CopyChildren(children);
  // auto &proj = physical_plan_->Make<PhysicalProjection>(table_scan.returned_types, std::move(children), 0);
  // proj.children.push_back(table_scan);

  // vector<unique_ptr<duckdb::Expression>> expressions;
  // vector<LogicalType> agg_returned_types;
  // for (auto &agg : apAggregator.aggregations()) {
  //     std::string func_name = "";
  //     switch (agg.func()) {
  //         case TSAggregatorSpec_Func_SUM: {
  //             func_name = "sum";
  //             break;
  //         }
  //         case TSAggregatorSpec_Func_AVG: {
  //             func_name = "avg";
  //             break;
  //         }
  //         case TSAggregatorSpec_Func_COUNT: {
  //             func_name = "count";
  //             break;
  //         }
  //         case TSAggregatorSpec_Func_MAX: {
  //             func_name = "max";
  //             break;
  //         }
  //         case TSAggregatorSpec_Func_MIN: {
  //             func_name = "min";
  //             break;
  //         }
  //         default: {
  //             throw InvalidInputException("unsupported agg function");
  //         }
  //     }
  //     EntryLookupInfo lookup_info(CatalogType::AGGREGATE_FUNCTION_ENTRY, func_name);
  //     auto entry_retry = CatalogEntryRetriever(*context_);
  //     auto func_entry = entry_retry.GetEntry("", "", lookup_info, OnEntryNotFound::RETURN_NULL);
  //     if (!func_entry) {
  //         throw std::runtime_error("Aggregate function not found: " + func_name);
  //     }
  //     auto &func = func_entry->Cast<AggregateFunctionCatalogEntry>();
  //     unique_ptr<duckdb::Expression> filter;
  //     unique_ptr<FunctionData> bind_info;
  //     auto aggr_type = AggregateType::NON_DISTINCT;
  //
  //     vector<unique_ptr<duckdb::Expression>> ref;
  //     for (auto idx : agg.col_idx()) {
  //         ref.push_back(make_uniq<BoundReferenceExpression>(table_scan.returned_types[idx], idx));
  //         auto agg_func = func.functions.GetFunctionByArguments(*context_, {table_scan.returned_types[idx]});
  //         if (agg_func.bind) {
  //             bind_info = agg_func.bind(*context_, agg_func, ref);
  //             // we may have lost some arguments in the bind
  //             ref.resize(MinValue(agg_func.arguments.size(), ref.size()));
  //         }
  //         auto agg_expr = make_uniq<BoundAggregateExpression>(agg_func,
  //                                                             std::move(ref), std::move(filter),
  //                                                             std::move(bind_info), aggr_type);
  //         agg_returned_types.push_back(agg_expr->return_type);
  //         expressions.push_back(std::move(agg_expr));
  //     }

  // }
    if (groups.size() == 0) {
        auto &res = physical_plan_->Make<PhysicalUngroupedAggregate>(agg_returned_types,
                                                               std::move(aggregates), 0);
        res.children.push_back(proj);
        return res;
    }
    // groups! create a GROUP BY aggregator
    // use a partitioned or perfect hash aggregate if possible
    // vector<column_t> partition_columns;
    // vector<idx_t> required_bits;
    // vector<unique_ptr<BaseStatistics>> group_stats;
    // for (auto &group : groups) {
    //     group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(group->return_type)));
    // }
    // group_stats.push_back(make_uniq<BaseStatistics>())
    auto &group_by = physical_plan_->Make<PhysicalHashAggregate>(*context_, agg_returned_types, std::move(aggregates),
                                                            std::move(groups), 0);
    group_by.children.push_back(proj);
    return group_by;
}

}  // namespace kwdbap
