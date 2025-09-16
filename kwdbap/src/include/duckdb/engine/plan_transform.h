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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "duckdb/execution/physical_plan_generator.hpp"
#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"
#include "ap_parse_query.h"

namespace kwdbap {
typedef duckdb::PhysicalOperator &PhyOpRef;
typedef std::map<idx_t, idx_t> IdxMap;
typedef std::vector<duckdb::PhysicalOperator *> PhyOpRefVec;
/**
 * @brief Physical plan processor
 *
 */
class TransFormPlan {
 public:
  explicit TransFormPlan(duckdb::ClientContext &context,
                         duckdb::PhysicalPlan *plan, std::string &db_path);
  ~TransFormPlan() = default;

  PhyOpRef TransFormPhysicalPlan(const kwdbts::TSProcessorSpec &procSpec,
                                 const kwdbts::PostProcessSpec &post,
                                 const kwdbts::TSProcessorCoreUnion &core,
                                 PhyOpRefVec &child);

 private:
  /**
  * @brief TransForm TableScan
  *
  */
  PhyOpRef TransFormTableScan(const kwdbts::TSProcessorSpec &procSpec, const kwdbts::PostProcessSpec &post,
                              const kwdbts::TSProcessorCoreUnion &core);
  /**
  * @brief TransForm Aggregator
  *
  */
  PhyOpRef TransFormAggregator(const kwdbts::PostProcessSpec &post, const kwdbts::TSProcessorCoreUnion &core,
                               PhyOpRef child);
  /**
  * @brief TransForm HashJoin
  *
  */
  PhyOpRef TransFormHashJoin(const kwdbts::PostProcessSpec &post, const kwdbts::TSProcessorCoreUnion &core,
                             PhyOpRef left_child, PhyOpRef right_child);

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> BuildAPExpr(const std::string &str, ParseExprParam& param);

  duckdb::vector<duckdb::column_t> GetColsFromRenderExpr(
      const std::string &str, duckdb::TableCatalogEntry &table);

  PhyOpRef VerifyProjectionByTableScan(PhyOpRef plan, IdxMap &col_map);

  PhyOpRef AddAPFilters(PhyOpRef plan, const kwdbts::PostProcessSpec &post,
                        ParseExprParam &param, std::unordered_set<idx_t> &scan_filter_idx);

  duckdb::unique_ptr<duckdb::TableFilterSet> CreateTableFilters(
      const duckdb::vector<duckdb::ColumnIndex> &column_ids, const kwdbts::PostProcessSpec &post,
      ParseExprParam &param, std::unordered_set<idx_t> &scan_filter_idx, bool &all_filter_push_scan);

  PhyOpRef AddAPProjection(PhyOpRef plan, const kwdbts::PostProcessSpec &post, ParseExprParam &param);
  
  duckdb::PhysicalOperator InitAggregateExpressions(
      duckdb::PhysicalOperator &child1,
      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &aggregates,
      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &groups,
      kwdbts::TSAggregatorSpec agg_spec);
  //  duckdb::unique_ptr<duckdb::PhysicalPlan> physical_planner_;

 private:
  duckdb::ClientContext *context_;
  std::string db_path_;
  duckdb::PhysicalPlan *physical_plan_;
};

}  // namespace kwdbap
