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
#include <vector>

#include "duckdb/execution/physical_plan_generator.hpp"
#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"

namespace kwdbap {
typedef duckdb::unique_ptr<duckdb::PhysicalPlan> PhyPlanPtr;
typedef std::map<idx_t, idx_t> IdxMap;
/**
 * @brief Physical plan processor
 *
 */
class TransFormPlan {
 public:
  explicit TransFormPlan(duckdb::ClientContext &context, std::string &db_path);
  ~TransFormPlan() {}

  PhyPlanPtr TransFormPhysicalPlan(const kwdbts::ProcessorSpec &procSpec,
                                   const kwdbts::PostProcessSpec &post,
                                   const kwdbts::ProcessorCoreUnion &core,
                                   std::vector<PhyPlanPtr> &child);

 private:
  PhyPlanPtr TransFormTableScan(const kwdbts::ProcessorSpec &procSpec,
                                const kwdbts::PostProcessSpec &post,
                                const kwdbts::ProcessorCoreUnion &core);

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> BuildAPExpr(
      const std::string &str, duckdb::TableCatalogEntry &table,
      IdxMap &col_map);

  duckdb::vector<duckdb::column_t> GetColsFromRenderExpr(const std::string &str,
                                            duckdb::TableCatalogEntry &table);

  static PhyPlanPtr VerifyProjectionByTableScan(PhyPlanPtr plan, IdxMap &col_map);

  PhyPlanPtr AddAPFilters(PhyPlanPtr plan, const kwdbts::PostProcessSpec &post,
                          duckdb::TableCatalogEntry &table, IdxMap &col_map);

  PhyPlanPtr TransFormAggregator(
      const kwdbts::PostProcessSpec &post,
      const kwdbts::ProcessorCoreUnion &core,
      std::vector<duckdb::unique_ptr<duckdb::PhysicalPlan>> &child);

  PhyPlanPtr AddAPProjection(PhyPlanPtr plan,
                             const kwdbts::PostProcessSpec &post,
                             duckdb::TableCatalogEntry &table, IdxMap &col_map);
  duckdb::PhysicalOperator InitAggregateExpressions(duckdb::PhysicalOperator &child1,
                                                                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &aggregates,
                                                                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &groups,
                                                                     kwdbts::TSAggregatorSpec agg_spec);
  //  duckdb::unique_ptr<duckdb::PhysicalPlan> physical_planner_;

 private:
  duckdb::ClientContext *context_;
  std::string db_path_;
};

}  // namespace kwdbap
