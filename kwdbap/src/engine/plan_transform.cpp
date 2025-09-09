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

using namespace kwdbts;

namespace kwdbap {

TransFormPlan::TransFormPlan(duckdb::ClientContext &context, std::string &db_path) {
//  physical_planner_ = duckdb::make_uniq<PhysicalPlan>(Allocator::Get(context));
  context_ = &context;
  db_path_ = db_path;
}

duckdb::unique_ptr<duckdb::PhysicalPlan> TransFormAggregator(
    const ProcessorSpec& procSpec, const PostProcessSpec& post, const ProcessorCoreUnion& core,
    std::vector<duckdb::unique_ptr<duckdb::PhysicalPlan>> &child) {
  return nullptr;
}

duckdb::unique_ptr<duckdb::PhysicalPlan> TransFormPlan::TransFormPhysicalPlan(
      const ProcessorSpec& procSpec, const PostProcessSpec& post, const ProcessorCoreUnion& core,
      std::vector<duckdb::unique_ptr<duckdb::PhysicalPlan>> &child) {
  // New operator by type
  if (core.has_aptablereader()) {
    return TransFormTableScan(procSpec, post, core);
  } else if (core.has_aggregator()) {
    return TransFormAggregator(procSpec, post, core, child);
  }
  EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid operator type");
  return nullptr;
}

vector<unique_ptr<duckdb::Expression>> TransFormPlan::BuildAPExpr(
    const std::string& str, TableCatalogEntry& table,
    std::map<idx_t, idx_t>& col_map) {
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<Tokens>(
      str.data(), str.data() + str.size(), max_query_size);
  IParser::Pos pos(tokens_ptr, max_parser_depth);
  APParseQuery parser(str, pos);
  auto node_list = parser.APParseImpl();  // expr tree
  size_t i = 0;
  vector<unique_ptr<duckdb::Expression>> expressions;
  while (i < node_list.size()) {
    unique_ptr<duckdb::Expression> expr;
    auto construct_ret = parser.ConstructAPExpr(*context_, table, &i, &expr, col_map);
    if (construct_ret != SUCCESS) {
      expressions.clear();
      return expressions;
    }
    expressions.emplace_back(std::move(expr));
  }
  //    if (nullptr == *expr) {
  //      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
  //      "Invalid expr"); LOG_ERROR("Parse expr failed, expr is: %s",
  //      str.c_str()); ret = KStatus::FAIL;
  //    }
  return expressions;
}

}  // namespace kwdbap