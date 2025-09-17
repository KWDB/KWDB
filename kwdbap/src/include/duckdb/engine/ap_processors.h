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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <utility>

#include "kwdb_type.h"
#include "ee_pb_plan.pb.h"
#include "duckdb/engine/plan_transform.h"

namespace kwdbap {
typedef duckdb::reference<duckdb::PhysicalOperator> PhyOpReference;
typedef std::unordered_map<kwdbts::k_uint32, duckdb::PhysicalOperator*> InPutStreamSrcMap;
/**
* @brief Physical plan processor
*
*/
class Processors {
 public:
  Processors(duckdb::ClientContext &context, std::string &db_path);
  
  ~Processors();
  
  /**
  * @brief Init processors
  * @param ctx
  * @param fspec
  * @return KStatus
  */
  kwdbts::KStatus Init(char* message, uint32_t len);
  
  
  inline duckdb::unique_ptr<duckdb::PhysicalPlan> GetPlan() { return std::move(physical_planner_); }
  
  
  void Reset();
  
 private:
  kwdbts::KStatus BuildOperator();
  
  PhyOpRef BuildOperatorImp(const kwdbts::TSProcessorSpec &procSpec, InPutStreamSrcMap &inputSrc);
  
  std::shared_ptr<TransFormPlan> transFormPlan_;
  
  // flow spec of physical plan
  std::unique_ptr<kwdbts::FlowSpec> spec_;
  
  // mark init
  kwdbts::k_bool b_init_{0};
  // mark close
  kwdbts::k_bool b_close_{0};
  
  duckdb::unique_ptr<duckdb::PhysicalPlan> physical_planner_;
};

}