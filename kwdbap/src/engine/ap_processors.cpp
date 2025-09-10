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

#include "duckdb/engine/ap_processors.h"

#include "duckdb/engine/plan_transform.h"

using namespace kwdbts;

namespace kwdbap {

Processors::Processors(duckdb::ClientContext &context, string &db_path) {
  spec_ = std::make_unique<kwdbts::FlowSpec>();
  transFormPlan_ = std::make_shared<TransFormPlan>(context, db_path);
}

Processors::~Processors() {
  Reset();
}
// Init processors
KStatus Processors::Init(char* message, uint32_t len) {
  if (b_init_) {
    LOG_ERROR("is inited status");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid init Processors");
    return KStatus::FAIL;
  }
  
  bool proto_parse = false;
  try {
    proto_parse = spec_->ParseFromArray(message, len);
  } catch (...) {
    LOG_ERROR("Throw exception where parsing physical plan.");
  }
  
  if (!proto_parse) {
    LOG_ERROR("Parse physical plan err when query setup.");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid physical plan");
    return KStatus::FAIL;
  }
  
  
  // DebugPrint(fspec);
  if (spec_->processors_size() < 1) {
//    LOG_ERROR("The flowspec has no processors.");
    return KStatus::FAIL;
  }
  
//  FindTopProcessorId();
  // New operator
  KStatus ret = BuildOperator();
  if (KStatus::SUCCESS != ret) {
//    LOG_ERROR("resolve flowspec error.");
    return KStatus::FAIL;
  }
  
  // connect operator
//  ret = ConnectOperator(ctx);
//  if (KStatus::SUCCESS != ret) {
//    LOG_ERROR("connect operator error.");
//    return KStatus::FAIL;
//  }
  // DebugPrintOperator(operators_.back());
  
//  ret = ReCreateOperator(ctx);
//  if (KStatus::SUCCESS != ret) {
//    LOG_ERROR("re create operator error.");
//    return KStatus::FAIL;
//  }
  
  // DebugPrintOperator(operators_.back());
  
  // add inbound/outbound operator
//  ret = TransformOperator(ctx);
//  if (KStatus::SUCCESS != ret) {
//    LOG_ERROR("transform operator error.");
//    return KStatus::FAIL;
//  }
  
//  ret = BuildTopOperator(ctx);
//  if (KStatus::SUCCESS != ret) {
//    LOG_ERROR("build top operator error.");
//    return KStatus::FAIL;
//  }
  
  // DebugPrintOperator(root_iterator_);
  // create pipeline
//  ret = BuildPipeline(ctx);
//  if (KStatus::SUCCESS != ret) {
//    LOG_ERROR("build pipeline error.");
//    return KStatus::FAIL;
//  }
  // DebugPrintPipeline(root_pipeline_);
  
//  if (CheckCancel(ctx) != SUCCESS) {
//    return KStatus::FAIL;
//  }
  
//  EEIteratorErrCode code = root_iterator_->Init(ctx);
//  INJECT_DATA_FAULT(FAULT_EE_DML_SETUP_PREINIT_MSG_FAIL, code,
//                    EEIteratorErrCode::EE_ERROR, nullptr);
//  if (code != EEIteratorErrCode::EE_OK) {
//    LOG_ERROR("Preinit iterator error when initing processors.");
//    return KStatus::FAIL;
//  }
//  if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
//    root_iterator_->Close(ctx);
//    return KStatus::FAIL;
//  }
  b_init_ = KTRUE;
  return KStatus::SUCCESS;
}
  
duckdb::unique_ptr<duckdb::PhysicalPlan> Processors::BuildOperatorImp(const ProcessorSpec &procSpec,
                                  std::unordered_map<k_uint32, duckdb::unique_ptr<duckdb::PhysicalPlan>> &inputSrc) {
  std::vector<const kwdbts::ProcessorSpec*> procs;
  const PostProcessSpec& post = procSpec.post();
  const ProcessorCoreUnion& core = procSpec.core();
//  k_int32 processor_id = procSpec.processor_id();
  auto inputPlan = std::vector<duckdb::unique_ptr<duckdb::PhysicalPlan>>(procSpec.input_size());
  auto Finish = false;
  if (0 == procSpec.input_size()) {
    // If there are no children, deal with yourself directly
    Finish = true;
  } else {
    Finish = true;
    for (auto m = 0; m < procSpec.input_size(); m++) {
      for (auto n = 0; n < procSpec.input(m).streams_size(); n++) {
        auto iter = inputSrc.find(procSpec.output(m).streams(n).stream_id());
        if (iter == inputSrc.end()) {
          // not finish
          Finish = false;
          break;
        } else {
          inputPlan[n] = std::move(iter->second);
          inputSrc.erase(iter);
        }
      }
    }
  }
  
  if (Finish) {
    // deal with self
    return transFormPlan_->TransFormPhysicalPlan(procSpec, post, core, inputPlan);
  }
  
  // error
  return nullptr;
}

KStatus Processors::BuildOperator() {
  std::unordered_map<k_uint32, duckdb::unique_ptr<duckdb::PhysicalPlan>> inputSrc;
  std::vector<const kwdbts::ProcessorSpec*> procs;
  duckdb::unique_ptr<duckdb::PhysicalPlan> plan;
  // find the child node, deal with the child node first, and then deal with yourself
  for (int i = 0; i < spec_->processors_size() - 1; ++i) {
    const ProcessorSpec& procSpec = spec_->processors(i);
    plan = BuildOperatorImp(procSpec, inputSrc);
    if (nullptr == plan) {
      // error
      return KStatus::FAIL;
    }
    
    for (auto m = 0; m < procSpec.output_size(); m++) {
      for (auto n = 0; n < procSpec.output(m).streams_size(); n++) {
        inputSrc[procSpec.output(m).streams(n).stream_id()] = std::move(plan);
      }
    }
  }
  
  const ProcessorSpec& procSpec = spec_->processors(spec_->processors_size() - 1);
  plan = BuildOperatorImp(procSpec, inputSrc);
  if (nullptr == plan) {
    // error
    return KStatus::FAIL;
  }
  
  physical_planner_ = std::move(plan);
  
  return KStatus::SUCCESS;
}

void Processors::Reset() {
  b_init_ = KFALSE;
  spec_.reset();
}

}