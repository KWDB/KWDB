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
  physical_planner_ = duckdb::make_uniq<PhysicalPlan>(Allocator::Get(context));
  transFormPlan_ = std::make_shared<TransFormPlan>(context, physical_planner_.get(), db_path);
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
  b_init_ = KTRUE;
  return KStatus::SUCCESS;
}

PhyOpRef Processors::BuildOperatorImp(const TSProcessorSpec &procSpec, InPutStreamSrcMap &inputSrc) {
  std::vector<const kwdbts::TSProcessorSpec*> procs;
  const PostProcessSpec& post = procSpec.post();
  const TSProcessorCoreUnion& core = procSpec.core();
//  k_int32 processor_id = procSpec.processor_id();
  PhyOpRefVec inputPlan;
  if (0 == procSpec.input_size()) {
    // If there are no children, deal with yourself directly
  } else {
    for (auto m = 0; m < procSpec.input_size(); m++) {
      for (auto n = 0; n < procSpec.input(m).streams_size(); n++) {
        auto iter = inputSrc.find(procSpec.input(m).streams(n).stream_id());
        if (iter == inputSrc.end()) {
          printf("can not find stream id %d\n", procSpec.input(m).streams(n).stream_id());
          for (auto iter1 = inputSrc.begin(); iter1 != inputSrc.end(); iter1++) {
            printf("map stream id %d\n", iter1->first);
          }
          throw InternalException("can not find stream id %d", procSpec.input(m).streams(n).stream_id());
        } else {
          inputPlan.push_back(iter->second);
          inputSrc.erase(iter);
        }
      }
    }
  }
  // deal with self
  return transFormPlan_->TransFormPhysicalPlan(procSpec, post, core, inputPlan);
}

KStatus Processors::BuildOperator() {
  InPutStreamSrcMap inputSrc;
  std::vector<const kwdbts::TSProcessorSpec*> procs;
  printf("\nprocessor size  is %d\n", spec_->processors_size());
  // find the child node, deal with the child node first, and then deal with yourself
  for (int i = 0; i < spec_->processors_size() - 1; ++i) {
    const TSProcessorSpec& procSpec = spec_->processors(i);
    printf("deal with processor %d\n", i);
    auto &plan = BuildOperatorImp(procSpec, inputSrc);
    for (auto m = 0; m < procSpec.output_size(); m++) {
      for (auto n = 0; n < procSpec.output(m).streams_size(); n++) {
        inputSrc[procSpec.output(m).streams(n).stream_id()] = &plan;
      }
    }
  }
  const TSProcessorSpec& procSpec = spec_->processors(spec_->processors_size() - 1);
  printf("deal with processor %d\n", spec_->processors_size() - 1);
  auto &plan = BuildOperatorImp(procSpec, inputSrc);
  physical_planner_->SetRoot(plan);
  return KStatus::SUCCESS;
}

void Processors::Reset() {
  b_init_ = KFALSE;
  spec_.reset();
}
}  // namespace kwdbap
