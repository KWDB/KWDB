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

#include "ee_base_op.h"

#include "ee_kwthd_context.h"
#include "ee_op_factory.h"
#include "ee_pipeline_group.h"

namespace kwdbts {

std::map<OperatorType, const char *> operator_type_str_map = {
    {OperatorType::OPERATOR_TAG_SCAN, "OPERATOR_TAG_SCAN"},
    {OperatorType::OPERATOR_HASH_TAG_SCAN, "OPERATOR_HASH_TAG_SCAN"},
    {OperatorType::OPERATOR_TABLE_SCAN, "OPERATOR_TABLE_SCAN"},
    {OperatorType::OPERATOR_AGG_SCAN, "OPERATOR_AGG_SCAN"},
    {OperatorType::OPERATOR_SORT_SCAN, "OPERATOR_SORT_SCAN"},
    {OperatorType::OPERATOR_STATISTIC_SCAN, "OPERATOR_STATISTIC_SCAN"},
    {OperatorType::OPERATOR_HASH_GROUP_BY, "OPERATOR_HASH_GROUP_BY"},
    {OperatorType::OPERATOR_SORT_GROUP_BY, "OPERATOR_SORT_GROUP_BY"},
    {OperatorType::OPERATOR_POST_AGG_SCAN, "OPERATOR_POST_AGG_SCAN"},
    {OperatorType::OPERATOR_ORDER_BY, "OPERATOR_ORDER_BY"},
    {OperatorType::OPERATOR_DISTINCT, "OPERATOR_DISTINCT"},
    {OperatorType::OPERATOR_NOOP, "OPERATOR_NOOP"},
    {OperatorType::OPERATOR_PASSTHROUGH_NOOP, "OPERATOR_PASSTHROUGH_NOOP"},
    {OperatorType::OPERATOR_WINDOWS, "OPERATOR_WINDOWS"},
    {OperatorType::OPERATOR_SAMPLER, "OPERATOR_SAMPLER"},
    {OperatorType::OPERATOR_RESULT_COLLECTOR, "OPERATOR_RESULT_COLLECTOR"},
    {OperatorType::OPERATOR_LOCAL_IN_BOUND, "OPERATOR_LOCAL_IN_BOUND"},
    {OperatorType::OPERATOR_REMOTE_IN_BOUND, "OPERATOR_REMOTE_IN_BOUND"},
    {OperatorType::OPERATOR_LOCAL_OUT_BOUND, "OPERATOR_LOCAL_OUT_BOUND"},
    {OperatorType::OPERATOR_REMOTR_OUT_BOUND, "OPERATOR_REMOTR_OUT_BOUND"},
    {OperatorType::OPERATOR_SYNCHRONIZER, "OPERATOR_SYNCHRONIZER"},
    {OperatorType::OPERATOR_REMOTE_MERGE_SORT_IN_BOUND,
     "OPERATOR_REMOTE_MERGE_SORT_IN_BOUND"},
    {OperatorType::OPERATOR_LOCAL_MERGE_IN_BOUND,
     "OPERATOR_LOCAL_MERGE_IN_BOUND"}};

const char *BaseOperator::GetTypeName() {
  return operator_type_str_map[Type()];
}

KStatus BaseOperator::BuildPipeline(PipelineGroup *pipeline,
                                    Processors *processor) {
  PipelineGroup *child_pipeline = pipeline;
  if (childrens_.empty()) {
    return KStatus::SUCCESS;
  }

  if (isSource()) {
    child_pipeline = new PipelineGroup(processor);
    if (nullptr == child_pipeline) {
      return KStatus::FAIL;
    }
    pipeline->SetDependenciePipeline(child_pipeline);
    this->BelongsToPipeline(pipeline);
    pipeline->SetSource(this);
  } else if (isSink()) {
    if (pipeline->operator_ != nullptr) {
      child_pipeline = new PipelineGroup(processor);
      if (nullptr == child_pipeline) {
        return KStatus::FAIL;
      }
      pipeline->SetDependenciePipeline(child_pipeline);
      this->BelongsToPipeline(child_pipeline);
      child_pipeline->SetPipelineOperator(this);
      child_pipeline->SetSink(this);
    } else {
      pipeline->SetPipelineOperator(this);
      pipeline->SetSink(this);
    }
  } else {
    pipeline->AddOperator(this);
  }

  for (auto it : childrens_) {
    KStatus ret = it->BuildPipeline(child_pipeline, processor);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }

  return KStatus::SUCCESS;
}

KStatus BaseOperator::CreateInputChannel(
    kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) {
  KStatus ret = KStatus::SUCCESS;
  std::vector<BaseOperator *> tmp_childrens = childrens_;
  TSInputSyncSpec *input_spec = rpcSpecInfo_.input_specs_[0];
  BaseOperator *inbound_operator = nullptr;
  bool is_remote = false;
  k_uint32 stream_size = input_spec->streams_size();
  for (k_uint32 i = 0; i < stream_size; i++) {
    StreamEndpointType output_stream_type = input_spec->streams(i).type();
    if (StreamEndpointType::REMOTE == output_stream_type) {
      is_remote = true;
      break;
    }
  }
  bool is_ordered = false;
  if (is_remote) {
    if (TSInputSyncSpec_Type::TSInputSyncSpec_Type_ORDERED ==
        input_spec->type()) {
      is_ordered = true;
    }
    ret = OpFactory::NewInboundOperator(ctx, collection_, input_spec,
                                        &inbound_operator, &table_, true,
                                        is_ordered);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
    current_thd->SetRemote(true);
  } else {
    bool create_local_inbound = false;
    if (TSInputSyncSpec_Type::TSInputSyncSpec_Type_ORDERED ==
        input_spec->type()) {
      create_local_inbound = true;
      is_ordered = true;
    } else if (tmp_childrens.size() > 1) {
      // The local standalone union serial plan does not include the special handling of the LOCAL_OUTBOUND operator.
      create_local_inbound = true;
    } else {
      for (auto it : tmp_childrens) {
        if (OperatorType::OPERATOR_LOCAL_OUT_BOUND == it->Type()) {
          create_local_inbound = true;
          break;
        }
      }
    }
    if (create_local_inbound) {
      ret = OpFactory::NewInboundOperator(ctx, collection_, input_spec,
                                          &inbound_operator, &table_, false,
                                          is_ordered);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }
  if (inbound_operator != nullptr) {
    new_operators.push_back(inbound_operator);
    AddDependency(inbound_operator);
    for (auto it : tmp_childrens) {
      ret = it->CreateOutputChannel(ctx, new_operators);
      if (KStatus::FAIL == ret) {
        return ret;
      }
    }
  } else {
    for (auto it : tmp_childrens) {
      ret = it->CreateInputChannel(ctx, new_operators);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }

  return ret;
}

KStatus BaseOperator::CreateOutputChannel(
    kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) {
  KStatus ret = KStatus::SUCCESS;
  TSOutputRouterSpec *output_spec = rpcSpecInfo_.output_specs_[0];
  BaseOperator *outbound_operator = nullptr;
  bool is_routor = false;
  k_uint32 stream_size = output_spec->streams_size();
  for (k_uint32 i = 0; i < stream_size; i++) {
    StreamEndpointType output_stream_type = output_spec->streams(i).type();
    if (StreamEndpointType::REMOTE == output_stream_type) {
      is_routor = true;
      break;
    }
  }

  if (is_routor) {
    ret = OpFactory::NewOutboundOperator(ctx, collection_, output_spec,
                                         &outbound_operator, &table_, true);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  } else {
    ret = OpFactory::NewOutboundOperator(ctx, collection_, output_spec,
                                         &outbound_operator, &table_, false);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }
  new_operators.push_back(outbound_operator);
  BaseOperator *input_bound = parent_operators_[0]->GetInboundOperator();
  input_bound->AddDependency(outbound_operator);
  outbound_operator->AddDependency(this);
  parent_operators_[0]->RemoveDependency(this);

  return CreateInputChannel(ctx, new_operators);
}

KStatus BaseOperator::CreateTopOutputChannel(
    kwdbContext_p ctx, std::vector<BaseOperator *> &operators) {
  KStatus ret = KStatus::SUCCESS;
  if (!parent_operators_.empty()) {
    return KStatus::FAIL;
  }

  TSOutputRouterSpec *output_spec = rpcSpecInfo_.output_specs_[0];
  BaseOperator *outbound_operator = nullptr;
  bool is_routor = false;
  k_uint32 stream_size = output_spec->streams_size();
  for (k_uint32 i = 0; i < stream_size; i++) {
    StreamEndpointType output_stream_type = output_spec->streams(i).type();
    if (StreamEndpointType::REMOTE == output_stream_type) {
      is_routor = true;
      break;
    }
  }

  if (is_routor) {
    ret = OpFactory::NewOutboundOperator(ctx, collection_, output_spec,
                                         &outbound_operator, &table_, true);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }

    outbound_operator->AddDependency(this);
    // "routor_outbound_iterator" is top operator
    operators.push_back(outbound_operator);
  }

  return ret;
}

}  // namespace kwdbts
