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

#include "ee_processors.h"

#include <stdlib.h>

#include <string>

#include "cm_fault_injection.h"
#include "ee_cancel_checker.h"
#include "ee_kwthd_context.h"
#include "ee_op_factory.h"
#include "ee_pb_plan.pb.h"
#include "ee_pg_result.h"
#include "lg_api.h"
#include "ee_pipeline_group.h"
#include "ee_pipeline_task.h"
#include "ee_exec_pool.h"
#include "ee_dml_exec.h"

// support returning multi-rows datas
#define TSSCAN_RS_MULTILINE_SEND 1
#define TSSCAN_RS_SIZE_LIMIT 32768
namespace kwdbts {

static std::string  StreamEndpointTypeToString(StreamEndpointType type) {
  switch (type) {
    case StreamEndpointType::LOCAL:
      return "LOCAL";
    case StreamEndpointType::REMOTE:
      return "REMOTE";
    case StreamEndpointType::SYNC_RESPONSE:
      return "SYNC_RESPONSE";
    case StreamEndpointType::QUEUE:
      return "QUEUE";
    default:
      return "UNKNOWN";
  }
}

static std::string TSOutputRouterSpec_TypeToString(TSOutputRouterSpec_Type type) {
  switch (type) {
    case TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH:
      return "TSOutputRouterSpec_Type_PASS_THROUGH";
    case TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_MIRROR:
      return "TSOutputRouterSpec_Type_MIRROR";
    case TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_HASH:
      return "TSOutputRouterSpec_Type_BY_HASH";
    case TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_RANGE:
      return "TSOutputRouterSpec_Type_BY_RANGE";
    case TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER:
      return "TSOutputRouterSpec_Type_BY_GATHER";
    default:
      return "UNKNOWN";
  }
}

static std::string  TSInputSyncSpec_TypeToString(TSInputSyncSpec_Type type) {
  switch (type) {
    case TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED:
      return "TSInputSyncSpec_Type_UNORDERED";
    case TSInputSyncSpec_Type::TSInputSyncSpec_Type_ORDERED:
      return "TSInputSyncSpec_Type_ORDERED";
    default:
      return "UNKNOWN";
  }
}

static void DebugPrint(const TSFlowSpec *fspec) {
  LOG_ERROR("================== Begin Debug Print ==================\n");
  // optional bytes flowID = 1;
  if (fspec->has_flowid()) {
    LOG_ERROR("TSFlowSpec flowID = %s", fspec->flowid().c_str());
  }
  // optional int32 gateway = 3;
  if (fspec->has_gateway()) {
    LOG_ERROR("TSFlowSpec gateway = %d", fspec->gateway());
  }
  // optional int64 queryID = 4;
  if (fspec->has_queryid()) {
    LOG_ERROR("TSFlowSpec queryID = %ld", fspec->queryid());
  }
  // repeated string brpcAddrs = 5;
  LOG_ERROR("TSFlowSpec brpcAddrs size = %d", fspec->brpcaddrs_size());
  for (k_int32 i = 0; i < fspec->brpcaddrs_size(); ++i) {
    LOG_ERROR("TSFlowSpec brpcAddrs[%d] = %s", i, fspec->brpcaddrs(i).c_str());
  }
  // repeated .kwdbts.TSProcessorSpec processors = 2;
  LOG_ERROR("TSFlowSpec processors size = %d\n", fspec->processors_size());
  for (k_int32 i = 0; i < fspec->processors_size(); ++i) {
    const TSProcessorSpec &procSpec = fspec->processors(i);
    const TSPostProcessSpec &post = procSpec.post();
    const TSProcessorCoreUnion &core = procSpec.core();
    // optional int32 processor_id = 5;
    if (procSpec.has_processor_id()) {
      LOG_ERROR("TSProcessorSpec[%d] processor_id = %d", i, procSpec.processor_id());
    }
    // iterator type
    if (core.has_tagreader()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.tagreader", i);
    } else if (core.has_tablereader()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.tablereader", i);
    } else if (core.has_statisticreader()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.statisticreader", i);
    } else if (core.has_aggregator()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.aggregator", i);
    } else if (core.has_sampler()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.sampler", i);
    } else if (core.has_noop()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.noop", i);
    } else if (core.has_synchronizer()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.synchronizer", i);
    } else if (core.has_sorter()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.sorter", i);
    } else if (core.has_distinct()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.distinct", i);
    } else if (core.has_window()) {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.window", i);
    } else {
      LOG_ERROR("------------------------TSProcessorSpec[%d] core.unknown", i);
    }
    // optional bool final_ts_processor = 6;
    if (procSpec.has_final_ts_processor()) {
      LOG_ERROR("TSProcessorSpec[%d] final_ts_processor = %d\n", i, procSpec.final_ts_processor());
    }
    // repeated .kwdbts.TSInputSyncSpec input = 1;
    LOG_ERROR("TSProcessorSpec[%d] input size = %d\n", i, procSpec.input_size());
    for (k_int32 j = 0; j < procSpec.input_size(); ++j) {
      const TSInputSyncSpec &input = procSpec.input(j);
      // optional .kwdbts.TSInputSyncSpec.Type type = 1;
      if (input.has_type()) {
        LOG_ERROR("TSInputSyncSpec[%d] type= %s", j, TSInputSyncSpec_TypeToString(input.type()).c_str());
      }
      // optional .kwdbts.TSOrdering ordering = 2;
      if (input.has_ordering()) {
        const TSOrdering &ordering = input.ordering();
        for (k_int32 k = 0; k < ordering.columns_size(); ++k) {
          LOG_ERROR("TSInputSyncSpec[%d] ordering.columns[%d] = %d, direction : %s", j, k, ordering.columns(k).col_idx(),
                    ordering.columns(k).direction() == TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC
                    ? "ASC" : "DESC");
        }
      }
      // repeated bytes column_types = 4;
      for (k_int32 k = 0; k < input.column_types_size(); ++k) {
        LOG_ERROR("TSInputSyncSpec[%d] column_types[%d] = %s", j, k, input.column_types(k).c_str());
      }
      // repeated .kwdbts.TSStreamEndpointSpec streams = 3;
      LOG_ERROR("TSInputSyncSpec[%d] streams size = %d\n", j, input.streams_size());
      for (k_int32 k = 0; k < input.streams_size(); ++k) {
        const TSStreamEndpointSpec &stream = input.streams(k);
        // optional .kwdbts.StreamEndpointType type = 1;
        if (stream.has_type()) {
          LOG_ERROR("TSStreamEndpointSpec[%d] type = %s", k, StreamEndpointTypeToString(stream.type()).c_str());
        }
        // optional int32 stream_id = 2;
        if (stream.has_stream_id()) {
          LOG_ERROR("TSStreamEndpointSpec[%d] stream_id = %d", k, stream.stream_id());
        }
        // optional int32 target_node_id = 3;
        if (stream.has_target_node_id()) {
          LOG_ERROR("TSStreamEndpointSpec[%d] target_node_id = %d", k, stream.target_node_id());
        }
        // optional int32 dest_processor = 4;
        if (stream.has_dest_processor()) {
          LOG_ERROR("TSStreamEndpointSpec[%d] dest_processor = %d\n", k, stream.dest_processor());
        }
      }
    }
    // repeated .kwdbts.TSOutputRouterSpec output = 4;
    LOG_ERROR("TSProcessorSpec output size = %d\n", procSpec.output_size());
    for (k_int32 j = 0; j < procSpec.output_size(); ++j) {
      const TSOutputRouterSpec &output = procSpec.output(j);
      // optional .kwdbts.TSOutputRouterSpec.Type type = 1;
      if (output.has_type()) {
        LOG_ERROR("TSOutputRouterSpec[%d] type = %s", j, TSOutputRouterSpec_TypeToString(output.type()).c_str());
      }
      // repeated uint32 hash_columns = 3;
      for (k_int32 k = 0; k < output.hash_columns_size(); ++k) {
        LOG_ERROR("TSOutputRouterSpec[%d] hash_columns[%d] = %u", j, k, output.hash_columns(k));
      }
      // repeated .kwdbts.TSStreamEndpointSpec streams = 2;
      LOG_ERROR("TSOutputRouterSpec[%d] streams size = %d\n", j, output.streams_size());
      for (k_int32 k = 0; k < output.streams_size(); ++k) {
        const TSStreamEndpointSpec &stream = output.streams(k);
        // optional.kwdbts.StreamEndpointType type = 1;
        if (stream.has_type()) {
          LOG_ERROR("TSStreamEndpointSpec[%d] type = %s", k, StreamEndpointTypeToString(stream.type()).c_str());
        }
        // optional int32 stream_id = 2;
        if (stream.has_stream_id()) {
          LOG_ERROR("TSStreamEndpointSpec[%d] stream_id = %d", k, stream.stream_id());
        }
        // optional int32 target_node_id = 3;
        if (stream.has_target_node_id()) {
          LOG_ERROR("TSOutputRouterSpec[%d] target_node_id = %d", k, stream.target_node_id());
        }
        // optional int32 dest_processor = 4;
        if (stream.has_dest_processor()) {
          LOG_ERROR("TSOutputRouterSpec[%d] dest_processor = %d\n", k, stream.dest_processor());
        }
      }
    }
  }
}

static void DebugPrintSubOperator(BaseOperator *node, int depth, bool is_last, bool parent_flags[]) {
  char buffer[4096] = {0};
  for (int i = 0; i < depth; i++) {
    if (i == depth - 1) {
      snprintf(buffer + strlen(buffer), 4096 - strlen(buffer), is_last ? "|__ " : "|-- ");
    } else {
      snprintf(buffer + strlen(buffer), 4096 - strlen(buffer), parent_flags[i] ? "|   " : "    ");
    }
  }

  LOG_ERROR("%s%s", buffer, node->GetTypeName());

  for (int i = 0; i < node->GetChildren().size(); i++) {
    bool is_child_last = (i == node->GetChildren().size() - 1);
    bool new_flags[100];
    memcpy(new_flags, parent_flags, sizeof(new_flags));
    if (depth < 100) {
      new_flags[depth] = !is_child_last;
    }
    DebugPrintSubOperator(node->GetChildren()[i], depth + 1, is_child_last, new_flags);
  }
}

static void DebugPrintOperator(BaseOperator *root) {
  LOG_ERROR("================== Begin Debug Print Operator ==================");
  if (!root)
    return;

  LOG_ERROR("%s", root->GetTypeName());
  bool flags[100] = {false};
  for (int i = 0; i < root->GetChildren().size(); i++) {
    bool is_last = (i == root->GetChildren().size() - 1);
    bool new_flags[100] = {false};
    new_flags[0] = !is_last;
    DebugPrintSubOperator(root->GetChildren()[i], 1, is_last, new_flags);
  }
}

static void DebugPrintPipeline(PipelineGroup* group) {
  std::vector<PipelineGroup *> all_pipeline;
  group->GetPipelines(all_pipeline, true);
  k_uint32 pipeline_size = all_pipeline.size();
  LOG_ERROR("================== Begin Debug Print Pipeline ==================");
  LOG_ERROR("pipeline size = %d\n", pipeline_size);
  for (k_uint32 i = 0; i < pipeline_size; ++i) {
    LOG_ERROR("================== Begin Debug Print Pipeline %d ==================\n", i);
    PipelineGroup *pipeline = all_pipeline[i];
    LOG_ERROR("pipeline %d top operator name %s, address %p", i,
                                                      pipeline->operator_->GetTypeName(), pipeline->operator_);
    BaseOperator *sink = pipeline->GetSink();
    if (sink) {
      LOG_ERROR("pipeline %d sink operator name %s", i, sink->GetTypeName());
    } else {
      LOG_ERROR("pipeline %d sink operator name is null", i);
    }
    std::vector<BaseOperator *> &operators = pipeline->GetOperator();
    k_uint32 operator_size = operators.size();
    LOG_ERROR("pipeline %d operator size = %d\n", i, operator_size);
    for (k_uint32 j = 0; j < operator_size; ++j) {
      LOG_ERROR("pipeline %d operator %d name %s", i, j, operators[j]->GetTypeName());
    }
    BaseOperator *source = pipeline->GetSource();
    if (source) {
      LOG_ERROR("pipeline %d source operator name %s\n", i, source->GetTypeName());
    } else {
      LOG_ERROR("pipeline %d source operator name is null\n", i);
    }
  }
}

void Processors::FindTopProcessorId() {
  k_int32 processor_id = fspec_->processors_size() - 1;
  const TSProcessorCoreUnion& core = fspec_->processors(processor_id).core();
  top_process_id_ = fspec_->processors(processor_id).processor_id();
  if (core.has_synchronizer()) {
    top_process_id_ = fspec_->processors(processor_id - 1).processor_id();
  }

  // LOG_ERROR("top_process_id_ = %d", top_process_id_);
}

KStatus Processors::BuildOperator(kwdbContext_p ctx) {
  EnterFunc();
  k_int32 processor_size = fspec_->processors_size();
  bool only_operator = (1 == processor_size ? true : false);
  for (k_int32 i = 0; i < processor_size; ++i) {
    const TSProcessorSpec& procSpec = fspec_->processors(i);
    const TSPostProcessSpec& post = procSpec.post();
    const TSProcessorCoreUnion& core = procSpec.core();
    k_int32 processor_id = procSpec.processor_id();
    if (core.has_tagreader()) {
      TABLE *table = nullptr;
      if (KStatus::FAIL == CreateTable(ctx, &table, core)) {
        Return(KStatus::FAIL);
      }
      tables_.push_back(table);
    }
    // create operator
    TABLE *table = tables_.empty() ? nullptr : tables_.back();
    BaseOperator* oper = nullptr;
    KStatus ret = OpFactory::NewOp(ctx, &collection_, procSpec, post, core, &oper,
                                                      &table, processor_id, only_operator);
    if (ret != SUCCESS) {
      Return(ret);
    }

    if (processor_id == top_process_id_) {
      oper->SetOutputEncoding(true);
      oper->SetUseQueryShortCircuit(fspec_->usequeryshortcircuit());
    }

    command_limit_ = post.commandlimit();
    operators_.push_back(oper);
    std::set<k_int32> &input_stream_ids = oper->GetInputStreamIds();
    for (auto id : input_stream_ids) {
      stream_input_id_.emplace(id, oper);
    }
  }

  // set brpc info
  DmlExec *dml = ctx->dml_exec_handle;
  if (dml != nullptr) {
    dml->SetBrpcInfo(fspec_);
  }

  Return(KStatus::SUCCESS);
}

KStatus Processors::CreateTable(kwdbContext_p ctx, TABLE **table, const TSProcessorCoreUnion& core) {
  EnterFunc();
  const TSTagReaderSpec& readerSpec = core.tagreader();
  k_uint32 sId = 0;
  k_uint64 objId = readerSpec.tableid();
  *table = KNEW TABLE(sId, objId);
  if (*table == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  if (KStatus::FAIL == (*table)->Init(ctx, &readerSpec)) {
    delete *table;
    *table = nullptr;
    LOG_ERROR("Init table error when creating TableScanIterator.");
    Return(KStatus::FAIL);
  }

  Return(KStatus::SUCCESS);
}

KStatus Processors::ConnectOperator(kwdbContext_p ctx) {
  EnterFunc();
  // operators_ size
  k_uint32 operator_size = operators_.size();
  // traverse operator
  for (k_uint32 i = 0; i < operators_.size(); ++i) {
    // current operator
    BaseOperator* oper = operators_[i];
    // operator input/output stream
    RpcSpecResolve &rpcResolve = oper->GetRpcSpecInfo();
    // operator output stream size
    k_uint32 output_size = rpcResolve.output_specs_.size();
    // traverse operator output stream
    for (k_uint32 j = 0; j < output_size; ++j) {
      // current output stream
      TSOutputRouterSpec *output_spec = rpcResolve.output_specs_[j];
      // output stream size
      k_int32 output_stream_size = output_spec->streams_size();
      for (k_uint32 k = 0; k < output_stream_size; ++k) {
        // current stream type
        StreamEndpointType output_stream_type = output_spec->streams(k).type();
        if (StreamEndpointType::REMOTE == output_stream_type) {
          continue;
        }
        // current stream id in local output stream
        k_int32 output_stream_id = output_spec->streams(k).stream_id();
        // find "output_stream_id" in stream_input_id_
        auto iter = stream_input_id_.find(output_stream_id);
        if (iter == stream_input_id_.end()) {
          // if dest_processor == 0, is top operator, continue
          if (oper->IsFinalOperator()) {
            continue;
          } else {
            LOG_ERROR("can't find output stream id %d, operator type is %s, output index %u, output stream type %s",
                         output_stream_id, oper->GetTypeName(), k,
                           StreamEndpointTypeToString(output_stream_type).c_str());
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "connect operator failed");
            Return(KStatus::FAIL);
          }
        } else {
          iter->second->AddDependency(oper);
        }
      }
    }
  }

  Return(KStatus::SUCCESS);
}

KStatus Processors::ReCreateOperator(kwdbContext_p ctx) {
  for (auto &oper : operators_) {
    if (OperatorType::OPERATOR_TABLE_SCAN != oper->Type()) {
      continue;
    }

    std::vector<BaseOperator*> &parents = oper->GetParent();
    if (1 != parents.size()) {
      break;
    }

    BaseOperator *parent = parents[0];
    if (OperatorType::OPERATOR_HASH_GROUP_BY != parent->Type()) {
      break;
    }

    return OpFactory::ReCreateOperoatr(ctx, oper, &operators_);
  }

  return KStatus::SUCCESS;
}

KStatus Processors::TransformOperator(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = KStatus::FAIL;

  ret = operators_.back()->CreateInputChannel(ctx, new_operators_);
  if (ret!= KStatus::SUCCESS) {
    Return(ret);
  }

  ret = operators_.back()->CreateTopOutputChannel(ctx, operators_);
  if (ret!= KStatus::SUCCESS) {
    Return(ret);
  }

  Return(ret);
}

KStatus Processors::BuildTopOperator(kwdbContext_p ctx) {
  EnterFunc();
  root_iterator_ = operators_.back();
  if (OperatorType::OPERATOR_REMOTR_OUT_BOUND == root_iterator_->Type() ||
              OperatorType::OPERATOR_LOCAL_OUT_BOUND == root_iterator_->Type()) {
    if (OperatorType::OPERATOR_REMOTR_OUT_BOUND == root_iterator_->Type()) {
      for (auto oper : operators_) {
        if (oper->GetProcessorId() == top_process_id_) {
          if (oper->IsUseQueryShortCircuit()) {
            oper->SetOutputEncoding(true);
          } else {
            oper->SetOutputEncoding(false);
          }
          break;
        }
      }
    }
    Return(KStatus::SUCCESS);
  }

  BaseOperator* oper = nullptr;
  KStatus ret = OpFactory::NewResultCollectorOp(ctx, &oper);
  if (ret != KStatus::SUCCESS) {
    Return(ret);
  }

  operators_.push_back(oper);
  oper->AddDependency(root_iterator_);

  root_iterator_ = operators_.back();

  Return(KStatus::SUCCESS);
}


KStatus Processors::BuildPipeline(kwdbContext_p ctx) {
  EnterFunc();
  root_pipeline_ = new PipelineGroup(this);
  if (nullptr == root_pipeline_) {
    Return(KStatus::FAIL);
  }

  KStatus ret = root_iterator_->BuildPipeline(root_pipeline_, this);
  root_pipeline_->GetPipelines(pipelines_, false);
  Return(KStatus::SUCCESS);
}

KStatus Processors::ScheduleTasks(kwdbContext_p ctx) {
  KStatus ret = KStatus::SUCCESS;
  std::map<PipelineGroup *, std::shared_ptr<PipelineTask> > task_map;
  std::vector<std::shared_ptr<PipelineTask> > tasks;
  for (auto pipeline : pipelines_) {
    std::shared_ptr<PipelineTask> task = pipeline->CreateTask(ctx);
    if (nullptr != task) {
      task_map.emplace(pipeline, task);
      tasks.push_back(task);
    }
  }

  for (auto entry : task_map) {
    PipelineGroup *pipeline = entry.first;
    for (auto dependency : pipeline->dependencies_) {
      auto dependency_entry = task_map.find(dependency);
      entry.second->AddDependency(dependency_entry->second);
    }
  }
  for (auto entry : task_map) {
    if (entry.first->IsParallel()) {
      k_int32 degree = entry.first->GetDegree();
      KStatus ret = entry.second->Clone(ctx, degree - 1, tasks, new_operators_);
      if (KStatus::FAIL == ret) {
        break;
      }
    }
  }

  weak_tasks_.reserve(tasks.size());
  for (auto &ptr : tasks) {
    weak_tasks_.push_back(ptr);
  }

  // push task to thread pool
  for (auto task : tasks) {
    ExecPool::GetInstance().PushTask(task);
  }

  return ret;
}

// Init processors
KStatus Processors::Init(kwdbContext_p ctx, const TSFlowSpec* fspec) {
  EnterFunc();
  AssertNotNull(fspec);
  fspec_ = fspec;
  // DebugPrint(fspec);
  if (fspec_->processors_size() < 1) {
    LOG_ERROR("The flowspec has no processors.");
    Return(KStatus::FAIL);
  }

  if (fspec_->isdist()) {
    current_thd->SetRemote(true);
  }

  FindTopProcessorId();
  // New operator
  KStatus ret = BuildOperator(ctx);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("resolve flowspec error.");
    Return(KStatus::FAIL);
  }

  // connect operator
  ret = ConnectOperator(ctx);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("connect operator error.");
    Return(KStatus::FAIL);
  }
  // DebugPrintOperator(operators_.back());

  ret = ReCreateOperator(ctx);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("re create operator error.");
    Return(KStatus::FAIL);
  }

  // DebugPrintOperator(operators_.back());

  // add inbound/outbound operator
  ret = TransformOperator(ctx);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("transform operator error.");
    Return(KStatus::FAIL);
  }

  ret = BuildTopOperator(ctx);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("build top operator error.");
    Return(KStatus::FAIL);
  }

  // DebugPrintOperator(root_iterator_);
  // create pipeline
  ret = BuildPipeline(ctx);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("build pipeline error.");
    Return(KStatus::FAIL);
  }
  // DebugPrintPipeline(root_pipeline_);

  if (CheckCancel(ctx) != SUCCESS) {
    Return(FAIL);
  }

  EEIteratorErrCode code = root_iterator_->Init(ctx);
  INJECT_DATA_FAULT(FAULT_EE_DML_SETUP_PREINIT_MSG_FAIL, code,
                    EEIteratorErrCode::EE_ERROR, nullptr);
  if (code != EEIteratorErrCode::EE_OK) {
    LOG_ERROR("Preinit iterator error when initing processors.");
    Return(KStatus::FAIL);
  }
  if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
    root_iterator_->Close(ctx);
    Return(KStatus::FAIL);
  }
  b_init_ = KTRUE;
  Return(KStatus::SUCCESS);
}

void Processors::Reset() {
  b_init_ = KFALSE;
  if (!b_close_) {
    auto ctx = ContextManager::GetThreadContext();
    CloseIterator(ctx);
  }

  for (auto it : pipelines_) {
    SafeDeletePointer(it)
  }
  pipelines_.clear();
  weak_tasks_.clear();
  for (auto it : operators_) {
    SafeDeletePointer(it)
  }
  operators_.clear();

  for (auto it : new_operators_) {
    SafeDeletePointer(it)
  }
  new_operators_.clear();

  SafeDeletePointer(root_pipeline_);
  root_pipeline_ = nullptr;
  root_iterator_ = nullptr;

  for (auto table : tables_) {
    SafeDeletePointer(table);
  }

  tables_.clear();
}

KStatus Processors::InitIterator(kwdbContext_p ctx, TsNextRetState nextState) {
  EnterFunc();
  if (!b_init_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "Can't init operators again");
    Return(KStatus::FAIL);
  }
  AssertNotNull(root_iterator_);
  EEPgErrorInfo::ResetPgErrorInfo();
  KWThdContext * thd = current_thd;
  thd->SetPgEncode(nextState);
  thd->SetCommandLimit(&command_limit_);
  thd->SetCountForLimit(&count_for_limit_);
  // create task
  KStatus ret = ScheduleTasks(ctx);
  if (KStatus::FAIL == ret) {
    Return(ret);
  }

  EEIteratorErrCode code = root_iterator_->Start(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    for (auto task : weak_tasks_) {
      if (auto sp = task.lock()) {
        sp->SetStop();
      }
    }
    CloseIterator(ctx);
    Return(KStatus::FAIL);
  }

  if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
    CloseIterator(ctx);
    Return(KStatus::FAIL);
  }
  Return(KStatus::SUCCESS);
}

KStatus Processors::CloseIterator(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = KStatus::FAIL;
  if (!b_init_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "query not start");
    Return(ret);
  }
  if (b_close_) {
    Return(KStatus::SUCCESS);
  }
  AssertNotNull(root_iterator_);
  // Stop all task
  for (auto task : weak_tasks_) {
    if (auto sp = task.lock()) {
      sp->SetStop();
    }
  }
  // Close operators
  EEIteratorErrCode code = root_iterator_->Close(ctx);
  if (EEIteratorErrCode::EE_OK == code) {
    ret = KStatus::SUCCESS;
  }
  b_close_ = KTRUE;
  Return(ret);
}

// Encode datachunk
EEIteratorErrCode Processors::EncodeDataChunk(kwdbContext_p ctx,
                                              DataChunk* chunk,
                                              EE_StringInfo msgBuffer,
                                              k_bool is_pg) {
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  KStatus st = FAIL;
  if (is_pg) {
    for (k_uint32 row = 0; row < chunk->Count(); ++row) {
      st = chunk->PgResultData(ctx, row, msgBuffer);
      if (st != SUCCESS) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        ret = EEIteratorErrCode::EE_ERROR;
        break;
      }
      count_for_limit_ = count_for_limit_ + 1;
      if (command_limit_ != 0 && count_for_limit_ > command_limit_) {
        ret = EEIteratorErrCode::EE_END_OF_RECORD;
        break;
      }
    }
  } else {
    for (k_uint32 row = 0; row < chunk->Count(); ++row) {
      for (k_uint32 col = 0; col < chunk->ColumnNum(); ++col) {
        st = chunk->EncodingValue(ctx, row, col, msgBuffer);
        if (st != SUCCESS) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          ret = EEIteratorErrCode::EE_ERROR;
          break;
        }
      }
      if (st != SUCCESS) {
        break;
      }
    }
  }
  return ret;
}

// Run processors
KStatus Processors::RunWithEncoding(kwdbContext_p ctx, char** buffer,
                                    k_uint32* length, k_uint32* count,
                                    k_bool* is_last_record) {
  EnterFunc();
  AssertNotNull(root_iterator_);
  EEPgErrorInfo::ResetPgErrorInfo();
  *count = 0;
  KStatus ret = KStatus::FAIL;
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  DataChunkPtr chunk = nullptr;
  // read data
  do {
    code = root_iterator_->Next(ctx, chunk);
    if (EEIteratorErrCode::EE_NEXT_CONTINUE == code) {
      chunk = nullptr;
      continue;
    }
    break;
  } while (true);

  if (EEIteratorErrCode::EE_OK == code) {
    chunk->GetEncodingBuffer(buffer, length, count);
    collection_.GetAnalyse(ctx);
    ret = KStatus::SUCCESS;
  } else if (EEIteratorErrCode::EE_END_OF_RECORD == code) {
    *is_last_record = KTRUE;
    collection_.GetAnalyse(ctx);
    ret = KStatus::SUCCESS;
  }

  // stop task and close all iterator
  if (EEIteratorErrCode::EE_OK != code) {
    KStatus st = CloseIterator(ctx);
    if (KStatus::SUCCESS != st) {
      ret = KStatus::FAIL;
    }
  }

  Return(ret);
}

KStatus Processors::RunWithVectorize(kwdbContext_p ctx, char **value, void *buffer, k_uint32 *length,
                                                              k_uint32 *count, k_bool *is_last_record) {
  EnterFunc();
  AssertNotNull(root_iterator_);

  DataInfo *info = nullptr;
  *count = 0;
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    DataChunkPtr chunk = nullptr;
    EEPgErrorInfo::ResetPgErrorInfo();
    ret = root_iterator_->Next(ctx, chunk);

    if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }

    if (ret != EEIteratorErrCode::EE_OK) {
      break;
    }

    if (chunk == nullptr || chunk->Count() == 0) {
      continue;
    }

    *count = *count + chunk->Count();
    info = static_cast<DataInfo*>(buffer);
    if (nullptr == info) {
      ret = EEIteratorErrCode::EE_ERROR;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      break;
    }
    ret = chunk->VectorizeData(ctx, info);
    //  chunk.release();
    break;
  } while (true);
  collection_.GetAnalyse(ctx);
  if (ret != EEIteratorErrCode::EE_OK) {
    *is_last_record = KTRUE;
    KStatus st = CloseIterator(ctx);
    if (st != KStatus::SUCCESS) {
      LOG_ERROR("Failed to close operator.");
      ret = EEIteratorErrCode::EE_ERROR;
    }
    if (ret == EEIteratorErrCode::EE_ERROR) {
      Return(KStatus::FAIL);
    }
  }

  if (info) {
    *value = reinterpret_cast<char *>(info);
    *length = sizeof(info);
  }

  Return(KStatus::SUCCESS);
}

}  // namespace kwdbts
