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

#include <sstream>

#define private public
#include "ee_processors.h"
#undef private

#include <deque>

#include "ee_aggregate_op.h"
#include "ee_data_chunk.h"
#include "ee_op_factory.h"
#include "ee_outbound_op.h"
#include "ee_op_test_base.h"
#include "ee_op_spec_utils.h"
#include "ee_kwthd_context.h"
#include "ee_pipeline_group.h"
#include "ee_pipeline_task.h"
#include "ee_scan_op.h"
#include "libkwdbts2.h"

namespace kwdbts {

namespace {

class OwnedProcessorChunk : public DataChunk {
 public:
  OwnedProcessorChunk(ColumnInfo* col_info, k_uint32 col_num, k_uint32 capacity)
      : DataChunk(col_info, col_num, capacity), owned_col_info_(col_info) {}

  ~OwnedProcessorChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

DataChunkPtr MakeProcessorChunk(const std::vector<k_int32>& values) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk =
      std::make_unique<OwnedProcessorChunk>(col_info, 1, values.size());
  EXPECT_TRUE(chunk->Initialize());
  for (size_t row = 0; row < values.size(); ++row) {
    chunk->AddCount();
    k_int32 value = values[row];
    chunk->InsertData(row, 0, reinterpret_cast<char*>(&value), sizeof(value));
  }
  static const unsigned char encoded[] = {'o', 'k'};
  EXPECT_TRUE(chunk->SetEncodingBuf(encoded, sizeof(encoded)));
  return chunk;
}

class ProcessorRootStub : public BaseOperator {
 public:
  ProcessorRootStub() : BaseOperator(nullptr, nullptr, nullptr, 0) {}

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }
  EEIteratorErrCode Init(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Start(kwdbContext_p) override {
    ++start_calls_;
    return start_code_;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr& chunk) override {
    ++next_calls_;
    if (next_index_ < next_codes_.size()) {
      EEIteratorErrCode code = next_codes_[next_index_++];
      if (code == EEIteratorErrCode::EE_OK && !chunks_.empty()) {
        chunk = std::move(chunks_.front());
        chunks_.pop_front();
      } else {
        chunk = nullptr;
      }
      return code;
    }
    chunk = nullptr;
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Close(kwdbContext_p) override {
    ++close_calls_;
    return close_code_;
  }

  EEIteratorErrCode start_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode close_code_{EEIteratorErrCode::EE_OK};
  std::vector<EEIteratorErrCode> next_codes_;
  std::deque<DataChunkPtr> chunks_;
  size_t next_index_{0};
  int start_calls_{0};
  int next_calls_{0};
  int close_calls_{0};
};

class ChannelTransformStub : public ProcessorRootStub {
 public:
  KStatus CreateInputChannel(kwdbContext_p,
                             std::vector<BaseOperator*>& new_operators) override {
    ++create_input_calls_;
    if (append_self_on_input_) {
      new_operators.push_back(this);
    }
    return create_input_status_;
  }

  KStatus CreateTopOutputChannel(kwdbContext_p,
                                 std::vector<BaseOperator*>& operators) override {
    ++create_top_calls_;
    if (append_self_on_top_) {
      operators.push_back(this);
    }
    return create_top_status_;
  }

  KStatus BuildPipeline(PipelineGroup*, Processors*) override {
    ++build_pipeline_calls_;
    return build_pipeline_status_;
  }

  KStatus create_input_status_{KStatus::SUCCESS};
  KStatus create_top_status_{KStatus::SUCCESS};
  KStatus build_pipeline_status_{KStatus::SUCCESS};
  bool append_self_on_input_{false};
  bool append_self_on_top_{false};
  int create_input_calls_{0};
  int create_top_calls_{0};
  int build_pipeline_calls_{0};
};

class StartPgErrorOperator : public ProcessorRootStub {
 public:
  EEIteratorErrCode Start(kwdbContext_p) override {
    ++start_calls_;
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR,
                                  "start reported pg error");
    return EEIteratorErrCode::EE_OK;
  }
};

class TableScanTypeStub : public ProcessorRootStub {
 public:
  OperatorType Type() override { return OperatorType::OPERATOR_TABLE_SCAN; }
};

class RemoteOutboundTypeStub : public OutboundOperator {
 public:
  RemoteOutboundTypeStub() : OutboundOperator(nullptr, &spec_, &table_) {}

  OperatorType Type() override {
    return OperatorType::OPERATOR_REMOTR_OUT_BOUND;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

 private:
  TSOutputRouterSpec spec_;
  TABLE table_{1, 198};
};

DataChunkPtr MakeEmptyProcessorChunk() {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk = std::make_unique<OwnedProcessorChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  return chunk;
}

void AddIntColMetaForProcessorTest(TSCol* col,
                                   roachpb::KWDBKTSColumn::ColumnType column_type) {
  col->set_storage_type(roachpb::DataType::INT);
  col->set_storage_len(sizeof(k_int32));
  col->set_column_type(column_type);
}

void InitBasicTableReaderForProcessorTest(TSReaderSpec* spec) {
  spec->set_tableid(1);
  spec->set_offsetopt(false);
  spec->set_tableversion(1);
  spec->set_tstablereaderid(1);
  spec->set_orderedscan(false);
  AddIntColMetaForProcessorTest(spec->add_colmetas(),
                                roachpb::KWDBKTSColumn::TYPE_DATA);
}

}  // namespace

class TestProcessors : public OperatorTestBase {
 public:
  TestProcessors() : OperatorTestBase() {}
  ~TestProcessors() {}
  virtual void SetUp() {
    OperatorTestBase::SetUp();
  }

  virtual void TearDown() {
    OperatorTestBase::TearDown();
  }
};

TEST_F(TestProcessors, TestProcessFlow) {
  KWThdContext *thd = new KWThdContext();
  current_thd = thd;
  TSFlowSpec flow;
  SpecAgg agg_spec(table_id_);
  agg_spec.PrepareFlowSpec(flow);
  agg_spec.PrepareInputOutputSpec(flow);

  char* result{nullptr};
  Processors* processors = new Processors();
  ASSERT_EQ(processors->Init(ctx_, &flow), SUCCESS);
  ASSERT_EQ(processors->InitIterator(ctx_, TsNextRetState::DML_NEXT), SUCCESS);
  k_uint32 count;
  k_uint32 size;
  k_bool is_last;
  EXPECT_EQ(processors->RunWithEncoding(ctx_, &result, &size, &count, &is_last), KStatus::SUCCESS);
  if (result) {
    free(result);
  }
  EXPECT_EQ(processors->CloseIterator(ctx_), KStatus::SUCCESS);
  processors->Reset();
  SafeDeletePointer(processors);
  thd->Reset();
  SafeDeletePointer(thd);
}

// Processors::InitProcessorsOptimization

TEST_F(TestProcessors, ProcessorsCloseIteratorRunWithEncodingAndVectorizeBranches) {
  {
    Processors processors;
    EXPECT_EQ(processors.CloseIterator(ctx_), KStatus::FAIL);
  }

  {
    Processors processors;
    ProcessorRootStub root;
    root.next_codes_ = {EEIteratorErrCode::EE_NEXT_CONTINUE,
                        EEIteratorErrCode::EE_END_OF_RECORD};
    processors.root_iterator_ = &root;
    processors.b_init_ = KTRUE;

    char* buffer = nullptr;
    k_uint32 length = 0;
    k_uint32 count = 0;
    k_bool is_last = KFALSE;
    EXPECT_EQ(processors.RunWithEncoding(ctx_, &buffer, &length, &count, &is_last),
              KStatus::SUCCESS);
    EXPECT_TRUE(is_last);
    EXPECT_EQ(root.next_calls_, 2);
    EXPECT_EQ(root.close_calls_, 1);
  }

  {
    Processors processors;
    ProcessorRootStub root;
    root.next_codes_ = {EEIteratorErrCode::EE_OK};
    root.chunks_.push_back(MakeProcessorChunk({42}));
    processors.root_iterator_ = &root;
    processors.b_init_ = KTRUE;

    DataInfo info = {};
    char* value = nullptr;
    k_uint32 length = 0;
    k_uint32 count = 0;
    k_bool is_last = KFALSE;
    EXPECT_EQ(
        processors.RunWithVectorize(ctx_, &value, &info, &length, &count, &is_last),
        KStatus::SUCCESS);
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(value, reinterpret_cast<char*>(&info));
    EXPECT_EQ(processors.CloseIterator(ctx_), KStatus::SUCCESS);
    SafeFreePointer(info.column_data_);
    SafeFreePointer(info.column_);
  }

  {
    Processors processors;
    ProcessorRootStub root;
    root.next_codes_ = {EEIteratorErrCode::EE_OK};
    root.chunks_.push_back(MakeProcessorChunk({7}));
    processors.root_iterator_ = &root;
    processors.b_init_ = KTRUE;

    char* value = nullptr;
    k_uint32 length = 0;
    k_uint32 count = 0;
    k_bool is_last = KFALSE;
    EXPECT_EQ(processors.RunWithVectorize(ctx_, &value, nullptr, &length, &count,
                                          &is_last),
              KStatus::FAIL);
    EXPECT_TRUE(is_last);
    EXPECT_EQ(root.close_calls_, 1);
  }
}

TEST_F(TestProcessors, ProcessorsInitAndTopologyBranches) {
  {
    Processors processors;
    TSFlowSpec empty_flow;
    EXPECT_EQ(processors.Init(ctx_, &empty_flow), KStatus::FAIL);
  }

  {
    Processors processors;
    TSFlowSpec sync_flow;
    auto* source = sync_flow.add_processors();
    source->set_processor_id(11);
    source->mutable_core()->mutable_noop();
    auto* sink = sync_flow.add_processors();
    sink->set_processor_id(22);
    sink->mutable_core()->mutable_synchronizer();
    processors.fspec_ = &sync_flow;
    processors.FindTopProcessorId();
    EXPECT_EQ(processors.top_process_id_, 11U);
  }

  {
    Processors processors;
    TSFlowSpec broken_flow;
    auto* producer = broken_flow.add_processors();
    producer->set_processor_id(31);
    producer->mutable_core()->mutable_noop();
    auto* output = producer->add_output();
    output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
    auto* out_stream = output->add_streams();
    out_stream->set_type(StreamEndpointType::LOCAL);
    out_stream->set_stream_id(9);

    auto* consumer = broken_flow.add_processors();
    consumer->set_processor_id(32);
    consumer->set_final_ts_processor(true);
    consumer->mutable_core()->mutable_noop();

    EXPECT_EQ(processors.Init(ctx_, &broken_flow), KStatus::FAIL);
  }
}

TEST_F(TestProcessors, ProcessorsBuildTopOperatorAddsCollectorForRemoteRoot) {
  Processors processors;
  BaseOperator* remote = new RemoteOutboundTypeStub();
  ASSERT_NE(remote, nullptr);

  processors.operators_.push_back(remote);
  processors.top_process_id_ = remote->GetProcessorId();
  ASSERT_EQ(processors.BuildTopOperator(ctx_), KStatus::SUCCESS);
  ASSERT_NE(processors.root_iterator_, nullptr);
  EXPECT_EQ(processors.root_iterator_->Type(), OperatorType::OPERATOR_RESULT_COLLECTOR);
  ASSERT_EQ(processors.root_iterator_->GetChildren().size(), 1U);
  EXPECT_EQ(processors.root_iterator_->GetChildren()[0], remote);
}

TEST_F(TestProcessors, ProcessorsBuildTopOperatorCoversEncodingBranches) {
  {
    Processors processors;
    auto* root = new ProcessorRootStub();
    EXPECT_FALSE(root->GetOutputEncoding());
    processors.operators_.push_back(root);

    ASSERT_EQ(processors.BuildTopOperator(ctx_), KStatus::SUCCESS);
    EXPECT_EQ(processors.root_iterator_, root);
    EXPECT_TRUE(root->GetOutputEncoding());
  }

  {
    Processors processors;
    BaseOperator* remote = new RemoteOutboundTypeStub();
    ASSERT_NE(remote, nullptr);
    remote->SetUseQueryShortCircuit(true);

    processors.operators_.push_back(remote);
    processors.top_process_id_ = remote->GetProcessorId();
    ASSERT_EQ(processors.BuildTopOperator(ctx_), KStatus::SUCCESS);
    EXPECT_TRUE(remote->GetOutputEncoding());
  }
}

TEST_F(TestProcessors,
       ProcessorsBuildPipelineAssignsLeafRootOperator) {
  Processors processors;
  auto* root = new ProcessorRootStub();
  processors.operators_.push_back(root);
  processors.root_iterator_ = root;

  ASSERT_EQ(processors.BuildPipeline(ctx_), KStatus::SUCCESS);
  ASSERT_NE(processors.root_pipeline_, nullptr);
  EXPECT_EQ(processors.root_pipeline_->operator_, root);
  EXPECT_TRUE(processors.root_pipeline_->HasOperator());
  EXPECT_TRUE(processors.pipelines_.empty());

  processors.b_close_ = KTRUE;
  processors.Reset();
}

TEST_F(TestProcessors, ProcessorsConnectOperatorLinksStreamsAndSkipsAllowedCases) {
  Processors processors;
  auto* producer = new ProcessorRootStub();
  auto* consumer = new ProcessorRootStub();
  auto* final_root = new ProcessorRootStub();
  auto* remote_only = new ProcessorRootStub();

  TSOutputRouterSpec local_output;
  local_output.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  auto* local_stream = local_output.add_streams();
  local_stream->set_type(StreamEndpointType::LOCAL);
  local_stream->set_stream_id(55);

  TSOutputRouterSpec final_output;
  final_output.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  auto* final_stream = final_output.add_streams();
  final_stream->set_type(StreamEndpointType::LOCAL);
  final_stream->set_stream_id(77);

  TSOutputRouterSpec remote_output;
  remote_output.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  auto* remote_stream = remote_output.add_streams();
  remote_stream->set_type(StreamEndpointType::REMOTE);
  remote_stream->set_stream_id(88);

  producer->GetRpcSpecInfo().output_specs_.push_back(&local_output);
  final_root->GetRpcSpecInfo().output_specs_.push_back(&final_output);
  final_root->SetFinalOperator(true);
  remote_only->GetRpcSpecInfo().output_specs_.push_back(&remote_output);

  processors.operators_ = {producer, consumer, final_root, remote_only};
  processors.stream_input_id_.emplace(55, consumer);

  ASSERT_EQ(processors.ConnectOperator(ctx_), KStatus::SUCCESS);
  ASSERT_EQ(consumer->GetChildren().size(), 1U);
  EXPECT_EQ(consumer->GetChildren()[0], producer);
  EXPECT_TRUE(final_root->GetChildren().empty());
  EXPECT_TRUE(remote_only->GetChildren().empty());

  processors.b_close_ = KTRUE;
  processors.Reset();
}

TEST_F(TestProcessors, ProcessorsTransformAndBuildPipelinePropagateStubStatuses) {
  {
    Processors processors;
    auto* oper = new ChannelTransformStub();
    processors.operators_.push_back(oper);

    ASSERT_EQ(processors.TransformOperator(ctx_), KStatus::SUCCESS);
    EXPECT_EQ(oper->create_input_calls_, 1);
    EXPECT_EQ(oper->create_top_calls_, 1);
    processors.b_close_ = KTRUE;
    processors.Reset();
  }

  {
    Processors processors;
    auto* oper = new ChannelTransformStub();
    oper->create_input_status_ = KStatus::FAIL;
    processors.operators_.push_back(oper);

    EXPECT_EQ(processors.TransformOperator(ctx_), KStatus::FAIL);
    EXPECT_EQ(oper->create_input_calls_, 1);
    EXPECT_EQ(oper->create_top_calls_, 0);
    processors.b_close_ = KTRUE;
    processors.Reset();
  }

  {
    Processors processors;
    auto* oper = new ChannelTransformStub();
    oper->create_top_status_ = KStatus::FAIL;
    processors.operators_.push_back(oper);

    EXPECT_EQ(processors.TransformOperator(ctx_), KStatus::FAIL);
    EXPECT_EQ(oper->create_input_calls_, 1);
    EXPECT_EQ(oper->create_top_calls_, 1);
    processors.b_close_ = KTRUE;
    processors.Reset();
  }

  {
    Processors processors;
    auto* oper = new ChannelTransformStub();
    oper->build_pipeline_status_ = KStatus::FAIL;
    processors.root_iterator_ = oper;
    processors.operators_.push_back(oper);

    EXPECT_EQ(processors.BuildPipeline(ctx_), KStatus::SUCCESS);
    EXPECT_EQ(oper->build_pipeline_calls_, 1);
    ASSERT_NE(processors.root_pipeline_, nullptr);
    EXPECT_EQ(processors.root_pipeline_->operator_, oper);
    processors.b_close_ = KTRUE;
    processors.Reset();
  }
}

TEST_F(TestProcessors, ProcessorsInitIteratorRejectsUninitializedAndCloseShortCircuits) {
  Processors processors;
  EXPECT_EQ(processors.InitIterator(ctx_, TsNextRetState::DML_NEXT), KStatus::FAIL);

  ProcessorRootStub root;
  processors.root_iterator_ = &root;
  processors.b_init_ = KTRUE;
  processors.b_close_ = KTRUE;
  EXPECT_EQ(processors.CloseIterator(ctx_), KStatus::SUCCESS);
}

TEST_F(TestProcessors, ProcessorsBuildOperatorAndCreateTableCoverFailureBranches) {
  {
    Processors processors;
    TSFlowSpec flow;
    flow.set_usequeryshortcircuit(true);
    flow.set_usecompresstype(7);
    flow.set_floatprec(4);
    flow.add_output_type_oid(23);
    flow.add_output_type_oid(701);
    auto* proc = flow.add_processors();
    proc->set_processor_id(91);
    proc->mutable_core()->mutable_noop();
    processors.fspec_ = &flow;
    processors.top_process_id_ = 91;

    ASSERT_EQ(processors.BuildOperator(ctx_), KStatus::SUCCESS);
    ASSERT_EQ(processors.operators_.size(), 1U);
    auto* oper = processors.operators_.front();
    EXPECT_TRUE(oper->GetOutputEncoding());
    EXPECT_TRUE(oper->IsUseQueryShortCircuit());
    EXPECT_EQ(oper->GetUseUseCompressType(), static_cast<PgCompressMode>(7));
    EXPECT_EQ(oper->GetFloatPrec(), 4);
    EXPECT_EQ(oper->GetOutputTypeOid(), (std::vector<k_uint32>{23, 701}));

    processors.b_close_ = KTRUE;
    processors.Reset();
  }

  {
    Processors processors;
    TSFlowSpec flow;
    auto* proc = flow.add_processors();
    proc->set_processor_id(92);
    processors.fspec_ = &flow;
    processors.top_process_id_ = 92;
    EXPECT_EQ(processors.BuildOperator(ctx_), KStatus::FAIL);
  }

  {
    Processors processors;
    TSProcessorCoreUnion core;
    auto* reader = core.mutable_tagreader();
    reader->set_tableid(1);
    auto* col = reader->add_colmetas();
    col->set_storage_type(roachpb::DataType::INT);
    col->set_storage_len(sizeof(k_int32));
    col->set_column_type(roachpb::KWDBKTSColumn::TYPE_DATA);
    reader->add_probecolids(0);

    TABLE* table = nullptr;
    EXPECT_EQ(processors.CreateTable(ctx_, &table, core), KStatus::FAIL);
    EXPECT_EQ(table, nullptr);
  }
}

TEST_F(TestProcessors, ProcessorsReCreateOperatorCoversSkipAndRewritePaths) {
  {
    Processors processors;
    auto* scan = new TableScanTypeStub();
    processors.operators_.push_back(scan);

    EXPECT_EQ(processors.ReCreateOperator(ctx_), KStatus::SUCCESS);
    processors.b_close_ = KTRUE;
    processors.Reset();
  }

  {
    Processors processors;
    auto* scan = new TableScanTypeStub();
    auto* parent = new ProcessorRootStub();
    scan->AddParent(parent);
    processors.operators_ = {scan, parent};

    EXPECT_EQ(processors.ReCreateOperator(ctx_), KStatus::SUCCESS);
    processors.b_close_ = KTRUE;
    processors.Reset();
  }

  {
    Processors processors;
    auto* rewrite_table = new TABLE(1, 102);
    rewrite_table->field_num_ = 1;
    rewrite_table->fields_ = static_cast<Field**>(malloc(sizeof(Field*)));
    rewrite_table->fields_[0] =
        new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32));
    rewrite_table->fields_[0]->set_column_type(
        roachpb::KWDBKTSColumn::TYPE_PTAG);

    TSReaderSpec reader;
    InitBasicTableReaderForProcessorTest(&reader);
    PostProcessSpec scan_post;
    scan_post.add_output_columns(0);
    auto* child =
        new TableScanOperator(nullptr, &reader, &scan_post, rewrite_table, 103);

    TSAggregatorSpec agg;
    agg.set_group_window_id(-1);
    agg.add_group_cols(0);
    PostProcessSpec agg_post;
    auto* parent =
        new HashAggregateOperator(nullptr, &agg, &agg_post, rewrite_table, 104);
    auto* grand_parent = new ProcessorRootStub();
    parent->AddDependency(child);
    grand_parent->AddDependency(parent);
    parent->SetFinalOperator(true);
    parent->SetOutputEncoding(true);
    parent->SetUseQueryShortCircuit(true);
    parent->SetUseUseCompressType(static_cast<PgCompressMode>(5));

    processors.operators_ = {parent, child};
    EXPECT_EQ(processors.ReCreateOperator(ctx_), KStatus::SUCCESS);
    ASSERT_EQ(processors.operators_.size(), 2U);
    EXPECT_EQ(processors.operators_[0]->Type(),
              OperatorType::OPERATOR_SORT_GROUP_BY);
    EXPECT_EQ(processors.operators_[1], child);

    processors.b_close_ = KTRUE;
    processors.Reset();
    delete grand_parent;
    delete rewrite_table;
  }
}
}  // namespace kwdbts
