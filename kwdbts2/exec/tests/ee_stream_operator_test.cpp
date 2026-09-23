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

#include <chrono>
#include <deque>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#define private public
#define protected public
#include "br_data_stream_recvr.h"
#include "br_mgr.h"
#include "ee_aggregate_op.h"
#include "ee_dml_exec.h"
#include "ee_exec_pool.h"
#include "ee_inbound_op.h"
#include "ee_local_inbound_op.h"
#include "ee_local_outbound_op.h"
#include "ee_noop_op.h"
#include "ee_op_factory.h"
#include "ee_outbound_op.h"
#include "ee_remote_inbound_op.h"
#include "ee_remote_merge_sort_inbound_op.h"
#include "ee_router_outbound_op.h"
#include "ee_pipeline_group.h"
#include "ee_pipeline_task.h"
#include "ee_scan_op.h"
#undef protected
#undef private

#include "ee_field_const.h"
#include "ee_internal_type.h"
#include "ee_kwthd_context.h"
#include "ee_mempool.h"
#include "ee_pipeline_task_poller.h"
#include "ee_processors.h"

extern "C" {
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}

namespace kwdbts {
namespace {

std::unique_ptr<DataChunk> MakeIntChunk(const std::vector<k_int32>& values) {
  static ColumnInfo col_info[] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily)};
  auto chunk =
      std::make_unique<DataChunk>(col_info, 1, static_cast<k_uint32>(values.size()));
  EXPECT_TRUE(chunk->Initialize());
  for (size_t i = 0; i < values.size(); ++i) {
    chunk->AddCount();
    auto value = values[i];
    chunk->InsertData(static_cast<k_uint32>(i), 0, reinterpret_cast<char*>(&value),
                      sizeof(value));
  }
  return chunk;
}

std::vector<k_int32> ReadChunkInts(DataChunk* chunk) {
  std::vector<k_int32> values;
  if (chunk == nullptr) {
    return values;
  }
  values.reserve(chunk->Count());
  for (k_uint32 row = 0; row < chunk->Count(); ++row) {
    values.push_back(*reinterpret_cast<k_int32*>(chunk->GetData(row, 0)));
  }
  return values;
}

void EnqueuePassThroughChunk(DataStreamRecvr* recvr, k_int64 query_id,
                             k_int32 dest_processor, k_int32 sender_id,
                             DataChunkPtr chunk) {
  const size_t chunk_size = chunk->Size() + 20;
  recvr->GetPassThroughContext().AppendChunk(sender_id, chunk, chunk_size, 0);

  PTransmitChunkParams request;
  request.set_query_id(query_id);
  request.set_dest_processor(dest_processor);
  request.set_sender_id(sender_id);
  request.set_be_number(sender_id);
  request.set_use_pass_through(true);
  EXPECT_EQ(recvr->AddChunks(request, nullptr).code(), BRStatusCode::OK);
}

PostProcessSpec MakeSingleOutputPost() {
  PostProcessSpec post;
  post.add_output_columns(0);
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  return post;
}

void AddIntColMeta(TSCol* col, roachpb::KWDBKTSColumn::ColumnType column_type) {
  col->set_storage_type(roachpb::DataType::INT);
  col->set_storage_len(sizeof(k_int32));
  col->set_column_type(column_type);
}

void InitBasicTagReader(TSTagReaderSpec* spec, TSTableReadMode access_mode) {
  spec->set_tableid(1);
  spec->set_tableversion(1);
  spec->set_accessmode(access_mode);
  spec->set_only_tag(false);
  spec->set_uniontype(0);
  AddIntColMeta(spec->add_colmetas(), roachpb::KWDBKTSColumn::TYPE_TAG);
}

void InitBasicTableReader(TSReaderSpec* spec) {
  spec->set_tableid(1);
  spec->set_offsetopt(false);
  spec->set_tableversion(1);
  spec->set_tstablereaderid(1);
  spec->set_orderedscan(false);
  AddIntColMeta(spec->add_colmetas(), roachpb::KWDBKTSColumn::TYPE_DATA);
}

struct ScopedExecPoolPoller {
  explicit ScopedExecPoolPoller(ExecPool* pool) : pool_(pool) {
    if (pool_->pipeline_task_poller_ == nullptr) {
      pool_->pipeline_task_poller_ = new PipelineTaskPoller(pool_);
      created_ = true;
    }
  }

  ~ScopedExecPoolPoller() {
    if (created_) {
      delete pool_->pipeline_task_poller_;
      pool_->pipeline_task_poller_ = nullptr;
    }
  }

  ExecPool* pool_;
  bool created_{false};
};

struct ScopedBrMgr {
  ScopedBrMgr(kwdbContext_p ctx, const std::string& addr) {
    auto& br_mgr = BrMgr::GetInstance();
    br_mgr.Destroy();
    try {
      br_mgr.query_rpc_pool_ = std::make_unique<PriorityThreadPool>(
          "query_rpc", 0, std::numeric_limits<uint32_t>::max());
      br_mgr.brpc_stub_cache_ = std::make_unique<BrpcStubCache>();
      br_mgr.data_stream_mgr_ = std::make_unique<DataStreamMgr>();
      auto pos = addr.rfind(':');
      if (pos == std::string::npos) {
        status_ = KStatus::FAIL;
        return;
      }
      br_mgr.address_.SetHostname(addr.substr(0, pos).c_str());
      br_mgr.address_.SetPort(static_cast<k_uint16>(std::stoi(addr.substr(pos + 1))));
      br_mgr.cluster_id_ = options_.cluster_id;
      status_ = KStatus::SUCCESS;
    } catch (...) {
      br_mgr.Destroy();
      status_ = KStatus::FAIL;
    }
  }

  ~ScopedBrMgr() { BrMgr::GetInstance().Destroy(); }

  EngineOptions options_;
  KStatus status_{KStatus::FAIL};
};

TSInputSyncSpec MakeRemoteInboundSpec(k_int32 dest_processor_id) {
  TSInputSyncSpec spec;
  auto* remote_stream = spec.add_streams();
  remote_stream->set_type(StreamEndpointType::REMOTE);
  remote_stream->set_target_node_id(1);
  remote_stream->set_dest_processor(dest_processor_id);
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  return spec;
}

void SetRemoteInboundQueryInfo(kwdbContext_p ctx, DmlExec* dml, k_int64 query_id) {
  TSFlowSpec flow;
  flow.set_queryid(query_id);
  dml->SetBrpcInfo(&flow);
  ctx->dml_exec_handle = dml;
}

void SetQueryInfoWithBrpcAddrs(kwdbContext_p ctx, DmlExec* dml, k_int64 query_id,
                               const std::vector<std::string>& brpc_addrs) {
  TSFlowSpec flow;
  flow.set_queryid(query_id);
  for (const auto& addr : brpc_addrs) {
    flow.add_brpcaddrs(addr);
  }
  dml->SetBrpcInfo(&flow);
  ctx->dml_exec_handle = dml;
}

void AddRouterStream(TSOutputRouterSpec* spec, StreamEndpointType type,
                     k_int32 target_node_id, k_int32 dest_processor_id,
                     k_int32 stream_id) {
  auto* stream = spec->add_streams();
  stream->set_type(type);
  stream->set_target_node_id(target_node_id);
  stream->set_dest_processor(dest_processor_id);
  stream->set_stream_id(stream_id);
}

void DestroySingleFieldTable(TABLE* table) {
  if (table == nullptr) {
    return;
  }
  if (table->fields_ != nullptr && table->field_num_ > 0) {
    SafeDeletePointer(table->fields_[0]);
    free(table->fields_);
    table->fields_ = nullptr;
  }
  delete table;
}

class StubStreamChildOperator : public BaseOperator {
 public:
  explicit StubStreamChildOperator(
      OperatorType type = OperatorType::OPERATOR_NOOP)
      : BaseOperator(nullptr, nullptr, nullptr, 0), type_(type) {
    renders_ = static_cast<Field**>(malloc(sizeof(Field*)));
    renders_[0] = new FieldInt(0, roachpb::DataType::INT, 4);
    num_ = 1;
    output_fields_.push_back(renders_[0]);
  }

  ~StubStreamChildOperator() override {
    free(renders_);
    renders_ = nullptr;
  }

  OperatorType Type() override { return type_; }

  EEIteratorErrCode Init(kwdbContext_p) override {
    ++init_calls_;
    return init_code_;
  }

  EEIteratorErrCode Start(kwdbContext_p) override {
    ++start_calls_;
    return start_code_;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr& chunk) override {
    ++next_calls_;
    if (!queued_chunks_.empty()) {
      chunk = std::move(queued_chunks_.front());
      queued_chunks_.pop_front();
      return EEIteratorErrCode::EE_OK;
    }
    chunk = nullptr;
    return next_code_;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override {
    ++reset_calls_;
    return reset_code_;
  }

  EEIteratorErrCode Close(kwdbContext_p) override {
    ++close_calls_;
    return close_code_;
  }

  BaseOperator* Clone() override {
    auto* cloned = new StubStreamChildOperator(type_);
    cloned->next_code_ = next_code_;
    return cloned;
  }

  int init_calls_{0};
  int start_calls_{0};
  int next_calls_{0};
  int reset_calls_{0};
  int close_calls_{0};
  EEIteratorErrCode init_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode start_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode next_code_{EEIteratorErrCode::EE_END_OF_RECORD};
  EEIteratorErrCode reset_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode close_code_{EEIteratorErrCode::EE_OK};
  std::deque<DataChunkPtr> queued_chunks_;

 private:
  OperatorType type_;
};

struct MergeStep {
  KStatus status{KStatus::SUCCESS};
  DataChunkPtr chunk{nullptr};
  k_bool eos{false};
  k_bool should_exit{false};
};

class ScriptedDataChunkMerger : public DataChunkMerger {
 public:
  explicit ScriptedDataChunkMerger(kwdbContext_p ctx) : DataChunkMerger(ctx) {}

  KStatus Init(const std::vector<DataChunkProvider>&,
               const std::vector<k_uint32>*,
               const SortingRules&) override {
    return KStatus::SUCCESS;
  }

  KStatus Init(const std::vector<DataChunkProvider>&,
               const std::vector<k_uint32>*,
               const std::vector<k_bool>*,
               const std::vector<k_bool>*) override {
    return KStatus::SUCCESS;
  }

  k_bool IsDataReady() override { return data_ready_ || !steps_.empty(); }

  KStatus GetNextMergeChunk(DataChunkPtr* merged_chunk,
                            std::atomic<k_bool>* is_end_eos,
                            k_bool* should_exit) override {
    if (steps_.empty()) {
      if (merged_chunk != nullptr) {
        merged_chunk->reset();
      }
      if (is_end_eos != nullptr) {
        *is_end_eos = true;
      }
      if (should_exit != nullptr) {
        *should_exit = true;
      }
      data_ready_ = false;
      return KStatus::SUCCESS;
    }

    MergeStep step = std::move(steps_.front());
    steps_.pop_front();
    if (merged_chunk != nullptr) {
      *merged_chunk = std::move(step.chunk);
    }
    if (is_end_eos != nullptr) {
      *is_end_eos = step.eos;
    }
    if (should_exit != nullptr) {
      *should_exit = step.should_exit;
    }
    data_ready_ = !steps_.empty();
    return step.status;
  }

  void PushStep(MergeStep step) {
    steps_.push_back(std::move(step));
    data_ready_ = true;
  }

  k_bool data_ready_{false};
  std::deque<MergeStep> steps_;
};

std::shared_ptr<DataStreamRecvr> MakeScriptedMergeRecvr(
    kwdbContext_p ctx, k_int64 query_id, k_int32 dest_processor_id,
    PassThroughChunkBuffer* pass_through_buffer,
    ScriptedDataChunkMerger** merger_out) {
  auto recvr = std::shared_ptr<DataStreamRecvr>(new DataStreamRecvr(
      nullptr, query_id, dest_processor_id, 1, 1024, true, true,
      pass_through_buffer));
  auto merger = std::make_unique<ScriptedDataChunkMerger>(ctx);
  if (merger_out != nullptr) {
    *merger_out = merger.get();
  }
  recvr->cascade_merger_ = std::move(merger);
  return recvr;
}

class SequencedRemoteMergeSortInboundOperator
    : public RemoteMergeSortInboundOperator {
 public:
  using RemoteMergeSortInboundOperator::RemoteMergeSortInboundOperator;

  k_bool IsFinished() override {
    if (!finished_results_.empty()) {
      k_bool result = finished_results_.front();
      finished_results_.pop_front();
      return result;
    }
    return RemoteMergeSortInboundOperator::IsFinished();
  }

  std::deque<k_bool> finished_results_;
};

class RecordingInboundOperator : public InboundOperator {
 public:
  using InboundOperator::InboundOperator;

  OperatorType Type() override { return OperatorType::OPERATOR_LOCAL_IN_BOUND; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  KStatus PushChunk(kwdbContext_p, DataChunkPtr& chunk, k_int32 stream_id,
                    EEIteratorErrCode code) override {
    ++push_calls_;
    last_stream_id_ = stream_id;
    last_code_ = code;
    last_chunk_count_ = chunk ? chunk->Count() : 0;
    return push_status_;
  }

  void PushFinish(EEIteratorErrCode code, k_int32 stream_id,
                  const EEPgErrorInfo& pgInfo) override {
    ++finish_calls_;
    finish_code_ = code;
    finish_stream_id_ = stream_id;
    finish_pg_info_ = pgInfo;
  }

  k_bool NeedInput() override { return need_input_; }

  int push_calls_{0};
  int finish_calls_{0};
  k_int32 last_stream_id_{-1};
  k_int32 finish_stream_id_{-1};
  k_uint32 last_chunk_count_{0};
  EEIteratorErrCode last_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode finish_code_{EEIteratorErrCode::EE_OK};
  EEPgErrorInfo finish_pg_info_;
  KStatus push_status_{KStatus::SUCCESS};
  k_bool need_input_{true};
};

class ExposedInboundOperator : public InboundOperator {
 public:
  using InboundOperator::InboundOperator;

  OperatorType Type() override { return OperatorType::OPERATOR_LOCAL_IN_BOUND; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
};

class ExposedOutboundOperator : public OutboundOperator {
 public:
  using OutboundOperator::OutboundOperator;

  OperatorType Type() override { return OperatorType::OPERATOR_LOCAL_OUT_BOUND; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
};

class ParentOperatorStub : public BaseOperator {
 public:
  ParentOperatorStub() : BaseOperator(nullptr, nullptr, nullptr, 0) {}

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }
  EEIteratorErrCode Start(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }
  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }
  EEIteratorErrCode Close(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }
};

class RunAwareOperator : public BaseOperator {
 public:
  enum class ThrowMode {
    kNone,
    kRuntime,
    kBadAlloc,
    kStdException,
    kUnknown,
  };

  RunAwareOperator() : BaseOperator(nullptr, nullptr, nullptr, 0) {}

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Start(kwdbContext_p) override {
    ++start_calls_;
    return start_code_;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    ++next_calls_;
    if (sleep_ms_ > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_));
    }
    switch (throw_mode_) {
      case ThrowMode::kRuntime:
        throw std::runtime_error("pipeline task failure");
      case ThrowMode::kBadAlloc:
        throw std::bad_alloc();
      case ThrowMode::kStdException:
        throw std::logic_error("pipeline task logic failure");
      case ThrowMode::kUnknown:
        throw 1;
      case ThrowMode::kNone:
        break;
    }
    if (next_index_ < next_codes_.size()) {
      return next_codes_[next_index_++];
    }
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Close(kwdbContext_p) override {
    ++close_calls_;
    return close_code_;
  }

  void PushFinish(EEIteratorErrCode code, k_int32,
                  const EEPgErrorInfo& pgInfo) override {
    ++push_finish_calls_;
    finish_code_ = code;
    finish_pg_info_ = pgInfo;
  }

  void PrintFinishLog() override { ++finish_log_calls_; }

  k_bool HasOutput() override { return has_output_; }

  k_bool NeedInput() override { return need_input_; }

  int start_calls_{0};
  int next_calls_{0};
  int close_calls_{0};
  int push_finish_calls_{0};
  int finish_log_calls_{0};
  size_t next_index_{0};
  EEIteratorErrCode start_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode close_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode finish_code_{EEIteratorErrCode::EE_OK};
  EEPgErrorInfo finish_pg_info_;
  ThrowMode throw_mode_{ThrowMode::kNone};
  int sleep_ms_{0};
  k_bool has_output_{true};
  k_bool need_input_{true};
  std::vector<EEIteratorErrCode> next_codes_;
};

class CloneablePipelineOperator : public BaseOperator {
 public:
  CloneablePipelineOperator() : BaseOperator(nullptr, nullptr, nullptr, 0) {}

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override {
    ++init_calls_;
    return init_code_;
  }

  EEIteratorErrCode Start(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Close(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  BaseOperator* Clone() override {
    if (clone_returns_null_) {
      return nullptr;
    }
    auto* cloned = new CloneablePipelineOperator();
    cloned->init_code_ = clone_init_code_;
    return cloned;
  }

  int init_calls_{0};
  EEIteratorErrCode init_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode clone_init_code_{EEIteratorErrCode::EE_OK};
  bool clone_returns_null_{false};
};

class StreamOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
    InitServerKWDBContext(&ctx_);
    EEPgErrorInfo::ResetPgErrorInfo();
    current_thd = KNEW KWThdContext();
    ExecPool::GetInstance().db_path_ = "./";
    default_br_mgr_ = std::make_unique<ScopedBrMgr>(&ctx_, "127.0.0.1:27000");
    ASSERT_EQ(default_br_mgr_->status_, KStatus::SUCCESS);
  }

  void TearDown() override {
    default_br_mgr_.reset();
    EEPgErrorInfo::ResetPgErrorInfo();
    SafeDeletePointer(current_thd);
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }

  kwdbContext_t ctx_{};
  std::unique_ptr<ScopedBrMgr> default_br_mgr_;
};

TEST_F(StreamOperatorTest, InboundParsesStreamsOrderingAndOutputFields) {
  TSInputSyncSpec spec;
  auto* order_col = spec.mutable_ordering()->add_columns();
  order_col->set_col_idx(1);
  order_col->set_direction(
      TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC);
  auto* local_stream = spec.add_streams();
  local_stream->set_type(StreamEndpointType::LOCAL);
  local_stream->set_stream_id(8);
  auto* remote_stream = spec.add_streams();
  remote_stream->set_type(StreamEndpointType::REMOTE);
  remote_stream->set_target_node_id(12);
  remote_stream->set_dest_processor(99);
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 8));

  TABLE table(1, 1);
  ExposedInboundOperator op(nullptr, &spec, &table);
  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.stream_size_, 2);
  EXPECT_EQ(op.stream_id_, 8);
  EXPECT_EQ(op.target_id_, 12);
  EXPECT_EQ(op.dest_processor_id_, 99);
  ASSERT_EQ(op.order_info_.size(), 1U);
  EXPECT_EQ(op.order_info_[0].col_idx, 1U);
  EXPECT_FALSE(op.asc_order_[0]);
  EXPECT_TRUE(op.null_first_[0]);
  EXPECT_TRUE(op.parser_output_fields_);
  EXPECT_EQ(op.output_fields_.size(), 2U);
  EXPECT_EQ(op.Start(&ctx_), EEIteratorErrCode::EE_OK);
}

TEST_F(StreamOperatorTest, InboundResetCloseAndInvalidFieldEncodingReportErrors) {
  TSInputSyncSpec valid_spec;
  TABLE table(1, 2);
  ExposedInboundOperator op(nullptr, &valid_spec, &table);
  StubStreamChildOperator child;
  child.reset_code_ = EEIteratorErrCode::EE_ERROR;
  child.close_code_ = EEIteratorErrCode::EE_ERROR;
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.Reset(&ctx_), EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_ERROR);

  TSInputSyncSpec invalid_spec;
  auto invalid_type = MarshalToOutputType(KWDBTypeFamily::IntFamily, 32);
  invalid_type.resize(1);
  invalid_spec.add_column_types(invalid_type);
  ExposedInboundOperator invalid_op(nullptr, &invalid_spec, &table);
  EXPECT_EQ(invalid_op.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
}

TEST_F(StreamOperatorTest, LocalInboundHandlesQueueAndFinishStates) {
  TSInputSyncSpec spec;
  TABLE table(1, 3);
  LocalInboundOperator op(nullptr, &spec, &table);
  StubStreamChildOperator child;
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(op.HasOutput());

  DataChunkPtr chunk = MakeIntChunk({1, 2});
  ctx_.wait_for_output = false;
  op.queue_data_size_ = 1024 * 1024 * 16 + 1;
  EXPECT_EQ(op.PushChunk(&ctx_, chunk, 0, EEIteratorErrCode::EE_OK),
            KStatus::FAIL);

  op.queue_data_size_ = 0;
  ASSERT_EQ(op.PushChunk(&ctx_, chunk, 0, EEIteratorErrCode::EE_OK),
            KStatus::SUCCESS);
  EXPECT_TRUE(op.HasOutput());

  DataChunkPtr out = nullptr;
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->Count(), 2U);

  EEPgErrorInfo pg_info;
  pg_info.code = 4321;
  std::snprintf(pg_info.msg, sizeof(pg_info.msg), "%s", "queue failed");
  op.PushFinish(EEIteratorErrCode::EE_ERROR, 0, pg_info);
  out = nullptr;
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);
  EXPECT_TRUE(op.HasOutput());
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
}

TEST_F(StreamOperatorTest, OutboundAndLocalOutboundCoverDegreeChannelsAndNext) {
  TSOutputRouterSpec spec;
  spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_HASH);
  spec.add_hash_columns(0);
  auto* stream = spec.add_streams();
  stream->set_type(StreamEndpointType::LOCAL);
  stream->set_stream_id(7);

  TABLE table(1, 4);
  table.only_tag_ = false;
  table.ptag_size_ = 3;
  table.SetAccessMode(0);

  ExposedOutboundOperator outbound(nullptr, &spec, &table);
  StubStreamChildOperator sampler_child(OperatorType::OPERATOR_SAMPLER);
  outbound.AddDependency(&sampler_child);
  ASSERT_EQ(outbound.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(outbound.part_type_, spec.type());
  EXPECT_EQ(outbound.group_cols_.size(), 1U);
  EXPECT_EQ(outbound.Start(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(outbound.Reset(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(outbound.Close(&ctx_), EEIteratorErrCode::EE_OK);
  outbound.SetDegree(5);
  EXPECT_EQ(outbound.GetDegree(), 1);
  EXPECT_EQ(outbound.CreateTopOutputChannel(&ctx_, outbound.GetChildren()),
            KStatus::SUCCESS);

  ParentOperatorStub parent;
  TSInputSyncSpec inbound_spec;
  RecordingInboundOperator inbound(nullptr, &inbound_spec, &table);
  parent.AddDependency(&outbound);
  parent.AddDependency(&inbound);
  std::vector<BaseOperator*> new_ops;
  EXPECT_EQ(outbound.CreateOutputChannel(&ctx_, new_ops), KStatus::SUCCESS);
  EXPECT_EQ(parent.GetChildren().size(), 1U);
  EXPECT_EQ(inbound.GetChildren().back(), &outbound);

  LocalOutboundOperator queue_outbound(nullptr, &spec, &table);
  StubStreamChildOperator queue_child;
  queue_outbound.AddDependency(&queue_child);
  RecordingInboundOperator queue_parent(nullptr, &inbound_spec, &table);
  queue_parent.push_status_ = KStatus::FAIL;
  queue_parent.need_input_ = false;
  queue_parent.AddDependency(&queue_outbound);
  ASSERT_EQ(queue_outbound.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(queue_outbound.NeedInput());

  DataChunkPtr queued = MakeIntChunk({9});
  EXPECT_EQ(queue_outbound.PushChunk(&ctx_, queued, 7, EEIteratorErrCode::EE_OK),
            KStatus::FAIL);

  queue_child.queued_chunks_.push_back(MakeIntChunk({9}));
  queue_parent.push_status_ = KStatus::SUCCESS;
  DataChunkPtr out = nullptr;
  EXPECT_EQ(queue_outbound.Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(queue_parent.push_calls_, 2);
  EXPECT_EQ(queue_parent.last_stream_id_, 7);
  queue_child.next_code_ = EEIteratorErrCode::EE_END_OF_RECORD;
  EXPECT_EQ(queue_outbound.Next(&ctx_, out), EEIteratorErrCode::EE_END_OF_RECORD);

  EEPgErrorInfo finish_info;
  finish_info.code = 88;
  std::snprintf(finish_info.msg, sizeof(finish_info.msg), "%s", "done");
  queue_outbound.PushFinish(EEIteratorErrCode::EE_END_OF_RECORD, 0, finish_info);
  EXPECT_EQ(queue_parent.finish_calls_, 1);
  EXPECT_EQ(queue_parent.finish_stream_id_, 7);

  LocalOutboundOperator local_out(nullptr, &spec, &table);
  StubStreamChildOperator local_child;
  local_child.queued_chunks_.push_back(MakeIntChunk({5, 6}));
  local_out.AddDependency(&local_child);
  ASSERT_EQ(local_out.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(local_out.stream_id_, 7U);
  out = nullptr;
  EXPECT_EQ(local_out.Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->Count(), 2U);
  EXPECT_EQ(local_out.total_push_count_, 1U);

  BaseOperator* cloned = local_out.Clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->Type(), OperatorType::OPERATOR_LOCAL_OUT_BOUND);
  delete cloned;
}

TEST_F(StreamOperatorTest, OpFactoryCreatesPublicOperatorEntryPoints) {
  TABLE table(1, 5);
  TABLE* table_ptr = &table;
  BaseOperator* iter = nullptr;
  ASSERT_EQ(OpFactory::NewResultCollectorOp(&ctx_, &iter), KStatus::SUCCESS);
  ASSERT_NE(iter, nullptr);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_RESULT_COLLECTOR);
  delete iter;
  iter = nullptr;

  TSInputSyncSpec input_spec;
  EXPECT_EQ(OpFactory::NewInboundOperator(&ctx_, nullptr, &input_spec, &iter,
                                          &table_ptr, false, false),
            KStatus::SUCCESS);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_LOCAL_IN_BOUND);
  delete iter;
  iter = nullptr;

  EXPECT_EQ(OpFactory::NewInboundOperator(&ctx_, nullptr, &input_spec, &iter,
                                          &table_ptr, false, true),
            KStatus::SUCCESS);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_LOCAL_MERGE_IN_BOUND);
  delete iter;
  iter = nullptr;

  EXPECT_EQ(OpFactory::NewInboundOperator(&ctx_, nullptr, &input_spec, &iter,
                                          &table_ptr, true, false),
            KStatus::SUCCESS);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_REMOTE_IN_BOUND);
  delete iter;
  iter = nullptr;

  EXPECT_EQ(OpFactory::NewInboundOperator(&ctx_, nullptr, &input_spec, &iter,
                                          &table_ptr, true, true),
            KStatus::SUCCESS);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_REMOTE_MERGE_SORT_IN_BOUND);
  delete iter;
  iter = nullptr;

  TSOutputRouterSpec output_spec;
  EXPECT_EQ(OpFactory::NewOutboundOperator(&ctx_, nullptr, &output_spec, &iter,
                                           &table_ptr, false),
            KStatus::SUCCESS);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_LOCAL_OUT_BOUND);
  delete iter;
  iter = nullptr;

  EXPECT_EQ(OpFactory::NewOutboundOperator(&ctx_, nullptr, &output_spec, &iter,
                                           &table_ptr, true),
            KStatus::SUCCESS);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_REMOTR_OUT_BOUND);
  delete iter;
  iter = nullptr;

  TSProcessorSpec proc_spec;
  TSProcessorCoreUnion noop_core;
  noop_core.mutable_noop();
  PostProcessSpec post;
  proc_spec.set_final_ts_processor(true);
  EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc_spec, post, noop_core, &iter,
                             &table_ptr, 11, false),
            KStatus::SUCCESS);
  ASSERT_NE(iter, nullptr);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_NOOP);
  EXPECT_TRUE(iter->IsFinalOperator());
  delete iter;
  iter = nullptr;

  EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc_spec, post, noop_core, &iter,
                             &table_ptr, 11, true),
            KStatus::SUCCESS);
  ASSERT_NE(iter, nullptr);
  EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_PASSTHROUGH_NOOP);
  delete iter;
  iter = nullptr;

  TSProcessorCoreUnion invalid_core;
  EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, TSProcessorSpec{}, post,
                             invalid_core, &iter, &table_ptr, 12, false),
            KStatus::FAIL);
  EXPECT_EQ(iter, nullptr);
}

TEST_F(StreamOperatorTest, OpFactoryCreatesDirectFactoryBranches) {
  TABLE table(1, 6);
  TABLE* table_ptr = &table;
  BaseOperator* iter = nullptr;

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    InitBasicTagReader(core.mutable_tagreader(), TSTableReadMode::metaTable);
    EXPECT_EQ(OpFactory::NewTagScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 1),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_TAG_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    InitBasicTagReader(core.mutable_tagreader(), TSTableReadMode::hashTagScan);
    EXPECT_EQ(OpFactory::NewTagScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 1),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_HASH_TAG_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post;
    TSProcessorCoreUnion core;
    InitBasicTableReader(core.mutable_tablereader());
    EXPECT_EQ(OpFactory::NewTableScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 2),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_TABLE_SCAN);
    EXPECT_EQ(post.output_columns_size(), 1);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* reader = core.mutable_tablereader();
    InitBasicTableReader(reader);
    reader->mutable_aggregator();
    EXPECT_EQ(OpFactory::NewTableScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 3),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_AGG_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* reader = core.mutable_tablereader();
    InitBasicTableReader(reader);
    reader->mutable_sorter();
    EXPECT_EQ(OpFactory::NewTableScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 4),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_SORT_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    core.mutable_aggregator()->set_agg_push_down(true);
    EXPECT_EQ(OpFactory::NewAgg(&ctx_, nullptr, post, core, &iter, &table_ptr, 5),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_POST_AGG_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* agg = core.mutable_aggregator();
    agg->add_group_cols(0);
    agg->add_ordered_group_cols(0);
    EXPECT_EQ(OpFactory::NewAgg(&ctx_, nullptr, post, core, &iter, &table_ptr, 6),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_SORT_GROUP_BY);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* agg = core.mutable_aggregator();
    agg->set_group_window_id(-1);
    agg->add_group_cols(0);
    EXPECT_EQ(OpFactory::NewAgg(&ctx_, nullptr, post, core, &iter, &table_ptr, 7),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_HASH_GROUP_BY);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* sorter = core.mutable_sorter()->mutable_output_ordering()->add_columns();
    sorter->set_col_idx(0);
    sorter->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);
    EXPECT_EQ(OpFactory::NewSort(&ctx_, nullptr, post, core, &iter, &table_ptr, 8),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_ORDER_BY);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    core.mutable_distinct()->add_distinct_columns(0);
    EXPECT_EQ(OpFactory::NewDistinct(&ctx_, nullptr, post, core, &iter, &table_ptr, 9),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_DISTINCT);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* statistic = core.mutable_statisticreader();
    statistic->set_tableid(1);
    statistic->set_tableversion(1);
    statistic->set_lastrowopt(false);
    EXPECT_EQ(OpFactory::NewStatisticScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 10),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_STATISTIC_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    core.mutable_window()->add_partitionby(0);
    EXPECT_EQ(OpFactory::NewWindowScan(&ctx_, nullptr, post, core, &iter, &table_ptr, 11),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_WINDOWS);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    auto* output = proc.add_output();
    output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
    auto* stream = output->add_streams();
    stream->set_type(StreamEndpointType::LOCAL);
    TSProcessorCoreUnion core;
    core.mutable_synchronizer()->set_degree(4);
    PostProcessSpec post = MakeSingleOutputPost();
    EXPECT_EQ(OpFactory::NewSynchronizer(&ctx_, nullptr, proc, post, core, &iter, &table_ptr, 12),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_LOCAL_OUT_BOUND);
    EXPECT_EQ(dynamic_cast<OutboundOperator*>(iter)->degree_, 4);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    auto* output = proc.add_output();
    output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
    auto* stream = output->add_streams();
    stream->set_type(StreamEndpointType::REMOTE);
    TSProcessorCoreUnion core;
    core.mutable_synchronizer()->set_degree(2);
    PostProcessSpec post = MakeSingleOutputPost();
    EXPECT_EQ(OpFactory::NewSynchronizer(&ctx_, nullptr, proc, post, core, &iter, &table_ptr, 13),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_REMOTR_OUT_BOUND);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorCoreUnion core;
    auto* sampler = core.mutable_sampler();
    sampler->set_sample_size(1);
    sampler->set_table_id(1);
    EXPECT_EQ(OpFactory::NewTsSampler(&ctx_, nullptr, core, &iter, &table_ptr, 14),
              KStatus::FAIL);
    EXPECT_EQ(iter, nullptr);
  }

  {
    TSProcessorCoreUnion core;
    auto* sampler = core.mutable_sampler();
    sampler->set_sample_size(1);
    sampler->set_table_id(1);
    auto* sketch = sampler->add_sketches();
    sketch->set_sketch_type(HLL_PLUS_PLUS);
    sketch->set_generatehistogram(false);
    sketch->add_col_idx(0);
    sketch->add_col_type(0);
    sketch->set_hasallptag(false);
    EXPECT_EQ(OpFactory::NewTsSampler(&ctx_, nullptr, core, &iter, &table_ptr, 15),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_SAMPLER);
    delete iter;
  }
}

TEST_F(StreamOperatorTest, OpFactoryNewOpAndRecreateCoverRemainingBranches) {
  TABLE table(1, 7);
  TABLE* table_ptr = &table;
  BaseOperator* iter = nullptr;

  {
    TSProcessorSpec proc;
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    auto* statistic = core.mutable_statisticreader();
    statistic->set_tableid(1);
    statistic->set_tableversion(1);
    statistic->set_lastrowopt(false);
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               20, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_STATISTIC_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    auto* sampler = core.mutable_sampler();
    sampler->set_sample_size(1);
    sampler->set_table_id(1);
    auto* sketch = sampler->add_sketches();
    sketch->set_sketch_type(HLL_PLUS_PLUS);
    sketch->set_generatehistogram(false);
    sketch->add_col_idx(0);
    sketch->add_col_type(0);
    sketch->set_hasallptag(false);
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               21, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_SAMPLER);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    core.mutable_window()->add_partitionby(0);
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               22, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_WINDOWS);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    auto* output = proc.add_output();
    output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
    output->add_streams()->set_type(StreamEndpointType::LOCAL);
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    core.mutable_synchronizer()->set_degree(2);
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               23, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_LOCAL_OUT_BOUND);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    auto* sorter = core.mutable_sorter()->mutable_output_ordering()->add_columns();
    sorter->set_col_idx(0);
    sorter->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               24, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_ORDER_BY);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    core.mutable_distinct()->add_distinct_columns(0);
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               25, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_DISTINCT);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorCoreUnion core;
    PostProcessSpec post = MakeSingleOutputPost();
    core.mutable_noop();
    EXPECT_EQ(OpFactory::NewNoop(&ctx_, nullptr, post, core, &iter, &table_ptr, 26,
                                 false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_NOOP);
    delete iter;
    iter = nullptr;

    EXPECT_EQ(OpFactory::NewNoop(&ctx_, nullptr, post, core, &iter, &table_ptr, 27,
                                 true),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_PASSTHROUGH_NOOP);
    delete iter;
    iter = nullptr;
  }

  {
    auto* rewrite_table = new TABLE(1, 9);
    rewrite_table->field_num_ = 1;
    rewrite_table->fields_ = static_cast<Field**>(malloc(sizeof(Field*)));
    rewrite_table->fields_[0] = new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32));
    rewrite_table->fields_[0]->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);

    TSReaderSpec reader;
    InitBasicTableReader(&reader);
    PostProcessSpec scan_post;
    auto* child = new TableScanOperator(nullptr, &reader, &scan_post, rewrite_table, 31);

    TSAggregatorSpec agg;
    agg.set_group_window_id(-1);
    agg.add_group_cols(0);
    PostProcessSpec agg_post;
    auto* parent = new HashAggregateOperator(nullptr, &agg, &agg_post, rewrite_table, 32);
    ParentOperatorStub grand_parent;
    parent->AddDependency(child);
    grand_parent.AddDependency(parent);
    parent->SetFinalOperator(true);
    parent->SetOutputEncoding(true);
    parent->SetUseQueryShortCircuit(true);
    parent->SetUseUseCompressType(static_cast<PgCompressMode>(3));

    std::vector<BaseOperator*> operators{parent};
    EXPECT_EQ(OpFactory::ReCreateOperoatr(&ctx_, child, &operators), KStatus::SUCCESS);
    ASSERT_EQ(operators.size(), 1U);
    ASSERT_NE(operators[0], nullptr);
    EXPECT_EQ(operators[0]->Type(), OperatorType::OPERATOR_SORT_GROUP_BY);
    EXPECT_TRUE(operators[0]->IsFinalOperator());
    EXPECT_TRUE(operators[0]->GetOutputEncoding());
    EXPECT_TRUE(operators[0]->IsUseQueryShortCircuit());
    EXPECT_EQ(operators[0]->GetUseUseCompressType(), static_cast<PgCompressMode>(3));

    delete operators[0];
    delete child;
    DestroySingleFieldTable(rewrite_table);
  }
}

TEST_F(StreamOperatorTest, OpFactoryNewOpDispatchesStatisticSamplerAndWindow) {
  TABLE table(1, 11);
  TABLE* table_ptr = &table;
  BaseOperator* iter = nullptr;

  {
    TSProcessorSpec proc;
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* statistic = core.mutable_statisticreader();
    statistic->set_tableid(1);
    statistic->set_tableversion(1);
    statistic->set_lastrowopt(false);

    ASSERT_TRUE(core.has_statisticreader());
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               40, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_STATISTIC_SCAN);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    auto* sampler = core.mutable_sampler();
    sampler->set_sample_size(1);
    sampler->set_table_id(1);
    auto* sketch = sampler->add_sketches();
    sketch->set_sketch_type(HLL_PLUS_PLUS);
    sketch->set_generatehistogram(false);
    sketch->add_col_idx(0);
    sketch->add_col_type(0);
    sketch->set_hasallptag(false);

    ASSERT_TRUE(core.has_sampler());
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               41, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_SAMPLER);
    delete iter;
    iter = nullptr;
  }

  {
    TSProcessorSpec proc;
    PostProcessSpec post = MakeSingleOutputPost();
    TSProcessorCoreUnion core;
    core.mutable_window()->add_partitionby(0);

    ASSERT_TRUE(core.has_window());
    EXPECT_EQ(OpFactory::NewOp(&ctx_, nullptr, proc, post, core, &iter, &table_ptr,
                               42, false),
              KStatus::SUCCESS);
    ASSERT_NE(iter, nullptr);
    EXPECT_EQ(iter->Type(), OperatorType::OPERATOR_WINDOWS);
    delete iter;
  }
}

TEST_F(StreamOperatorTest, OpFactoryRecreateOperatorRewritesHashAggregate) {
  auto* rewrite_table = new TABLE(1, 12);
  rewrite_table->field_num_ = 1;
  rewrite_table->fields_ = static_cast<Field**>(malloc(sizeof(Field*)));
  rewrite_table->fields_[0] =
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32));
  rewrite_table->fields_[0]->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);

  TSReaderSpec reader;
  InitBasicTableReader(&reader);
  PostProcessSpec scan_post;
  scan_post.add_output_columns(0);
  auto* child =
      new TableScanOperator(nullptr, &reader, &scan_post, rewrite_table, 43);

  TSAggregatorSpec agg;
  agg.set_group_window_id(-1);
  agg.add_group_cols(0);
  PostProcessSpec agg_post;
  auto* parent =
      new HashAggregateOperator(nullptr, &agg, &agg_post, rewrite_table, 44);
  ParentOperatorStub grand_parent;
  parent->AddDependency(child);
  grand_parent.AddDependency(parent);
  parent->SetFinalOperator(true);
  parent->SetOutputEncoding(true);
  parent->SetUseQueryShortCircuit(true);
  parent->SetUseUseCompressType(static_cast<PgCompressMode>(3));

  std::vector<BaseOperator*> operators{parent};
  EXPECT_EQ(OpFactory::ReCreateOperoatr(&ctx_, child, &operators),
            KStatus::SUCCESS);
  ASSERT_EQ(operators.size(), 1U);
  ASSERT_NE(operators[0], nullptr);
  EXPECT_EQ(operators[0]->Type(), OperatorType::OPERATOR_SORT_GROUP_BY);
  EXPECT_TRUE(operators[0]->IsFinalOperator());
  EXPECT_TRUE(operators[0]->GetOutputEncoding());
  EXPECT_TRUE(operators[0]->IsUseQueryShortCircuit());
  EXPECT_EQ(operators[0]->GetUseUseCompressType(), static_cast<PgCompressMode>(3));
  EXPECT_EQ(operators[0]->GetChildren().size(), 1U);
  EXPECT_EQ(operators[0]->GetChildren()[0], child);

  delete operators[0];
  delete child;
  DestroySingleFieldTable(rewrite_table);
}

TEST_F(StreamOperatorTest, OpFactoryRecreateOperatorKeepsParentOnBadRenderExpr) {
  auto* rewrite_table = new TABLE(1, 13);
  rewrite_table->field_num_ = 1;
  rewrite_table->fields_ = static_cast<Field**>(malloc(sizeof(Field*)));
  rewrite_table->fields_[0] =
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32));
  rewrite_table->fields_[0]->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);

  TSReaderSpec reader;
  InitBasicTableReader(&reader);
  PostProcessSpec scan_post;
  scan_post.add_render_exprs()->set_expr("@bad");
  auto* child =
      new TableScanOperator(nullptr, &reader, &scan_post, rewrite_table, 45);

  TSAggregatorSpec agg;
  agg.set_group_window_id(-1);
  agg.add_group_cols(0);
  PostProcessSpec agg_post;
  auto* parent =
      new HashAggregateOperator(nullptr, &agg, &agg_post, rewrite_table, 46);
  ParentOperatorStub grand_parent;
  parent->AddDependency(child);
  grand_parent.AddDependency(parent);

  std::vector<BaseOperator*> operators{parent};
  EXPECT_EQ(OpFactory::ReCreateOperoatr(&ctx_, child, &operators),
            KStatus::SUCCESS);
  ASSERT_EQ(operators.size(), 1U);
  EXPECT_EQ(operators[0], parent);
  EXPECT_EQ(parent->Type(), OperatorType::OPERATOR_HASH_GROUP_BY);

  delete parent;
  delete child;
  DestroySingleFieldTable(rewrite_table);
}

TEST_F(StreamOperatorTest, PipelineGroupAndTaskCoverDependencyAndParallelBranches) {
  TSOutputRouterSpec spec;
  spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_HASH);
  auto* stream = spec.add_streams();
  stream->set_type(StreamEndpointType::LOCAL);
  stream->set_stream_id(3);

  TABLE table(1, 4);
  table.only_tag_ = false;
  table.ptag_size_ = 4;
  table.SetAccessMode(0);

  StubStreamChildOperator child;
  ExposedOutboundOperator outbound(nullptr, &spec, &table);
  outbound.AddDependency(&child);
  ASSERT_EQ(outbound.Init(&ctx_), EEIteratorErrCode::EE_OK);
  outbound.SetDegree(3);

  PipelineGroup root(nullptr);
  root.SetPipelineOperator(&outbound);
  root.SetSink(&outbound);
  root.SetSource(&child);
  root.AddOperator(&outbound);
  EXPECT_TRUE(root.HasOperator());
  EXPECT_TRUE(root.IsParallel());
  EXPECT_EQ(root.GetDegree(), 3);

  PipelineGroup downstream(nullptr);
  root.SetDependenciePipeline(&downstream);
  std::vector<PipelineGroup*> pipelines;
  root.GetPipelines(pipelines, true);
  ASSERT_EQ(pipelines.size(), 2U);
  EXPECT_EQ(pipelines[0], &root);
  EXPECT_EQ(pipelines[1], &downstream);

  auto task = root.CreateTask(&ctx_);
  ASSERT_NE(task, nullptr);
  EXPECT_EQ(root.total_task_, 1U);
  EXPECT_EQ(task->operator_, &outbound);

  auto dependency = std::make_shared<PipelineTask>(&root);
  ASSERT_EQ(dependency->Init(&ctx_), KStatus::SUCCESS);
  task->AddDependency(dependency);
  EXPECT_TRUE(task->HasDependency());
  EXPECT_FALSE(task->is_can_schedule());
  task->state_ = PipelineTaskState::PS_HAS_OUTPUT;
  EXPECT_TRUE(task->is_can_schedule());
  task->state_ = PipelineTaskState::PS_STARTED;

  root.Cancel();
  EXPECT_TRUE(task->is_cancel_);

  EEPgErrorInfo pg_info;
  pg_info.code = 9001;
  std::snprintf(pg_info.msg, sizeof(pg_info.msg), "%s", "pipeline failed");
  EXPECT_EQ(root.FinishTask(EEIteratorErrCode::EE_ERROR, pg_info), 0U);
  EXPECT_EQ(root.code_, EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(root.pg_info_.code, 9001);

  task->UpdateStartTime();
  EXPECT_GE(task->DurationTimes(), 0);
  EXPECT_EQ(task->GetDegree(), 3);
}

TEST_F(StreamOperatorTest, PipelineTaskUtilityMethodsCloneAndBlocked) {
  ExecPool& pool = ExecPool::GetInstance();
  ScopedExecPoolPoller scoped_poller(&pool);
  Processors processors;
  TABLE table(1, 8);

  PipelineGroup group(&processors);
  CloneablePipelineOperator clone_source;
  auto task = std::make_shared<PipelineTask>(&group);
  ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
  task->SetOperator(&clone_source);
  task->SetTable(&table);
  EXPECT_EQ(task->table_, &table);
  EXPECT_EQ(task->GetDegree(), 1);
  task->SetStop();
  EXPECT_TRUE(task->is_stop_);
  task->Wait();

  task->Blocked(&ctx_);
  EXPECT_EQ(task->state_, PipelineTaskState::PS_BLOCKED);

  auto parent_task = std::make_shared<PipelineTask>(&group);
  ASSERT_EQ(parent_task->Init(&ctx_), KStatus::SUCCESS);
  task->parents_.push_back(parent_task.get());
  group.AddPipelineTask(task);

  std::vector<std::shared_ptr<PipelineTask>> cloned_tasks;
  std::vector<BaseOperator*> new_operators;
  EXPECT_EQ(task->Clone(&ctx_, 2, cloned_tasks, new_operators), KStatus::SUCCESS);
  EXPECT_EQ(cloned_tasks.size(), 2U);
  EXPECT_EQ(new_operators.size(), 2U);
  EXPECT_EQ(group.total_task_, 3U);
  for (auto& cloned_task : cloned_tasks) {
    EXPECT_TRUE(cloned_task->is_clone_);
    cloned_task->is_clone_ = false;
  }
  cloned_tasks.clear();
  for (auto* oper : new_operators) {
    delete oper;
  }

  clone_source.clone_returns_null_ = true;
  cloned_tasks.clear();
  new_operators.clear();
  EXPECT_EQ(task->Clone(&ctx_, 1, cloned_tasks, new_operators), KStatus::FAIL);

  clone_source.clone_returns_null_ = false;
  clone_source.clone_init_code_ = EEIteratorErrCode::EE_ERROR;
  EXPECT_EQ(task->Clone(&ctx_, 1, cloned_tasks, new_operators), KStatus::FAIL);

  RunAwareOperator close_oper;
  auto close_task = std::make_shared<PipelineTask>(&group);
  ASSERT_EQ(close_task->Init(&ctx_), KStatus::SUCCESS);
  close_task->SetOperator(&close_oper);
  close_task->is_clone_ = true;
  group.total_task_ = 2;
  group.finish_task_ = 0;
  close_task->Close(&ctx_, EEIteratorErrCode::EE_OK);
  EXPECT_EQ(close_oper.finish_log_calls_, 1);
  EXPECT_EQ(close_oper.close_calls_, 1);
  EXPECT_EQ(close_task->state_, PipelineTaskState::PS_FINISHED);
}

TEST_F(StreamOperatorTest, PipelineTaskRunCoversStartQueueFullAndExceptionPaths) {
  Processors processors;

  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    ctx_.wait_for_output = false;
    oper.next_codes_ = {EEIteratorErrCode::EE_QUEUE_FULL,
                        EEIteratorErrCode::EE_END_OF_RECORD};
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_TRUE(ctx_.wait_for_output);
    EXPECT_EQ(oper.start_calls_, 1);
    EXPECT_EQ(oper.next_calls_, 2);
    EXPECT_EQ(oper.push_finish_calls_, 1);
    EXPECT_EQ(task->state_, PipelineTaskState::PS_FINISHED);
  }

  EEPgErrorInfo::ResetPgErrorInfo();
  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.start_code_ = EEIteratorErrCode::EE_ERROR;
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.start_calls_, 1);
    EXPECT_EQ(oper.push_finish_calls_, 1);
    EXPECT_EQ(task->state_, PipelineTaskState::PS_FINISHED);
  }

  EEPgErrorInfo::ResetPgErrorInfo();
  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.throw_mode_ = RunAwareOperator::ThrowMode::kRuntime;
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.start_calls_, 1);
    EXPECT_EQ(oper.next_calls_, 1);
    EXPECT_EQ(oper.push_finish_calls_, 1);
    EXPECT_EQ(task->state_, PipelineTaskState::PS_FINISHED);
    EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, ERRCODE_INTERNAL_ERROR);
  }
}

TEST_F(StreamOperatorTest, PipelineTaskRunCoversAdditionalStateBranches) {
  ExecPool& pool = ExecPool::GetInstance();
  ScopedExecPoolPoller scoped_poller(&pool);
  Processors processors;

  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    auto task = std::make_shared<PipelineTask>(&group);
    ctx_.use_dst = true;
    std::snprintf(ctx_.timezone_name, sizeof(ctx_.timezone_name), "%s", "UTC");
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.push_finish_calls_, 1);
  }

  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    task->SetStop();
    task->Run(&ctx_);
    EXPECT_EQ(oper.start_calls_, 0);
    EXPECT_EQ(task->state_, PipelineTaskState::PS_STARTED);
  }

  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    RunAwareOperator source;
    source.has_output_ = false;
    group.SetPipelineOperator(&oper);
    group.SetSource(&source);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(task->state_, PipelineTaskState::PS_BLOCKED);
  }

  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.sleep_ms_ = 120;
    oper.next_codes_ = {EEIteratorErrCode::EE_NEXT_CONTINUE,
                        EEIteratorErrCode::EE_END_OF_RECORD};
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.next_calls_, 2);
    EXPECT_EQ(task->state_, PipelineTaskState::PS_FINISHED);
  }

  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.sleep_ms_ = 120;
    oper.next_codes_ = {EEIteratorErrCode::EE_OK, EEIteratorErrCode::EE_END_OF_RECORD};
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.next_calls_, 2);
    EXPECT_EQ(task->state_, PipelineTaskState::PS_FINISHED);
  }

  EEPgErrorInfo::ResetPgErrorInfo();
  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.throw_mode_ = RunAwareOperator::ThrowMode::kBadAlloc;
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.push_finish_calls_, 1);
    EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, ERRCODE_INTERNAL_ERROR);
  }

  EEPgErrorInfo::ResetPgErrorInfo();
  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.throw_mode_ = RunAwareOperator::ThrowMode::kStdException;
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.push_finish_calls_, 1);
    EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, ERRCODE_INTERNAL_ERROR);
  }

  EEPgErrorInfo::ResetPgErrorInfo();
  {
    PipelineGroup group(&processors);
    RunAwareOperator oper;
    oper.throw_mode_ = RunAwareOperator::ThrowMode::kUnknown;
    group.SetPipelineOperator(&oper);
    auto task = std::make_shared<PipelineTask>(&group);
    ASSERT_EQ(task->Init(&ctx_), KStatus::SUCCESS);
    task->SetOperator(&oper);
    group.AddPipelineTask(task);
    KWThdContext* main_thd = current_thd;
    task->Run(&ctx_);
    current_thd = main_thd;
    EXPECT_EQ(oper.push_finish_calls_, 1);
    EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, ERRCODE_INTERNAL_ERROR);
  }
}

TEST_F(StreamOperatorTest, RemoteInboundInitErrorAndFieldUpdateBranches) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 9);
  RemoteInboundOperator op(nullptr, &spec, &table);

  EXPECT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
  ASSERT_EQ(op.OutputFields().size(), 1U);

  auto chunk = MakeIntChunk({11});
  op.SetOutputFieldInfo(chunk);
  EXPECT_EQ(op.OutputFields()[0]->get_storage_type(), roachpb::DataType::INT);
  EXPECT_EQ(op.OutputFields()[0]->get_storage_length(), sizeof(k_int32));

  op.status_.add_error_msgs("");
  op.ReceiveChunkNotify(0, 0, "");
  EXPECT_EQ(op.new_chunks_num_, 1);
  op.ReceiveChunkNotify(0, 4321, "remote failed");
  EXPECT_EQ(op.status_.status_code(), 4321);
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
}

TEST_F(StreamOperatorTest, RouterOutboundChannelInitLocalityAndSelectiveRows) {
  TSOutputRouterSpec spec;
  TABLE table(1, 10);
  RouterOutboundOperator op(nullptr, &spec, &table);

  PassThroughChunkBuffer pass_through_buffer(77);
  TNetworkAddress empty_addr;
  RouterOutboundOperator::Channel invalid(
      &op, empty_addr, 1, 2, 3, 4, &pass_through_buffer);
  EXPECT_EQ(invalid.Init(), KStatus::FAIL);

  TNetworkAddress addr;
  addr.SetHostname("127.0.0.1");
  addr.SetPort(12345);

  RouterOutboundOperator::Channel local(
      &op, addr, -1, 2, 3, 4, &pass_through_buffer);
  local.SetType(StreamEndpointType::LOCAL);
  EXPECT_TRUE(local.IsLocal());
  EXPECT_EQ(local.Init(), KStatus::SUCCESS);
  EXPECT_TRUE(local.CheckPassThrough());
  EXPECT_TRUE(local.UsePassThrougth());
  EXPECT_EQ(local.Close(), KStatus::FAIL);

  auto chunk = MakeIntChunk({1, 2, 3});
  const k_uint32 indexes[] = {0, 2};
  EXPECT_EQ(local.AddRowsSelective(chunk.get(), 0, indexes, 2), KStatus::SUCCESS);
  ASSERT_EQ(local.chunks_.size(), 1U);
  ASSERT_NE(local.chunks_[0], nullptr);
  EXPECT_EQ(local.chunks_[0]->Count(), 2U);

  RouterOutboundOperator::Channel remote(
      &op, addr, -1, 6, 7, 8, &pass_through_buffer);
  remote.SetType(StreamEndpointType::REMOTE);
  EXPECT_FALSE(remote.IsLocal());
  EXPECT_FALSE(remote.CheckPassThrough());
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest, RemoteInboundCanPullQueuedChunksAndSurfaceErrors) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 11);
  PassThroughChunkBuffer pass_through_buffer(70031);

  RemoteInboundOperator op(nullptr, &spec, &table);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");
  auto recvr = std::shared_ptr<DataStreamRecvr>(
      new DataStreamRecvr(nullptr, 70031, 19, 1, 1024, false, false,
                          &pass_through_buffer));
  op.stream_recvr_ = recvr;
  op.query_id_ = 70031;
  op.dest_processor_id_ = 19;

  DataChunkPtr chunk = MakeIntChunk({4, 5});
  const size_t chunk_size = chunk->Size() + 20;
  recvr->GetPassThroughContext().AppendChunk(7, chunk, chunk_size, 0);

  PTransmitChunkParams request;
  request.set_query_id(70031);
  request.set_dest_processor(19);
  request.set_sender_id(7);
  request.set_be_number(7);
  request.set_use_pass_through(true);
  EXPECT_EQ(recvr->AddChunks(request, nullptr).code(), BRStatusCode::OK);
  recvr->RemoveSender(7, 7);

  EXPECT_TRUE(op.HasOutput());
  EXPECT_EQ(op.SetFinishing(), KStatus::SUCCESS);

  DataChunkPtr out;
  ASSERT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->Count(), 2U);
  EXPECT_EQ(op.total_rows_, 2U);
  EXPECT_FALSE(op.parser_output_fields_);

  out.reset();
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_TRUE(op.IsFinished());
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);

  RemoteInboundOperator err_op(nullptr, &spec, &table);
  err_op.status_.set_status_code(0);
  err_op.status_.add_error_msgs("");
  auto err_recvr = std::shared_ptr<DataStreamRecvr>(
      new DataStreamRecvr(nullptr, 70032, 23, 1, 1024, false, false,
                          &pass_through_buffer));
  err_op.stream_recvr_ = err_recvr;
  err_op.ReceiveChunkNotify(0, 90210, "remote inbound failed");
  out.reset();
  EXPECT_EQ(err_op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(err_op.status_.status_code(), 90210);
  EXPECT_EQ(err_op.Close(&ctx_), EEIteratorErrCode::EE_OK);

  recvr->closed_ = true;
  op.stream_recvr_.reset();
  err_recvr->closed_ = true;
  err_op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest, RemoteInboundTreatsFinishedReceiverAsOutput) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 111);
  PassThroughChunkBuffer pass_through_buffer(70041);

  RemoteInboundOperator op(nullptr, &spec, &table);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");
  auto recvr = std::shared_ptr<DataStreamRecvr>(
      new DataStreamRecvr(nullptr, 70041, 31, 1, 1024, false, false,
                          &pass_through_buffer));
  op.stream_recvr_ = recvr;
  op.query_id_ = 70041;
  op.dest_processor_id_ = 31;

  EXPECT_FALSE(op.HasOutput());
  recvr->RemoveSender(7, 7);
  EXPECT_TRUE(op.IsFinished());
  EXPECT_TRUE(op.HasOutput());

  DataChunkPtr out = nullptr;
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_END_OF_RECORD);

  recvr->closed_ = true;
  op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest, RemoteInboundPullChunkReturnsEmptySuccessWithoutQueuedData) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 117);
  PassThroughChunkBuffer pass_through_buffer(70040);

  RemoteInboundOperator op(nullptr, &spec, &table);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");
  op.stream_recvr_ = std::shared_ptr<DataStreamRecvr>(
      new DataStreamRecvr(nullptr, 70040, 30, 1, 1024, false, false,
                          &pass_through_buffer));

  DataChunkPtr out = nullptr;
  EXPECT_EQ(op.PullChunk(&ctx_, out), KStatus::SUCCESS);
  EXPECT_EQ(out, nullptr);

  op.stream_recvr_->closed_ = true;
  op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest, RemoteInboundPushFinishErrorAndSetFinishBranches) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 118);
  RemoteInboundOperator op(nullptr, &spec, &table);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");

  EEPgErrorInfo err_info;
  err_info.code = 789;
  std::strncpy(err_info.msg, "push finish failed", sizeof(err_info.msg) - 1);
  op.PushFinish(EEIteratorErrCode::EE_ERROR, 6, err_info);
  EXPECT_EQ(op.status_.status_code(), 789);
  EXPECT_EQ(op.status_.error_msgs(0), "push finish failed");
  EXPECT_EQ(op.SetFinish(), KStatus::SUCCESS);
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
}

TEST_F(StreamOperatorTest, RemoteInboundInitPushChunkFinishAndDestructorBranches) {
  ScopedBrMgr br_mgr(&ctx_, "127.0.0.1:27351");
  ASSERT_EQ(br_mgr.status_, KStatus::SUCCESS);

  DmlExec dml;
  SetRemoteInboundQueryInfo(&ctx_, &dml, 71051);

  TSInputSyncSpec spec = MakeRemoteInboundSpec(51);
  TABLE table(1, 151);
  auto* op = new RemoteInboundOperator(nullptr, &spec, &table);

  ASSERT_EQ(op->Init(&ctx_), EEIteratorErrCode::EE_OK);
  ASSERT_NE(op->stream_recvr_, nullptr);
  EXPECT_EQ(op->query_id_, 71051);
  EXPECT_EQ(op->dest_processor_id_, 51);
  EXPECT_EQ(op->status_.status_code(), 0);
  EXPECT_EQ(op->status_.error_msgs(0), "");

  DataChunkPtr input = MakeIntChunk({7, 8});
  EXPECT_EQ(op->PushChunk(&ctx_, input, 33, EEIteratorErrCode::EE_OK),
            KStatus::SUCCESS);
  EXPECT_EQ(op->send_count_, 2);

  EEPgErrorInfo ok_info;
  std::memset(&ok_info, 0, sizeof(ok_info));
  op->PushFinish(EEIteratorErrCode::EE_OK, 33, ok_info);
  EXPECT_EQ(op->send_count_, 0);

  DataChunkPtr out = nullptr;
  ASSERT_EQ(op->Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(ReadChunkInts(out.get()), (std::vector<k_int32>{7, 8}));
  EXPECT_FALSE(op->parser_output_fields_);
  EXPECT_EQ(op->total_rows_, 2U);

  out.reset();
  EXPECT_EQ(op->Next(&ctx_, out), EEIteratorErrCode::EE_END_OF_RECORD);

  delete op;
  ctx_.dml_exec_handle = nullptr;
}

TEST_F(StreamOperatorTest, RemoteInboundInitCoversChildInitFailureAndChildOutputFields) {
  ScopedBrMgr br_mgr(&ctx_, "127.0.0.1:27352");
  ASSERT_EQ(br_mgr.status_, KStatus::SUCCESS);

  DmlExec dml;
  SetRemoteInboundQueryInfo(&ctx_, &dml, 71052);

  TSInputSyncSpec spec = MakeRemoteInboundSpec(52);
  TABLE table(1, 152);
  RemoteInboundOperator op(nullptr, &spec, &table);
  auto* child = new StubStreamChildOperator();
  child->init_code_ = EEIteratorErrCode::EE_ERROR;
  op.AddDependency(child);

  EXPECT_EQ(&op.OutputFields(), &child->OutputFields());
  EXPECT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(child->init_calls_, 1);

  delete child;
  ctx_.dml_exec_handle = nullptr;
}

TEST_F(StreamOperatorTest, RemoteInboundStopFieldInfoAndPullChunkFailureBranches) {
  ScopedBrMgr br_mgr(&ctx_, "127.0.0.1:27353");
  ASSERT_EQ(br_mgr.status_, KStatus::SUCCESS);

  DmlExec dml;
  SetRemoteInboundQueryInfo(&ctx_, &dml, 71053);

  TSInputSyncSpec spec = MakeRemoteInboundSpec(53);
  TABLE table(1, 153);
  RemoteInboundOperator op(nullptr, &spec, &table);
  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);

  DataChunkPtr null_chunk = nullptr;
  op.SetOutputFieldInfo(null_chunk);

  TSInputSyncSpec mismatch_spec = MakeRemoteInboundSpec(54);
  mismatch_spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE mismatch_table(1, 154);
  RemoteInboundOperator mismatch_op(nullptr, &mismatch_spec, &mismatch_table);
  SetRemoteInboundQueryInfo(&ctx_, &dml, 71054);
  ASSERT_EQ(mismatch_op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  auto one_col_chunk = MakeIntChunk({9});
  mismatch_op.SetOutputFieldInfo(one_col_chunk);

  op.stream_recvr_->CancelStream();
  DataChunkPtr out = nullptr;
  EXPECT_EQ(op.PullChunk(&ctx_, out), KStatus::FAIL);

  op.is_stop_ = true;
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);

  ctx_.dml_exec_handle = nullptr;
}

TEST_F(StreamOperatorTest, RemoteMergeSortInboundMergesAndLimitsChunks) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 112);
  PassThroughChunkBuffer pass_through_buffer(70042);

  RemoteMergeSortInboundOperator op(nullptr, &spec, &table);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");
  op.query_id_ = 70042;
  op.dest_processor_id_ = 41;
  op.stream_id_ = 1;
  op.order_column_ids_ = {0};
  op.asc_order_ = {true};
  op.null_first_ = {true};
  op.limit_ = 3;

  auto recvr = std::shared_ptr<DataStreamRecvr>(
      new DataStreamRecvr(nullptr, 70042, 41, 2, 1024, true, true,
                          &pass_through_buffer));
  op.stream_recvr_ = recvr;
  ASSERT_EQ(recvr->CreateMergerForPipeline(&ctx_, &op.order_column_ids_,
                                           &op.asc_order_, &op.null_first_),
            KStatus::SUCCESS);

  EnqueuePassThroughChunk(recvr.get(), 70042, 41, 1, MakeIntChunk({1, 3}));
  EnqueuePassThroughChunk(recvr.get(), 70042, 41, 2, MakeIntChunk({2, 4}));
  recvr->RemoveSender(1, 1);
  recvr->RemoveSender(2, 2);

  EXPECT_TRUE(op.HasOutput());
  EXPECT_TRUE(op.NeedInput());

  DataChunkPtr out = nullptr;
  ASSERT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(ReadChunkInts(out.get()), std::vector<k_int32>({1, 2}));

  out.reset();
  ASSERT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_OK);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(ReadChunkInts(out.get()), std::vector<k_int32>({3}));

  out.reset();
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_TRUE(op.IsFinished());
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);

  recvr->closed_ = true;
  op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RemoteMergeSortInboundFinishedTryPullChunkBranch) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  TABLE table(1, 119);
  PassThroughChunkBuffer pass_through_buffer(70045);

  RemoteMergeSortInboundOperator op(nullptr, &spec, &table);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");
  op.query_id_ = 70045;
  op.dest_processor_id_ = 45;
  op.stream_id_ = 1;
  op.is_finished_ = true;

  auto recvr = std::shared_ptr<DataStreamRecvr>(
      new DataStreamRecvr(nullptr, 70045, 45, 1, 1024, true, true,
                          &pass_through_buffer));
  op.stream_recvr_ = recvr;

  DataChunkPtr out = nullptr;
  EXPECT_TRUE(op.IsFinished());
  EXPECT_TRUE(op.NeedInput());
  EXPECT_EQ(op.TryPullChunk(&ctx_, out), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_EQ(out, nullptr);

  recvr->closed_ = true;
  op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest, RemoteMergeSortInboundInitAndErrorBranches) {
  {
    TSInputSyncSpec spec;
    spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
    TABLE table(1, 123);
    RemoteMergeSortInboundOperator op(nullptr, &spec, &table);
    EXPECT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
  }

  {
    TSInputSyncSpec spec;
    spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
    TABLE table(1, 124);
    PassThroughChunkBuffer pass_through_buffer(70053);

    RemoteMergeSortInboundOperator status_op(nullptr, &spec, &table);
    status_op.status_.set_status_code(8123);
    status_op.status_.add_error_msgs("merge failed");
    status_op.stream_recvr_ = std::shared_ptr<DataStreamRecvr>(
        new DataStreamRecvr(nullptr, 70053, 53, 1, 1024, true, true,
                            &pass_through_buffer));
    DataChunkPtr out = nullptr;
    EXPECT_EQ(status_op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);

    RemoteMergeSortInboundOperator stop_op(nullptr, &spec, &table);
    stop_op.status_.set_status_code(0);
    stop_op.status_.add_error_msgs("");
    stop_op.stream_recvr_ = std::shared_ptr<DataStreamRecvr>(
        new DataStreamRecvr(nullptr, 70054, 54, 1, 1024, true, true,
                            &pass_through_buffer));
    stop_op.is_stop_ = true;
    out.reset();
    EXPECT_EQ(stop_op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);

    RemoteMergeSortInboundOperator close_op(nullptr, &spec, &table);
    StubStreamChildOperator child;
    child.close_code_ = EEIteratorErrCode::EE_ERROR;
    close_op.AddDependency(&child);
    EXPECT_EQ(close_op.Close(&ctx_), EEIteratorErrCode::EE_ERROR);

    status_op.stream_recvr_->closed_ = true;
    status_op.stream_recvr_.reset();
    stop_op.stream_recvr_->closed_ = true;
    stop_op.stream_recvr_.reset();
    pass_through_buffer.Unref();
  }
}

TEST_F(StreamOperatorTest, RemoteMergeSortInboundInitSuccessCreatesMerger) {
  ScopedBrMgr br_mgr(&ctx_, "127.0.0.1:27356");
  ASSERT_EQ(br_mgr.status_, KStatus::SUCCESS);

  DmlExec dml;
  SetRemoteInboundQueryInfo(&ctx_, &dml, 71056);

  TSInputSyncSpec spec = MakeRemoteInboundSpec(56);
  TABLE table(1, 156);
  RemoteMergeSortInboundOperator op(nullptr, &spec, &table);
  op.order_column_ids_ = {0};
  op.asc_order_ = {true};
  op.null_first_ = {true};

  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  ASSERT_NE(op.stream_recvr_, nullptr);
  ASSERT_NE(op.stream_recvr_->cascade_merger_, nullptr);
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);

  ctx_.dml_exec_handle = nullptr;
}

TEST_F(StreamOperatorTest, RemoteMergeSortInboundGetNextMergingReturnsWhenFinished) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));

  TABLE table(1, 157);
  PassThroughChunkBuffer pass_through_buffer(70056);
  RemoteMergeSortInboundOperator finished_op(nullptr, &spec, &table);
  ScriptedDataChunkMerger* merger = nullptr;
  finished_op.stream_recvr_ =
      MakeScriptedMergeRecvr(&ctx_, 70056, 56, &pass_through_buffer, &merger);
  finished_op.is_finished_ = true;

  DataChunkPtr out = nullptr;
  EXPECT_EQ(finished_op.GetNextMerging(&ctx_, out), KStatus::SUCCESS);
  EXPECT_EQ(out, nullptr);

  finished_op.stream_recvr_->closed_ = true;
  finished_op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RemoteMergeSortInboundGetNextMergingHandlesOffsetWithoutMaterializedChunk) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));

  TABLE table(1, 159);
  PassThroughChunkBuffer pass_through_buffer(70058);
  SequencedRemoteMergeSortInboundOperator empty_op(nullptr, &spec, &table);
  ScriptedDataChunkMerger* merger = nullptr;
  empty_op.stream_recvr_ =
      MakeScriptedMergeRecvr(&ctx_, 70058, 58, &pass_through_buffer, &merger);
  empty_op.finished_results_.push_back(false);
  empty_op.is_finished_ = true;
  empty_op.offset_ = 2;
  merger->PushStep({KStatus::SUCCESS, nullptr, true, false});

  DataChunkPtr out = nullptr;
  EXPECT_EQ(empty_op.GetNextMerging(&ctx_, out), KStatus::SUCCESS);
  EXPECT_EQ(out, nullptr);

  empty_op.stream_recvr_->closed_ = true;
  empty_op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RemoteMergeSortInboundGetNextMergingReturnsFailOnOffsetFetchError) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));

  TABLE table(1, 162);
  PassThroughChunkBuffer pass_through_buffer(70061);
  SequencedRemoteMergeSortInboundOperator fail_op(nullptr, &spec, &table);
  ScriptedDataChunkMerger* merger = nullptr;
  fail_op.stream_recvr_ =
      MakeScriptedMergeRecvr(&ctx_, 70061, 61, &pass_through_buffer, &merger);
  fail_op.finished_results_.push_back(false);
  fail_op.is_finished_ = true;
  fail_op.offset_ = 1;
  merger->PushStep({KStatus::FAIL, MakeIntChunk({1, 2}), true, false});

  DataChunkPtr out = nullptr;
  EXPECT_EQ(fail_op.GetNextMerging(&ctx_, out), KStatus::FAIL);

  fail_op.stream_recvr_->closed_ = true;
  fail_op.stream_recvr_.reset();
  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RemoteMergeSortInboundFailurePathsPropagateThroughPullAndTryPull) {
  TSInputSyncSpec spec;
  spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));

  {
    TABLE table(1, 160);
    PassThroughChunkBuffer pass_through_buffer(70059);
    RemoteMergeSortInboundOperator merge_fail_op(nullptr, &spec, &table);
    ScriptedDataChunkMerger* merger = nullptr;
    merge_fail_op.stream_recvr_ = MakeScriptedMergeRecvr(
        &ctx_, 70059, 59, &pass_through_buffer, &merger);
    merger->PushStep({KStatus::FAIL, nullptr, false, false});

    DataChunkPtr out = nullptr;
    EXPECT_EQ(merge_fail_op.GetNextMerging(&ctx_, out), KStatus::FAIL);

    merge_fail_op.stream_recvr_->closed_ = true;
    merge_fail_op.stream_recvr_.reset();
    pass_through_buffer.Unref();
  }

  {
    TABLE table(1, 161);
    PassThroughChunkBuffer pass_through_buffer(70060);
    RemoteMergeSortInboundOperator try_pull_fail_op(nullptr, &spec, &table);
    try_pull_fail_op.status_.set_status_code(0);
    try_pull_fail_op.status_.add_error_msgs("");
    try_pull_fail_op.is_finished_ = false;
    ScriptedDataChunkMerger* merger = nullptr;
    try_pull_fail_op.stream_recvr_ = MakeScriptedMergeRecvr(
        &ctx_, 70060, 60, &pass_through_buffer, &merger);
    merger->PushStep({KStatus::FAIL, nullptr, false, false});

    DataChunkPtr out = nullptr;
    EXPECT_EQ(try_pull_fail_op.TryPullChunk(&ctx_, out),
              EEIteratorErrCode::EE_ERROR);

    try_pull_fail_op.stream_recvr_->closed_ = true;
    try_pull_fail_op.stream_recvr_.reset();
    pass_through_buffer.Unref();
  }
}

TEST_F(StreamOperatorTest, RouterOutboundStatusAndCompressionBranches) {
  TNetworkAddress addr;
  addr.SetHostname("127.0.0.1");
  addr.SetPort(12345);
  PassThroughChunkBuffer buffer(70043);

  TSOutputRouterSpec spec;
  TABLE table(1, 113);
  RouterOutboundOperator op(nullptr, &spec, &table);
  op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{}, false);
  op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER;
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");

  auto channel = std::make_unique<RouterOutboundOperator::Channel>(
      &op, addr, -1, 4, 12, 6, &buffer);
  channel->SetType(StreamEndpointType::LOCAL);
  ASSERT_EQ(channel->Init(), KStatus::SUCCESS);
  EXPECT_TRUE(channel->CheckPassThrough());
  auto* channel_ptr = channel.get();
  op.target_id2channel_.emplace(12, std::move(channel));
  op.channels_.push_back(channel_ptr);
  op.channel_indices_ = {0};

  EXPECT_TRUE(op.CheckReady(&ctx_));
  EXPECT_TRUE(op.SendErrorMessage(1234, "ignored for local gather"));

  StubStreamChildOperator child;
  child.next_code_ = EEIteratorErrCode::EE_END_OF_RECORD;
  op.AddDependency(&child);
  op.status_.set_status_code(4567);
  op.status_.set_error_msgs(0, "router failed");
  DataChunkPtr out = nullptr;
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(op.SetFinishing(), KStatus::FAIL);

  RouterOutboundOperator compressed(nullptr, &spec, &table);
  compressed.compress_type_ = CompressionTypePB::LZ4_COMPRESSION;
  GetBlockCompressor(compressed.compress_type_, &compressed.compress_codec_);
  k_bool first_chunk = true;
  ChunkPB pb;
  auto chunk = MakeIntChunk({9, 10});
  ASSERT_EQ(compressed.SerializeChunk(chunk.get(), &pb, &first_chunk),
            KStatus::SUCCESS);
  EXPECT_EQ(pb.compress_type(), CompressionTypePB::LZ4_COMPRESSION);
  EXPECT_GT(pb.columns_size(), 0);
  EXPECT_FALSE(pb.data().empty());

  auto request = std::make_shared<PTransmitChunkParams>();
  *request->add_chunks() = pb;
  butil::IOBuf attachment;
  EXPECT_EQ(compressed.ConstrucBrpcAttachment(request, attachment), 1024);
  EXPECT_TRUE(request->chunks(0).data().empty());

  buffer.Unref();
}

TEST_F(StreamOperatorTest, RouterOutboundInitCoversSuccessAndErrorBranches) {
  TSOutputRouterSpec spec;
  spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  AddRouterStream(&spec, StreamEndpointType::LOCAL, 0, 61, 5);
  TABLE table(1, 163);

  {
    RouterOutboundOperator missing_ctx(nullptr, &spec, &table);
    StubStreamChildOperator child;
    missing_ctx.AddDependency(&child);
    EXPECT_EQ(missing_ctx.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
  }

  {
    RouterOutboundOperator child_fail(nullptr, &spec, &table);
    StubStreamChildOperator child;
    child.init_code_ = EEIteratorErrCode::EE_ERROR;
    child_fail.AddDependency(&child);
    EXPECT_EQ(child_fail.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
  }

  DmlExec dml;
  SetQueryInfoWithBrpcAddrs(&ctx_, &dml, -1,
                           {"127.0.0.1:27361", "127.0.0.1:27361"});

  TSOutputRouterSpec ok_spec;
  ok_spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  AddRouterStream(&ok_spec, StreamEndpointType::REMOTE, 1, 62, 6);
  AddRouterStream(&ok_spec, StreamEndpointType::REMOTE, 2, 63, 7);
  AddRouterStream(&ok_spec, StreamEndpointType::REMOTE, 2, 64, 8);

  RouterOutboundOperator op(nullptr, &ok_spec, &table);
  StubStreamChildOperator child;
  child.queued_chunks_.push_back(MakeIntChunk({11, 12}));
  child.next_code_ = EEIteratorErrCode::EE_END_OF_RECORD;
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  ASSERT_NE(op.buffer_, nullptr);
  EXPECT_EQ(op.query_id_, -1);
  EXPECT_EQ(op.target_id2channel_.size(), 2U);
  EXPECT_EQ(op.channels_.size(), 3U);
  EXPECT_EQ(op.channel_indices_.size(), 3U);
  ASSERT_NE(op.compress_codec_, nullptr);

  auto remote_it = op.target_id2channel_.find(1);
  ASSERT_NE(remote_it, op.target_id2channel_.end());
  EXPECT_FALSE(remote_it->second->UsePassThrougth());
  auto remote_dup_it = op.target_id2channel_.find(2);
  ASSERT_NE(remote_dup_it, op.target_id2channel_.end());
  EXPECT_EQ(op.channels_[1], remote_dup_it->second.get());
  EXPECT_EQ(op.channels_[2], remote_dup_it->second.get());

  op.buffer_->is_completing_ = true;
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
  ctx_.dml_exec_handle = nullptr;
}

TEST_F(StreamOperatorTest, RouterOutboundPushFinishErrorAndCheckReadyStopPaths) {
  TSOutputRouterSpec spec;
  TABLE table(1, 126);

  RouterOutboundOperator stop_op(nullptr, &spec, &table);
  stop_op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{}, false);
  stop_op.is_tp_stop_ = true;
  EXPECT_FALSE(stop_op.CheckReady(&ctx_));

  RouterOutboundOperator error_op(nullptr, &spec, &table);
  error_op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{}, false);
  error_op.status_.set_status_code(0);
  error_op.status_.add_error_msgs("");
  error_op.SetCollected(true);

  TSInputSyncSpec input_spec;
  input_spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  RecordingInboundOperator parent(nullptr, &input_spec, &table);
  parent.AddDependency(&error_op);

  EEPgErrorInfo pg_info;
  pg_info.code = 9527;
  std::strncpy(pg_info.msg, "router push finish failed", sizeof(pg_info.msg) - 1);
  error_op.PushFinish(EEIteratorErrCode::EE_ERROR, 9, pg_info);
  EXPECT_TRUE(error_op.is_finished_);
  EXPECT_EQ(parent.finish_calls_, 1);
  EXPECT_EQ(parent.finish_stream_id_, 9);
  EXPECT_EQ(parent.finish_pg_info_.code, 9527);
  EXPECT_FALSE(error_op.NeedInput());
}

TEST_F(StreamOperatorTest,
       RouterOutboundChannelFlushAndTimesliceBranchesWithoutRpc) {
  TNetworkAddress addr;
  addr.SetHostname("127.0.0.1");
  addr.SetPort(12345);
  PassThroughChunkBuffer buffer(70046);

  FragmentDestination destination;
  destination.query_id = 1;
  destination.target_node_id = 33;
  destination.brpc_addr = addr;

  TSOutputRouterSpec spec;
  TABLE table(1, 127);
  RouterOutboundOperator op(nullptr, &spec, &table);
  op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER;
  op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{destination}, false);
  op.buffer_->is_completing_ = true;
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");

  auto channel = std::make_unique<RouterOutboundOperator::Channel>(
      &op, addr, -1, 4, 33, 6, &buffer);
  channel->SetType(StreamEndpointType::LOCAL);
  ASSERT_EQ(channel->Init(), KStatus::SUCCESS);
  EXPECT_TRUE(channel->CheckPassThrough());

  DataChunkPtr chunk = MakeIntChunk({31, 32});
  k_bool is_real_sent = false;
  channel->SetSendLastChunk(true);
  EXPECT_EQ(channel->SendOneChunk(chunk, 0, false, &is_real_sent),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_real_sent);
  EXPECT_EQ(chunk, nullptr);
  EXPECT_EQ(channel->chunk_request_, nullptr);
  EXPECT_EQ(channel->current_request_bytes_, 0U);
  EXPECT_EQ(channel->send_count_, 0);

  auto request = std::make_shared<PTransmitChunkParams>();
  request->set_query_id(1);
  request->set_dest_processor(4);
  request->set_sender_id(6);
  request->set_be_number(6);
  EXPECT_EQ(channel->SendChunkRequest(request, butil::IOBuf(), 0),
            KStatus::SUCCESS);
  EXPECT_EQ(channel->SendFinish(), KStatus::SUCCESS);

  auto* channel_ptr = channel.get();
  op.target_id2channel_.emplace(33, std::move(channel));
  op.channels_.push_back(channel_ptr);
  op.channel_indices_ = {0};

  StubStreamChildOperator child;
  child.next_code_ = EEIteratorErrCode::EE_TIMESLICE_OUT;
  op.AddDependency(&child);
  DataChunkPtr out = nullptr;
  EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_TIMESLICE_OUT);
  EXPECT_EQ(op.chunk_request_, nullptr);

  buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RouterOutboundChannelRpcHelpersAndBufferedClosePaths) {
  TNetworkAddress addr;
  addr.SetHostname("127.0.0.1");
  addr.SetPort(27362);

  FragmentDestination destination;
  destination.query_id = 72062;
  destination.target_node_id = 1;
  destination.brpc_addr = addr;

  TSOutputRouterSpec spec;
  TABLE table(1, 164);
  RouterOutboundOperator op(nullptr, &spec, &table);
  op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{destination}, false);

  PassThroughChunkBuffer pass_through_buffer(72062);
  RouterOutboundOperator::Channel channel(
      &op, addr, -1, 62, 1, 9, &pass_through_buffer);
  channel.SetType(StreamEndpointType::LOCAL);

  ASSERT_EQ(channel.Init(), KStatus::SUCCESS);
  EXPECT_TRUE(channel.CheckPassThrough());

  op.buffer_->is_completing_ = true;
  channel.chunks_.resize(1);
  channel.chunks_[0] = MakeIntChunk({1, 2});
  channel.SetSendLastChunk(true);
  EXPECT_EQ(channel.SendOneChunk(channel.chunks_[0], 0, false), KStatus::SUCCESS);
  DataChunkPtr empty_chunk = nullptr;
  EXPECT_EQ(channel.SendOneChunk(empty_chunk, 0, false), KStatus::SUCCESS);
  EXPECT_EQ(channel.CloseInternal(), KStatus::FAIL);
  EXPECT_EQ(channel.chunk_request_, nullptr);
  EXPECT_EQ(channel.current_request_bytes_, 0U);
  EXPECT_EQ(channel.SendFinish(), KStatus::SUCCESS);

  auto request = std::make_shared<PTransmitChunkParams>();
  EXPECT_EQ(channel.SendChunkRequest(request, butil::IOBuf(), 0),
            KStatus::SUCCESS);

  pass_through_buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RouterOutboundSendLastChunkFailureAndChildErrorBranches) {
  TSOutputRouterSpec spec;
  TABLE table(1, 128);

  {
    RouterOutboundOperator op(nullptr, &spec, &table);
    op.buffer_ = std::make_shared<OutboundBuffer>(
        std::vector<FragmentDestination>{}, false);
    op.status_.set_status_code(0);
    op.status_.add_error_msgs("");

    StubStreamChildOperator child;
    child.next_code_ = EEIteratorErrCode::EE_ERROR;
    op.AddDependency(&child);
    DataChunkPtr out = nullptr;
    EXPECT_EQ(op.Next(&ctx_, out), EEIteratorErrCode::EE_ERROR);
    EXPECT_EQ(op.chunk_request_, nullptr);
  }

  {
    TNetworkAddress addr;
    addr.SetHostname("127.0.0.1");
    addr.SetPort(12345);
    FragmentDestination destination;
    destination.query_id = 1;
    destination.target_node_id = 39;
    destination.brpc_addr = addr;

    PassThroughChunkBuffer pass_through_buffer(70047);
    RouterOutboundOperator op(nullptr, &spec, &table);
    op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH;
    op.buffer_ = std::make_shared<OutboundBuffer>(
        std::vector<FragmentDestination>{destination}, false);

    auto channel = std::make_unique<RouterOutboundOperator::Channel>(
        &op, addr, -1, 4, 39, 6, &pass_through_buffer);
    channel->SetType(StreamEndpointType::LOCAL);
    ASSERT_EQ(channel->Init(), KStatus::SUCCESS);
    op.target_id2channel_.emplace(39, std::move(channel));

    EXPECT_EQ(op.SendLastChunk(), KStatus::FAIL);
    pass_through_buffer.Unref();
  }
}

TEST_F(StreamOperatorTest, RouterOutboundLifecycleBranchesWithoutRpcSend) {
  TSOutputRouterSpec spec;
  TABLE table(1, 125);
  RouterOutboundOperator op(nullptr, &spec, &table);
  op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH;
  op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{}, false);
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");

  StubStreamChildOperator child;
  op.AddDependency(&child);
  EXPECT_EQ(op.Start(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.buffer_->uncancelled_sinker_count_, 1);

  op.chunk_request_ = std::make_shared<PTransmitChunkParams>();
  op.chunk_request_->set_query_id(70055);
  auto* chunk = op.chunk_request_->add_chunks();
  chunk->set_data("abc");
  EXPECT_EQ(op.SendLastChunk(), KStatus::SUCCESS);
  EXPECT_EQ(op.chunk_request_, nullptr);

  op.SetCollected(true);
  TSInputSyncSpec input_spec;
  input_spec.add_column_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32));
  RecordingInboundOperator parent(nullptr, &input_spec, &table);
  parent.AddDependency(&op);
  EEPgErrorInfo pg_info;
  op.PushFinish(EEIteratorErrCode::EE_END_OF_RECORD, 7, pg_info);
  EXPECT_TRUE(op.is_finished_);
  EXPECT_TRUE(op.is_real_finished_);
  EXPECT_EQ(parent.finish_calls_, 1);
  EXPECT_EQ(parent.finish_stream_id_, 7);

  BaseOperator* cloned = op.Clone();
  ASSERT_NE(cloned, nullptr);
  auto* cloned_router = dynamic_cast<RouterOutboundOperator*>(cloned);
  ASSERT_NE(cloned_router, nullptr);
  EXPECT_TRUE(cloned_router->is_collected_);
  delete cloned;

  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(child.close_calls_, 1);
  EXPECT_EQ(op.buffer_, nullptr);
}

TEST_F(StreamOperatorTest,
       RouterOutboundPushChunkCoversPassThroughHashGatherAndRange) {
  TNetworkAddress addr;
  addr.SetHostname("127.0.0.1");
  addr.SetPort(12345);
  PassThroughChunkBuffer buffer(88);

  {
    TSOutputRouterSpec spec;
    TABLE table(1, 12);
    RouterOutboundOperator op(nullptr, &spec, &table);
    op.buffer_ = std::make_shared<OutboundBuffer>(
        std::vector<FragmentDestination>{}, false);
    op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH;

    auto channel = std::make_unique<RouterOutboundOperator::Channel>(
        &op, addr, -1, 2, 3, 4, &buffer);
    channel->SetType(StreamEndpointType::LOCAL);
    ASSERT_EQ(channel->Init(), KStatus::SUCCESS);
    EXPECT_TRUE(channel->CheckPassThrough());
    auto* channel_ptr = channel.get();
    op.target_id2channel_.emplace(3, std::move(channel));
    op.channels_.push_back(channel_ptr);
    op.channel_indices_.push_back(0);

    DataChunkPtr chunk = MakeIntChunk({1, 2});
    EXPECT_EQ(op.PushChunk(&ctx_, chunk, 0, EEIteratorErrCode::EE_OK),
              KStatus::SUCCESS);
    EXPECT_EQ(op.total_rows_, 2U);
    EXPECT_EQ(channel_ptr->send_count_, 2);
    EXPECT_GT(channel_ptr->current_request_bytes_, 0U);
    EXPECT_NE(channel_ptr->chunk_request_, nullptr);
    EXPECT_EQ(chunk, nullptr);
    EXPECT_TRUE(op.NeedInput());
  }

  {
    TSOutputRouterSpec spec;
    TABLE table(1, 13);
    RouterOutboundOperator op(nullptr, &spec, &table);
    op.buffer_ = std::make_shared<OutboundBuffer>(
        std::vector<FragmentDestination>{}, false);
    op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_HASH;
    op.group_cols_.push_back(0);

    auto channel0 = std::make_unique<RouterOutboundOperator::Channel>(
        &op, addr, 1, 2, 10, 4, &buffer);
    channel0->SetType(StreamEndpointType::LOCAL);
    channel0->is_inited_ = true;
    channel0->use_pass_through_ = true;
    auto* channel0_ptr = channel0.get();
    op.target_id2channel_.emplace(10, std::move(channel0));
    op.channels_.push_back(channel0_ptr);

    auto channel1 = std::make_unique<RouterOutboundOperator::Channel>(
        &op, addr, 1, 3, 11, 5, &buffer);
    channel1->SetType(StreamEndpointType::LOCAL);
    channel1->is_inited_ = true;
    channel1->use_pass_through_ = true;
    auto* channel1_ptr = channel1.get();
    op.target_id2channel_.emplace(11, std::move(channel1));
    op.channels_.push_back(channel1_ptr);
    op.channel_indices_ = {0, 1};

    DataChunkPtr chunk = MakeIntChunk({10, 20, 30});
    EXPECT_EQ(op.PushChunk(&ctx_, chunk, 0, EEIteratorErrCode::EE_OK),
              KStatus::SUCCESS);
    const k_uint32 total_rows =
        (channel0_ptr->chunks_[0] ? channel0_ptr->chunks_[0]->Count() : 0U) +
        (channel1_ptr->chunks_[0] ? channel1_ptr->chunks_[0]->Count() : 0U);
    EXPECT_EQ(total_rows, 3U);
  }

  {
    TSOutputRouterSpec spec;
    TABLE table(1, 14);
    RouterOutboundOperator op(nullptr, &spec, &table);
    op.buffer_ = std::make_shared<OutboundBuffer>(
        std::vector<FragmentDestination>{}, false);
    op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER;

    auto local = std::make_unique<RouterOutboundOperator::Channel>(
        &op, addr, -1, 4, 12, 6, &buffer);
    local->SetType(StreamEndpointType::LOCAL);
    ASSERT_EQ(local->Init(), KStatus::SUCCESS);
    EXPECT_TRUE(local->CheckPassThrough());
    auto* local_ptr = local.get();
    op.target_id2channel_.emplace(12, std::move(local));
    op.channels_.push_back(local_ptr);

    auto remote = std::make_unique<RouterOutboundOperator::Channel>(
        &op, addr, -1, 5, 13, 7, &buffer);
    remote->SetType(StreamEndpointType::REMOTE);
    ASSERT_EQ(remote->Init(), KStatus::SUCCESS);
    auto* remote_ptr = remote.get();
    op.target_id2channel_.emplace(13, std::move(remote));
    op.channels_.push_back(remote_ptr);
    op.channel_indices_ = {0, 1};

    DataChunkPtr chunk = MakeIntChunk({7, 8});
    EXPECT_EQ(op.PushChunk(&ctx_, chunk, 0, EEIteratorErrCode::EE_OK),
              KStatus::SUCCESS);
    EXPECT_EQ(local_ptr->send_count_, 0);
    EXPECT_EQ(remote_ptr->send_count_, 2);
    EXPECT_GT(remote_ptr->current_request_bytes_, 0U);
    EXPECT_NE(remote_ptr->chunk_request_, nullptr);
  }

  {
    TSOutputRouterSpec spec;
    TABLE table(1, 15);
    RouterOutboundOperator op(nullptr, &spec, &table);
    op.buffer_ = std::make_shared<OutboundBuffer>(
        std::vector<FragmentDestination>{}, false);
    op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_RANGE;
    DataChunkPtr chunk = MakeIntChunk({99});
    EXPECT_EQ(op.PushChunk(&ctx_, chunk, 0, EEIteratorErrCode::EE_OK),
              KStatus::FAIL);
  }

  buffer.Unref();
}

TEST_F(StreamOperatorTest,
       RouterOutboundSendLastChunkAndFinishingCoverBufferedPaths) {
  TNetworkAddress addr;
  addr.SetHostname("127.0.0.1");
  addr.SetPort(12345);

  PassThroughChunkBuffer pass_through_buffer(72063);
  TSOutputRouterSpec spec;
  TABLE table(1, 165);
  RouterOutboundOperator op(nullptr, &spec, &table);
  op.part_type_ = TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER;
  op.buffer_ = std::make_shared<OutboundBuffer>(
      std::vector<FragmentDestination>{}, false);
  op.buffer_->is_completing_ = true;
  op.status_.set_status_code(0);
  op.status_.add_error_msgs("");

  auto local = std::make_unique<RouterOutboundOperator::Channel>(
      &op, addr, 72063, 2, 10, 4, &pass_through_buffer);
  local->SetType(StreamEndpointType::LOCAL);
  local->is_inited_ = true;
  local->use_pass_through_ = true;
  auto* local_ptr = local.get();
  op.target_id2channel_.emplace(10, std::move(local));
  op.channels_.push_back(local_ptr);

  auto remote = std::make_unique<RouterOutboundOperator::Channel>(
      &op, addr, 72063, 3, 11, 5, &pass_through_buffer);
  remote->SetType(StreamEndpointType::REMOTE);
  remote->is_inited_ = true;
  remote->use_pass_through_ = false;
  auto* remote_ptr = remote.get();
  op.target_id2channel_.emplace(11, std::move(remote));
  op.channels_.push_back(remote_ptr);
  op.channel_indices_ = {0, 1};

  op.chunk_request_ = std::make_shared<PTransmitChunkParams>();
  op.chunk_request_->add_chunks()->set_data("payload");
  op.current_request_bytes_ = 7;
  EXPECT_EQ(op.SendLastChunk(), KStatus::SUCCESS);
  EXPECT_EQ(op.chunk_request_, nullptr);
  EXPECT_EQ(op.current_request_bytes_, 0U);

  EXPECT_EQ(op.SetFinishing(), KStatus::SUCCESS);
  EXPECT_TRUE(op.is_real_finished_);

  pass_through_buffer.Unref();
}

}  // namespace
}  // namespace kwdbts
