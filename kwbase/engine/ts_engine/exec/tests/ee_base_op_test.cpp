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

#include <limits>
#include <memory>
#include <sstream>
#include <algorithm>
#include <vector>

#include "gtest/gtest.h"

#define private public
#define protected public
#include "br_mgr.h"
#include "ee_base_op.h"
#undef protected
#undef private

#include "ee_kwthd_context.h"
#include "ee_pipeline_group.h"

namespace kwdbts {
namespace {

struct ScopedBrMgr {
  explicit ScopedBrMgr(const std::string& addr) {
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
      br_mgr.address_.SetPort(
          static_cast<k_uint16>(std::stoi(addr.substr(pos + 1))));
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

class BaseOperatorContextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
    br_mgr_ = std::make_unique<ScopedBrMgr>("127.0.0.1:27000");
    ASSERT_EQ(br_mgr_->status_, KStatus::SUCCESS);
  }

  void TearDown() override { br_mgr_.reset(); }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  std::unique_ptr<ScopedBrMgr> br_mgr_;
};

class FakeOperator : public BaseOperator {
 public:
  FakeOperator(OperatorType type, bool is_source = false, bool is_sink = false)
      : BaseOperator(nullptr, nullptr, nullptr, 0),
        type_(type),
        is_source_(is_source),
        is_sink_(is_sink) {}

  OperatorType Type() override { return type_; }
  EEIteratorErrCode Init(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Start(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  EEIteratorErrCode Reset(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Close(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }

  bool isSink() override { return is_sink_; }
  bool isSource() override { return is_source_; }

 private:
  OperatorType type_;
  bool is_source_;
  bool is_sink_;
};

class RecordingOperator : public FakeOperator {
 public:
  explicit RecordingOperator(OperatorType type)
      : FakeOperator(type, false, false) {}

  KStatus CreateInputChannel(kwdbContext_p,
                             std::vector<BaseOperator*>&) override {
    ++input_calls_;
    return input_ret_;
  }

  KStatus CreateOutputChannel(kwdbContext_p,
                              std::vector<BaseOperator*>&) override {
    ++output_calls_;
    return output_ret_;
  }

  int input_calls_{0};
  int output_calls_{0};
  KStatus input_ret_{KStatus::SUCCESS};
  KStatus output_ret_{KStatus::SUCCESS};
};

class FakeTopOutputOperator : public FakeOperator {
 public:
  using FakeOperator::FakeOperator;

  KStatus CreateTopOutputChannel(kwdbContext_p,
                                 std::vector<BaseOperator*>& operators) override {
    if (!GetParent().empty()) {
      return KStatus::FAIL;
    }
    TSOutputRouterSpec* output_spec = GetRpcSpecInfo().output_specs_[0];
    for (int i = 0; i < output_spec->streams_size(); ++i) {
      if (output_spec->streams(i).type() == StreamEndpointType::REMOTE) {
        auto* outbound = new FakeOperator(OperatorType::OPERATOR_REMOTR_OUT_BOUND);
        outbound->AddDependency(this);
        operators.push_back(outbound);
        break;
      }
    }
    return KStatus::SUCCESS;
  }
};

TEST(BaseOperatorTest, GetTypeNameUsesOperatorTypeMap) {
  FakeOperator op(OperatorType::OPERATOR_NOOP);
  EXPECT_STREQ(op.GetTypeName(), "OPERATOR_NOOP");
}

TEST(BaseOperatorTest, BuildPipelineOrganizesSourceMiddleAndSink) {
  FakeOperator source(OperatorType::OPERATOR_TABLE_SCAN, true, false);
  FakeOperator middle(OperatorType::OPERATOR_NOOP, false, false);
  FakeOperator sink(OperatorType::OPERATOR_RESULT_COLLECTOR, false, true);

  source.AddDependency(&middle);
  middle.AddDependency(&sink);

  PipelineGroup pipeline(nullptr);
  EXPECT_EQ(source.BuildPipeline(&pipeline, nullptr), KStatus::SUCCESS);
  EXPECT_EQ(pipeline.GetSource(), &source);
  ASSERT_EQ(pipeline.GetDependencies().size(), 1U);

  PipelineGroup* child_pipeline = pipeline.GetDependencies()[0];
  ASSERT_NE(child_pipeline, nullptr);
  ASSERT_EQ(child_pipeline->GetOperator().size(), 1U);
  EXPECT_EQ(child_pipeline->GetOperator()[0], &middle);
  EXPECT_EQ(child_pipeline->operator_, nullptr);
  EXPECT_EQ(child_pipeline->GetSink(), nullptr);

  delete child_pipeline;
}

TEST(BaseOperatorTest, BuildPipelineCreatesDependencyForSinkWhenNeeded) {
  FakeOperator existing_sink(OperatorType::OPERATOR_RESULT_COLLECTOR, false,
                             true);
  FakeOperator new_sink(OperatorType::OPERATOR_RESULT_COLLECTOR, false, true);
  FakeOperator child(OperatorType::OPERATOR_NOOP, false, false);
  new_sink.AddDependency(&child);

  PipelineGroup pipeline(nullptr);
  pipeline.SetPipelineOperator(&existing_sink);

  EXPECT_EQ(new_sink.BuildPipeline(&pipeline, nullptr), KStatus::SUCCESS);
  ASSERT_EQ(pipeline.GetDependencies().size(), 1U);

  PipelineGroup* child_pipeline = pipeline.GetDependencies()[0];
  ASSERT_NE(child_pipeline, nullptr);
  EXPECT_EQ(child_pipeline->operator_, &new_sink);
  EXPECT_EQ(child_pipeline->GetSink(), &new_sink);
  EXPECT_TRUE(child_pipeline->GetOperator().empty());

  delete child_pipeline;
}

TEST(BaseOperatorTest, BuildPipelineSucceedsForLeafOperator) {
  FakeOperator leaf(OperatorType::OPERATOR_NOOP, false, false);
  PipelineGroup pipeline(nullptr);
  EXPECT_EQ(leaf.BuildPipeline(&pipeline, nullptr), KStatus::SUCCESS);
  EXPECT_TRUE(pipeline.GetDependencies().empty());
  EXPECT_EQ(pipeline.GetSource(), nullptr);
}

TEST_F(BaseOperatorContextTest, CreateInputChannelDelegatesWhenNoExchangeNeeded) {
  FakeOperator root(OperatorType::OPERATOR_NOOP);
  RecordingOperator child(OperatorType::OPERATOR_NOOP);

  root.AddDependency(&child);

  TSInputSyncSpec input_spec;
  input_spec.set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);
  auto* stream = input_spec.add_streams();
  stream->set_type(StreamEndpointType::LOCAL);
  root.GetRpcSpecInfo().input_specs_.push_back(&input_spec);

  std::vector<BaseOperator*> new_operators;
  EXPECT_EQ(root.CreateInputChannel(ctx_, new_operators), KStatus::SUCCESS);
  EXPECT_EQ(child.input_calls_, 1);
  EXPECT_TRUE(new_operators.empty());
}

TEST_F(BaseOperatorContextTest, CreateInputChannelBuildsInboundAndCallsChildrenOutputs) {
  FakeOperator root(OperatorType::OPERATOR_NOOP);
  RecordingOperator left(OperatorType::OPERATOR_NOOP);
  RecordingOperator right(OperatorType::OPERATOR_NOOP);
  root.AddDependency(&left);
  root.AddDependency(&right);

  TSInputSyncSpec input_spec;
  input_spec.set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);
  auto* stream = input_spec.add_streams();
  stream->set_type(StreamEndpointType::LOCAL);
  root.GetRpcSpecInfo().input_specs_.push_back(&input_spec);

  std::vector<BaseOperator*> new_operators;
  EXPECT_EQ(root.CreateInputChannel(ctx_, new_operators), KStatus::SUCCESS);
  ASSERT_EQ(new_operators.size(), 1U);
  EXPECT_EQ(new_operators[0]->Type(), OperatorType::OPERATOR_LOCAL_IN_BOUND);
  EXPECT_EQ(left.output_calls_, 1);
  EXPECT_EQ(right.output_calls_, 1);
  EXPECT_EQ(root.GetChildren().back(), new_operators[0]);

  delete new_operators[0];
}

TEST_F(BaseOperatorContextTest, CreateInputChannelBuildsOrderedInboundForOrderedStreams) {
  FakeOperator root(OperatorType::OPERATOR_NOOP);
  RecordingOperator child(OperatorType::OPERATOR_NOOP);
  root.AddDependency(&child);

  TSInputSyncSpec input_spec;
  input_spec.set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_ORDERED);
  auto* stream = input_spec.add_streams();
  stream->set_type(StreamEndpointType::LOCAL);
  root.GetRpcSpecInfo().input_specs_.push_back(&input_spec);

  std::vector<BaseOperator*> new_operators;
  EXPECT_EQ(root.CreateInputChannel(ctx_, new_operators), KStatus::SUCCESS);
  ASSERT_EQ(new_operators.size(), 1U);
  EXPECT_EQ(new_operators[0]->Type(), OperatorType::OPERATOR_LOCAL_MERGE_IN_BOUND);
  EXPECT_EQ(child.output_calls_, 1);

  delete new_operators[0];
}

TEST_F(BaseOperatorContextTest, CreateOutputChannelRewiresDependenciesAndRecurses) {
  RecordingOperator leaf(OperatorType::OPERATOR_NOOP);
  FakeOperator parent(OperatorType::OPERATOR_NOOP);
  FakeOperator inbound(OperatorType::OPERATOR_LOCAL_IN_BOUND);
  parent.AddDependency(&leaf);
  parent.AddDependency(&inbound);

  TSOutputRouterSpec output_spec;
  output_spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  auto* stream = output_spec.add_streams();
  stream->set_type(StreamEndpointType::LOCAL);
  leaf.GetRpcSpecInfo().output_specs_.push_back(&output_spec);

  std::vector<BaseOperator*> new_operators;
  EXPECT_EQ(leaf.BaseOperator::CreateOutputChannel(ctx_, new_operators),
            KStatus::SUCCESS);
  ASSERT_EQ(new_operators.size(), 1U);
  EXPECT_EQ(new_operators[0]->Type(), OperatorType::OPERATOR_LOCAL_OUT_BOUND);
  EXPECT_EQ(leaf.input_calls_, 1);
  EXPECT_EQ(new_operators[0]->GetChildren().front(), &leaf);
  EXPECT_TRUE(std::find(parent.GetChildren().begin(), parent.GetChildren().end(),
                        &leaf) == parent.GetChildren().end());
  EXPECT_EQ(inbound.GetChildren().back(), new_operators[0]);

  delete new_operators[0];
}

TEST_F(BaseOperatorContextTest, CreateTopOutputChannelHandlesLocalRemoteAndInvalidParent) {
  FakeOperator local_root(OperatorType::OPERATOR_NOOP);
  TSOutputRouterSpec local_spec;
  local_spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  local_spec.add_streams()->set_type(StreamEndpointType::LOCAL);
  local_root.GetRpcSpecInfo().output_specs_.push_back(&local_spec);

  std::vector<BaseOperator*> operators;
  EXPECT_EQ(local_root.CreateTopOutputChannel(ctx_, operators), KStatus::SUCCESS);
  EXPECT_TRUE(operators.empty());

  FakeTopOutputOperator remote_root(OperatorType::OPERATOR_NOOP);
  TSOutputRouterSpec remote_spec;
  remote_spec.set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  remote_spec.add_streams()->set_type(StreamEndpointType::REMOTE);
  remote_root.GetRpcSpecInfo().output_specs_.push_back(&remote_spec);

  EXPECT_EQ(remote_root.CreateTopOutputChannel(ctx_, operators), KStatus::SUCCESS);
  ASSERT_EQ(operators.size(), 1U);
  EXPECT_EQ(operators[0]->Type(), OperatorType::OPERATOR_REMOTR_OUT_BOUND);
  EXPECT_EQ(operators[0]->GetChildren().front(), &remote_root);

  FakeOperator parent(OperatorType::OPERATOR_NOOP);
  parent.AddDependency(&remote_root);
  EXPECT_EQ(remote_root.CreateTopOutputChannel(ctx_, operators), KStatus::FAIL);

  delete operators[0];
}

}  // namespace
}  // namespace kwdbts
