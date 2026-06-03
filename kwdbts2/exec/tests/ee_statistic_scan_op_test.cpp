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

#include "ee_dml_exec.h"
#include "ee_op_spec_utils.h"
#include "ee_op_test_base.h"
#include "gtest/gtest.h"

namespace kwdbts {

// Statistic scan operation test spec
class SpecStatisticScan : public SpecBase {
 public:
  explicit SpecStatisticScan(k_uint64 table_id) : SpecBase(table_id) {
  }

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    // tag reader
    PrepareTagReaderProcessor(flow, 0);

    // statistic reader
    PrepareStatisticReaderProcessor(flow, 1);

    // synchronizer
    PrepareSynchronizerProcessor(flow, 2);
  }

 protected:
  // include hostname in the Tag Reader Spec.
  void initTagReaderPostSpec(PostProcessSpec& post) override {
    // primary tag
    post.add_output_columns(4);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));

    // normal tag
    post.add_output_columns(5);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(6);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    post.set_projection(true);
  }

  // output columns for statistic reader
  void initStatisticReaderPostSpec(PostProcessSpec& post) override {
    // timestamp columns
    post.add_output_columns(0);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampTZFamily, 8));

    // metrics columns
    post.add_output_columns(1);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(2);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(3);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types for statistic reader
  void initOutputTypes(PostProcessSpec& post) override {
    // timestamp columns
    post.add_output_types(MarshalToOutputType(TimestampTZFamily, 8));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(StringFamily, 10));

    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(StringFamily, 10));
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {
  }

  void initAggOutputTypes(PostProcessSpec& post) override {
  }
};

class TestStatisticScanOp : public OperatorTestBase {
 public:
  TestStatisticScanOp() : OperatorTestBase() {
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }
};

// Test statistic scan operation
TEST_F(TestStatisticScanOp, TestDmlExecStatisticScan) {
  TSFlowSpec flow;
  SpecStatisticScan statistic_scan_spec(table_id_);
  statistic_scan_spec.PrepareFlowSpec(flow);
  statistic_scan_spec.PrepareInputOutputSpec(flow);

  size_t size = flow.ByteSizeLong();

  auto req = make_unique<char[]>(sizeof(QueryInfo));
  auto resp = make_unique<char[]>(sizeof(QueryInfo));
  auto message = make_unique<char[]>(size);
  flow.SerializeToArray(message.get(), size);

  auto* info = reinterpret_cast<QueryInfo*>(req.get());
  auto* info2 = reinterpret_cast<QueryInfo*>(resp.get());
  info->tp = EnMqType::MQ_TYPE_DML_SETUP;
  info->len = size;
  info->id = 3;
  info->unique_id = 34716;
  info->handle = nullptr;
  info->value = message.get();
  info->ret = 0;
  info->time_zone = 0;
  info->relBatchData = nullptr;
  info->relRowCount = 0;

  KStatus status = DmlExec::ExecQuery(ctx_, info, info2);
  ASSERT_EQ(status, KStatus::SUCCESS);

  auto* result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));
  ASSERT_EQ(result->ret, SUCCESS);

  // next
  info->tp = EnMqType::MQ_TYPE_DML_NEXT;

  do {
    ASSERT_EQ(DmlExec::ExecQuery(ctx_, info, info2), KStatus::SUCCESS);
    result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));

    if (result->value) {
      free(result->value);
      result->value = nullptr;
    }
  } while (result->code != -1);

  ASSERT_EQ(result->ret, SUCCESS);

  info->handle = result->handle;
  info->tp = EnMqType::MQ_TYPE_DML_CLOSE;
  DmlExec::ExecQuery(ctx_, info, info2);
}

}  // namespace kwdbts