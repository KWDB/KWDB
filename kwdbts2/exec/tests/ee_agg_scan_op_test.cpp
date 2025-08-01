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

#include "gtest/gtest.h"
#include "ee_dml_exec.h"
#include "ee_op_test_base.h"
#include "ee_op_spec_utils.h"

namespace kwdbts {

class TestAggScanOp : public OperatorTestBase {
 public:
  TestAggScanOp() : OperatorTestBase() {}

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }
};

// select hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
// from benchmark.cpu group by hostname  order by min_usage;
TEST_F(TestAggScanOp, TestDmlExecAgg) {
  TSFlowSpec flow;
  AggScanSpec agg_spec(table_id_);
  agg_spec.PrepareFlowSpec(flow);
  agg_spec.PrepareInputOutputSpec(flow);

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
