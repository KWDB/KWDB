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

#include "ee_op_test_base.h"
#include "ee_op_spec_utils.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

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
}  // namespace kwdbts
