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

#include "ee_iterator_data_test.h"
#include "engine.h"
#include "../../engine/tests/test_util.h"
#include "ee_op_test_base.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

class TestProcessors : public OperatorTestBase {
 public:
  TestProcessors() : OperatorTestBase() {}
  ~TestProcessors() {}
  virtual void SetUp() {
    OperatorTestBase::SetUp();
    CreateScanFlowSpecAllCases(ctx_, &flow_, table_id_);
  }

  virtual void TearDown() {
    OperatorTestBase::TearDown();
    SafeDeletePointer(flow_);
  }
  TSFlowSpec* flow_{nullptr};
};

TEST_F(TestProcessors, TestProcessFlow) {
  KWThdContext *thd = new KWThdContext();
  current_thd = thd;

  char* result{nullptr};
  Processors* processors = new Processors();
  ASSERT_EQ(processors->Init(ctx_, flow_), SUCCESS);
  ASSERT_EQ(processors->InitIterator(ctx_, TsNextRetState::DML_NEXT), SUCCESS);
  k_uint32 count;
  k_uint32 size;
  k_bool is_last;
  EXPECT_EQ(processors->RunWithEncoding(ctx_, &result, &size, &count, &is_last),
            KStatus::SUCCESS);
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
