// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "tr_impl.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "cm_config.h"
#include "mq_queue_manager.h"
#include "tr_api.h"

namespace kwdbts {
class TraceImplTest : public ::testing::Test {
 protected:
  kwdbContext_p ctx = ContextManager::GetThreadContext();
  kwdbts::Tracer *tracer = nullptr;

  void SetUp() override {
    tracer = &kwdbts::TRACER;
    tracer->init();
    tracer->SetTraceConfigStr("st 0,ma 0,mm 0,th 2,cn 3,ee 4");
  }

  void TearDown() override {
    DestroyKWDBContext(ctx);
  }

  static void TearDownTestCase() {
    kwdbContext_p ctx = ContextManager::GetThreadContext();
    kwdbts::TRACER.Destroy(ctx);
    std::cout << "run after last case..." << std::endl;
  }
};

TEST_F(TraceImplTest, traceTest) {
  kwdbts::TRACER.Trace(ctx, "src/st_test.cpp", __LINE__, 0, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  kwdbts::TRACER.Trace(ctx, "src/st_test.cpp", __LINE__, 1, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  kwdbts::TRACER.Trace(ctx, "src/st_test.cpp", __LINE__, 2, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
}
TEST_F(TraceImplTest, TRACETest) {
  TRACE(ctx, 0, "test");
  TRACE(ctx, 1, "test");
  TRACE(ctx, 2, "test");
  TRACE_LEVEL4("test");
  TRACE_LEVEL3("test");
  TRACE_LEVEL2("test");
  TRACE_LEVEL1("test");
}
TEST_F(TraceImplTest, setModuleLevel) {
  kwdbts::TRACER.setModuleLevel(kwdbts::KwdbModule::ST, 1);
  EXPECT_FALSE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::ST, 2));
  EXPECT_TRUE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::ST, 1));
}

TEST_F(TraceImplTest, SetTraceConfigStr) {
  kwdbts::TRACER.SetTraceConfigStr("st 0,ma 0,mm 0,th 2,cn 3,ee 5,sv 11");
  EXPECT_FALSE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::ST, 2));
  EXPECT_FALSE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::ME, 2));
  EXPECT_TRUE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::SV, 2));
  EXPECT_TRUE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::CN, 2));

  kwdbts::TRACER.SetTraceConfigStr("");
  EXPECT_FALSE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::ST, 2));
  EXPECT_FALSE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::CN, 2));
}

TEST_F(TraceImplTest, SetTraceConfigStrErr) {
  kwdbts::TRACER.SetTraceConfigStr("on");
  EXPECT_FALSE(kwdbts::TRACER.isEnableFor(kwdbts::KwdbModule::ST, 2));
}

TEST_F(TraceImplTest, splitString) {
  int index = 0;
  splitString(",,st 0,,sv 11,", ',', [&index](char *token) {
    if (index == 0) {
      EXPECT_STREQ(token, "st 0");
    }
    if (index == 1) {
      EXPECT_STREQ(token, "sv 11");
    }
    index++;
  });
  EXPECT_EQ(index, 2);
}
TEST_F(TraceImplTest, parseConfigStr) {
  int index = 0;
  parseConfigStr(",,st 0,, sv  11,", [&index](const char *key, k_int32 value) {
    if (index == 0) {
      EXPECT_STREQ(key, "st");
      EXPECT_EQ(value, 0);
    }
    if (index == 1) {
      EXPECT_STREQ(key, "sv");
      EXPECT_EQ(value, 11);
    }
    index++;
  });
  EXPECT_EQ(index, 2);
}

}  // namespace kwdbts
