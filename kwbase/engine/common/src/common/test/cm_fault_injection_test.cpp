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

#include "cm_fault_injection.h"
#include "gtest/gtest.h"

namespace kwdbts {

template<typename T>
void BasicFunction(T val, T def_val, T expect) {
  INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, val, def_val, nullptr);
  EXPECT_EQ(val, expect);
}

template <typename T>
class BasicTypeTest : public testing::Test {};

typedef testing::Types<char, int16_t, int32_t, float, double> BasicTypes;
TYPED_TEST_CASE(BasicTypeTest, BasicTypes);

TYPED_TEST(BasicTypeTest, DataFaultBasicType) {
  k_int32 ret = InitFaultInjection();
  EXPECT_EQ(ret , 0);
  std::ostringstream oss;
  BasicFunction<TypeParam>(1, 2, 1);
  // active fault after the second time.
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 2", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<TypeParam>(1, 2, 1);
  BasicFunction<TypeParam>(1, 2, 2);  // active status from now on
  BasicFunction<TypeParam>(1, 2, 2);
  ret = ProcInjectFault("deactive FAULT_FOR_FAULT_INJECTION_UT", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<TypeParam>(1, 2, 1);  // deactive status from now on
  BasicFunction<TypeParam>(1, 2, 1);
  // reactive fault now.
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<TypeParam>(1, 2, 2);
  BasicFunction<TypeParam>(1, 2, 2);
  // re deactive
  ret = ProcInjectFault("deactive FAULT_FOR_FAULT_INJECTION_UT", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<TypeParam>(1, 2, 1);  // deactive status from now on
  ret = ProcInjectFault("trigger FAULT_FOR_FAULT_INJECTION_UT 2", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<TypeParam>(1, 2, 1);  // not effect
  BasicFunction<TypeParam>(1, 2, 2);  // actived and auto reset
  BasicFunction<TypeParam>(1, 2, 1);  // deactive status from now on
  BasicFunction<TypeParam>(1, 2, 1);
}

TEST(TestFaultInjection, DataTypeString) {
  k_int32 ret = InitFaultInjection();
  EXPECT_EQ(ret , 0);
  std::ostringstream oss;
  BasicFunction<KString>("origin_value", "fault_value", "origin_value");
  // active fault after the second time.
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 2", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<KString>("origin_value", "fault_value", "origin_value");
  BasicFunction<KString>("origin_value", "fault_value", "fault_value");  // active status from now on
  BasicFunction<KString>("origin_value", "fault_value", "fault_value");
  ret = ProcInjectFault("deactive FAULT_FOR_FAULT_INJECTION_UT", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<KString>("origin_value", "fault_value", "origin_value");  // deactive status from now on
  BasicFunction<KString>("origin_value", "fault_value", "origin_value");
  // reactive fault now.
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<KString>("origin_value", "fault_value", "fault_value");  // active status from now on
  BasicFunction<KString>("origin_value", "fault_value", "fault_value");
  // re deactive
  ret = ProcInjectFault("deactive FAULT_FOR_FAULT_INJECTION_UT", oss);
  EXPECT_EQ(ret , 0);
  BasicFunction<KString>("origin_value", "fault_value", "origin_value");  // deactive status from now on
}

TEST(TestFaultInjection, DataTypeStruct_member) {
  typedef struct tag_TestStruct_ {
    int int_val;
    KString str_val;
    char arr_val[5];
    char* ptr_val;
    tag_TestStruct_(int val): int_val(val) {}
  } TestStruct;

  auto StructFunction = []() {
    TestStruct val(4);
    EXPECT_EQ(val.int_val, 4);
    INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, val.int_val, 1, nullptr);
    EXPECT_EQ(val.int_val, 1);
    INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, val.str_val, "fault_value", nullptr);
    EXPECT_EQ(val.str_val, "fault_value");
    // set to another pointer
    INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, val.ptr_val, reinterpret_cast<char*>(0x0012345bad), nullptr);
    EXPECT_EQ(val.ptr_val, reinterpret_cast<char*>(0x12345bad));
    INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, val.ptr_val, nullptr, nullptr);
    EXPECT_EQ(val.ptr_val, nullptr);
    INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, val, TestStruct(100), nullptr);
    EXPECT_EQ(val.int_val, 100);
    // Arr type doesn't a construct function. So use the complex version.
    INJECT_DATA_FAULT_CMPX(FAULT_FOR_FAULT_INJECTION_UT, (void*)val.arr_val, [](const KString& data, void* val){
      (static_cast<char*>(val))[0] = 'a';
      (static_cast<char*>(val))[2] = 'b';
      (static_cast<char*>(val))[4] = 'c';
    });
    EXPECT_EQ(val.arr_val[0], 'a');
    EXPECT_EQ(val.arr_val[2], 'b');
    EXPECT_EQ(val.arr_val[4], 'c');
  };
  k_int32 ret = InitFaultInjection();
  EXPECT_EQ(ret , 0);
  std::ostringstream oss;
  // active fault right now.
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1", oss);
  EXPECT_EQ(ret , 0);
  StructFunction();
  StructFunction();
}

TEST(TestFaultInjection, Interpreter) {
  auto InterpreterFunction = [](const KString &expect) {
    KString data = "origin_value";
    INJECT_DATA_FAULT(FAULT_FOR_FAULT_INJECTION_UT, data, "fault_value", [](const KString &input, void* val){
      if (input == "fault1") {
        *(static_cast<KString *>(val)) = "fault_data1";
      } else {
        *(static_cast<KString *>(val)) = input;
        // *((KString *)val) = input;
      }
    });
    EXPECT_EQ(data, expect);
  };
  // active fault right now.
  std::ostringstream oss;
  k_int32 ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1", oss);
  EXPECT_EQ(ret, 0);
  // use default value
  InterpreterFunction("fault_value");
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1 fault1", oss);
  EXPECT_EQ(ret, 0);
  // set value by the interpreter
  InterpreterFunction("fault_data1");
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1 fault2", oss);
  EXPECT_EQ(ret, 0);
  // set value by the interpreter
  InterpreterFunction("fault2");
}

TEST(TestFaultInjection, FaultDelay) {
  auto DelayFunction = []() {
    INJECT_DELAY_FAULT(FAULT_FOR_FAULT_INJECTION_UT, 1*1000*1000);  // default delay 1s
  };
  // active fault right now.
  std::ostringstream oss;
  k_int32 ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1", oss);
  EXPECT_EQ(ret, 0);
  // use default value
  auto start = std::chrono::duration_cast<std::chrono::microseconds >(
          std::chrono::system_clock::now().time_since_epoch()).count();
  DelayFunction();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds >(
          std::chrono::system_clock::now().time_since_epoch()).count() - start;
  EXPECT_GE(duration, 1*1000*1000);
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 1 100000", oss);
  EXPECT_EQ(ret, 0);
  // use input value
  start = std::chrono::duration_cast<std::chrono::microseconds >(
          std::chrono::system_clock::now().time_since_epoch()).count();
  DelayFunction();
  duration = std::chrono::duration_cast<std::chrono::microseconds >(
          std::chrono::system_clock::now().time_since_epoch()).count() - start;
  EXPECT_GE(duration, 100000);
}

TEST(TestFaultInjection, FaultAbort) {
  auto AbortFunction = []() {
    INJECT_ABORT_FAULT(FAULT_FOR_FAULT_INJECTION_UT);
  };
  std::ostringstream oss;
  k_int32 ret = ProcInjectFault("deactive FAULT_FOR_FAULT_INJECTION_UT", oss);
  EXPECT_EQ(ret, 0);
  ret = ProcInjectFault("active FAULT_FOR_FAULT_INJECTION_UT 2", oss);
  EXPECT_EQ(ret, 0);
  AbortFunction();  // haven't active
  EXPECT_DEATH(AbortFunction(), "");  // active now
}

}  //  namespace kwdbts
