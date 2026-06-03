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
#include "ee_global.h"

namespace kwdbts {
class SafeMacrosTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
// test SafeDeletePointer macro
TEST(SafeMacrosTest, SafeDeletePointer) {
  int* ptr = new int(42);
  SafeDeletePointer(ptr);
  EXPECT_EQ(ptr, nullptr);  // Check if the pointer is set to nullptr
}

// test SafeFreePointer macro
TEST(SafeMacrosTest, SafeFreePointer) {
  int* ptr = static_cast<int*>(malloc(sizeof(int)));
  *ptr = 42;
  SafeFreePointer(ptr);
  EXPECT_EQ(ptr, nullptr);  // Check if the pointer is set to nullptr
}

// Test EEIteratorErrCodeToString function with all branches
TEST(EEIteratorErrCodeToStringTest, AllBranches) {
  // Test EE_OK
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_OK),
            "EEIteratorErrCode::EE_OK");

  // Test EE_ERROR
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_ERROR),
            "EEIteratorErrCode::EE_ERROR");

  // Test EE_DATA_ERROR
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_DATA_ERROR),
            "EEIteratorErrCode::EE_DATA_ERROR");

  // Test EE_END_OF_RECORD
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_END_OF_RECORD),
            "EEIteratorErrCode::EE_END_OF_RECORD");

  // Test EE_NEXT_CONTINUE
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_NEXT_CONTINUE),
            "EEIteratorErrCode::EE_NEXT_CONTINUE");

  // Test EE_KILLED
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_KILLED),
            "EEIteratorErrCode::EE_KILLED");

  // Test EE_QUIT
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_QUIT),
            "EEIteratorErrCode::EE_QUIT");

  // Test EE_Sample
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_Sample),
            "EEIteratorErrCode::EE_Sample");

  // Test EE_TIMESLICE_OUT
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_TIMESLICE_OUT),
            "EEIteratorErrCode::EE_TIMESLICE_OUT");

  // Test EE_ERROR_ZERO
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_ERROR_ZERO),
            "EEIteratorErrCode::EE_ERROR_ZERO");

  // Test EE_PTAG_COUNT_NOT_MATCHED
  EXPECT_EQ(EEIteratorErrCodeToString(EEIteratorErrCode::EE_PTAG_COUNT_NOT_MATCHED),
            "EEIteratorErrCode::EE_PTAG_COUNT_NOT_MATCHED");

  // Test default branch with unknown code
  EXPECT_EQ(EEIteratorErrCodeToString(static_cast<EEIteratorErrCode>(999)),
            "EEIteratorErrCode::UNKNOWN");
}

// Test KWDBTypeFamilyToString function with all branches
TEST(KWDBTypeFamilyToStringTest, AllBranches) {
  // Test BoolFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::BoolFamily), "BOOL");

  // Test IntFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::IntFamily), "INT");

  // Test FloatFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::FloatFamily), "FLOAT");

  // Test DecimalFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::DecimalFamily), "Decimal");

  // Test DateFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::DateFamily), "Date");

  // Test TimestampFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::TimestampFamily), "Timestamp");

  // Test IntervalFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::IntervalFamily), "Interval");

  // Test StringFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::StringFamily), "String");

  // Test BytesFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::BytesFamily), "Bytes");

  // Test TimestampTZFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::TimestampTZFamily), "TimestampTZ");

  // Test CollatedStringFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::CollatedStringFamily), "CollatedString");

  // Test OidFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::OidFamily), "Oid");

  // Test UnknownFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::UnknownFamily), "Unknown");

  // Test UuidFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::UuidFamily), "Uuid");

  // Test ArrayFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::ArrayFamily), "Array");

  // Test INetFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::INetFamily), "INet");

  // Test TimeFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::TimeFamily), "Time");

  // Test JsonFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::JsonFamily), "Json");

  // Test TimeTZFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::TimeTZFamily), "TimeTZ");

  // Test TupleFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::TupleFamily), "Tuple");

  // Test BitFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::BitFamily), "Bit");

  // Test AnyFamily
  EXPECT_EQ(KWDBTypeFamilyToString(KWDBTypeFamily::AnyFamily), "Any");
}

}  // namespace kwdbts
