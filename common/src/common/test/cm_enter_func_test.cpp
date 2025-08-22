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

#include "cm_func.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

class TestKWDBEnterFunc : public ::testing::Test {
 public:
  // function to use EnterFunc/Return SUCCESS macros
  KStatus myEnterFuncSuccess(kwdbContext_p ctx) {
    EnterFunc();
#ifdef K_DEBUG
    EXPECT_EQ(ctx->frame_level, 1);
    EXPECT_EQ(ctx->frame_level, my_frame_level);
#endif
    Return(KStatus::SUCCESS);
  }

  // function to test EnterFunc/Return SUCCESS macros
  KStatus testFuncEnterSuccess() {
    kwdbContext_p ctx = ContextManager::GetThreadContext();
    kwdbts::InitServerKWDBContext(ctx);
    return myEnterFuncSuccess(ctx);
  }

  // function to use EnterFunc/Return FAIL macros
  KStatus myEnterFuncFail(kwdbContext_p ctx) {
    EnterFunc();
#ifdef K_DEBUG
    EXPECT_EQ(ctx->frame_level, 1);
    EXPECT_EQ(ctx->frame_level, my_frame_level);
#endif
    Return(KStatus::FAIL);
  }

  // function to test EnterFunc/Return FAIL macros
  KStatus testFuncEnterFail() {
    kwdbContext_p ctx = ContextManager::GetThreadContext();
    return myEnterFuncFail(ctx);
  }
};

// test EnterFunc & Return SUCCESS
TEST_F(TestKWDBEnterFunc, sampleTestEnterFuncSuccess) {
  KStatus ret = testFuncEnterSuccess();
  EXPECT_EQ(ret, KStatus::SUCCESS);
}

// test EnterFunc & Return FAIL
TEST_F(TestKWDBEnterFunc, sampleTestEnterFuncFail) {
  KStatus ret = testFuncEnterFail();
  EXPECT_EQ(ret, KStatus::FAIL);
}

class TestBitmapFunc : public ::testing::Test {};

TEST_F(TestBitmapFunc, basicTest) {
  char *bitmap = KNEW char[10];
  // Each element in the cols array indicates whether the value of the corresponding column is valid.
  // First initialize the values to 0
  // For example, the value of cols[1] indicates whether the value of the first column is valid,
  // with 0 representing valid and 1 representing NULL
  std::vector<k_uint32> cols(80, 0);
  // The value of each element in the array represents a column with a NULL value,
  // and is randomly initialized to columns 4, 25, 49, 50, 59, 70, and 79
  std::vector<k_uint32> invalid_cols{4, 25, 49, 50, 59, 70, 79};
  //Set the values of the columns corresponding to invalid_cols in the cols array to 1
  for (auto invalid_col : invalid_cols) {
    cols[invalid_col] = 1;
  }
  // Set the value of each corresponding bit in the bitmap based on the values of the cols array elements
  for (k_uint32 i = 0; i != cols.size(); ++i) {
    if (0 == cols[i]) {
      SetObjectColNotNull(bitmap, i);
    } else {
      SetObjectColNull(bitmap, i);
    }
  }
  // Verify that each column of invalid_cols has its corresponding bit correctly set to NULL (1) in the bitmap
  for (auto invalid_col : invalid_cols) {
    EXPECT_EQ(IsObjectColNull(bitmap, invalid_col), KTRUE);
  }
  // Verify that the corresponding bit of each column in the bitmap, except for invalid_cols, is correctly set to valid
  for (k_uint32 i = 0; i != cols.size(); ++i) {
    if (std::find(invalid_cols.begin(), invalid_cols.end(), i) == invalid_cols.end()) {
      EXPECT_EQ(IsObjectColNull(bitmap, i), KFALSE);
    }
  }
  delete[] bitmap;
}

TEST(TestKWDB64FMT, normal) {
  char buf[128] = {0};
  k_uint64 u64Var = 12345678901234567890ULL;

  snprintf(buf, sizeof(buf), "%" K_UINT64_FMT "\n", u64Var);
  EXPECT_STREQ(buf, "12345678901234567890\n");

  memset(buf, 0, sizeof(buf));
  k_int64 i64Var = -1234567890123456789LL;
  snprintf(buf, sizeof(buf), "%" K_INT64_FMT "\n", i64Var);
  EXPECT_STREQ(buf, "-1234567890123456789\n");
}

TEST(TestToGMT, correctness) {
  time_t ts = 0;
  tm kw_tm;
  tm glibc_tm;
  for (int i = 0; i < 62; ++i) {
    ToGMT(ts, kw_tm);
    gmtime_r(&ts, &glibc_tm);
    EXPECT_EQ(kw_tm, glibc_tm);
    ts += 1;
    ts *= 2;
  }
}
}  //  namespace kwdbts
