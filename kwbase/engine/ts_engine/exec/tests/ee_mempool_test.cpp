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

#include "ee_mempool.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestMempool : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

TEST_F(TestMempool, TestCreateMempool) {
  kwdbts::EE_PoolInfoDataPtr pstPoolTestInfo = kwdbts::EE_MemPoolInit(1, 16);
  EXPECT_EQ((pstPoolTestInfo != nullptr), true);
  kwdbts::k_char* pcTestMsg = kwdbts::EE_MemPoolMalloc(pstPoolTestInfo, ROW_BUFFER_SIZE);
  EXPECT_EQ((pcTestMsg != nullptr), true);
  kwdbts::KStatus status = kwdbts::EE_MemPoolFree(pstPoolTestInfo, pcTestMsg);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  status = kwdbts::EE_MemPoolCleanUp(pstPoolTestInfo);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  pstPoolTestInfo = nullptr;
}

TEST_F(TestMempool, TestWriteAndRead) {
  k_uint32 numOfBlock = 81920;
  kwdbts::EE_PoolInfoDataPtr pstPoolTestInfo = kwdbts::EE_MemPoolInit(numOfBlock, ROW_BUFFER_SIZE);
  EXPECT_EQ((pstPoolTestInfo != nullptr), true);
  kwdbts::k_char* pcTestMsg[numOfBlock];
  // allocate all memory.
  for (int i = 0; i < numOfBlock; ++i) {
    pcTestMsg[i] = kwdbts::EE_MemPoolMalloc(pstPoolTestInfo, ROW_BUFFER_SIZE);
    EXPECT_EQ((pcTestMsg != nullptr), true);
    // write some data in the block
    for (int j = 0; j < ROW_BUFFER_SIZE; ++j) {
      pcTestMsg[i][j] = i + j;
    }
  }
  kwdbts::KStatus status;
  for (int i = 0; i < numOfBlock; ++i) {
    if (i % 3 == 0) {
      status = kwdbts::EE_MemPoolFree(pstPoolTestInfo, pcTestMsg[i]);
      EXPECT_EQ(status, kwdbts::SUCCESS);
    }
  }
  for (int i = numOfBlock - 1; i >= 0; --i) {
    if (i % 3 == 0) {
      pcTestMsg[i] = kwdbts::EE_MemPoolMalloc(pstPoolTestInfo, ROW_BUFFER_SIZE);
      EXPECT_EQ(status, kwdbts::SUCCESS);
      // write some data in the block
      for (int j = 0; j < ROW_BUFFER_SIZE; ++j) {
        pcTestMsg[i][j] = i + j;
      }
    }
  }
  for (int i = 0; i < numOfBlock; ++i) {
    // verify the data is still correct
    int j = ROW_BUFFER_SIZE / 2;
    char val = i + j;
    EXPECT_EQ(pcTestMsg[i][j], val);
  }
  for (int i = 0; i < numOfBlock; ++i) {
    status = kwdbts::EE_MemPoolFree(pstPoolTestInfo, pcTestMsg[i]);
    EXPECT_EQ(status, kwdbts::SUCCESS);
  }
  status = kwdbts::EE_MemPoolCleanUp(pstPoolTestInfo);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  pstPoolTestInfo = nullptr;
}

TEST_F(TestMempool, TestInvalidArgumentsAndExternalBuffers) {
  EXPECT_EQ(kwdbts::EE_MemPoolInit(0, 16), nullptr);
  EXPECT_EQ(kwdbts::EE_MemPoolInit(1, 0), nullptr);
  EXPECT_EQ(kwdbts::EE_MemPoolMalloc(nullptr, 8), nullptr);
  EXPECT_EQ(kwdbts::EE_MemPoolFree(nullptr, reinterpret_cast<kwdbts::k_char*>(1)),
            kwdbts::FAIL);
  EXPECT_EQ(kwdbts::EE_MemPoolCleanUp(nullptr), kwdbts::FAIL);

  kwdbts::EE_PoolInfoDataPtr pool = kwdbts::EE_MemPoolInit(2, 16);
  ASSERT_NE(pool, nullptr);

  EXPECT_EQ(kwdbts::EE_MemPoolFree(pool, nullptr), kwdbts::FAIL);

  kwdbts::k_char* oversized = kwdbts::EE_MemPoolMalloc(pool, 32);
  ASSERT_NE(oversized, nullptr);
  EXPECT_EQ(kwdbts::EE_MemPoolFree(pool, oversized), kwdbts::SUCCESS);

  kwdbts::k_char* pooled = kwdbts::EE_MemPoolMalloc(pool, 16);
  ASSERT_NE(pooled, nullptr);
  EXPECT_EQ(kwdbts::EE_MemPoolFree(pool, pool->data_ + 1), kwdbts::FAIL);
  EXPECT_EQ(kwdbts::EE_MemPoolFree(pool, pooled), kwdbts::SUCCESS);
  EXPECT_EQ(kwdbts::EE_MemPoolCleanUp(pool), kwdbts::SUCCESS);
}

}  // namespace kwdbts
