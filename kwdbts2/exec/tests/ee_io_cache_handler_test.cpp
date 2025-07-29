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

#include "ee_io_cache_handler.h"

#include <chrono>
#include <iostream>

#include "ee_global.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_exec_pool.h"

using namespace kwdbts;  // NOLINT
#define USER_BUFFER_SIZE 268435456
class IOCacheHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ExecPool::GetInstance().db_path_ = "./";
    handler = new IOCacheHandler(MAX_FILE_SIZE);
  }

  void TearDown() override { delete handler; }

  IOCacheHandler* handler;
};

TEST_F(IOCacheHandlerTest, WriteBasicTest) {
  const char* testData = "Test data";
  KStatus status = handler->Write(testData, strlen(testData));
  EXPECT_EQ(status, KStatus::SUCCESS);
}

TEST_F(IOCacheHandlerTest, ReadBasicTest) {
  handler->Reset();

  k_uint32 writeDuration = 0;
  k_char* buffer = new k_char[USER_BUFFER_SIZE];
  const char str_content[] = "z0x1c5vb6n8ma9s2dfg3hjklqw4erty7uiop";
  memset(buffer, 0, USER_BUFFER_SIZE);
  for (k_uint32 j = 0; j < USER_BUFFER_SIZE; ++j) {
    k_int32 pos = j % 36;
    buffer[j] = str_content[pos];
  }
  for (int i = 0; i < 8; ++i) {
    auto writeStart = std::chrono::high_resolution_clock::now();

    handler->Write(buffer, USER_BUFFER_SIZE);
    auto writeEnd = std::chrono::high_resolution_clock::now();
    writeDuration += std::chrono::duration_cast<std::chrono::microseconds>(
                         writeEnd - writeStart)
                         .count();
  }
  std::cout << "Write time: " << writeDuration << " microseconds" << std::endl;

  char read_buffer[37] = {0};

  auto readStart = std::chrono::high_resolution_clock::now();
  KStatus status =
      handler->Read(read_buffer, strlen(str_content) * 10, strlen(str_content));
  auto readEnd = std::chrono::high_resolution_clock::now();
  auto readDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(readEnd - readStart)
          .count();
  std::cout << "Read time: " << readDuration << " microseconds" << std::endl;

  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_STREQ(read_buffer, str_content);
  SafeDeleteArray(buffer);
}

TEST_F(IOCacheHandlerTest, ResetTest) {
  const char* testData = "Test data";
  handler->Write(testData, strlen(testData));

  KStatus status = handler->Reset();
  EXPECT_EQ(status, KStatus::SUCCESS);
}
