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

#include "lg_writer_console.h"

#include <gtest/gtest.h>

class LogWriterConsoleTest : public ::testing::Test {
 protected:
  kwdbts::LogWriterConsole *log_writer_ = nullptr;
  kwdbts::LogItem *pItem_ = nullptr;
  kwdbts::kwdbContext_p ctx = kwdbts::ContextManager::GetThreadContext();

  void SetUp() override {
    log_writer_ = KNEW kwdbts::LogWriterConsole;
    // TODO(KNEW): add nullptr check
    log_writer_->Init();
    kwdbts::k_int32 len = 64;
    pItem_ = static_cast<kwdbts::LogItem *>(malloc(sizeof(kwdbts::LogItem) + len));
    if (nullptr == pItem_) {
      return;
    }
    memset(pItem_, 0, sizeof(kwdbts::LogItem) + len);

    pItem_->module = kwdbts::KwdbModule::ME;
    pItem_->severity = kwdbts::LogSeverity::INFO;
    strncpy(pItem_->file_name, "cn_test.cpp", sizeof(pItem_->file_name));
    pItem_->line = 100;
    pItem_->time_stamp = time(nullptr);
    pItem_->buf_size = len;
    strncpy(pItem_->buf, "this is a test.\r\n", pItem_->buf_size);
  }

  void TearDown() override {
    log_writer_->Destroy();
    delete log_writer_;
    DestroyKWDBContext(ctx);
  }
};
TEST_F(LogWriterConsoleTest, write_nullptr) {
  EXPECT_EQ(log_writer_->Write( nullptr), kwdbts::KStatus::FAIL);
  free(pItem_);
}

TEST_F(LogWriterConsoleTest, write) {
  EXPECT_EQ(log_writer_->Write( pItem_), kwdbts::KStatus::SUCCESS);
  free(pItem_);
}
