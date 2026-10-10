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

#include "lg_writer_file.h"

#include <gtest/gtest.h>
#include <cstring>
#include "cm_config.h"

namespace kwdbts {

class MockConfigImpl : public kwdbts::MockConfig {
 public:
  k_bool DoMockConfig(Config* config) override {
    std::cout << "Use Mock config\n";
    const int log_file_max_size = 10;
    config->AddOrUpdate("E1Primary:ME", "log_dir", "./kwdb_log");
    config->AddOrUpdate("E1Primary:ME", "log_file_max_size", std::to_string(log_file_max_size));
    return KTRUE;
  }
};
class MockConfigImpl2 : public kwdbts::MockConfig {
 public:
  k_bool DoMockConfig(Config* config) override {
    std::cout << "Use Mock config\n";
    const int log_file_max_size = 10;
    config->AddOrUpdate("E1Primary:ME", "log_dir", "");
    config->AddOrUpdate("E1Primary:ME", "log_file_max_size", std::to_string(log_file_max_size));
    return KTRUE;
  }
};
class MockConfigImpl3 : public kwdbts::MockConfig {
  k_bool DoMockConfig(Config* config) override {
    std::cout << "Use Mock config\n";
    const int log_file_max_size = -1;
    config->AddOrUpdate("E1Primary:ME", "log_dir", "./kwdb_log");
    config->AddOrUpdate("E1Primary:ME", "log_file_max_size", std::to_string(log_file_max_size));
    return KTRUE;
  }
};

extern MockConfig *mockobj;
class LogWriterFileTest : public ::testing::Test {
 protected:
  kwdbts::LogWriterFile *log_writer_ = nullptr;
  kwdbts::LogItem *pItem_ = nullptr;
  kwdbContext_p ctx = ContextManager::GetThreadContext();

  void SetUp() override {
    mockobj = KNEW MockConfigImpl();
    char buf[256] = {0};
    getcwd(buf, sizeof(buf));
    std::string cfg_path = buf;
    size_t pos = cfg_path.find("/ZDP/");
    cfg_path = cfg_path.substr(0, pos) + "/ZDP/kwdbts/server/config/kaiwudb.cfg";
    InitSysConfig(ctx, cfg_path.c_str());
    SetKwdbProcName("E1Primary", "ME");
    SetMeSection("E1Primary:ME");
    log_writer_ = KNEW kwdbts::LogWriterFile;
    log_writer_->Init();
    kwdbts::k_int32 len = 64;
    pItem_ = static_cast<kwdbts::LogItem *>(malloc(sizeof(kwdbts::LogItem) + len));
    if (nullptr == pItem_) {
      return;
    }
    memset(pItem_, 0, sizeof(kwdbts::LogItem) + len);

    pItem_->module = kwdbts::KwdbModule::ME;
    pItem_->severity = kwdbts::LogSeverity::INFO;
    pItem_->line = 100;
    pItem_->time_stamp = time(nullptr);
    pItem_->buf_size = len;
    strncpy(pItem_->buf, "this is a test.\r", pItem_->buf_size);
  }

  void TearDown() override {
    log_writer_->Destroy();
    delete log_writer_;
    char temp_dir_buf[256] = {0};
    snprintf(temp_dir_buf, sizeof(temp_dir_buf), "rm -r %s", GetMeSysConfig("log_dir"));
    system(temp_dir_buf);
    free(pItem_);
    DestroySysConfig(ctx);
    DestroyKWDBContext(ctx);
    delete mockobj;
  }
};

TEST_F(LogWriterFileTest, write_nullptr) { EXPECT_EQ(log_writer_->Write( nullptr), kwdbts::KStatus::FAIL); }

TEST_F(LogWriterFileTest, write) { EXPECT_EQ(log_writer_->Write( pItem_), kwdbts::KStatus::SUCCESS); }
TEST_F(LogWriterFileTest, dirCheck) {
  kwdbContext_p ctx2 = ContextManager::GetThreadContext();
  delete mockobj;
  DestroySysConfig(ctx);
  log_writer_->Destroy();
  delete log_writer_;
  log_writer_ = KNEW kwdbts::LogWriterFile;
  mockobj = KNEW MockConfigImpl2();
  kwdbts::InitServerKWDBContext(ctx2);
  char buf[256] = {0};
  getcwd(buf, sizeof(buf));
  std::string cfg_path = buf;
  size_t pos = cfg_path.find("/ZDP/");
  cfg_path = cfg_path.substr(0, pos) + "/ZDP/kwdbts/server/config/kaiwudb.cfg";
  InitSysConfig(ctx2, cfg_path.c_str());
  SetKwdbProcName("E1Primary", "ME");
  SetMeSection("E1Primary:ME");
  EXPECT_EQ(log_writer_->Init(), kwdbts::KStatus::FAIL);
  DestroySysConfig(ctx2);
  DestroyKWDBContext(ctx2);
}

TEST_F(LogWriterFileTest, write_check) {
  log_writer_->Write( pItem_);
  FILE *fp;
  char temp_dir_buf[128] = {0};
  snprintf(temp_dir_buf, sizeof(temp_dir_buf), "%s", log_writer_->GetLogFileName());
  EXPECT_NE((fp = fopen(temp_dir_buf, "r+")), nullptr);
  char buf[2048];
  fgets(buf, sizeof(buf) - 1, fp);
  // The length of the log statement output according to the set condition is matched
  EXPECT_EQ(54, strlen(buf));
  fclose(fp);
}
TEST_F(LogWriterFileTest, RollerCheck) {
  delete mockobj;
  DestroySysConfig(ctx);
  log_writer_->Destroy();
  delete log_writer_;
  char temp_dir_buf[256] = {0};
  snprintf(temp_dir_buf, sizeof(temp_dir_buf), "rm -r %s", GetMeSysConfig("log_dir"));
  system(temp_dir_buf);
  kwdbContext_p ctx3 = ContextManager::GetThreadContext();
  char buf[256] = {0};
  getcwd(buf, sizeof(buf));
  std::string cfg_path = buf;
  size_t pos = cfg_path.find("/ZDP/");
  cfg_path = cfg_path.substr(0, pos) + "/ZDP/kwdbts/server/config/kaiwudb.cfg";
  mockobj = KNEW MockConfigImpl3();
  kwdbts::InitServerKWDBContext(ctx3);
  InitSysConfig(ctx3, cfg_path.c_str());
  SetKwdbProcName("E1Primary", "ME");
  SetMeSection("E1Primary:ME");
  log_writer_ = KNEW kwdbts::LogWriterFile;
  log_writer_->Init();
  EXPECT_EQ(log_writer_->Write( pItem_), kwdbts::KStatus::SUCCESS);
  DestroySysConfig(ctx3);
  DestroyKWDBContext(ctx3);
}

}  // namespace kwdbts
