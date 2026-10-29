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

#include <gtest/gtest.h>

#include "cm_config.h"
#include "lg_api.h"

namespace kwdbts {
class MockConfigImpl : public kwdbts::MockConfig {
 public:
  k_bool DoMockConfig(Config* config) override {
    const int log_file_max_size = 10;
    config->AddOrUpdate("E1Primary:ME", "log_file_max_size", std::to_string(log_file_max_size));
    config->AddOrUpdate("E1Primary:ME", "log_level", "st 0,mm 4,kwsa 4");
    return KTRUE;
  }
};
extern MockConfig* mockobj;

std::string getLineFromStdin() {
  fd_set fdSet;
  FD_ZERO(&fdSet);
  FD_SET(STDIN_FILENO, &fdSet);

  timeval timeout = {.tv_sec = 0, .tv_usec = 1000};

  std::stringstream ss("");
  while (true) {
    if (select(STDIN_FILENO + 1, &fdSet, nullptr, nullptr, &timeout) <= 0) {
      break;
    }
    std::string inStr;
    std::getline(std::cin, inStr);
    ss << inStr;
  }
  return ss.str();
}
class LogImplTest : public ::testing::Test {
 protected:
  kwdbContext_p ctx = ContextManager::GetThreadContext();
  int originalStdOut_;
  int originalStdIn_;
  int pipe_fd_[2];

  void SetUp() override {
    mockobj = new MockConfigImpl;
    // TODO(KNEW): add nullptr check
    kwdbContext_p ctx = ContextManager::GetThreadContext();
    char buf[256] = {0};
    getcwd(buf, sizeof(buf));
    std::string cfg_path = buf;
    size_t pos = cfg_path.find("/ZDP/");
    cfg_path = cfg_path.substr(0, pos) + "/ZDP/kwdbts/server/config/kaiwudb.cfg";
    InitSysConfig(ctx, cfg_path.c_str());
    SetKwdbProcName("E1Primary", "ME");
    SetMeSection("E1Primary:ME");
    originalStdOut_ = dup(STDOUT_FILENO);
    originalStdIn_ = dup(STDIN_FILENO);
    pipe(pipe_fd_);
    dup2(pipe_fd_[1], STDOUT_FILENO);
    dup2(pipe_fd_[0], STDIN_FILENO);
  }

  void TearDown() override {
    DestroySysConfig(ctx);
    DestroyKWDBContext(ctx);
    delete mockobj;
    dup2(originalStdOut_, STDOUT_FILENO);
    dup2(originalStdIn_, STDIN_FILENO);
    close(pipe_fd_[0]);
    close(pipe_fd_[1]);
  }

  static void TearDownTestCase() {
    kwdbContext_p ctx = ContextManager::GetThreadContext();
    kwdbts::LOGGER.Destroy();
    std::cout << "run after last case..." << std::endl;
  }
};

TEST_F(LogImplTest, infoLocal) {
  kwdbts::LOGGER.SetModuleSeverity(kwdbts::ST, kwdbts::INFO);
  kwdbts::LOGGER.Log( kwdbts::DEBUG,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  kwdbts::LOGGER.Log( kwdbts::INFO,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  kwdbts::LOGGER.Log( kwdbts::WARN,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  kwdbts::LOGGER.Log( kwdbts::ERROR,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  kwdbts::LOGGER.Log( kwdbts::FATAL,gettid(), "src/test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
}
TEST_F(LogImplTest, info) {
  kwdbts::LOGGER.Init();
  kwdbts::LOGGER.SetModuleSeverity(kwdbts::ST, kwdbts::INFO);
  kwdbts::LOGGER.Log( kwdbts::DEBUG,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::INFO,gettid(), "src/st_test.cpp", 89, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "I-st TEST 01/01 00:00:00.000000 st_test.cpp:89 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::WARN,gettid(), "src/st_test.cpp", 93, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), gettid(),"W-st TEST 01/01 00:00:00.000000 st_test.cpp:93 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::ERROR,gettid(), "src/st_test.cpp", 94, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "E-st TEST 01/01 00:00:00.000000 st_test.cpp:94 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::FATAL,gettid(), "src/test.cpp", 95, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "F-un TEST 01/01 00:00:00.000000 test.cpp:95 0 0 this is a test.2:10.700000. OK\r");
}
TEST_F(LogImplTest, error) {
  kwdbts::LOGGER.Init();
  kwdbts::LOGGER.SetModuleSeverity(kwdbts::ST, kwdbts::ERROR);
  kwdbts::LOGGER.Log(kwdbts::DEBUG,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log(kwdbts::INFO,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log(kwdbts::WARN,gettid(), "src/st_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log(kwdbts::ERROR,gettid(), "src/st_test.cpp", 112, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "E-st TEST 01/01 00:00:00.000000 st_test.cpp:112 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log(kwdbts::FATAL,gettid(), "src/st_test.cpp", 113, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "F-st TEST 01/01 00:00:00.000000 st_test.cpp:113 0 0 this is a test.2:10.700000. OK\r");
}
TEST_F(LogImplTest, setLogModLevelTest) {
  kwdbts::LOGGER.Init();
  kwdbts::LOGGER.setLogModLevel();
  kwdbts::LOGGER.Log( kwdbts::DEBUG,gettid(), "src/st_test.cpp", 120, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "D-st TEST 01/01 00:00:00.000000 st_test.cpp:120 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::INFO,gettid(), "src/st_test.cpp", 121, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "I-st TEST 01/01 00:00:00.000000 st_test.cpp:121 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::WARN,gettid(), "src/st_test.cpp", 122, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "W-st TEST 01/01 00:00:00.000000 st_test.cpp:122 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::ERROR,gettid(), "src/st_test.cpp", 123, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "E-st TEST 01/01 00:00:00.000000 st_test.cpp:123 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::FATAL, "src/st_test.cpp", 124, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "F-st TEST 01/01 00:00:00.000000 st_test.cpp:124 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::INFO,gettid(), "src/mm_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::WARN,gettid(), "src/mm_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::ERROR,gettid(), "src/mm_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::FATAL,gettid(), "src/mm_test.cpp", 128, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),gettid(), "F-mm TEST 01/01 00:00:00.000000 mm_test.cpp:128 0 0 this is a test.2:10.700000. OK\r");
  kwdbts::LOGGER.Log( kwdbts::DEBUG,gettid(), "src/kwsa_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::INFO,gettid(), "src/kwsa_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::WARN,gettid(), "src/kwsa_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::ERROR,gettid(), "src/kwsa_test.cpp", __LINE__, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(), "");
  kwdbts::LOGGER.Log( kwdbts::FATAL,gettid(), "src/kwsa_test.cpp", 133, "this is a test.%d:%f. %s\r\n", 2, 10.7, "OK");
  EXPECT_EQ(getLineFromStdin(),
            "F-kwsa TEST 01/01 00:00:00.000000 kwsa_test.cpp:133 0 0 this is a test.2:10.700000. OK\r");
}

TEST_F(LogImplTest, getLogLevelByMod) {
  KString configStr = "st 1";
  LogSeverity logSeverity;
  logSeverity = kwdbts::LOGGER.getLogLevelByMod(KwdbModule::ST, &configStr);
  EXPECT_EQ(logSeverity, LogSeverity::INFO);

  configStr = "st 2";
  logSeverity = kwdbts::LOGGER.getLogLevelByMod(KwdbModule::ST, &configStr);
  EXPECT_EQ(logSeverity, LogSeverity::WARN);

  configStr = "st 1";
  // If not configured, it defaults to the WARN level
  logSeverity = kwdbts::LOGGER.getLogLevelByMod(KwdbModule::ME, &configStr);
  EXPECT_EQ(logSeverity, LogSeverity::WARN);

  configStr = "st a";
  logSeverity = kwdbts::LOGGER.getLogLevelByMod(KwdbModule::ST, &configStr);
  EXPECT_EQ(logSeverity, LogSeverity::DEBUG);

  configStr = "st dfa,me,";
  logSeverity = kwdbts::LOGGER.getLogLevelByMod(KwdbModule::ST, &configStr);
  EXPECT_EQ(logSeverity, LogSeverity::WARN);
  logSeverity = kwdbts::LOGGER.getLogLevelByMod(KwdbModule::ME, &configStr);
  EXPECT_EQ(logSeverity, LogSeverity::DEBUG);
}

}  // namespace kwdbts
