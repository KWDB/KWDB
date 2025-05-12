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

#include "st_wal_internal_buffer_mgr.h"

#include "engine.h"
#include "st_wal_internal_logfile_mgr.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db_buf";

const string TestBigTableInstance::kw_home_ = kDbPath;
const string TestBigTableInstance::db_name_ = "tsdb";  // database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestWalManagerWriter : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  WALMgr* wal_;
  uint64_t table_id_ = 10001;
  vector<AttributeInfo> schema_;
  EngineOptions opts_{};
  WALBufferMgr* buf_;
  WALFileMgr* wal_file_mgr_;

  TestWalManagerWriter() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    // clear data dir
    system(("rm -rf " + kDbPath + "/*").c_str());
    if (access(kDbPath.c_str(), 0)) {
      system(("mkdir " + kDbPath).c_str());
    }

    opts_.db_path = kDbPath + "/1/";
    opts_.wal_buffer_size = 1;
    opts_.wal_file_in_group = 10;
    opts_.wal_file_size = 1;
    wal_ = new WALMgr(opts_.db_path, table_id_, 101, &opts_);
    wal_->Create(ctx_);

    wal_file_mgr_ = new WALFileMgr(opts_.db_path + "11/", table_id_, &opts_);
    KStatus res = wal_file_mgr_->initWalFile(0, BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE);
    EXPECT_EQ(res, KStatus::SUCCESS);

    buf_ = new WALBufferMgr(&opts_, wal_file_mgr_);
    buf_->init(BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE);
  }

  ~TestWalManagerWriter() override {
    delete wal_file_mgr_;
    wal_file_mgr_ = nullptr;
    delete wal_;
    wal_ = nullptr;
    delete buf_;
    buf_ = nullptr;
  }

  static vector<LogEntry*> GenerateInsertMetricsLog(uint32_t num, uint32_t char_num) {
    vector<LogEntry*> logs;
    char log[char_num];
    memset(log, 'A', char_num);
    for (int i = 0; i < num; i++) {
      std::stringstream ss;
      ss << std::setw(10) << std::setfill('0') << i;

      std::string result = ss.str();
      memcpy(log, result.c_str(), 10);
      TS_LSN l = 0;
      auto* insert_log = new InsertLogMetricsEntry(l, WALLogType::INSERT, i, WALTableType::DATA,
                                                   i, i, char_num, log, char_num, log);

      logs.push_back(insert_log);
    }
    return logs;
  }

  static void CheckLogs(uint32_t num, vector<LogEntry*>& logs, uint32_t char_num) {
    EXPECT_EQ(logs.size(), num);

    for (int i = 0; i < logs.size(); i++) {
      std::stringstream ss;
      ss << std::setw(10) << std::setfill('0') << i;

      std::string result = ss.str();
      char* id = const_cast<char*>(result.c_str());
      auto* insert_log = (InsertLogMetricsEntry*) ((logs)[i]);
      EXPECT_EQ(i, insert_log->getXID());
      EXPECT_EQ(i, insert_log->time_partition_);
      EXPECT_EQ(i, insert_log->offset_);
      EXPECT_EQ(char_num, insert_log->length_);
      EXPECT_EQ(char_num, insert_log->p_tag_len_);
      EXPECT_EQ(WALLogType::INSERT, insert_log->getType());
      EXPECT_EQ(WALTableType::DATA, insert_log->getTableType());

      for (int j = 0; j < 10; j++) {

        EXPECT_EQ(id[j], insert_log->data_[j]);
        EXPECT_EQ(id[j], insert_log->encoded_primary_tags_[j]);
//      if (id[j] != insert_log->data[j]) {
//        std::cout << endl;
//      }
      }
      for (int j = 10; j < char_num; j++) {
        EXPECT_EQ('A', insert_log->data_[j]);
        EXPECT_EQ('A', insert_log->encoded_primary_tags_[j]);
//      if (insert_log->data[j] != 'A') {
        //        char* l = logs[i];
        //        std::cout<<"111"<<endl;
//      }
      }
    }

  }

  //  virtual void SetUp() {
  //  }
  //
  //  virtual void TearDown() {
  //  }
};

TEST_F(TestWalManagerWriter, TestInit) {
  EngineOptions opts;
  WALFileMgr wal_mgr = WALFileMgr(kDbPath + "/", table_id_, &opts);
  KStatus res;

  res = wal_mgr.initWalFile(0, 4096 + 12);
  EXPECT_EQ(res, SUCCESS);

  HeaderBlock hb = wal_mgr.readHeaderBlock();

  EXPECT_EQ(hb.getStartLSN(), 0);
  EXPECT_EQ(hb.getFirstLSN(), 4108);
  EXPECT_EQ(hb.getStartBlockNo(), 0);
  EXPECT_EQ(hb.getBlockNum(), opts.GetBlockNumPerFile());
  EXPECT_EQ(hb.getCheckpointNo(), 0);
  EXPECT_EQ(hb.getCheckpointLSN(), 4108);
}

TEST_F(TestWalManagerWriter, TestInsertLogTagsEntry) {
  auto opts = new EngineOptions();

  opts->db_path = kDbPath + "/";
  opts->wal_buffer_size = 1;
  opts->wal_file_in_group = 100;
  opts->wal_file_size = 4;

  WALFileMgr wal_mgr = WALFileMgr(opts->db_path, table_id_, opts);
  KStatus res;
  res = wal_mgr.initWalFile(0, 4096 + 12);
  EXPECT_EQ(res, SUCCESS);

  EXPECT_EQ(res, KStatus::SUCCESS);

  HeaderBlock hb = wal_mgr.readHeaderBlock();
  EXPECT_EQ(hb.getFirstLSN(), 4108);
  EXPECT_EQ(hb.getCheckpointNo(), 0);
  EXPECT_EQ(hb.getCheckpointLSN(), 4108);
  WALBufferMgr buf = WALBufferMgr(opts, &wal_mgr);
  buf.init(4108);
  char log[80];
  memset(log, 'A', 80);
  uint32_t num = 100;
  for (int i = 0; i < num; i++) {
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << i;

    std::string result = ss.str();
    memcpy(log, result.c_str(), 10);
    TS_LSN l = 0;
    auto insert_log = InsertLogTagsEntry(0, WALLogType::INSERT, i, WALTableType::TAG,
                                         i, i, 80, log);

    char* wal_log = insert_log.encode();
    size_t length = insert_log.getLen();

    KStatus ret = buf.writeWAL(nullptr, wal_log, length, l);
    delete[] wal_log;
    EXPECT_EQ(ret, SUCCESS);
    EXPECT_EQ(l, buf.getCurrentLsn());
  }
  buf.flushInternal(true);

  vector<EntryBlock*> blocks;
  wal_mgr.readEntryBlocks(blocks, 0, 27941);
  size_t index = 0;

  for (auto block : blocks) {
    EXPECT_EQ(block->getBlockNo(), index++);
    delete block;
  }

  vector<LogEntry*> logs;
  std::vector<uint64_t> ignore;
  buf.readWALLogs(logs, 4096 + 12, buf.getCurrentLsn(), ignore);
  EXPECT_EQ(logs.size(), num);

  for (int i = 0; i < logs.size(); i++) {
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << i;

    std::string result = ss.str();
    char* id = const_cast<char*>(result.c_str());
    auto* insert_log = (InsertLogTagsEntry*) (logs[i]);
    EXPECT_EQ(i, insert_log->getXID());
    EXPECT_EQ(i, insert_log->time_partition_);
    EXPECT_EQ(i, insert_log->offset_);
    EXPECT_EQ(80, insert_log->length_);
    EXPECT_EQ(WALLogType::INSERT, insert_log->getType());
    EXPECT_EQ(WALTableType::TAG, insert_log->getTableType());
    for (int j = 0; j < 10; j++) {
      EXPECT_EQ(id[j], insert_log->data_[j]);
      if (id[j] != insert_log->data_[j]) {
        std::cout << endl;
      }
    }
    for (int j = 10; j < 80; j++) {
      EXPECT_EQ('A', insert_log->data_[j]);
      // if (insert_log->data[j] != 'A') {
      //     char* l = logs[i];
      //     std::cout<<"111"<<endl;
      // }
    }
  }
  EXPECT_EQ(logs.size(), num);
  delete opts;
  for (auto& l : logs) {
    l->prettyPrint();
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestInsertLogMetricsEntry) {
  auto opts = new EngineOptions();

  opts->db_path = kDbPath + "/";
  opts->wal_buffer_size = 4;
  opts->wal_file_in_group = 10;
  opts->wal_file_size = 128;

  WALFileMgr wal_mgr = WALFileMgr(opts->db_path, table_id_, opts);
  KStatus res;
  res = wal_mgr.initWalFile(0, 4096 + 12);
  EXPECT_EQ(res, SUCCESS);

  WALBufferMgr buf = WALBufferMgr(opts, &wal_mgr);
  buf.init(4108);
  uint32_t num = 100;
  TS_LSN lsn;
  vector<LogEntry*> logs = GenerateInsertMetricsLog(num, 80);
  for (int i = 0; i < logs.size(); i++) {
    char* wal_log = logs[i]->encode();
    size_t len = logs[i]->getLen();
    KStatus s = buf.writeWAL(ctx_, wal_log, len, lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
  }

  buf.flushInternal(true);

  vector<LogEntry*> r_logs;
  std::vector<uint64_t> ignore;
  buf.readWALLogs(r_logs, 4096 + 12, buf.getCurrentLsn(), ignore);
  CheckLogs(num, r_logs, 80);
  for (auto& l : logs) {
    l->prettyPrint();
    delete l;
  }
  for (auto& l : r_logs) {
    delete l;
  }
  delete opts;
}

TEST_F(TestWalManagerWriter, TestBufferRestart) {
  auto* opts = new EngineOptions();

  opts->wal_level = WALMode::SYNC;

  opts->db_path = kDbPath + "/";
  opts->wal_buffer_size = 4;
  opts->wal_file_in_group = 10;
  opts->wal_file_size = 16;

  auto* wal_mgr = new WALFileMgr(kDbPath + "/wal/", table_id_, opts);
  KStatus res = wal_mgr->initWalFile(0, 4096 + 12);
  EXPECT_EQ(res, SUCCESS);
  auto* buf = new WALBufferMgr(opts, wal_mgr);
  buf->init(4096 + 12);
  TS_LSN lsn;
  vector<LogEntry*> logs = GenerateInsertMetricsLog(20, 4000);
  for (int i = 0; i < 20; i++) {
    char* wal_log = logs[i]->encode();
    size_t len = logs[i]->getLen();
    KStatus s = buf->writeWAL(ctx_, wal_log, len, lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
  }
  buf->setHeaderBlockCheckpointInfo(lsn, 1);
  buf->flushInternal(true);
  std::cout << "lsn: " << lsn << endl;
  delete buf;
  delete wal_mgr;

  wal_mgr = new WALFileMgr(kDbPath + "/wal/", table_id_, opts);
  res = wal_mgr->Open();
  EXPECT_EQ(res, SUCCESS);
  buf = new WALBufferMgr(opts, wal_mgr);
  buf->init(lsn);

  auto hb = wal_mgr->readHeaderBlock();
  EXPECT_EQ(hb.getCheckpointNo(), 1);
  EXPECT_EQ(hb.getCheckpointLSN(), lsn);

  vector<LogEntry*> read_logs;
  std::vector<uint64_t> ignore;
  buf->readWALLogs(read_logs, 4096 + 12, lsn, ignore);
  EXPECT_EQ(read_logs.size(), 20);

  for (int i = 0; i < 20; i++) {
    char* wal_log = logs[i]->encode();
    size_t len = logs[i]->getLen();
    KStatus s = buf->writeWAL(ctx_, wal_log, len, lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
  }

  for (auto& l : read_logs) {
    delete l;
  }
  read_logs.clear();
  buf->readWALLogs(read_logs, 4096 + 12, lsn, ignore);
  EXPECT_EQ(read_logs.size(), 40);

  delete buf;
  delete wal_mgr;
  delete opts;
  for (auto& l : logs) {
    delete l;
  }
  for (auto& l : read_logs) {
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestRestart) {
  auto opts = EngineOptions();
  opts.wal_level = WALMode::SYNC;
  opts.db_path = kDbPath;
  opts.wal_buffer_size = 4;
  opts.wal_file_in_group = 10;
  opts.wal_file_size = 1;

  // WALFileMgr wal_file_mgr = WALFileMgr(db_path, table_id_, &opts);
  auto wal = new WALMgr(kDbPath+ "/", table_id_, 999, &opts);
  KStatus s = wal->Create(ctx_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  vector<LogEntry*> logs = GenerateInsertMetricsLog(300, 4096);
  for (int i = 0; i < 300; i++) {
    KStatus s = wal->writeLogEntry(ctx_, *logs[i]);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  delete wal;
  wal = nullptr;

  wal = new WALMgr(kDbPath+ "/", table_id_, 999, &opts);
  s = wal->Init(ctx_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  for (int i = 0; i < 20; i++) {
    s = wal->writeLogEntry(ctx_, *logs[i]);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  for (auto& l : logs) {
    delete l;
  }

  std::vector<LogEntry*> read_logs;
std::vector<uint64_t> ignore;
  s = wal->ReadWALLog(read_logs, 4096 + 12, wal->FetchCurrentLSN(), ignore);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(read_logs.size(), 320);
  for (auto& l : read_logs) {
    delete l;
  }

  delete wal;
}

TEST_F(TestWalManagerWriter, TestReadLogFile) {
  auto opts = EngineOptions();

  opts.db_path = kDbPath;
  // opts->db_path = "/home/fxy/gozdp/src/gitee.com/kwbasedb/install/bin/kwbase-data/tsdb/78/1/wal/";
  // opts->db_path = "./";
  opts.wal_buffer_size = 4;
  opts.wal_file_in_group = 10;
  opts.wal_file_size = 16;

  WALFileMgr wal_mgr = WALFileMgr(kDbPath + "/", table_id_, &opts);
  KStatus res = wal_mgr.initWalFile(0, 4096 + 12);
  EXPECT_EQ(res, KStatus::SUCCESS);

  WALBufferMgr buf = WALBufferMgr(&opts, &wal_mgr);
  buf.init(4096 + 12);
  int log_num = 20;
  vector<LogEntry*> logs = GenerateInsertMetricsLog(log_num, 80);
  for (int i = 0; i < log_num; i++) {
    char* wal_log = logs[i]->encode();
    size_t len = logs[i]->getLen();
    TS_LSN l;
    KStatus s = buf.writeWAL(ctx_, wal_log, len, l);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
    delete logs[i];
  }
  buf.flushInternal(true);

  vector<LogEntry*> read_logs;
  std::vector<uint64_t> ignore;
  buf.readWALLogs(read_logs, 4096 + 12, buf.getCurrentLsn(), ignore);
  CheckLogs(log_num, read_logs, 80);
  for (auto& entry : read_logs) {
    entry->prettyPrint();
    std::cout << endl;
    delete entry;
  }
}

TEST_F(TestWalManagerWriter, TestLogFileGroup) {
  uint total_records = 1024 - 3;
  auto opts = EngineOptions();

  opts.db_path = kDbPath+ "/";
  opts.wal_buffer_size = 1;
  opts.wal_file_in_group = 4;
  opts.wal_file_size = 1;

  WALFileMgr wal_file_mgr = WALFileMgr(opts.db_path, table_id_, &opts);
  KStatus res;
  res = wal_file_mgr.initWalFile(0, 4096 + 12);
  EXPECT_EQ(res, KStatus::SUCCESS);

  auto hb = wal_file_mgr.readHeaderBlock();
  EXPECT_EQ(hb.getStartLSN(), 0);
  EXPECT_EQ(hb.getFirstLSN(), 4108);
  EXPECT_EQ(hb.getStartBlockNo(), 0);
  EXPECT_EQ(hb.getBlockNum(), 255);
  EXPECT_EQ(hb.getCheckpointNo(), 0);
  EXPECT_EQ(hb.getCheckpointLSN(), 4108);
  WALBufferMgr buf = WALBufferMgr(&opts, &wal_file_mgr);
  buf.init(4108);
  char log[4046];
  memset(log, 'A', 4046);

  TS_LSN end_lsn = 0;
  TS_LSN checkpoint_lsn = 0;

  auto write_start = std::chrono::system_clock::now();

  for (int i = 0; i < total_records; i++) {
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << i;

    std::string result = ss.str();
    memcpy(log, result.c_str(), 10);

    auto insert_log = InsertLogTagsEntry(0, WALLogType::INSERT, i, WALTableType::TAG,
                                         i, i, 4046, log);

    char* wal_log = insert_log.encode();
    size_t length = insert_log.getLen();

    KStatus ret = buf.writeWAL(nullptr, wal_log, length, end_lsn);
    delete[] wal_log;
    EXPECT_EQ(ret, SUCCESS);
    EXPECT_EQ(end_lsn, buf.getCurrentLsn());
    if (i == 500) {
      checkpoint_lsn = end_lsn;
      buf.setHeaderBlockCheckpointInfo(checkpoint_lsn, 1);
      res = buf.flushInternal(true);
      opts.wal_file_size = 2;
    }
  }

  res = buf.flushInternal(true);
  EXPECT_EQ(end_lsn, buf.getCurrentLsn());
  EXPECT_EQ(res, SUCCESS);

  auto write_end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
  std::cout << "Duration for writing logs is: " << duration.count() << "ms" << std::endl;

  uint end_block = wal_file_mgr.GetBlockNoFromLsn(end_lsn);
  std::vector<EntryBlock*> blocks;
  wal_file_mgr.readEntryBlocks(blocks, 0, end_block);
  EXPECT_EQ(end_block, total_records);
  cout << "The last block number is " << end_block << endl;
  EXPECT_EQ(blocks.size(), total_records + 1);
  cout << "EntryBlock number is " << blocks.size() << endl;
  uint32_t index = 0;
  for (auto block : blocks) {
    EXPECT_NE(block, nullptr);
    EXPECT_EQ(block->getBlockNo(), index);
    index++;
    delete block;
  }

  hb = wal_file_mgr.readHeaderBlock();
  // the first block is full and not enough for the current log
  EXPECT_EQ(hb.getStartLSN(), (4 << 20));
  EXPECT_EQ(hb.getFirstLSN(), (4 << 20) + 4108);
  EXPECT_EQ(hb.getStartBlockNo(), 1024 - 3);
  EXPECT_EQ(hb.getBlockNum(), 511);
  EXPECT_EQ(hb.getCheckpointNo(), 1);
  EXPECT_EQ(hb.getCheckpointLSN(), checkpoint_lsn);

  auto read_start = std::chrono::system_clock::now();

  duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_start - write_end);
  std::cout << "Duration for reading all entry blocks is: " << duration.count() << "ms" << std::endl;

  vector<LogEntry*> logs;
  std::vector<uint64_t> ignore;
  KStatus status = buf.readWALLogs(logs, 4096 + 12, buf.getCurrentLsn(), ignore);
  EXPECT_EQ(status, SUCCESS);
  EXPECT_EQ(logs.size(), total_records);

  for (int i = 0; i < logs.size(); i++) {
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << i;

    std::string result = ss.str();
    char* id = const_cast<char*>(result.c_str());
    auto* insert_log = (InsertLogTagsEntry*) (logs[i]);
    EXPECT_EQ(i, insert_log->getXID());
    EXPECT_EQ(i, insert_log->time_partition_);
    EXPECT_EQ(i, insert_log->offset_);
    EXPECT_EQ(4046, insert_log->length_);
    EXPECT_EQ(WALLogType::INSERT, insert_log->getType());
    EXPECT_EQ(WALTableType::TAG, insert_log->getTableType());
    for (int j = 0; j < 10; j++) {
      EXPECT_EQ(id[j], insert_log->data_[j]);
      if (id[j] != insert_log->data_[j]) {
        std::cout << endl;
      }
    }
    for (int j = 10; j < 80; j++) {
      EXPECT_EQ('A', insert_log->data_[j]);
    }
    delete insert_log;
  }

  auto read_end = std::chrono::system_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_start);
  std::cout << "Duration for reading logs is: " << duration.count() << "ms" << std::endl;

  res = wal_file_mgr.Open();
  EXPECT_EQ(res, KStatus::SUCCESS);
  hb = wal_file_mgr.readHeaderBlock();
  EXPECT_EQ(hb.getStartLSN(), 0);
  EXPECT_EQ(hb.getFirstLSN(), 4096 + 12);
  EXPECT_EQ(hb.getStartBlockNo(), 0);
  EXPECT_EQ(hb.getBlockNum(), 255);
  EXPECT_EQ(hb.getCheckpointNo(), 0);
  EXPECT_EQ(hb.getCheckpointLSN(), 4096 + 12);

  res = wal_file_mgr.Open();
  EXPECT_EQ(res, KStatus::SUCCESS);
  hb = wal_file_mgr.readHeaderBlock();
  EXPECT_EQ(hb.getStartLSN(), (1 << 20));
  EXPECT_EQ(hb.getFirstLSN(), (1 << 20) + 4096 + 12);
  EXPECT_EQ(hb.getStartBlockNo(), 256 - 1);
  EXPECT_EQ(hb.getBlockNum(), 255);
  EXPECT_EQ(hb.getCheckpointNo(), 1);
  EXPECT_EQ(hb.getCheckpointLSN(), checkpoint_lsn);

  res = wal_file_mgr.Open();
  EXPECT_EQ(res, KStatus::SUCCESS);
  hb = wal_file_mgr.readHeaderBlock();
  EXPECT_EQ(hb.getStartLSN(), (2 << 20));
  EXPECT_EQ(hb.getFirstLSN(), (2 << 20) + 4096 + 12);
  EXPECT_EQ(hb.getStartBlockNo(), 512 - 2);
  EXPECT_EQ(hb.getBlockNum(), 511);
  EXPECT_EQ(hb.getCheckpointNo(), 1);
  EXPECT_EQ(hb.getCheckpointLSN(), checkpoint_lsn);

  res = wal_file_mgr.Open();
  EXPECT_EQ(res, KStatus::SUCCESS);
  hb = wal_file_mgr.readHeaderBlock();
  EXPECT_EQ(hb.getStartLSN(), (4 << 20));
  EXPECT_EQ(hb.getFirstLSN(), (4 << 20) + 4096 + 12);
  EXPECT_EQ(hb.getStartBlockNo(), 1024 - 3);
  EXPECT_EQ(hb.getBlockNum(), 511);
  EXPECT_EQ(hb.getCheckpointNo(), 1);
  EXPECT_EQ(hb.getCheckpointLSN(), checkpoint_lsn);

  for (uint i = total_records; i < total_records + 511 + 510 + 1; i++) {
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << i;

    std::string result = ss.str();
    memcpy(log, result.c_str(), 10);

    auto insert_log = InsertLogTagsEntry(0, WALLogType::INSERT, i, WALTableType::TAG,
                                         i, i, 4046, log);

    char* wal_log = insert_log.encode();
    size_t length = insert_log.getLen();

    KStatus ret = buf.writeWAL(nullptr, wal_log, length, end_lsn);
    delete[] wal_log;
    EXPECT_EQ(ret, SUCCESS);
    EXPECT_EQ(end_lsn, buf.getCurrentLsn());
    ret = buf.flushInternal(true);
    if (ret == kwdbts::FAIL) {
      // occur file is full, report an error(total_records:file 1 - 3, 511: file 4, 510:checkpoint file 1)
      EXPECT_EQ(i,  total_records + 511 + 510);
    }
  }
}

TEST_F(TestWalManagerWriter, TestLogSpecific) {
  EngineOptions opts;
  opts.db_path = kDbPath+ "/";
  opts.wal_buffer_size = 1;
  opts.wal_file_in_group = 10;
  opts.wal_file_size = 1;

  WALFileMgr wal_file_mgr = WALFileMgr(opts.db_path, table_id_, &opts);
  KStatus res;
  res = wal_file_mgr.initWalFile(0, BLOCK_SIZE + 12);
  EXPECT_EQ(res, KStatus::SUCCESS);

  WALBufferMgr buf = WALBufferMgr(&opts, &wal_file_mgr);
  buf.init(BLOCK_SIZE + 12);
  uint32_t num = 5;
  TS_LSN lsn;
  // LOG_BLOCK_MAX_LOG_SIZE = 4080
  vector<LogEntry*> logs = GenerateInsertMetricsLog(num, 2019);
  for (auto& log : logs) {
    char* wal_log = log->encode();
    size_t len = log->getLen();
    EXPECT_EQ(len, LOG_BLOCK_MAX_LOG_SIZE);
    KStatus s = buf.writeWAL(ctx_, wal_log, len, lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
    delete log;
  }

  vector<LogEntry*> r_logs;
  std::vector<uint64_t> ignore;
  buf.readWALLogs(r_logs, BLOCK_SIZE + 12, buf.getCurrentLsn(), ignore);
  CheckLogs(num, r_logs, 2019);

  // BLOCK_SIZE = 4096
  logs = GenerateInsertMetricsLog(num, 2027);
  for (auto& log : logs) {
    char* wal_log = log->encode();
    size_t len = log->getLen();
    EXPECT_EQ(len, BLOCK_SIZE);
    KStatus s = buf.writeWAL(ctx_, wal_log, len, lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
    delete log;
  }

  TS_LSN now_lsn = lsn;

  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();
  buf.readWALLogs(r_logs, BLOCK_SIZE * (num + 1) + 12, lsn, ignore);
  CheckLogs(num, r_logs, 2027);

  logs = GenerateInsertMetricsLog(num, 2028);
  for (auto& log : logs) {
    char* wal_log = log->encode();
    size_t len = log->getLen();
    EXPECT_EQ(len, 4098);
    KStatus s = buf.writeWAL(ctx_, wal_log, len, lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);
    delete[] wal_log;
    delete log;
  }

  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();
  buf.readWALLogs(r_logs, now_lsn, lsn, ignore);
  CheckLogs(num, r_logs, 2028);

  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();
  buf.readWALLogs(r_logs, BLOCK_SIZE * (num + 1) + 12, now_lsn, ignore);
  CheckLogs(num, r_logs, 2027);

  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();
  buf.readWALLogs(r_logs, BLOCK_SIZE + 12, buf.getCurrentLsn(), ignore);
  EXPECT_EQ(num * 3, r_logs.size());

  buf.flushInternal(true);

  vector<EntryBlock*> blocks;
  wal_file_mgr.readEntryBlocks(blocks, 15, 15);
  size_t last_offset = lsn - 16 * BLOCK_SIZE - 12;

  EXPECT_EQ(blocks.size(), 1);
  EXPECT_EQ(blocks[0]->getDataLen(), last_offset);
  for (auto& b : blocks) {
    delete b;
  }

  char data[BLOCK_SIZE];
  memset(data, 'Z', BLOCK_SIZE);
  std::fstream file;

  file.open(opts.db_path + "kwdb_wal0", std::ios::in | std::ios::out);
  file.seekg(16 * BLOCK_SIZE, std::ios::beg);
  file.write(data, BLOCK_SIZE);
  file.close();

  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();
  KStatus status = buf.readWALLogs(r_logs, 4096 + 12, buf.getCurrentLsn(), ignore);
  EXPECT_EQ(status, SUCCESS);

  // the last block is not broken since we use the buffer cache for the last one.
  EXPECT_EQ(num * 3, r_logs.size());

  file.open(opts.db_path + "kwdb_wal0", std::ios::in | std::ios::out);
  file.seekg(15 * BLOCK_SIZE, std::ios::beg);
  file.write(data, BLOCK_SIZE);
  file.close();

  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();

  status = buf.readWALLogs(r_logs, 4096 + 12, buf.getCurrentLsn(), ignore);

  // failed to read all log records since the broken blocks.
  EXPECT_EQ(status, FAIL);

  // the last two log records are broken since we cannot read the broken blocks since 14th.
  EXPECT_EQ(num * 3 - 2, r_logs.size());
  for (auto& l : r_logs) {
    delete l;
  }
  r_logs.clear();

  file.open(opts.db_path + "kwdb_wal0", std::ios::in | std::ios::out);
  file.seekg(1 * BLOCK_SIZE, std::ios::beg);
  file.write(data, BLOCK_SIZE);
  file.close();

  r_logs.clear();
  status = buf.readWALLogs(r_logs, 4096 + 12, buf.getCurrentLsn(), ignore);
  EXPECT_EQ(status, FAIL);
  EXPECT_EQ(0, r_logs.size());
}

TEST_F(TestWalManagerWriter, TestWALDeleteData) {
  uint64_t x_id = 1;
  string p_tag = "11111";
  timestamp64 timestamp = 1680000000;
  uint32_t range_size = 3;
  vector<DelRowSpan> drs;
  DelRowSpan d1 = {36000, 1, "1111"};
  DelRowSpan d2 = {46000, 2, "1100"};
  DelRowSpan d3 = {66000, 3, "0000"};
  drs.push_back(d1);
  drs.push_back(d2);
  drs.push_back(d3);

  TS_LSN lsn;


  KwTsSpan span{timestamp, timestamp + 1,};
  KStatus s = wal_->WriteDeleteMetricsWAL(ctx_, x_id, p_tag, {span}, drs);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteDeleteMetricsWAL(ctx_, x_id, p_tag, {span}, drs);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, BLOCK_SIZE + 12, wal_->FetchCurrentLSN(), ignore);

  EXPECT_EQ(redo_logs.size(), 2);

  auto* redo = reinterpret_cast<DeleteLogMetricsEntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::DELETE);
  EXPECT_EQ(redo->getXID(), x_id);
  EXPECT_EQ(redo->getTableType(), WALTableType::DATA);
  EXPECT_EQ(redo->getPrimaryTag(), p_tag);
  EXPECT_EQ(redo->start_ts_, 0);
  EXPECT_EQ(redo->end_ts_, 0);
  vector<DelRowSpan> partitions = redo->getRowSpans();
  EXPECT_EQ(partitions.size(), range_size);
  for (int i = 0; i < range_size; i++) {
    EXPECT_EQ(partitions[i].partition_ts, drs[i].partition_ts);
    EXPECT_EQ(partitions[i].blockitem_id, drs[i].blockitem_id);
    EXPECT_EQ(partitions[i].delete_flags[0], drs[i].delete_flags[0]);
  }

  for (auto& l : redo_logs) {
    l->prettyPrint();
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestWALDeleteTag) {
  TS_LSN lsn;
  uint64_t x_id = 1;
  string p_tag = "11111";
  string tag_pack = "22222222";

  TSSlice tags{tag_pack.data(), tag_pack.size()};
  KStatus s = wal_->WriteDeleteTagWAL(ctx_, x_id, p_tag, 1, 1, tags);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteDeleteTagWAL(ctx_, x_id, p_tag, 1, 1, tags);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, BLOCK_SIZE + 12, wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 2);

  auto* redo = reinterpret_cast<DeleteLogTagsEntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::DELETE);
  EXPECT_EQ(redo->getXID(), x_id);
  auto ptg = redo->getPrimaryTag();
  EXPECT_EQ(string(ptg.data, ptg.len), p_tag);
  auto check = redo->getTags();
  EXPECT_EQ(string(check.data, check.len), tag_pack);
  EXPECT_EQ(redo->group_id_, 1);
  EXPECT_EQ(redo->entity_id_, 1);

  for (auto& l : redo_logs) {
    l->prettyPrint();
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestWALMTR) {
  TS_LSN lsn;
  uint64_t x_id = wal_->FetchCurrentLSN();
  string tsx_id = "1234567890123456"; // the mocked UUID (128 bits / 8 = 16 bytes)

  char* wal_log{nullptr};
  size_t len;

  auto log = MTRBeginEntry(0, x_id, tsx_id.c_str(), 1, 100);
  wal_log = log.encode();
  len = log.getLen();

  KStatus s = wal_->WriteWAL(ctx_, wal_log, len, lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteMTRWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::MTR_COMMIT);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteMTRWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::MTR_ROLLBACK);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLogForMtr(x_id, redo_logs, ignore);
  EXPECT_EQ(redo_logs.size(), 3);

  auto* redo = reinterpret_cast<MTREntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::MTR_BEGIN);
  EXPECT_EQ(redo->getXID(), x_id);
  EXPECT_EQ(redo->getTsxID(), tsx_id);
  redo = reinterpret_cast<MTREntry*>(redo_logs[1]);
  EXPECT_EQ(redo->getType(), WALLogType::MTR_COMMIT);
  redo = reinterpret_cast<MTREntry*>(redo_logs[2]);
  EXPECT_EQ(redo->getType(), WALLogType::MTR_ROLLBACK);

  delete[] wal_log;
  for (auto& l : redo_logs) {
    l->prettyPrint();
    delete l;
  }

}

TEST_F(TestWalManagerWriter, TestWALTTR) {
  uint64_t x_id = 1;
  string tsx_id = "1234567890123456"; // the mocked UUID (128 bits / 8 = 16 bytes)

  KStatus s = wal_->WriteTSxWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::TS_BEGIN);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteTSxWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::TS_COMMIT);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteTSxWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::TS_ROLLBACK);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 3);

  auto* redo = reinterpret_cast<TTREntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::TS_BEGIN);
  EXPECT_EQ(redo->getTsxID(), tsx_id);
  redo = reinterpret_cast<TTREntry*>(redo_logs[1]);
  EXPECT_EQ(redo->getType(), WALLogType::TS_COMMIT);
  redo = reinterpret_cast<TTREntry*>(redo_logs[2]);
  EXPECT_EQ(redo->getType(), WALLogType::TS_ROLLBACK);
  for (auto& l : redo_logs) {
    l->prettyPrint();
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestWALDDL) {
  TS_LSN lsn;
  uint64_t x_id = 1;

  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, "testTable", table_id_);

  roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(meta.k_column_size());
  column->set_name("column" + std::to_string(meta.k_column_size()));
  size_t column_size = column->ByteSizeLong();
  char* buffer = KNEW char[column_size];
  column->SerializeToArray(buffer, column_size);
  TSSlice column_slice{buffer, column_size};


  RangeGroup test_range{101, 0};
  std::vector<RangeGroup> ranges{test_range};

  KStatus s = wal_->WriteDDLCreateWAL(ctx_, x_id, table_id_, &meta, &ranges);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteDDLAlterWAL(ctx_, x_id, table_id_, AlterType::ADD_COLUMN, 1, 2, column_slice);
  EXPECT_EQ(s, KStatus::SUCCESS);

  delete[] buffer;

  s = wal_->WriteDDLDropWAL(ctx_, x_id, table_id_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 3);

  auto* redo1 = reinterpret_cast<DDLCreateEntry*>(redo_logs[0]);
  EXPECT_EQ(redo1->getType(), WALLogType::DDL_CREATE);
  EXPECT_EQ(redo1->getXID(), x_id);
  EXPECT_EQ(redo1->getObjectID(), table_id_);

  auto* redo2 = reinterpret_cast<DDLAlterEntry*>(redo_logs[1]);
  EXPECT_EQ(redo2->getType(), WALLogType::DDL_ALTER_COLUMN);
  EXPECT_EQ(redo2->getXID(), x_id);
  EXPECT_EQ(redo2->getObjectID(), table_id_);

  auto* redo3 = reinterpret_cast<DDLDropEntry*>(redo_logs[2]);
  EXPECT_EQ(redo3->getType(), WALLogType::DDL_DROP);
  EXPECT_EQ(redo3->getXID(), x_id);
  EXPECT_EQ(redo3->getObjectID(), table_id_);
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestWALCheckpoint) {
  TS_LSN lsn;
  uint64_t x_id = 1;

  KStatus s = wal_->WriteCheckpointWAL(ctx_, x_id, lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteCheckpointWAL(ctx_, x_id, lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);


  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 2);

  auto* redo = reinterpret_cast<CheckpointEntry*>(redo_logs[0]);
  redo->prettyPrint();
  EXPECT_EQ(redo->getType(), WALLogType::CHECKPOINT);
  EXPECT_EQ(redo->getXID(), x_id);
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestWALSnapshot) {
  uint64_t x_id = 1;
  TSTableID table_id = 9;
  uint64_t b_hash = 3, e_hash = 5;
  KwTsSpan span{123, 456};
  KStatus s = wal_->WriteSnapshotWAL(ctx_, x_id, table_id, b_hash, e_hash, span);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteSnapshotWAL(ctx_, x_id, table_id, b_hash, e_hash, span);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 2);

  for (size_t i = 0; i < 2; i++) {
    auto* redo = reinterpret_cast<SnapshotEntry*>(redo_logs[i]);
    redo->prettyPrint();
    EXPECT_EQ(redo->getType(), WALLogType::RANGE_SNAPSHOT);
    EXPECT_EQ(redo->getXID(), x_id);
    HashIdSpan hash_span;
    KwTsSpan ts_span;
    redo->GetRangeInfo(&hash_span, &ts_span);
    EXPECT_EQ(hash_span.begin, b_hash);
    EXPECT_EQ(hash_span.end, e_hash);
    EXPECT_EQ(span.begin, ts_span.begin);
    EXPECT_EQ(span.end, ts_span.end);
  }

  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestWALTempDirectory) {
  uint64_t x_id = 1;
  string file_path = "./test111/222/333/";
  int log_num = 5;
  for (size_t i = 0; i < 5; i++) {
    KStatus s = wal_->WriteTempDirectoryWAL(ctx_, x_id, file_path + intToString(i));
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), log_num);

  for (size_t i = 0; i < log_num; i++) {
    auto* redo = reinterpret_cast<TempDirectoryEntry*>(redo_logs[i]);
    redo->prettyPrint();
    EXPECT_EQ(redo->getType(), WALLogType::SNAPSHOT_TMP_DIRCTORY);
    EXPECT_EQ(redo->getXID(), x_id);
    EXPECT_EQ(redo->GetPath(), file_path + intToString(i));
  }
  for (auto& l : redo_logs) {
    delete l;
  }
}
TEST_F(TestWalManagerWriter, TestPartitionTierChange) {
  uint64_t x_id = 1;
  char path[4096];
  getcwd(path, 4096);
  std::string abs_path(path);
  string link_path = abs_path + "/link_path";
  string tier_path = abs_path + "/tier_path";
  ASSERT_TRUE(MakeDirectory(tier_path));
  std::string link_cmd = "ln -s " + tier_path + " " + link_path;
  ASSERT_TRUE(System(link_cmd));
  KStatus s = wal_->WritePartitionTierWAL(ctx_, x_id, link_path, tier_path);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 1);

  auto* redo = reinterpret_cast<PartitionTierChangeEntry*>(redo_logs[0]);
  redo->prettyPrint();
  EXPECT_EQ(redo->getType(), WALLogType::PARTITION_TIER_CHANGE);
  EXPECT_EQ(redo->getXID(), x_id);
  EXPECT_EQ(redo->GetLinkPath(), link_path);
  EXPECT_EQ(redo->GetTierPath(), tier_path);

  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWalManagerWriter, TestCleanUp) {
  EngineOptions opts;
  opts.db_path = kDbPath+ "/";
  opts.wal_buffer_size = 1;
  opts.wal_file_in_group = 10;
  opts.wal_file_size = 1;

  WALFileMgr wal_file_mgr = WALFileMgr(opts.db_path, table_id_, &opts);
  KStatus res;
  res = wal_file_mgr.initWalFile(0, BLOCK_SIZE + 12);
  EXPECT_EQ(res, KStatus::SUCCESS);

  for (uint16_t i = 0; i < 10; i++) {
    string path = opts.db_path + "kwdb_wal" + to_string(i);
    std::fstream file;
    file.open(path, std::ios::in | std::ios::out | std::ios::trunc);
    file.close();
  }

  TS_LSN checkpoint_lsn = (3 << 20) + BLOCK_SIZE + 100;
  TS_LSN current_lsn = (8 << 20) + BLOCK_SIZE + 100;
  wal_file_mgr.CleanUp(checkpoint_lsn, current_lsn);
  int arr[4] = {9, 0, 1, 2};
  for (auto i : arr) {
    string path = opts.db_path + "kwdb_wal" + to_string(i);
    EXPECT_EQ(IsExists(path), false);
  }

  checkpoint_lsn = (7 << 20) + BLOCK_SIZE + 100;
  current_lsn = (3 << 20) + BLOCK_SIZE + 100;
  wal_file_mgr.CleanUp(checkpoint_lsn, current_lsn);
  int arr2[3] = {4, 5, 6};
  for (auto i : arr2) {
    string path = opts.db_path + "kwdb_wal" + to_string(i);
    EXPECT_EQ(IsExists(path), false);
  }
}
