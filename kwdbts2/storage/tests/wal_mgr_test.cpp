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

#include "engine.h"
#include "payload.h"
#include "st_transaction_mgr.h"
#include "../../engine/tests/test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;  // NOLINT

RangeGroup kTestRange{101, 0};

class TestWALManager : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;
  uint64_t table_id_ = 10001;
  roachpb::CreateTsTable meta_;
  WALMgr* wal_;
  MMapRootTableManager* mbt_;


  TestWALManager() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 1;
    opts_.wal_buffer_size = 4;
    opts_.db_path = kDbPath + "/";

    system(("rm -rf " + kDbPath + "/*").c_str());
    // clear data dir
    KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
    EXPECT_EQ(s, KStatus::SUCCESS);

    ConstructRoachpbTable(&meta_, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", table_id_);
    std::vector<RangeGroup> ranges{kTestRange};
    ts_engine_->CreateTsTable(ctx_, table_id_, &meta_, ranges);

    ErrorInfo err_info;
    string tbl_sub_path = std::to_string(table_id_) + "/";
    mbt_ = TsTable::OpenMMapRootTableManager(opts_.db_path, tbl_sub_path, table_id_, err_info);
    EXPECT_EQ(err_info.errcode, 0);

    wal_ = new WALMgr(kDbPath + "/", table_id_, kTestRange.range_group_id, &opts_);
    s = wal_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestWALManager() {
    std::shared_ptr<TsTable> ts_table;
    KStatus s = ts_engine_->GetTsTable(ctx_, table_id_, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = ts_table->DropAll(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // CLOSE engine
    delete mbt_;
    s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    ts_engine_ = nullptr;
    delete wal_;
    wal_ = nullptr;
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  char* GenPayloadData(kwdbContext_p ctx, k_uint32 count, k_uint64& payload_length,
                       KTimestamp start_ts, roachpb::CreateTsTable* meta,
                       k_uint32 ms_interval = 10, int test_value = 0, bool fix_entity_id = true) {
    k_uint32 len = 0;
    char* payload = GenSomePayloadData(ctx, count, len, start_ts, meta, ms_interval, test_value, fix_entity_id);
    payload_length = len;
    return payload;
  }

  KTimestamp GetStartTs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
  }

  void AddInsertWal(uint64_t x_id) {
    int row_num = 10;
    TS_LSN lsn = wal_->FetchCurrentLSN();

    KTimestamp start_ts = GetStartTs();
    size_t p_len = 0;
    char* data_value = GenPayloadData(ctx_, row_num, p_len, start_ts, &meta_);
    TSSlice payload{data_value, p_len};
    std::vector<AttributeInfo> schema;
    KStatus s = mbt_->GetSchemaInfoExcludeDropped(&schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    auto pd1 = Payload(schema, mbt_->GetIdxForValidCols(), payload);
    auto p_tag = pd1.GetPrimaryTag();

    s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, payload);
    EXPECT_EQ(s, KStatus::SUCCESS);

    TS_LSN entry_lsn;
    s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);

    delete[] data_value;
  }

  void AddDeleteDataWal(uint64_t x_id) {
    string p_tag = "11111";
    timestamp64 timestamp = 1680000000;
    vector<DelRowSpan> drs;
    DelRowSpan d1 = {36000, 1,  "1111"};
    DelRowSpan d2 = {46000, 2,  "1100"};
    DelRowSpan d3 = {66000, 3,  "0000"};
    drs.push_back(d1);
    drs.push_back(d2);
    drs.push_back(d3);

    KwTsSpan span{timestamp, timestamp + 1,};
    KStatus s = wal_->WriteDeleteMetricsWAL(ctx_, x_id, p_tag, {span}, drs);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }

  void AddDeleteTagWal(uint64_t x_id) {
    string p_tag = "11111";
    string tag_pack = "22222";
    TSSlice tags{tag_pack.data(), tag_pack.size()};
    KStatus s = wal_->WriteDeleteTagWAL(ctx_, x_id, p_tag, 0, 0, tags);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  void AddMtrWal(WALLogType type, uint64_t x_id) const {
    if (type == WALLogType::MTR_BEGIN) {
      auto log = MTRBeginEntry(0, x_id, LogEntry::DEFAULT_TS_TRANS_ID, 1, 100);
      auto wal_log = log.encode();
      auto log_len = log.getLen();

      KStatus s = wal_->WriteWAL(ctx_, wal_log, log_len, x_id);
      delete[] wal_log;
      EXPECT_EQ(s, KStatus::SUCCESS);
    } else {
      string tsx_id = "1234567890123456"; // the mocked UUID (128 bits / 8 = 16 bytes)

      KStatus s = wal_->WriteMTRWAL(ctx_, x_id, tsx_id.c_str(), type);
      EXPECT_EQ(s, KStatus::SUCCESS);
    }
  }
};

TEST_F(TestWALManager, TestWALInit) {
  int row_num = 10;
  uint64_t x_id = 0;
  TS_LSN lsn = wal_->FetchCurrentLSN();
  ErrorInfo err_info;
  string tbl_sub_path = std::to_string(table_id_) + "/";
  opts_.db_path += "/";
  auto bt = TsTable::OpenMMapRootTableManager(opts_.db_path, tbl_sub_path, table_id_, err_info);
  EXPECT_EQ(err_info.errcode, 0);

  KTimestamp start_ts = GetStartTs();
  size_t p_len = 0;
  char* data_value = GenPayloadData(ctx_, row_num, p_len, start_ts, &meta_);
  TSSlice payload{data_value, p_len};
  std::vector<AttributeInfo> schema;
  KStatus s = bt->GetSchemaInfoExcludeDropped(&schema);
  EXPECT_EQ(s, KStatus::SUCCESS);
  Payload pd1(schema, bt->GetIdxForValidCols(), payload);

  s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, payload);
  EXPECT_EQ(s, KStatus::SUCCESS);

  delete wal_;

  wal_ = new WALMgr(kDbPath + "/", table_id_, kTestRange.range_group_id, &opts_);
  wal_->Init(ctx_);
//  EXPECT_EQ(lsn + log_len, wal_->FetchCurrentLSN());
//  EXPECT_EQ(lsn + log_len, wal_->FetchFlushedLSN());
  delete[] data_value;
  delete bt;
}

TEST_F(TestWALManager, TestWALInsert) {
  int row_num = 10;
  uint64_t x_id = 0;
  TS_LSN lsn = wal_->FetchCurrentLSN();

  KTimestamp start_ts = GetStartTs();
  size_t p_len = 0;
  char* data_value = GenPayloadData(ctx_, row_num, p_len, start_ts, &meta_);
  TSSlice payload{data_value, p_len};
  std::vector<AttributeInfo> schema;
  KStatus s = mbt_->GetSchemaInfoExcludeDropped(&schema);
  EXPECT_EQ(s, KStatus::SUCCESS);
  auto pd1 = Payload(schema, mbt_->GetIdxForValidCols(), payload);
  auto p_tag = pd1.GetPrimaryTag();

  s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, payload);
  EXPECT_EQ(s, KStatus::SUCCESS);

  TS_LSN entry_lsn;
  s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;

//  EXPECT_EQ(lsn + tag_len + log_len, wal_->FetchCurrentLSN());
  EXPECT_EQ(lsn, wal_->FetchFlushedLSN());
  EXPECT_EQ(lsn, wal_->FetchCheckpointLSN());

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);

  EXPECT_EQ(redo_logs.size(), 2);

  auto* redo = reinterpret_cast<InsertLogTagsEntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::INSERT);
  EXPECT_EQ(redo->getTableType(), WALTableType::TAG);
  EXPECT_EQ(redo->time_partition_, 0);
  EXPECT_EQ(redo->offset_, 0);
  EXPECT_EQ(redo->length_, payload.len);
  // auto p_tag_check = redo->getPayload();
  // EXPECT_EQ(string(p_tag_check.data, p_tag_check.len), string(p_tag.data, p_tag.len));

  auto* redo2 = reinterpret_cast<InsertLogMetricsEntry*>(redo_logs[1]);
  EXPECT_EQ(redo2->getType(), WALLogType::INSERT);
  EXPECT_EQ(redo2->getTableType(), WALTableType::DATA);
  EXPECT_EQ(redo2->time_partition_, 0);
  EXPECT_EQ(redo2->offset_, 0);
  EXPECT_EQ(redo2->length_, payload.len);
  Payload pd2(schema, mbt_->GetIdxForValidCols(), {redo2->data_, redo2->length_});
  TSSlice sl = pd2.GetColumnValue(0, 0);
  KTimestamp ts_chk = 0;
  memcpy(&ts_chk, sl.data, sizeof(KTimestamp));
  EXPECT_EQ(ts_chk, convertMSToPrecisionTS(start_ts, mbt_->GetTsColDataType()));

  sl = pd2.GetTagValue(0);
  memcpy(&ts_chk, sl.data, sl.len);
  EXPECT_EQ(ts_chk, convertMSToPrecisionTS(start_ts, mbt_->GetTsColDataType()));

  s = wal_->CreateCheckpoint(ctx_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(wal_->FetchFlushedLSN(), wal_->FetchCurrentLSN());
  EXPECT_EQ(wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN());
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManager, TestWALDeleteData) {
  uint64_t x_id = 1;
  string p_tag = "11111";
  timestamp64 timestamp = 1680000000;
  uint32_t range_size = 3;
  vector<DelRowSpan> drs;
  DelRowSpan d1 = {36000, 1,  "1111"};
  DelRowSpan d2 = {46000, 2,  "1100"};
  DelRowSpan d3 = {66000, 3,  "0000"};
  drs.push_back(d1);
  drs.push_back(d2);
  drs.push_back(d3);

  KwTsSpan span{timestamp, timestamp + 1,};
  KStatus s = wal_->WriteDeleteMetricsWAL(ctx_, x_id, p_tag, {span}, drs);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);

  EXPECT_EQ(redo_logs.size(), 1);

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
    delete l;
  }
}

TEST_F(TestWALManager, TestWALDeleteTag) {
  uint64_t x_id = 1;
  string p_tag = "11111";
  string tag_pack = "22222";
  TSSlice tags{tag_pack.data(), tag_pack.size()};
  KStatus s = wal_->WriteDeleteTagWAL(ctx_, x_id, p_tag, 1, 1, tags);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);

  EXPECT_EQ(redo_logs.size(), 1);

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
    delete l;
  }
}

TEST_F(TestWALManager, TestWALMTR) {
  uint64_t x_id = wal_->FetchCurrentLSN();
  string tsx_id = "1234567890123456"; // the mocked UUID (128 bits / 8 = 16 bytes)

  KStatus s = wal_->WriteMTRWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::MTR_COMMIT);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 1);

  auto* redo = reinterpret_cast<MTREntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::MTR_COMMIT);
  EXPECT_EQ(redo->getXID(), x_id);
EXPECT_EQ(redo->getTsxID(), tsx_id);

  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManager, TestWALMTRRollback) {
  auto tsx_mgr = TSxMgr(wal_);
  uint64_t x_id = 0;
  KStatus s = tsx_mgr.MtrBegin(ctx_, 1, 100, x_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  int row_num = 10;
  ErrorInfo err_info;
  string tbl_sub_path = std::to_string(table_id_) + "/";
  opts_.db_path += "/";
  auto bt = TsTable::OpenMMapRootTableManager(opts_.db_path, tbl_sub_path, table_id_, err_info);
  EXPECT_EQ(err_info.errcode, 0);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  size_t p_len = 0;
  char* data_value = GenPayloadData(ctx_, row_num, p_len, start_ts, &meta_);
  TSSlice payload{data_value, p_len};
  std::vector<AttributeInfo> schema;
  s = bt->GetSchemaInfoExcludeDropped(&schema);
  EXPECT_EQ(s, KStatus::SUCCESS);
  auto pd1 = Payload(schema, bt->GetIdxForValidCols(), payload);

  TS_LSN entry_lsn;

  s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteInsertWAL(ctx_, x_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> undo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLogForMtr(x_id, undo_logs, ignore);
  EXPECT_EQ(undo_logs.size(), 3);

  delete[] data_value;
  for (auto& l : undo_logs) {
    delete l;
  }
  delete bt;
}

TEST_F(TestWALManager, TestWALTTR) {
  uint64_t x_id = 1;
  string tsx_id = "1234567890123456"; // the mocked UUID (128 bits / 8 = 16 bytes)

  KStatus s = wal_->WriteTSxWAL(ctx_, x_id, tsx_id.c_str(), WALLogType::TS_BEGIN);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 1);

  auto* redo = reinterpret_cast<TTREntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::TS_BEGIN);
  EXPECT_EQ(redo->getTsxID(), tsx_id);
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManager, TestWALDDL) {
  uint64_t x_id = 1;

  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, "testTable", table_id_);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = wal_->WriteDDLCreateWAL(ctx_, x_id, table_id_, &meta, &ranges);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal_->WriteDDLDropWAL(ctx_, x_id, table_id_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 2);

  auto* redo1 = reinterpret_cast<DDLCreateEntry*>(redo_logs[0]);
  EXPECT_EQ(redo1->getType(), WALLogType::DDL_CREATE);
  EXPECT_EQ(redo1->getXID(), x_id);
  EXPECT_EQ(redo1->getObjectID(), table_id_);
  EXPECT_EQ(redo1->getMeta()->k_column_size(), 7);


  auto* redo2 = reinterpret_cast<DDLDropEntry*>(redo_logs[1]);
  EXPECT_EQ(redo2->getType(), WALLogType::DDL_DROP);
  EXPECT_EQ(redo2->getXID(), x_id);
  EXPECT_EQ(redo2->getObjectID(), table_id_);
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManager, TestWALCheckpoint) {
  uint64_t x_id = 1;

  TS_LSN entry_lsn;
  KStatus s = wal_->WriteCheckpointWAL(ctx_, x_id, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 1);

  auto* redo = reinterpret_cast<CheckpointEntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::CHECKPOINT);
  EXPECT_EQ(redo->getXID(), x_id);
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManager, TestWALRead) {
  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCheckpointLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 0);
  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN() + 100, ignore);
  EXPECT_EQ(redo_logs.size(), 0);

  AddInsertWal(0);
  uint64_t x_id = wal_->FetchCurrentLSN();
  AddMtrWal(WALLogType::MTR_BEGIN, x_id);
  for (int i = 0; i < 10; i++) {
    AddInsertWal(x_id);
  }

  for (int i = 0; i < 2; i++) {
    AddDeleteDataWal(x_id);
  }

  AddMtrWal(WALLogType::MTR_ROLLBACK, x_id);

  for (int i = 0; i < 2; i++) {
    AddDeleteTagWal(x_id);
  }

  AddMtrWal(WALLogType::MTR_COMMIT, x_id);

  wal_->ReadWALLog(redo_logs, wal_->FetchCheckpointLSN(), wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 29);
  for (auto& l : redo_logs) {
    delete l;
  }

  redo_logs.clear();
  wal_->ReadWALLog(redo_logs, x_id, wal_->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), 27);
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManager, TestWALSyncInsert) {
  opts_.wal_level = 2;
  opts_.wal_buffer_size = 1;
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, kTestRange.range_group_id + 1, &opts_);
  wal2->Init(ctx_);


  int row_num = 10;
  int payload_num = 10000;
  uint64_t x_id = 0;
  TS_LSN lsn = wal2->FetchCurrentLSN();

  for (int i = 0; i < payload_num; i++) {
    KTimestamp start_ts = GetStartTs();
    size_t p_len = 0;
    char* data_value = GenPayloadData(ctx_, row_num, p_len, start_ts, &meta_);
    TSSlice payload{data_value, p_len};
    std::vector<AttributeInfo> schema;
    KStatus  s = mbt_->GetSchemaInfoExcludeDropped(&schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    auto pd1 = Payload(schema, mbt_->GetIdxForValidCols(), payload);
    auto p_tag = pd1.GetPrimaryTag();

    s = wal2->WriteInsertWAL(ctx_, x_id, 0, 0, payload);
    EXPECT_EQ(s, KStatus::SUCCESS);

    TS_LSN entry_lsn;
    s = wal2->WriteInsertWAL(ctx_, x_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
    EXPECT_EQ(s, KStatus::SUCCESS);

    delete[] data_value;
  }

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal2->ReadWALLog(redo_logs, wal2->FetchCheckpointLSN(), wal2->FetchCurrentLSN(), ignore);
  EXPECT_EQ(redo_logs.size(), payload_num * 2);
  for (auto& l : redo_logs) {
    delete l;
  }
  delete wal2;
}