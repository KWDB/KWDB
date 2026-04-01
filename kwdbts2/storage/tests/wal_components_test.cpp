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

#include "libkwdbts2.h"
#include "st_wal_mgr.h"
#include "st_wal_internal_log_structure.h"
#include "st_wal_internal_buffer_mgr.h"
#include "st_wal_internal_logfile_mgr.h"
#include "st_wal_internal_logblock.h"

#include "../../ts_engine/tests/test_util.h"

namespace kwdbts {

class TestWALComponents : public ::testing::Test {
 protected:
  kwdbContext_t context_;
  kwdbContext_p ctx_ = nullptr;
  uint64_t tbl_grp_id_ = 123;
  EngineOptions opts_;
  std::string test_dir_;

  void SetUp() override {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 1;
    opts_.wal_buffer_size = 4;
    test_dir_ = "./wal_components_test/";
    opts_.db_path = test_dir_;
    
    // Clean up test directory
    fs::remove_all(test_dir_);
  }

  void TearDown() override {
    // Clean up test directory
    fs::remove_all(test_dir_);
  }
};

// ============================================================================
// EntryBlock Tests
// ============================================================================

TEST_F(TestWALComponents, TestEntryBlock_Constructor) {
  EntryBlock block(1, 10);
  EXPECT_EQ(block.getBlockNo(), 1);
  EXPECT_EQ(block.getFirstRecOffset(), 0);
  EXPECT_EQ(block.getDataLen(), 0);
  EXPECT_EQ(block.getCheckSum(), 0);
}

TEST_F(TestWALComponents, TestEntryBlock_EncodeDecode) {
  EntryBlock block(5, 20);
  
  // Write some data
  char test_data[] = "Hello WAL";
  size_t written = block.writeBytes(test_data, sizeof(test_data), false);
  EXPECT_EQ(written, sizeof(test_data));
  
  // Encode
  char* encoded = block.encode();
  ASSERT_NE(encoded, nullptr);
  
  // Decode
  EntryBlock decoded(encoded);
  EXPECT_EQ(decoded.getBlockNo(), 5);
  EXPECT_EQ(decoded.getFirstRecOffset(), 0);
  EXPECT_EQ(decoded.getDataLen(), sizeof(test_data));
  
  delete[] encoded;
}

TEST_F(TestWALComponents, TestEntryBlock_WriteBytes) {
  EntryBlock block(2, 0);
  
  char data1[] = "First write";
  char data2[] = "Second write";
  
  size_t written1 = block.writeBytes(data1, sizeof(data1), false);
  EXPECT_EQ(written1, sizeof(data1));
  
  size_t written2 = block.writeBytes(data2, sizeof(data2), false);
  EXPECT_EQ(written2, sizeof(data2));
  
  EXPECT_EQ(block.getDataLen(), sizeof(data1) + sizeof(data2));
}

TEST_F(TestWALComponents, TestEntryBlock_ReadBytes) {
  EntryBlock block(3, 0);
  
  char test_data[] = "Test data for reading";
  block.writeBytes(test_data, sizeof(test_data), false);
  
  char* result = nullptr;
  size_t offset = 0;
  size_t read_size = 0;
  
  KStatus s = block.readBytes(offset, result, 10, read_size);
  EXPECT_EQ(s, SUCCESS);
  EXPECT_EQ(read_size, 10);
  
  delete[] result;
}

TEST_F(TestWALComponents, TestEntryBlock_Reset) {
  EntryBlock block(4, 5);
  char data[] = "Some data";
  block.writeBytes(data, sizeof(data), false);
  
  block.reset(10);
  EXPECT_EQ(block.getBlockNo(), 10);
  EXPECT_EQ(block.getDataLen(), 0);
  EXPECT_EQ(block.getFirstRecOffset(), 0);
}

TEST_F(TestWALComponents, TestEntryBlock_Format) {
  EntryBlock block(7, 8);
  char data[] = "Data to format";
  block.writeBytes(data, sizeof(data), false);
  
  block.format();
  EXPECT_EQ(block.getBlockNo(), 0);
  EXPECT_EQ(block.getDataLen(), 0);
}

// ============================================================================
// HeaderBlock Tests
// ============================================================================

TEST_F(TestWALComponents, TestHeaderBlock_Constructor) {
  HeaderBlock block(100, 1, 100, 1000, 2000, 1500, 5);

  EXPECT_EQ(block.getStartBlockNo(), 1);
  EXPECT_EQ(block.getBlockNum(), 100);
  EXPECT_EQ(block.getStartLSN(), 1000);
  EXPECT_EQ(block.getFirstLSN(), 2000);
  EXPECT_EQ(block.getCheckpointLSN(), 1500);
  EXPECT_EQ(block.getCheckpointNo(), 5);
}

TEST_F(TestWALComponents, TestHeaderBlock_EncodeDecode) {
  HeaderBlock block(200, 5, 50, 5000, 6000, 5500, 10);
  
  char* encoded = block.encode();
  ASSERT_NE(encoded, nullptr);
  
  HeaderBlock decoded(encoded);
  EXPECT_EQ(decoded.getStartBlockNo(), 5);
  EXPECT_EQ(decoded.getBlockNum(), 50);
  EXPECT_EQ(decoded.getStartLSN(), 5000);
  EXPECT_EQ(decoded.getFirstLSN(), 6000);
  EXPECT_EQ(decoded.getCheckpointLSN(), 5500);
  EXPECT_EQ(decoded.getCheckpointNo(), 10);
  
  delete[] encoded;
}

TEST_F(TestWALComponents, TestHeaderBlock_SetCheckpointInfo) {
  HeaderBlock block;
  block.setCheckpointInfo(8000, 15);
  
  EXPECT_EQ(block.getCheckpointLSN(), 8000);
  EXPECT_EQ(block.getCheckpointNo(), 15);
}

TEST_F(TestWALComponents, TestHeaderBlock_SetFirstLSN) {
  HeaderBlock block;
  block.setFirstLSN(9000);
  EXPECT_EQ(block.getFirstLSN(), 9000);
}

TEST_F(TestWALComponents, TestHeaderBlock_SetFlushedLsn) {
  HeaderBlock block;
  block.setFlushedLsn(7500);
  EXPECT_EQ(block.getFlushedLsn(), 7500);
}

TEST_F(TestWALComponents, TestHeaderBlock_GetEndBlockNo) {
  HeaderBlock block(0, 10, 50, 0, 0, 0, 0);
  EXPECT_EQ(block.getEndBlockNo(), 59);  // start_block_no + block_num - 1
}

// ============================================================================
// LogEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestLogEntry_BasicConstructor) {
  LogEntry entry(100, WALLogType::INSERT, 1, 123, 50, 1000);
  
  EXPECT_EQ(entry.getLSN(), 100);
  EXPECT_EQ(entry.getType(), WALLogType::INSERT);
  EXPECT_EQ(entry.getXID(), 1);
  EXPECT_EQ(entry.getVGroupID(), 123);
  EXPECT_EQ(entry.getOldLSN(), 50);
  EXPECT_EQ(entry.getTableID(), 1000);
}

TEST_F(TestWALComponents, TestLogEntry_Encode) {
  LogEntry entry(200, WALLogType::UPDATE, 2, 456, 100, 2000);
  char* encoded = entry.encode();
  EXPECT_EQ(encoded, nullptr);  // Base class returns nullptr
}

// ============================================================================
// InsertLogTagsEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestInsertLogTagsEntry) {
  char payload[] = "Test payload data";
  InsertLogTagsEntry entry(300, WALLogType::INSERT, 3, WALTableType::TAG,
                           1000, 50, sizeof(payload), payload, 789, 150, 5000);
  
  EXPECT_EQ(entry.getLSN(), 300);
  EXPECT_EQ(entry.getType(), WALLogType::INSERT);
  EXPECT_EQ(entry.getXID(), 3);
  EXPECT_EQ(entry.getTableType(), WALTableType::TAG);
  EXPECT_EQ(entry.getTimePartition(), 1000);
  EXPECT_EQ(entry.getOffset(), 50);
  EXPECT_EQ(entry.getVGroupID(), 789);
  
  TSSlice slice = entry.getPayload();
  EXPECT_EQ(slice.len, sizeof(payload));
  
  size_t len = entry.getLen();
  EXPECT_GT(len, 0);
}

TEST_F(TestWALComponents, TestInsertLogTagsEntry_PrettyPrint) {
  char payload[] = "TestData123";
  InsertLogTagsEntry entry(400, WALLogType::INSERT, 4, WALTableType::TAG,
                           2000, 100, sizeof(payload), payload, 890, 200, 6000);
  
  // Just ensure it doesn't crash
  entry.prettyPrint();
}

// ============================================================================
// InsertLogMetricsEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestInsertLogMetricsEntry) {
  char primary_tag[] = "primary_tag";
  char payload[] = "metrics payload";
  
  InsertLogMetricsEntry entry(500, WALLogType::INSERT, 5, WALTableType::DATA,
                              3000, 200, sizeof(payload), payload, 
                              sizeof(primary_tag), primary_tag, 901, 250);
  
  EXPECT_EQ(entry.getLSN(), 500);
  EXPECT_EQ(entry.getType(), WALLogType::INSERT);
  EXPECT_EQ(entry.getXID(), 5);
  EXPECT_EQ(entry.getTableType(), WALTableType::DATA);
  EXPECT_EQ(entry.getTimePartition(), 3000);
  EXPECT_EQ(entry.getOffset(), 200);
  
  auto ptag = entry.getPrimaryTag();
  EXPECT_EQ(ptag.size(), sizeof(primary_tag));
  
  size_t len = entry.getLen();
  EXPECT_GT(len, 0);
  
  entry.prettyPrint();
}

// ============================================================================
// UpdateLogTagsEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestUpdateLogTagsEntry) {
  char new_data[] = "new data";
  char old_data[] = "old data";
  
  UpdateLogTagsEntry entry(600, WALLogType::UPDATE, 6, WALTableType::TAG,
                           4000, 300, sizeof(new_data), sizeof(old_data),
                           new_data, 1012, 300, 7000);
  
  EXPECT_EQ(entry.getLSN(), 600);
  EXPECT_EQ(entry.getType(), WALLogType::UPDATE);
  EXPECT_EQ(entry.getXID(), 6);
  EXPECT_EQ(entry.getTimePartition(), 4000);
  EXPECT_EQ(entry.getOffset(), 300);
  
  TSSlice new_payload = entry.getPayload();
  EXPECT_EQ(new_payload.len, sizeof(new_data));
  
  TSSlice old_payload = entry.getOldPayload();
  EXPECT_EQ(old_payload.len, sizeof(old_data));
  
  entry.prettyPrint();
}

// ============================================================================
// DeleteLogMetricsEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestDeleteLogMetricsEntry) {
  char primary_tag[] = "delete_primary";
  DelRowSpan row_spans[] = {{36000, 1, "1111"}, {46000, 2, "0000"}};
  
  DeleteLogMetricsEntry entry(700, WALLogType::DELETE, 7, WALTableType::DATA,
                              sizeof(primary_tag), 2, primary_tag, 1123, 350);
  
  EXPECT_EQ(entry.getLSN(), 700);
  EXPECT_EQ(entry.getType(), WALLogType::DELETE);
  EXPECT_EQ(entry.getXID(), 7);
  EXPECT_EQ(entry.getTableType(), WALTableType::DATA);
  
  string ptag = entry.getPrimaryTag().c_str();
  EXPECT_EQ(ptag, primary_tag);
  
  vector<DelRowSpan> spans = entry.getRowSpans();
  EXPECT_EQ(spans.size(), 2);
  
  size_t len = entry.getLen();
  EXPECT_GT(len, 0);
}

// ============================================================================
// DeleteLogMetricsEntryV2 Tests
// ============================================================================

TEST_F(TestWALComponents, TestDeleteLogMetricsEntryV2) {
  char primary_tag[] = "delete_primary_v2";
  KwTsSpan ts_spans[] = {{3600, 7200}, {23600, 27200}};
  
  DeleteLogMetricsEntryV2 entry(800, WALLogType::DELETE, 8, WALTableType::DATA_V2,
                                1001, sizeof(primary_tag), 2, primary_tag, 1234, 400, 50);
  
  EXPECT_EQ(entry.getLSN(), 800);
  EXPECT_EQ(entry.getType(), WALLogType::DELETE);
  EXPECT_EQ(entry.getXID(), 8);
  EXPECT_EQ(entry.getTableType(), WALTableType::DATA_V2);
  EXPECT_EQ(entry.getTableId(), 1001);
  
  string ptag = entry.getPrimaryTag().c_str();
  EXPECT_EQ(ptag, primary_tag);
  
  vector<KwTsSpan> spans = entry.getTsSpans();
  EXPECT_EQ(spans.size(), 2);
  
  size_t len = entry.getLen();
  EXPECT_GT(len, 0);
}

// ============================================================================
// DeleteLogTagsEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestDeleteLogTagsEntry) {
  char encoded_data[] = "primarytagdata";
  
  DeleteLogTagsEntry entry(900, WALLogType::DELETE, 9, WALTableType::TAG,
                           1, 2, 6, 8, encoded_data, 1345, 450, 8000, 60);
  
  EXPECT_EQ(entry.getLSN(), 900);
  EXPECT_EQ(entry.getType(), WALLogType::DELETE);
  EXPECT_EQ(entry.getXID(), 9);
  EXPECT_EQ(entry.getTableType(), WALTableType::TAG);
  EXPECT_EQ(entry.getOSN(), 60);
  
  TSSlice ptag = entry.getPrimaryTag();
  EXPECT_GT(ptag.len, 0);
  
  size_t len = entry.getLen();
  EXPECT_GT(len, 0);
}

// ============================================================================
// MTREntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestMTREntry) {
  char tsx_id[] = "test_transaction_001";
  MTREntry entry(1000, WALLogType::MTR_COMMIT, 10, tsx_id);
  
  EXPECT_EQ(entry.getLSN(), 1000);
  EXPECT_EQ(entry.getType(), WALLogType::MTR_COMMIT);
  EXPECT_EQ(entry.getXID(), 10);
  
  size_t len = entry.getLen();
  EXPECT_EQ(len, sizeof(WALLogType) + sizeof(uint64_t) + LogEntry::TS_TRANS_ID_LEN);
  
  entry.prettyPrint();
}

// ============================================================================
// MTRBeginEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestMTRBeginEntry) {
  char tsx_id[] = "test_begin_001";
  MTRBeginEntry entry(1100, 11, tsx_id, 5, 1);
  
  EXPECT_EQ(entry.getLSN(), 1100);
  EXPECT_EQ(entry.getType(), WALLogType::MTR_BEGIN);
  EXPECT_EQ(entry.getXID(), 11);
  EXPECT_EQ(entry.getRangeID(), 5);
  EXPECT_EQ(entry.getIndex(), 1);
  
  char* encoded = entry.encode();
  EXPECT_NE(encoded, nullptr);
  delete[] encoded;
}

// ============================================================================
// CheckpointEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestCheckpointEntry_Construct) {
  CheckpointPartition partitions[2];
  partitions[0] = {1000, 50};
  partitions[1] = {2000, 100};

  char* entry_ptr = CheckpointEntry::construct(WALLogType::CHECKPOINT, 12, 3, 0, 2, partitions);
  ASSERT_NE(entry_ptr, nullptr);
  
  // The LSN is not set in construct, it's set by the constructor
  // We can't directly access type_ and x_id_ as they are protected members of LogEntry
  
  size_t len = sizeof(WALLogType) + sizeof(uint64_t) + sizeof(uint32_t) + 
               sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(CheckpointPartition);
  
  delete[] entry_ptr;
}

TEST_F(TestWALComponents, TestCheckpointEntry_DecodeConstructor) {
  CheckpointPartition partitions[2];
  partitions[0] = {1000, 50};
  partitions[1] = {2000, 100};
  
  auto* encoded_entry = CheckpointEntry::construct(WALLogType::CHECKPOINT, 13, 4, 0, 2, partitions);
  char* encoded = reinterpret_cast<char*>(encoded_entry);
  
  CheckpointEntry decoded(1200, WALLogType::CHECKPOINT, encoded);
  
  EXPECT_EQ(decoded.getPartitionLen(), 8192);
  
  delete[] encoded_entry;
}

// ============================================================================
// SnapshotEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestSnapshotEntry) {
  char* entry_ptr = SnapshotEntry::construct(WALLogType::RANGE_SNAPSHOT, 14, 2000, 
                                              100, 200, 3000, 4000);
  ASSERT_NE(entry_ptr, nullptr);
  
  // We can't directly test the object methods since construct returns char*
  // The actual testing would be done through the constructor
  
  delete[] entry_ptr;
}

// ============================================================================
// TempDirectoryEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestTempDirectoryEntry) {
  std::string path = "/tmp/test_directory";
  char* entry_ptr = TempDirectoryEntry::construct(WALLogType::SNAPSHOT_TMP_DIRCTORY, 15, path);
  ASSERT_NE(entry_ptr, nullptr);
  
  // We can't directly test the object methods since construct returns char*
  
  delete[] entry_ptr;
}

// ============================================================================
// CreateIndexEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestCreateIndexEntry) {
  std::array<int32_t, 10> col_ids{};
  col_ids[0] = 1;
  col_ids[1] = 2;
  col_ids[2] = -1;  // Rest are -1
  
  char* entry_ptr = CreateIndexEntry::construct(WALLogType::CREATE_INDEX, 16, 3000, 100,
                                                1, 2, col_ids);
  ASSERT_NE(entry_ptr, nullptr);
  
  // We can't directly test the object methods since construct returns char*
  
  delete[] entry_ptr;
}

// ============================================================================
// DropIndexEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestDropIndexEntry) {
  std::array<int32_t, 10> col_ids{};
  col_ids[0] = 3;
  col_ids[1] = 4;
  
  char* entry_ptr = DropIndexEntry::construct(WALLogType::DROP_INDEX, 17, 4000, 200,
                                              2, 3, col_ids);
  ASSERT_NE(entry_ptr, nullptr);
  
  // We can't directly test the object methods since construct returns char*
  
  delete[] entry_ptr;
}

// ============================================================================
// DDLCreateEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestDDLCreateEntry) {
  // Create a minimal CreateTsTable message for testing
  roachpb::CreateTsTable meta;
  
  RangeGroup ranges[2];
  ranges[0] = {1, 0};
  ranges[1] = {2, 1};
  
  // Serialize the meta to get its size
  int meta_length = meta.ByteSizeLong();
  uint64_t range_size = 2;
  
  char* entry_ptr = DDLCreateEntry::construct(WALLogType::DDL_CREATE, 18, 5000,
                                              meta_length, range_size, &meta, ranges);
  ASSERT_NE(entry_ptr, nullptr);
  
  delete[] entry_ptr;
}

// ============================================================================
// DDLDropEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestDDLDropEntry) {
  char* entry_ptr = DDLDropEntry::construct(WALLogType::DDL_DROP, 19, 6000);
  ASSERT_NE(entry_ptr, nullptr);
  
  // We can't directly test the object methods since construct returns char*
  
  delete[] entry_ptr;
}

// ============================================================================
// DDLAlterEntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestDDLAlterEntry) {
  char column_meta[] = "column_metadata";
  TSSlice slice{column_meta, sizeof(column_meta)};
  
  char* entry_ptr = DDLAlterEntry::construct(WALLogType::DDL_ALTER_COLUMN, 20, 7000,
                                             AlterType::ADD_COLUMN, 1, 2, slice);
  ASSERT_NE(entry_ptr, nullptr);
  
  delete[] entry_ptr;
}

// ============================================================================
// TTREntry Tests
// ============================================================================

TEST_F(TestWALComponents, TestTTREntry) {
  char tsx_id[] = "tsx_transaction_001";
  TTREntry entry(1300, WALLogType::TS_BEGIN, 21, tsx_id);
  
  EXPECT_EQ(entry.getLSN(), 1300);
  EXPECT_EQ(entry.getType(), WALLogType::TS_BEGIN);
  EXPECT_EQ(entry.getXID(), 21);
  
  size_t len = entry.getLen();
  EXPECT_GT(len, 0);
}

}  // namespace kwdbts
