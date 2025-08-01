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

#include "br_pass_through_chunk_buffer.h"

#include <gtest/gtest.h>

#include "../../exec/tests/ee_op_test_base.h"
#include "ee_data_chunk.h"

namespace kwdbts {

class BrPassThroughChunkBufferTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    InitializeColumnInfo();
    chunk_ = CreateTestDataChunk();

    query_id_ = 12345;
    processor_id_ = 67890;
    sender_id_ = 1;
    chunk_size_ = 1024;
    driver_sequence_ = 1;
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
    chunk_.reset();
    delete[] col_info_;
  }

  void InitializeColumnInfo() {
    col_info_ = KNEW ColumnInfo[5];
    col_info_[0] = ColumnInfo(8, roachpb::DataType::TIMESTAMPTZ, KWDBTypeFamily::TimestampTZFamily);
    col_info_[1] = ColumnInfo(31, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
    col_info_[2] = ColumnInfo(1, roachpb::DataType::BOOL, KWDBTypeFamily::BoolFamily);

    TSTagReaderSpec spec;
    spec.set_tableid(1);
    spec.set_tableversion(1);
    for (int i = 0; i < 3; ++i) {
      TSCol* col = spec.add_colmetas();
      col->set_storage_type(col_info_[i].storage_type);
      col->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
      col->set_storage_len(col_info_[i].storage_len);
    }
  }

  // Create test data chunk
  std::unique_ptr<DataChunk> CreateTestDataChunk() {
    return std::make_unique<DataChunk>(col_info_, 3, 1);
  }

  KQueryId query_id_;
  KProcessorId processor_id_;
  k_int32 sender_id_;
  k_size_t chunk_size_;
  k_int32 driver_sequence_;
  ColumnInfo* col_info_;
  std::unique_ptr<DataChunk> chunk_;
};

// Comprehensive test for PassThroughChunkBuffer
TEST_F(BrPassThroughChunkBufferTest, PassThroughChunkBufferComprehensiveTest) {
  // 1. Basic functionality test
  // Create PassThroughChunkBuffer instance
  PassThroughChunkBuffer buffer(query_id_);
  // Test reference counting
  EXPECT_EQ(buffer.Ref(), 2);
  EXPECT_EQ(buffer.Unref(), 1);
  // Create Key
  PassThroughChunkBuffer::Key key(query_id_, processor_id_);
  // Get or create Channel
  PassThroughChannel* channel = buffer.GetOrCreateChannel(key);
  EXPECT_NE(channel, nullptr);
  // Get the same Channel again
  PassThroughChannel* same_channel = buffer.GetOrCreateChannel(key);
  EXPECT_EQ(channel, same_channel);

  // 2. Context functionality test
  // Create PassThroughContext
  PassThroughContext context(&buffer, query_id_, processor_id_);
  // Initialize Context
  context.Init();
  // Test AppendChunk
  context.AppendChunk(sender_id_, chunk_.get(), chunk_size_, driver_sequence_);
  // Test PullChunks
  ChunkUniquePtrVector chunks;
  std::vector<k_size_t> bytes;
  context.PullChunks(sender_id_, &chunks, &bytes);
  // Verify results
  EXPECT_EQ(chunks.size(), 1);
  EXPECT_EQ(bytes.size(), 1);
  EXPECT_EQ(bytes[0], chunk_size_);
  EXPECT_EQ(chunks[0].second, driver_sequence_);

  // 3. Manager functionality test
  PassThroughChunkBufferManager manager;
  // Test OpenQueryInstance
  EXPECT_EQ(manager.OpenQueryInstance(query_id_), KStatus::SUCCESS);
  // Test Get
  PassThroughChunkBuffer* buffer_ptr = manager.Get(query_id_);
  EXPECT_NE(buffer_ptr, nullptr);
  // Test repeated Open
  EXPECT_EQ(manager.OpenQueryInstance(query_id_), KStatus::SUCCESS);

  // 4. Multi-threading test - using mutex to avoid concurrency issues
  const int kNumThreads = 4;
  const int kNumOperations = 10;  // Reduce operation count to speed up testing
  std::vector<std::thread> threads;
  std::mutex test_mutex;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      KProcessorId thread_processor_id = processor_id_ + i;
      PassThroughContext thread_context(buffer_ptr, query_id_, thread_processor_id);
      thread_context.Init();
      for (int j = 0; j < kNumOperations; ++j) {
        // Use mutex to protect shared resources
        std::lock_guard<std::mutex> lock(test_mutex);
        // Create new data chunk instead of sharing the same one
        auto thread_chunk = CreateTestDataChunk();
        thread_context.AppendChunk(sender_id_, thread_chunk.get(), chunk_size_, j);
        ChunkUniquePtrVector thread_chunks;
        std::vector<k_size_t> thread_bytes;
        thread_context.PullChunks(sender_id_, &thread_chunks, &thread_bytes);
        if (!thread_chunks.empty()) {
          EXPECT_EQ(thread_bytes.size(), thread_chunks.size());
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // 5. Error handling test
  // Test non-existent query_id
  EXPECT_EQ(manager.Get(99999), nullptr);
  // Test closing non-existent query_id
  EXPECT_EQ(manager.CloseQueryInstance(99999), KStatus::SUCCESS);

  // 6. Cleanup test
  // Test CloseQueryInstance
  EXPECT_EQ(manager.CloseQueryInstance(query_id_), KStatus::SUCCESS);
  // Close again
  EXPECT_EQ(manager.CloseQueryInstance(query_id_), KStatus::SUCCESS);
  // Test Close
  manager.Close();
  // Verify buffer has been deleted
  EXPECT_EQ(manager.Get(query_id_), nullptr);
}

}  // namespace kwdbts
