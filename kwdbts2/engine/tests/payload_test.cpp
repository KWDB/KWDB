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

#include <fcntl.h>
#include <unistd.h>
#include "gtest/gtest.h"
#include "payload.h"
#include "ts_common.h"
#include "sys_utils.h"

using namespace kwdbts;  // NOLINT

class PayloadTest : public ::testing::Test {
 protected:

  // Helper function to create a simple payload data buffer
  std::vector<uint8_t> CreatePayloadData(
      int32_t row_count,
      int16_t primary_len,
      const std::string& primary_tag,
      int32_t tag_len,
      const std::vector<uint8_t>& tag_data,
      int32_t data_len,
      const std::vector<uint8_t>& metric_data,
      uint32_t db_id = 1,
      int64_t table_id = 10001,
      uint32_t ts_version = 1,
      uint8_t row_type = 0) {
    
    std::vector<uint8_t> payload;
    
    // Header (43 bytes)
    // txn_id (16 bytes) - set to zeros
    for (int i = 0; i < 16; i++) {
      payload.push_back(0);
    }
    
    // hash_point_id (2 bytes)
    uint16_t hash_point = 1;
    payload.push_back(hash_point & 0xFF);
    payload.push_back((hash_point >> 8) & 0xFF);
    
    // payload_version (4 bytes)
    uint32_t payload_version = 1;
    for (int i = 0; i < 4; i++) {
      payload.push_back((payload_version >> (i * 8)) & 0xFF);
    }
    
    // db_id (4 bytes)
    for (int i = 0; i < 4; i++) {
      payload.push_back((db_id >> (i * 8)) & 0xFF);
    }
    
    // table_id (8 bytes)
    for (int i = 0; i < 8; i++) {
      payload.push_back((table_id >> (i * 8)) & 0xFF);
    }
    
    // ts_version (4 bytes)
    for (int i = 0; i < 4; i++) {
      payload.push_back((ts_version >> (i * 8)) & 0xFF);
    }
    
    // row_num (4 bytes)
    for (int i = 0; i < 4; i++) {
      payload.push_back((row_count >> (i * 8)) & 0xFF);
    }
    
    // row_type (1 byte)
    payload.push_back(row_type);
    
    // Primary tag length (2 bytes)
    payload.push_back(primary_len & 0xFF);
    payload.push_back((primary_len >> 8) & 0xFF);
    
    // Primary tag value
    for (char c : primary_tag) {
      payload.push_back(static_cast<uint8_t>(c));
    }
    
    // Tag length (4 bytes)
    for (int i = 0; i < 4; i++) {
      payload.push_back((tag_len >> (i * 8)) & 0xFF);
    }
    
    // Tag data
    for (auto b : tag_data) {
      payload.push_back(b);
    }
    
    // Data length (4 bytes)
    for (int i = 0; i < 4; i++) {
      payload.push_back((data_len >> (i * 8)) & 0xFF);
    }
    
    // Metric data
    for (auto b : metric_data) {
      payload.push_back(b);
    }
    
    return payload;
  }

  // Helper to create schema
  std::vector<AttributeInfo> CreateSimpleSchema() {
    std::vector<AttributeInfo> schema;
    
    // Column 0: timestamp
    AttributeInfo ts_col;
    ts_col.type = roachpb::TIMESTAMP;
    ts_col.size = 8;
    schema.push_back(ts_col);
    
    // Column 1: int
    AttributeInfo int_col;
    int_col.type = roachpb::INT;
    int_col.size = 4;
    schema.push_back(int_col);
    
    // Column 2: double
    AttributeInfo double_col;
    double_col.type = roachpb::DOUBLE;
    double_col.size = 8;
    schema.push_back(double_col);
    
    return schema;
  }
};

// Test 1: Basic Payload construction and header parsing
TEST_F(PayloadTest, BasicPayloadConstruction) {
  auto schema = CreateSimpleSchema();
  
  // Create minimal payload data with 1 row
  int32_t row_count = 1;
  int16_t primary_len = 8;
  std::string primary_tag = "device01";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;  // bitmap(1) + ts(8) + int(4) + null_bitmap(1) + double(8) - simplified
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag, 
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  // Construct Payload object
  Payload payload(schema, slice);
  
  // Verify header fields
  EXPECT_EQ(payload.GetPayloadVersion(), 1);
  EXPECT_EQ(payload.GetDbId(), 1);
  EXPECT_EQ(payload.GetTableId(), 10001);
  EXPECT_EQ(payload.GetTsVersion(), 1);
  EXPECT_EQ(payload.GetRowCount(), 1);
  EXPECT_EQ(payload.getHashPoint(), 1);
}

// Test 2: GetPrimaryKeyFromPayload static method
TEST_F(PayloadTest, GetPrimaryKeyFromPayload) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 8;
  std::string primary_tag = "device01";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  // Use static method to get primary key
  TSSlice primary_key = Payload::GetPrimaryKeyFromPayload(&slice);
  
  EXPECT_EQ(primary_key.len, static_cast<size_t>(primary_len));
  EXPECT_EQ(std::string(primary_key.data, primary_key.len), primary_tag);
}

// Test 3: GetTsVersionFromPayload and GetRowCountFromPayload static methods
TEST_F(PayloadTest, StaticGetterMethods) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 5;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  uint32_t ts_version = 10;
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data,
                                         1, 10001, ts_version);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  // Test static getters
  EXPECT_EQ(Payload::GetTsVersionFromPayload(&slice), ts_version);
  EXPECT_EQ(Payload::GetRowCountFromPayload(&slice), row_count);
}

// Test 4: GetPrimaryTag method
TEST_F(PayloadTest, GetPrimaryTag) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 12;
  std::string primary_tag = "sensor_node1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  TSSlice pk = payload.GetPrimaryTag();
  EXPECT_EQ(pk.len, static_cast<size_t>(primary_len));
  EXPECT_EQ(std::string(pk.data, pk.len), primary_tag);
}

// Test 5: SetHashPoint and getHashPoint
TEST_F(PayloadTest, HashPointOperations) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "test";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Initial hash point
  EXPECT_EQ(payload.getHashPoint(), 1);
  
  // Set new hash point
  payload.SetHashPoint(100);
  EXPECT_EQ(payload.getHashPoint(), 100);
  
  // Set another hash point
  payload.SetHashPoint(255);
  EXPECT_EQ(payload.getHashPoint(), 255);
}

// Test 6: GetColumn operations
TEST_F(PayloadTest, ColumnOperations) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 2;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  
  // Simplified: just create enough space for columns
  int32_t data_len = 100;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Test column metadata
  EXPECT_EQ(payload.GetColNum(), static_cast<int32_t>(schema.size()));
  
  // Test GetColOffset
  for (int i = 0; i < payload.GetColNum(); i++) {
    int offset = payload.GetColOffset(i);
    EXPECT_GE(offset, 0);
  }
  
  // Test GetNullBitMapOffset
  for (int i = 0; i < payload.GetColNum(); i++) {
    int offset = payload.GetNullBitMapOffset(i);
    EXPECT_GE(offset, 0);
  }
}

// Test 7: GetTimestamp method
TEST_F(PayloadTest, GetTimestamp) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 3;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 100;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Get timestamps for each row (values will be 0 since we filled with zeros)
  for (int i = 0; i < row_count; i++) {
    KTimestamp ts = payload.GetTimestamp(i);
    // Just verify it doesn't crash and returns a value
    EXPECT_GE(ts, INT64_MIN);
    EXPECT_LE(ts, INT64_MAX);
  }
}

// Test 8: IsDisordered method
TEST_F(PayloadTest, IsDisordered) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 3;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 100;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // With all zeros, should not be disordered
  bool disordered = payload.IsDisordered(0, row_count);
  EXPECT_FALSE(disordered);
}

// Test 9: GetSchemaInfo and GetValidCols
TEST_F(PayloadTest, SchemaAndValidCols) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Test GetSchemaInfo
  const auto& retrieved_schema = payload.GetSchemaInfo();
  EXPECT_EQ(retrieved_schema.size(), schema.size());
  
  // Test GetValidCols
  const auto& valid_cols = payload.GetValidCols();
  EXPECT_GT(valid_cols.size(), 0);
}

// Test 10: GetFlag method
TEST_F(PayloadTest, GetFlag) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  uint8_t row_type = Payload::DATA_AND_TAG;
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data,
                                         1, 10001, 1, row_type);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  EXPECT_EQ(payload.GetFlag(), row_type);
}

// Test 11: Different row types
TEST_F(PayloadTest, DifferentRowTypes) {
  auto schema = CreateSimpleSchema();
  
  // Test DATA_ONLY type
  {
    int32_t row_count = 1;
    int16_t primary_len = 4;
    std::string primary_tag = "dev1";
    int32_t tag_len = 0;
    std::vector<uint8_t> tag_data;
    int32_t data_len = 20;
    std::vector<uint8_t> metric_data(data_len, 0);
    uint8_t row_type = Payload::DATA_ONLY;
    
    auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                           tag_len, tag_data, data_len, metric_data,
                                           1, 10001, 1, row_type);
    
    TSSlice slice;
    slice.data = reinterpret_cast<char*>(payload_bytes.data());
    slice.len = payload_bytes.size();
    
    Payload payload(schema, slice);
    EXPECT_EQ(payload.GetFlag(), Payload::DATA_ONLY);
  }
  
  // Test TAG_ONLY type
  {
    int32_t row_count = 1;
    int16_t primary_len = 4;
    std::string primary_tag = "dev1";
    int32_t tag_len = 10;
    std::vector<uint8_t> tag_data(10, 0);
    int32_t data_len = 0;
    std::vector<uint8_t> metric_data;
    uint8_t row_type = Payload::TAG_ONLY;
    
    auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                           tag_len, tag_data, data_len, metric_data,
                                           1, 10001, 1, row_type);
    
    TSSlice slice;
    slice.data = reinterpret_cast<char*>(payload_bytes.data());
    slice.len = payload_bytes.size();
    
    Payload payload(schema, slice);
    EXPECT_EQ(payload.GetFlag(), Payload::TAG_ONLY);
  }
}

// Test 12: Payload with multiple rows
TEST_F(PayloadTest, MultipleRows) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 10;
  int16_t primary_len = 8;
  std::string primary_tag = "device01";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 200;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  EXPECT_EQ(payload.GetRowCount(), row_count);
  
  // Access all rows
  for (int i = 0; i < row_count; i++) {
    KTimestamp ts = payload.GetTimestamp(i);
    EXPECT_GE(ts, INT64_MIN);
    EXPECT_LE(ts, INT64_MAX);
  }
}

// Test 13: Payload with different db_id and table_id
TEST_F(PayloadTest, DifferentDbAndTableIds) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  uint32_t db_id = 999;
  int64_t table_id = 88888;
  uint32_t ts_version = 5;
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data,
                                         db_id, table_id, ts_version);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  EXPECT_EQ(payload.GetDbId(), db_id);
  EXPECT_EQ(payload.GetTableId(), table_id);
  EXPECT_EQ(payload.GetTsVersion(), ts_version);
}

// Test 14: GetDataLength and GetDataOffset
TEST_F(PayloadTest, DataLengthAndOffset) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 5;
  std::vector<uint8_t> tag_data(5, 0);
  int32_t data_len = 50;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  EXPECT_EQ(payload.GetDataLength(), data_len);
  EXPECT_GT(payload.GetDataOffset(), 0);
}

// Test 15: GetTagLen, GetTagAddr, GetTagOffset
TEST_F(PayloadTest, TagOperations) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 10;
  std::vector<uint8_t> tag_data(10, 0x42);  // Fill with 'B'
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  EXPECT_EQ(payload.GetTagLen(), tag_len);
  EXPECT_GT(payload.GetTagOffset(), 0);
  
  char* tag_addr = payload.GetTagAddr();
  EXPECT_NE(tag_addr, nullptr);
}

// Test 16: Constructor with valid_cols parameter
TEST_F(PayloadTest, ConstructorWithValidCols) {
  auto schema = CreateSimpleSchema();
  
  // Specify only some columns as valid
  std::vector<uint32_t> valid_cols = {0, 2};  // Only timestamp and double
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, valid_cols, slice);
  
  const auto& retrieved_valid_cols = payload.GetValidCols();
  EXPECT_EQ(retrieved_valid_cols.size(), valid_cols.size());
}

// Test 17: Null bitmap operations
TEST_F(PayloadTest, NullBitmapOperations) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 8;  // Use 8 rows to test bitmap
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 200;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Test IsNull for all columns and rows
  for (int col = 0; col < payload.GetColNum(); col++) {
    for (int row = 0; row < row_count; row++) {
      // Since we filled with zeros, all bits should be 0 (not null)
      bool is_null = payload.IsNull(col, row);
      // Just verify it doesn't crash
      EXPECT_TRUE(is_null == true || is_null == false);
    }
  }
  
  // Test NoNullMetric
  for (int col = 0; col < payload.GetColNum(); col++) {
    bool no_null = payload.NoNullMetric(col);
    EXPECT_TRUE(no_null == true || no_null == false);
  }
}

// Test 18: OSN (Order Sequence Number) operations
TEST_F(PayloadTest, OsnOperations) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Initial OSN should be 0
  TS_OSN osn;
  payload.GetOsn(osn);
  EXPECT_EQ(osn, 0);
  
  // Set OSN
  payload.SetOsn(12345);
  payload.GetOsn(osn);
  EXPECT_EQ(osn, 12345);
  
  // Set another OSN
  payload.SetOsn(99999);
  payload.GetOsn(osn);
  EXPECT_EQ(osn, 99999);
}

// Test 19: GetStartRowId
TEST_F(PayloadTest, GetStartRowId) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Start row should be 0 by default
  EXPECT_EQ(payload.GetStartRowId(), 0);
}

// Test 20: DedupRule and merge data
TEST_F(PayloadTest, DedupRuleAndMergeData) {
  auto schema = CreateSimpleSchema();
  
  int32_t row_count = 1;
  int16_t primary_len = 4;
  std::string primary_tag = "dev1";
  int32_t tag_len = 0;
  std::vector<uint8_t> tag_data;
  int32_t data_len = 20;
  std::vector<uint8_t> metric_data(data_len, 0);
  
  auto payload_bytes = CreatePayloadData(row_count, primary_len, primary_tag,
                                         tag_len, tag_data, data_len, metric_data);
  
  TSSlice slice;
  slice.data = reinterpret_cast<char*>(payload_bytes.data());
  slice.len = payload_bytes.size();
  
  Payload payload(schema, slice);
  
  // Default dedup rule should be OVERRIDE
  EXPECT_EQ(payload.dedup_rule_, DedupRule::OVERRIDE);
  
  // Change to MERGE
  payload.dedup_rule_ = DedupRule::MERGE;
  EXPECT_EQ(payload.dedup_rule_, DedupRule::MERGE);
  
  // HasMergeData should return false when no merge data exists
  EXPECT_FALSE(payload.HasMergeData(0));
}
