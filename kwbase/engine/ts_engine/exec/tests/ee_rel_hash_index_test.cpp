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

#include "ee_dynamic_hash_index.h"
#include "gtest/gtest.h"
#include <cstring>

using namespace kwdbts;  // NOLINT

// TestDynamicHashIndex for multiple model processing
class TestDynamicHashIndex : public ::testing::Test {
 protected:
  DynamicHashIndex* index;

  void SetUp() override {
    // Initialize the DynamicHashIndex with 1 bucket instance and 8 buckets per instance
    index = new DynamicHashIndex();
    index->init(32);  // Initialize with an empty path; real implementation would use a file path
  }

  void TearDown() override {
    delete index;
  }

 public:
  TestDynamicHashIndex() = default;
};

// DynamicHashIndex test cases for multiple model processing
TEST_F(TestDynamicHashIndex, TestPutAndGet) {
  char* key1 = new char[32]();
  strcpy(key1, "test_key_1");
  RowIndice row_index1 = {1, 1};

  char* key2 = new char[32]();
  strcpy(key2, "test_key_2");
  RowIndice row_index2 = {2, 2};

  // Test put
  ASSERT_EQ(index->put(key1, strlen(key1), row_index1), 0);
  ASSERT_EQ(index->put(key2, strlen(key2), row_index2), 0);

  // Test get
  RowIndice row_index;
  ASSERT_EQ(index->get(key1, strlen(key1), row_index), 0);
  ASSERT_EQ(row_index.batch_no, row_index1.batch_no);
  ASSERT_EQ(row_index.offset_in_batch, row_index1.offset_in_batch);
  ASSERT_EQ(index->get(key2, strlen(key2), row_index), 0);
  ASSERT_EQ(row_index.batch_no, row_index2.batch_no);
  ASSERT_EQ(row_index.offset_in_batch, row_index2.offset_in_batch);

  delete[] key1;
  delete[] key2;
}

TEST_F(TestDynamicHashIndex, TestRehash) {
  char* key = new char[32]();
  strcpy(key, "test_key_for_resize");
  RowIndice row_index1 = {4, 4};

  // Insert a key-value pair
  ASSERT_EQ(index->put(key, strlen(key), row_index1), 0);

  // Resize buckets
  size_t new_bucket_count = 16;
  index->publicRehash(new_bucket_count);

  // Ensure the key is still accessible after resizing
  RowIndice row_index;
  ASSERT_EQ(index->get(key, strlen(key), row_index), 0);
  ASSERT_EQ(row_index.batch_no, row_index1.batch_no);
  ASSERT_EQ(row_index.offset_in_batch, row_index1.offset_in_batch);

  delete[] key;
}