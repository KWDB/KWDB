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

#include "ee_sort_chunk_merger.h"

#include "ee_op_test_base.h"
#include "ee_row_batch.h"
#include "gtest/gtest.h"

namespace kwdbts {

class DataChunkMergerTest : public OperatorTestBase {
 public:
  DataChunkMergerTest() : OperatorTestBase() {
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    // Create sort column indices
    sort_column_indices_ = {0};  // Sort by first column

    // Create sort directions (true for descending)
    sort_directions_ = {false};  // Ascending

    // Create nulls first flags
    nulls_first_flags_ = {false};  // Nulls last

    // Create sorting rules
    sort_rules_ = SortingRules(sort_directions_, nulls_first_flags_);
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }

  std::vector<k_uint32> sort_column_indices_;
  std::vector<k_bool> sort_directions_;
  std::vector<k_bool> nulls_first_flags_;
  SortingRules sort_rules_;
};

// Test CascadeDataChunkMerger
TEST_F(DataChunkMergerTest, TestCascadeDataChunkMerger) {
  // Create CascadeDataChunkMerger
  auto merger = std::make_unique<CascadeDataChunkMerger>(ctx_);

  // Create mock chunk provider that returns false (no data)
  std::vector<DataChunkProvider> chunk_providers;
  chunk_providers.push_back([](DataChunkPtr* output, k_bool* eos) {
    if (eos) {
      *eos = true;
    }
    return false;
  });

  // Test Init with SortingRules
  KStatus status = merger->Init(chunk_providers, &sort_column_indices_, sort_rules_);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Test IsDataReady
  EXPECT_FALSE(merger->IsDataReady());

  // Test GetNextMergeChunk
  DataChunkPtr output;
  std::atomic<k_bool> eos(false);
  k_bool should_exit = false;
  status = merger->GetNextMergeChunk(&output, &eos, &should_exit);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(eos);
  EXPECT_TRUE(should_exit);
  EXPECT_FALSE(output);
}

// Test CascadeDataChunkMerger with sort directions and nulls first flags
TEST_F(DataChunkMergerTest, TestCascadeDataChunkMergerWithDirections) {
  // Create CascadeDataChunkMerger
  auto merger = std::make_unique<CascadeDataChunkMerger>(ctx_);

  // Create mock chunk provider that returns false (no data)
  std::vector<DataChunkProvider> chunk_providers;
  chunk_providers.push_back([](DataChunkPtr* output, k_bool* eos) {
    if (eos) {
      *eos = true;
    }
    return false;
  });

  // Test Init with sort directions and nulls first flags
  KStatus status = merger->Init(chunk_providers, &sort_column_indices_, &sort_directions_, &nulls_first_flags_);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Test IsDataReady
  EXPECT_FALSE(merger->IsDataReady());

  // Test GetNextMergeChunk
  DataChunkPtr output;
  std::atomic<k_bool> eos(false);
  k_bool should_exit = false;
  status = merger->GetNextMergeChunk(&output, &eos, &should_exit);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(eos);
  EXPECT_TRUE(should_exit);
  EXPECT_FALSE(output);
}

// Test ConstDataChunkMerger
TEST_F(DataChunkMergerTest, TestConstDataChunkMerger) {
  // Create ConstDataChunkMerger
  auto merger = std::make_unique<ConstDataChunkMerger>(ctx_);

  // Create empty chunk providers
  std::vector<DataChunkProvider> chunk_providers;

  // Test Init with SortingRules
  KStatus status = merger->Init(chunk_providers, &sort_column_indices_, sort_rules_);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Test IsDataReady
  EXPECT_FALSE(merger->IsDataReady());

  // Test GetNextMergeChunk
  DataChunkPtr output;
  std::atomic<k_bool> eos(false);
  k_bool should_exit = false;
  status = merger->GetNextMergeChunk(&output, &eos, &should_exit);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(eos);
  EXPECT_TRUE(should_exit);
  EXPECT_FALSE(output);
}

// Test ConstDataChunkMerger with sort directions and nulls first flags
TEST_F(DataChunkMergerTest, TestConstDataChunkMergerWithDirections) {
  // Create ConstDataChunkMerger
  auto merger = std::make_unique<ConstDataChunkMerger>(ctx_);

  // Create empty chunk providers
  std::vector<DataChunkProvider> chunk_providers;

  // Test Init with sort directions and nulls first flags
  KStatus status = merger->Init(chunk_providers, &sort_column_indices_, &sort_directions_, &nulls_first_flags_);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Test IsDataReady
  EXPECT_FALSE(merger->IsDataReady());

  // Test GetNextMergeChunk
  DataChunkPtr output;
  std::atomic<k_bool> eos(false);
  k_bool should_exit = false;
  status = merger->GetNextMergeChunk(&output, &eos, &should_exit);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(eos);
  EXPECT_TRUE(should_exit);
  EXPECT_FALSE(output);
}

}  // namespace kwdbts