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

// Unit tests for TsAggIteratorImpl::MergeBlockSpans.
//
// Regression coverage for commit 78955df09c ("fix merge block span"):
// block_id is only unique within a single segment type (mem / last / entity).
// Spans from different segment types may share the same block_id, and must NOT
// be merged across segments. The fix switched the merge key from GetBlockID()
// to GetTsBlockAddr() (the in-memory address of the underlying TsBlock), which
// is process-unique.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "ts_block.h"
#include "ts_iterator_v2_impl.h"

namespace kwdbts {

namespace {

std::shared_ptr<TSBlkDataTypeConvert> kEmptyConvert = nullptr;

// Stub TsBlock whose GetBlockID() is configurable, so tests can construct
// distinct physical blocks that report the SAME block_id (simulating the
// cross-segment id collision the production bug hit).
class StubTsBlock : public TsBlock {
 private:
  uint64_t block_id_;
  size_t rows_;
  timestamp64 first_ts_;
  uint64_t osn_{0};

 public:
  StubTsBlock(uint64_t block_id, size_t rows, timestamp64 first_ts)
      : block_id_(block_id), rows_(rows), first_ts_(first_ts) {}

  uint32_t GetBlockVersion() const override { return 1; }
  TSTableID GetTableId() override { return 1; }
  uint32_t GetTableVersion() override { return 1; }
  size_t GetRowNum() override { return rows_; }
  uint64_t GetBlockID() override { return block_id_; }
  timestamp64 GetTS(int row_num, TsScanStats* = nullptr) override { return first_ts_ + row_num; }
  timestamp64 GetFirstTS() override { return first_ts_; }
  timestamp64 GetLastTS() override { return first_ts_ + rows_ - 1; }

  // HasPreAgg() must return false here — TsBlockSpan's constructor caches the
  // result and pre-agg paths are not what this test exercises.
  bool HasPreAgg(uint32_t, uint32_t) override { return false; }

  KStatus GetColAddr(uint32_t, const std::vector<AttributeInfo>*, char**,
                     TsScanStats* = nullptr, DirectColumnDataCopy* = nullptr) override {
    return KStatus::FAIL;
  }
  KStatus GetColBitmap(uint32_t, const std::vector<AttributeInfo>*,
                       std::unique_ptr<TsBitmapBase>*, TsScanStats* = nullptr) override {
    return KStatus::FAIL;
  }
  KStatus GetValueSlice(int, int, const std::vector<AttributeInfo>*, TSSlice&,
                        TsScanStats* = nullptr) override {
    return KStatus::FAIL;
  }
  bool IsColNull(int, int, const std::vector<AttributeInfo>*, TsScanStats* = nullptr) override {
    return false;
  }
  void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) override {
    min_osn = osn_; max_osn = osn_;
  }
  void GetMinAndMaxOSN(int, int, uint64_t&, uint64_t&) override {}
  uint64_t GetOSN(int) override { return osn_; }
  uint64_t GetFirstOSN() override { return osn_; }
  uint64_t GetLastOSN() override { return osn_; }
  const uint64_t* GetOSNAddr(int, TsScanStats* = nullptr) override { return &osn_; }
  KStatus GetCompressDataFromFile(uint32_t, int32_t, TsBufferBuilder*) override { return KStatus::FAIL; }
  KStatus GetPreCount(uint32_t, const std::vector<FixedBlockAggColumnLayout>*,
                      TsScanStats*, uint16_t&) override { return KStatus::FAIL; }
  KStatus GetPreSum(uint32_t, const std::vector<FixedBlockAggColumnLayout>*,
                    int32_t, TsScanStats*, void*&, bool&) override { return KStatus::FAIL; }
  KStatus GetPreMax(uint32_t, const std::vector<FixedBlockAggColumnLayout>*,
                    TsScanStats*, void*&) override { return KStatus::FAIL; }
  KStatus GetPreMin(uint32_t, const std::vector<FixedBlockAggColumnLayout>*,
                    int32_t, TsScanStats*, void*&) override { return KStatus::FAIL; }
};

// Build a TsBlockSpan over a brand-new StubTsBlock (so the underlying block
// address is unique). `block_id` is the value the stub will report from
// GetBlockID(); collisions across calls are allowed and intentional.
std::shared_ptr<TsBlockSpan> MakeSpan(uint64_t block_id, int start_row, int nrow,
                                      int total_rows = 0, timestamp64 first_ts = 1000) {
  if (total_rows == 0) {
    total_rows = start_row + nrow;
  }
  auto block = std::make_shared<StubTsBlock>(block_id, total_rows, first_ts);
  return std::make_shared<TsBlockSpan>(/*vgroup_id=*/0, /*entity_id=*/1, block,
                                       start_row, nrow, kEmptyConvert, /*scan_schema=*/nullptr);
}

// Build a TsBlockSpan that references an existing block (so it shares both
// block_id AND ts_block_addr with sibling spans from the same block).
std::shared_ptr<TsBlockSpan> MakeSpanFromBlock(std::shared_ptr<StubTsBlock> block,
                                               int start_row, int nrow) {
  return std::make_shared<TsBlockSpan>(/*vgroup_id=*/0, /*entity_id=*/1, block,
                                       start_row, nrow, kEmptyConvert, /*scan_schema=*/nullptr);
}

}  // namespace

class MergeBlockSpansTest : public ::testing::Test {};

// Sanity: empty input returns empty.
TEST_F(MergeBlockSpansTest, Empty) {
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  EXPECT_TRUE(result.empty());
}

// Single span: passes through untouched (it is both first and last).
TEST_F(MergeBlockSpansTest, SingleSpan) {
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpan(/*block_id=*/100, 0, 10));
  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0].get(), spans[0].get());
}

// Two spans: both are first/last, no middle to merge.
TEST_F(MergeBlockSpansTest, TwoSpansNeverMerged) {
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpan(100, 0, 10));
  spans.push_back(MakeSpan(101, 0, 10));
  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  ASSERT_EQ(result.size(), 2u);
  EXPECT_EQ(result[0].get(), spans[0].get());
  EXPECT_EQ(result[1].get(), spans[1].get());
}

// Two middle spans pointing at the SAME physical block with contiguous rows
// are merged into one span covering both ranges.
TEST_F(MergeBlockSpansTest, ContiguousSameBlockMerged) {
  auto block = std::make_shared<StubTsBlock>(/*block_id=*/100, /*rows=*/100, /*first_ts=*/1000);
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpan(/*block_id=*/1, 0, 5));                 // first — boundary
  spans.push_back(MakeSpanFromBlock(block, 10, 5));                // middle: rows [10,15)
  spans.push_back(MakeSpanFromBlock(block, 15, 5));                // middle: rows [15,20)  contiguous
  spans.push_back(MakeSpanFromBlock(block, 20, 5));                // middle: rows [20,25)  contiguous
  spans.push_back(MakeSpan(/*block_id=*/2, 0, 5));                 // last — boundary

  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  ASSERT_EQ(result.size(), 3u);  // first + one merged middle + last
  EXPECT_EQ(result[0].get(), spans[0].get());
  EXPECT_EQ(result[1]->GetStartRow(), 10);
  EXPECT_EQ(result[1]->GetRowNum(), 15);  // 5 + 5 + 5
  EXPECT_EQ(result[1]->GetTsBlockRaw(), block.get());
  EXPECT_EQ(result[2].get(), spans[4].get());
}

// REGRESSION for commit 78955df09c.
// Three middle spans each backed by a DIFFERENT physical TsBlock but all
// reporting the SAME block_id (the cross-segment collision that triggered
// the original bug). They must NOT be merged — even when their row ranges
// look contiguous, because each block has its own row space.
TEST_F(MergeBlockSpansTest, SameBlockIdDifferentBlocksNotMerged) {
  const uint64_t kCollidingId = 42;  // mem segment block_id == last segment block_id == entity segment block_id
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpan(/*block_id=*/1, 0, 5));                       // first — boundary
  spans.push_back(MakeSpan(kCollidingId, 10, 5));                        // middle: distinct block A, rows [10,15)
  spans.push_back(MakeSpan(kCollidingId, 15, 5));                        // middle: distinct block B, rows [15,20)
  spans.push_back(MakeSpan(kCollidingId, 20, 5));                        // middle: distinct block C, rows [20,25)
  spans.push_back(MakeSpan(/*block_id=*/2, 0, 5));                       // last — boundary

  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  // Each colliding-id middle span has a unique TsBlock address → no merge.
  ASSERT_EQ(result.size(), 5u);
  for (size_t i = 0; i < spans.size(); ++i) {
    EXPECT_EQ(result[i].get(), spans[i].get()) << "span " << i << " was unexpectedly rewritten";
  }
  // Pre-fix, the function used GetBlockID() as the merge key, so the three
  // middle spans would have collapsed into one span spanning rows [10,25)
  // of whichever block landed first — producing wrong aggregation results.
}

// Mixed scenario: some middle spans are same-block contiguous (mergeable),
// others share block_id only by coincidence (must stay separate).
TEST_F(MergeBlockSpansTest, MixedSameBlockAndCollidingId) {
  const uint64_t kCollidingId = 7;
  auto shared_block = std::make_shared<StubTsBlock>(kCollidingId, /*rows=*/100, /*first_ts=*/2000);
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpan(/*block_id=*/1, 0, 3));                       // first
  spans.push_back(MakeSpanFromBlock(shared_block, 0, 4));                // middle A1: shared_block rows [0,4)
  spans.push_back(MakeSpan(kCollidingId, 4, 4));                         // middle B: DIFFERENT block, same id
  spans.push_back(MakeSpanFromBlock(shared_block, 4, 4));                // middle A2: shared_block rows [4,8) — contiguous with A1
  spans.push_back(MakeSpan(/*block_id=*/2, 0, 3));                       // last

  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  // Expected:
  //   result[0] = first
  //   result[1] = merged A1+A2 over shared_block, rows [0,8)
  //   result[2] = B (must NOT merge into A even though block_id matches)
  //   result[3] = last
  ASSERT_EQ(result.size(), 4u);
  EXPECT_EQ(result[0].get(), spans[0].get());
  EXPECT_EQ(result[1]->GetTsBlockRaw(), shared_block.get());
  EXPECT_EQ(result[1]->GetStartRow(), 0);
  EXPECT_EQ(result[1]->GetRowNum(), 8);
  EXPECT_EQ(result[2].get(), spans[2].get());
  EXPECT_EQ(result[2]->GetBlockID(), kCollidingId);
  EXPECT_EQ(result[3].get(), spans[4].get());
}

// Same physical block, but a gap between two middle ranges: the first range
// is finalized in place and a new entry is started for the second range.
TEST_F(MergeBlockSpansTest, SameBlockWithGapProducesTwoEntries) {
  auto block = std::make_shared<StubTsBlock>(/*block_id=*/100, /*rows=*/100, /*first_ts=*/1000);
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpan(/*block_id=*/1, 0, 3));                       // first
  spans.push_back(MakeSpanFromBlock(block, 10, 5));                      // middle: [10,15)
  spans.push_back(MakeSpanFromBlock(block, 15, 5));                      // middle: [15,20) — contiguous, merges
  spans.push_back(MakeSpanFromBlock(block, 30, 5));                      // middle: [30,35) — GAP, starts new entry
  spans.push_back(MakeSpanFromBlock(block, 35, 5));                      // middle: [35,40) — contiguous with the new entry
  spans.push_back(MakeSpan(/*block_id=*/2, 0, 3));                       // last

  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  ASSERT_EQ(result.size(), 4u);  // first + merged [10,20) + merged [30,40) + last
  EXPECT_EQ(result[0].get(), spans[0].get());
  EXPECT_EQ(result[1]->GetTsBlockRaw(), block.get());
  EXPECT_EQ(result[1]->GetStartRow(), 10);
  EXPECT_EQ(result[1]->GetRowNum(), 10);
  EXPECT_EQ(result[2]->GetTsBlockRaw(), block.get());
  EXPECT_EQ(result[2]->GetStartRow(), 30);
  EXPECT_EQ(result[2]->GetRowNum(), 10);
  EXPECT_EQ(result[3].get(), spans[5].get());
}

// First and last spans are excluded from merging even when they reference the
// SAME physical block as a middle span — boundaries must be preserved so
// first_row / last_row aggregation can still inspect them.
TEST_F(MergeBlockSpansTest, FirstAndLastNeverMergedEvenWhenSameBlock) {
  auto block = std::make_shared<StubTsBlock>(/*block_id=*/77, /*rows=*/100, /*first_ts=*/1000);
  std::vector<std::shared_ptr<TsBlockSpan>> spans;
  spans.push_back(MakeSpanFromBlock(block, 0, 5));                       // first — shares block
  spans.push_back(MakeSpanFromBlock(block, 5, 5));                       // middle — would-be contiguous with first
  spans.push_back(MakeSpanFromBlock(block, 10, 5));                      // last  — would-be contiguous with middle

  auto result = TsAggIteratorImpl::MergeBlockSpans(spans);
  ASSERT_EQ(result.size(), 3u);
  EXPECT_EQ(result[0].get(), spans[0].get());
  EXPECT_EQ(result[0]->GetStartRow(), 0);
  EXPECT_EQ(result[0]->GetRowNum(), 5);
  EXPECT_EQ(result[1].get(), spans[1].get());
  EXPECT_EQ(result[2].get(), spans[2].get());
  EXPECT_EQ(result[2]->GetStartRow(), 10);
  EXPECT_EQ(result[2]->GetRowNum(), 5);
}

}  // namespace kwdbts
