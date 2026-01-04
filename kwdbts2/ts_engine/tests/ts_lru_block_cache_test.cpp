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

#include "ts_lru_block_cache.h"
#include <gtest/gtest.h>

using namespace kwdbts;  // NOLINT
using namespace roachpb;

class TsLRUBlockCacheTest : public ::testing::Test {
 protected:
  std::vector<std::shared_ptr<TsEntitySegment>> entity_segment;

 public:
  TsLRUBlockCacheTest() {}

  ~TsLRUBlockCacheTest() {}

  void SetUp() override {
    EngineOptions::block_cache_max_size = 100 * 1024;
    entity_segment.resize(10);
    for (int i = 0; i < 10; ++i) {
      entity_segment[i] = std::make_shared<TsEntitySegment>((i + 1) * 1000);
    }
  }

  void TearDown() override {}
};

TEST_F(TsLRUBlockCacheTest, basicTest) {
  TsEntitySegmentBlockItem block_item;
  uint32_t hit_count;
  uint32_t miss_count;
  uint64_t memory_size;
  TsLRUBlockCache::GetInstance().GetRecentHitInfo(&hit_count, &miss_count, &memory_size);
  ASSERT_EQ(hit_count, 0);
  ASSERT_EQ(miss_count, 0);
  ASSERT_EQ(memory_size, 0);
  for (int i = 0; i < 1000; ++i) {
    block_item.block_id = i + 1;
    block_item.n_cols = 5;
    std::shared_ptr<TsEntityBlock> entity_block = std::make_shared<TsEntityBlock>(1, &block_item, entity_segment[0]->GetSegmentBlockContainer());
    entity_segment[0]->GetSegmentBlockContainer()->AddEntityBlock(block_item.block_id, entity_block);
    TsLRUBlockCache::GetInstance().Add(entity_block);
    ASSERT_EQ(TsLRUBlockCache::GetInstance().GetMemorySize(), i <= 100 ? i * 1024 : 100 * 1024);
    TsLRUBlockCache::GetInstance().AddMemory(entity_block.get(), 1024);
  }
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetMemorySize(), 100 * 1024);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheHitCount(), 0);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheMissCount(), 1000);
  TsLRUBlockCache::GetInstance().GetRecentHitInfo(&hit_count, &miss_count, &memory_size);
  ASSERT_EQ(hit_count, 0);
  ASSERT_EQ(miss_count, 1000);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetHitRatio(), 0.0);
  for (int i = 0; i < 900; ++i) {
    ASSERT_EQ(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
  for (int i = 900; i < 1000; ++i) {
    ASSERT_NE(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }

  for (int i = 900; i < 950; ++i) {
    std::shared_ptr<TsEntityBlock> entity_block = entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1);
    TsLRUBlockCache::GetInstance().Access(entity_block);
  }
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheHitCount(), 50);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheMissCount(), 1000);
  TsLRUBlockCache::GetInstance().GetRecentHitInfo(&hit_count, &miss_count, &memory_size);
  ASSERT_EQ(hit_count, 50);
  ASSERT_EQ(miss_count, 1000);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetHitRatio(), (float)50 / (float)1050);

  for (int i = 0; i < 50; ++i) {
    block_item.block_id = i + 1;
    block_item.n_cols = 5;
    std::shared_ptr<TsEntityBlock> entity_block = std::make_shared<TsEntityBlock>(1, &block_item, entity_segment[0]->GetSegmentBlockContainer());
    entity_segment[0]->GetSegmentBlockContainer()->AddEntityBlock(block_item.block_id, entity_block);
    TsLRUBlockCache::GetInstance().Add(entity_block);
    TsLRUBlockCache::GetInstance().AddMemory(entity_block.get(), 1024);
    ASSERT_EQ(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(951 + i), nullptr);
  }
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheHitCount(), 50);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheMissCount(), 1050);
  TsLRUBlockCache::GetInstance().GetRecentHitInfo(&hit_count, &miss_count, &memory_size);
  ASSERT_EQ(hit_count, 50);
  ASSERT_EQ(miss_count, 1050);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetHitRatio(), (float)50 / (float)1100);

  for (int i = 49; i >= 0; --i) {
    std::shared_ptr<TsEntityBlock> entity_block = entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1);
    TsLRUBlockCache::GetInstance().Access(entity_block);
  }
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheHitCount(), 100);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetCacheMissCount(), 1050);
  TsLRUBlockCache::GetInstance().GetRecentHitInfo(&hit_count, &miss_count, &memory_size);
  ASSERT_EQ(hit_count, 100);
  ASSERT_EQ(miss_count, 1050);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetHitRatio(), (float)100 / (float)1150);

  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetMemorySize(), 100 * 1024);
  for (int i = 0; i < 50; ++i) {
    ASSERT_NE(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
  for (int i = 50; i < 900; ++i) {
    ASSERT_EQ(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
  for (int i = 900; i < 950; ++i) {
    ASSERT_NE(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
  for (int i = 950; i < 1000; ++i) {
    ASSERT_EQ(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
}

TEST_F(TsLRUBlockCacheTest, multiThreads) {
  auto EntityBlockReader = [&](int thread_index) {
    TsEntitySegmentBlockItem block_item;
    for (int j = 0; j < 2 * (10 - thread_index); ++j) {
      for (int i = 0; i < (thread_index + 1) * 1000; ++i) {
        block_item.block_id = i + 1;
        block_item.n_cols = 5;
        std::shared_ptr<TsEntityBlock> entity_block = entity_segment[thread_index]->GetSegmentBlockContainer()->GetEntityBlock(block_item.block_id);
        if (entity_block == nullptr) {
          entity_block = std::make_shared<TsEntityBlock>(1, &block_item, entity_segment[thread_index]->GetSegmentBlockContainer());
          entity_segment[thread_index]->GetSegmentBlockContainer()->AddEntityBlock(block_item.block_id, entity_block);
          TsLRUBlockCache::GetInstance().Add(entity_block);
          TsLRUBlockCache::GetInstance().AddMemory(entity_block.get(), 1024 + i * 10);
        } else {
          TsLRUBlockCache::GetInstance().Access(entity_block);
        }
      }
      TsLRUBlockCache::GetInstance().SetMaxMemorySize((thread_index + 1) * 256 * 1024);
      for (int i = (thread_index + 1) * 1000 - 1; i >= 0; --i) {
        block_item.block_id = i + 1;
        block_item.n_cols = 5;
        std::shared_ptr<TsEntityBlock> entity_block = entity_segment[thread_index]->GetSegmentBlockContainer()->GetEntityBlock(block_item.block_id);
        if (entity_block == nullptr) {
          entity_block = std::make_shared<TsEntityBlock>(1, &block_item, entity_segment[thread_index]->GetSegmentBlockContainer());
          entity_segment[thread_index]->GetSegmentBlockContainer()->AddEntityBlock(block_item.block_id, entity_block);
          TsLRUBlockCache::GetInstance().Add(entity_block);
          TsLRUBlockCache::GetInstance().AddMemory(entity_block.get(), 1024 + i * 10);
        } else {
          TsLRUBlockCache::GetInstance().Access(entity_block);
        }
      }
      TsLRUBlockCache::GetInstance().SetMaxMemorySize(100 * 1024);
    }
  };

  std::vector<std::shared_ptr<std::thread>> t_reader;
  t_reader.resize(20);
  for (int i = 0; i < 20; ++i) {
    t_reader[i] = std::make_shared<std::thread>(EntityBlockReader, i % 10);
  }
  for (int i = 0; i < 20; ++i) {
    t_reader[i]->join();
  }

  ASSERT_LE(TsLRUBlockCache::GetInstance().GetMemorySize(), 100 * 1024);
  TsEntitySegmentBlockItem block_item;
  for (int i = 0; i < 1000; ++i) {
    block_item.block_id = i + 1;
    block_item.n_cols = 5;
    std::shared_ptr<TsEntityBlock> entity_block = std::make_shared<TsEntityBlock>(1, &block_item, entity_segment[0]->GetSegmentBlockContainer());
    entity_segment[0]->GetSegmentBlockContainer()->AddEntityBlock(block_item.block_id, entity_block);
    TsLRUBlockCache::GetInstance().Add(entity_block);
    TsLRUBlockCache::GetInstance().AddMemory(entity_block.get(), 1024);
  }
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetMemorySize(), 100 * 1024);
  for (int j = 1; j < 10; ++j) {
    for (int i = 0; i < (j + 1) * 1000; ++i) {
      ASSERT_EQ(entity_segment[j]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
    }
  }
  for (int i = 0; i < 900; ++i) {
    ASSERT_EQ(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
  for (int i = 900; i < 1000; ++i) {
    ASSERT_NE(entity_segment[0]->GetSegmentBlockContainer()->GetEntityBlock(i + 1), nullptr);
  }
}