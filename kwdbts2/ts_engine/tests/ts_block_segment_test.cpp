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

#include <gtest/gtest.h>
#include <memory>
#include "ts_block_segment.h"
#include "sys_utils.h"

using namespace kwdbts;  // NOLINT

const string block_segment_path = "./blk_segment";
class TestTsBlockSegment : public ::testing::Test {
 public:
  TsBlockSegment * block_seg_{nullptr};
 public:
  TestTsBlockSegment() {
    Remove(block_segment_path);
    MakeDirectory(block_segment_path);
    block_seg_ = new TsBlockSegment(block_segment_path);
    KStatus s = block_seg_->Open();
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  ~TestTsBlockSegment() {
    if (block_seg_) {
      delete block_seg_;
    }
  }
};

TEST_F(TestTsBlockSegment, empty) {
}

TEST_F(TestTsBlockSegment, InsertOneBlock) {
//  TsBlockItem item = TsBlockItem::NewBlockItem(1);
//  item->Info().row_count = 7;
//  item->Info().is_agg_res_available = true;
//  item->Info().entity_id = 3;
//  item->Info().is_overflow = true;
//  item->Info().max_ts_in_block = 666555;
//  item->Info().min_ts_in_block = 555666;
//  bool del;
//  item->isDeleted(6, &del);
//  EXPECT_EQ(del, false);
//  item->setDeleted(6);
//  item->isDeleted(6, &del);
//  EXPECT_EQ(del, true);
//
//  std::vector<TsBlockItem*> items;
//  KStatus s = block_seg_->GetAllBlockItems(1, &items);
//  EXPECT_EQ(s, KStatus::SUCCESS);
//  EXPECT_EQ(0, items.size());
//
//  std::string data_str = "hello 1342xdfxcvaser";
//  TSSlice data{data_str.data(), data_str.length() + 1};
//
//  s = block_seg_->AppendBlockData(item, data, data);
//  EXPECT_EQ(s, KStatus::SUCCESS);
//
//  items.clear();
//  s = block_seg_->GetAllBlockItems(1, &items);
//  EXPECT_EQ(s, KStatus::SUCCESS);
//  EXPECT_EQ(0, items.size());
//
//  items.clear();
//  s = block_seg_->GetAllBlockItems(3, &items);
//  EXPECT_EQ(s, KStatus::SUCCESS);
//  EXPECT_EQ(1, items.size());
//  EXPECT_EQ(items[0]->Info().row_count, 7);
//  EXPECT_EQ(items[0]->Info().is_agg_res_available, false);
//  EXPECT_EQ(items[0]->Info().entity_id, 3);
//  EXPECT_EQ(items[0]->Info().min_ts_in_block, 555666);
//  items[0]->isDeleted(6, &del);
//  EXPECT_EQ(del, true);
//
//  auto buff = std::make_unique<char[]>(items[0]->Info().data_size);
//  s = block_seg_->GetBlockData(items[0], buff.get());
//  EXPECT_EQ(s, KStatus::SUCCESS);
//  EXPECT_EQ(memcmp(data.data, buff.get(), data.len), 0);
//
//  TsBlockItem::DeleteBlockItem(item);
//  TsBlockItem::DeleteBlockItem(items[0]);
}

TEST_F(TestTsBlockSegment, InsertSomeBlocks) {
//  int blk_item_num = 10;
//  std::string data_str_prefix = "hello 1342xdfxcvaser_";
//  bool del;
//  std::vector<TsBlockItem*> gen_items;
//  for (size_t i = 0; i < blk_item_num; i++) {
//    TsBlockItem* item = TsBlockItem::NewBlockItem(200 + i);
//    item->Info().row_count = 200 + i;
//    item->Info().is_agg_res_available = true;
//    item->Info().entity_id = 1 + i;
//    item->Info().is_overflow = true;
//    item->Info().max_ts_in_block = 666555;
//    item->Info().min_ts_in_block = 555666 + i;
//    item->isDeleted(100 + i, &del);
//    EXPECT_EQ(del, false);
//    item->setDeleted(100 + i);
//    item->isDeleted(100 + i, &del);
//    EXPECT_EQ(del, true);
//    std::string data_str = data_str_prefix + std::to_string(i);
//    TSSlice data{data_str.data(), data_str.length() + 1};
//    auto s = block_seg_->AppendBlockData(item, data, data);
//    EXPECT_EQ(s, KStatus::SUCCESS);
//    gen_items.push_back(item);
//  }
//
//  for (size_t i = 0; i < blk_item_num; i++) {
//    std::vector<TsBlockItem*> items;
//    auto s = block_seg_->GetAllBlockItems(1 + i, &items);
//    EXPECT_EQ(s, KStatus::SUCCESS);
//    EXPECT_EQ(1, items.size());
//    EXPECT_EQ(items[0]->Info().row_count, 200 + i);
//    EXPECT_EQ(items[0]->Info().is_agg_res_available, false);
//    EXPECT_EQ(items[0]->Info().entity_id, 1 + i);
//    EXPECT_EQ(items[0]->Info().min_ts_in_block, 555666 + i);
//    items[0]->isDeleted(100 + i, &del);
//    EXPECT_EQ(del, true);
//    items[0]->isDeleted(99, &del);
//    EXPECT_EQ(del, false);
//
//    // char* buff = (char*)(malloc(items[0]->Info().data_size));
//    auto buff = std::make_unique<char[]>(items[0]->Info().data_size);
//    s = block_seg_->GetBlockData(items[0], buff.get());
//    EXPECT_EQ(s, KStatus::SUCCESS);
//    std::string data_str = data_str_prefix + std::to_string(i);
//    TSSlice data{data_str.data(), data_str.length() + 1};
//    EXPECT_EQ(memcmp(data.data, buff.get(), data.len), 0);
//
//    TsBlockItem::DeleteBlockItem(items[0]);
//  }
//
//  for (size_t i = 0; i < blk_item_num; i++) {
//    TsBlockItem::DeleteBlockItem(gen_items[i]);
//  }
}
