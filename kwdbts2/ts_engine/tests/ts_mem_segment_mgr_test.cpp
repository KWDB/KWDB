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
#include <filesystem>
#include "ts_mem_segment_mgr.h"

using namespace kwdbts;  // NOLINT

KStatus TsMemSegmentManager::PutData(const TSSlice& payload, TSEntityID entity_id, TS_LSN lsn) {
  TSMemSegRowData* row_data = reinterpret_cast<TSMemSegRowData*>(payload.data);
  uint32_t row_num = 1;
  segment_lock_.lock();
  if (segment_.size() == 0) {
    segment_.push_front(TsMemSegment::Create(12));
  }
  std::shared_ptr<TsMemSegment> cur_mem_seg = segment_.front();
  cur_mem_seg->AllocRowNum(row_num);
  segment_lock_.unlock();
  cur_mem_seg->AppendOneRow(*row_data);
  return KStatus::SUCCESS;
}

KStatus TsMemSegBlock::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value) {
  auto iter = col_based_mems_.find(col_id);
  if (iter != col_based_mems_.end() && iter->second != nullptr) {
    *value = iter->second;
    return KStatus::SUCCESS;
  }
  auto col_based_len = row_data_[0]->row_data.len * row_data_.size();
  char* col_based_mem = reinterpret_cast<char*>(malloc(col_based_len));
  if (col_based_mem == nullptr) {
    LOG_ERROR("malloc memroy failed.");
    return KStatus::FAIL;
  }
  col_based_mems_[col_id] = col_based_mem;

  TSSlice value_slice;
  char* cur_offset = col_based_mem;
  for (int i = 0; i < row_data_.size(); i++) {
    auto row = row_data_[i];
    auto col_len = row->row_data.len;
    memcpy(cur_offset, row->row_data.data, col_len);
    cur_offset += col_len;
  }
  *value = col_based_mem;
  return KStatus::SUCCESS;
}


// KStatus TsBlockSpan::GetFixLenColAddr(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type, char** value, TsBitmap& bitmap) {
//   auto s = block_->GetColAddr(blk_col_idx, schema, value);
//   return s;
// }

KStatus TsMemSegBlock::GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) {
  value = row_data_[row_num]->row_data;
  return KStatus::SUCCESS;
}


class TsMemSegMgrTest : public ::testing::Test {
 public:
  TsMemSegmentManager mem_seg_mgr_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;

  virtual void SetUp() override {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

 public:
  TsMemSegMgrTest() : mem_seg_mgr_(nullptr) {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
  }
  ~TsMemSegMgrTest() {

  }
};

TEST_F(TsMemSegMgrTest, empty) {
}

TEST_F(TsMemSegMgrTest, insertOneRow) {
  uint64_t row_value = 123456789;
  TSMemSegRowData tmp_data(1, 1001, 9, 100001);
  tmp_data.SetData(10086, 1009, TSSlice{reinterpret_cast<char*>(&row_value), sizeof(row_value)});
  auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
  ASSERT_TRUE(s == KStatus::SUCCESS);
}

TEST_F(TsMemSegMgrTest, insertOneRowAndSearch) {
  uint64_t row_value = 123456789;
  TSMemSegRowData tmp_data(1, 1001, 9, 100001);
  tmp_data.SetData(10086, 1009, TSSlice{reinterpret_cast<char*>(&row_value), sizeof(row_value)});
  auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  std::list<shared_ptr<TsBlockSpan>> blocks;
  std::vector<KwTsSpan> ts_span = {{INT64_MIN, INT64_MAX}};
  TsBlockItemFilterParams params{tmp_data.database_id, tmp_data.table_id, tmp_data.entity_id, ts_span};
  s = mem_seg_mgr_.GetBlockSpans(params, blocks);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(blocks.size(), 1);
  auto block = blocks.front();
  ASSERT_EQ(block->GetEntityID(), tmp_data.entity_id);
  ASSERT_EQ(block->GetRowNum(), 1);
  ASSERT_EQ(block->GetTableID(), tmp_data.table_id);
  std::vector<AttributeInfo> schema;
  ASSERT_EQ(block->GetTS(0), tmp_data.ts);
  char* value;
  TsBitmap bitmap;
  // TODO(zqh): hide the following code temporarily
  // s = block->GetFixLenColAddr(0, &value, bitmap);
  // ASSERT_TRUE(s == KStatus::SUCCESS);
  // ASSERT_EQ(KUint64(value), row_value);
}

TEST_F(TsMemSegMgrTest, insertSomeRowsAndSearch) {
  uint64_t row_value = 123456789;
  TSEntityID entity_id = 11;
  uint32_t db_id = 22;
  TSTableID table_id = 33;
  uint32_t row_num = 10;
  std::list<uint64_t*> values;
  for (size_t i = 0; i < row_num; i++) {
    uint64_t* cur_value = new uint64_t(row_value + i);
    values.push_back(cur_value);
    TSMemSegRowData tmp_data(db_id, table_id, 9, entity_id);
    tmp_data.SetData(10086 + i, 1009 + i, TSSlice{reinterpret_cast<char*>(cur_value), sizeof(uint64_t)});
    auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
    ASSERT_TRUE(s == KStatus::SUCCESS);
  }
  std::list<shared_ptr<TsBlockSpan>> blocks;
  std::vector<KwTsSpan> ts_span = {{INT64_MIN, INT64_MAX}};
  TsBlockItemFilterParams params{db_id, table_id, entity_id, ts_span};
  auto s = mem_seg_mgr_.GetBlockSpans(params, blocks);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(blocks.size(), 1);
  auto block = blocks.front();
  ASSERT_EQ(block->GetEntityID(), entity_id);
  ASSERT_EQ(block->GetTableID(), table_id);
  ASSERT_EQ(block->GetRowNum(), row_num);
  std::vector<AttributeInfo> schema;
  AttributeInfo dest_type;
  char* value;
  TsBitmap bitmap;
  // TODO(zqh): hide the following code temporarily
  // s = block->GetFixLenColAddr(0, &value, bitmap);
  // ASSERT_TRUE(s == KStatus::SUCCESS);
  // for (size_t i = 0; i < row_num; i++) {
  //   ASSERT_EQ(block->GetTS(i), 10086 + i);
  //   ASSERT_EQ(KUint64(value + 8 * i), row_value + i);
  // }
  for (auto v : values) {
    delete v;
  }
}

TEST_F(TsMemSegMgrTest, DiffLSNAndSearch) {
  EngineOptions::g_dedup_rule = DedupRule::KEEP;
  uint64_t row_value = 123456789;
  TSEntityID entity_id = 11;
  uint32_t db_id = 22;
  TSTableID table_id = 33;
  uint32_t row_num = 10;
  std::list<uint64_t*> values;
  for (size_t i = 0; i < row_num; i++) {
    uint64_t* cur_value = new uint64_t(row_value + i);
    values.push_back(cur_value);
    TSMemSegRowData tmp_data(db_id, table_id, 9, entity_id);
    tmp_data.SetData(10086, 1009 + i, TSSlice{reinterpret_cast<char*>(cur_value), sizeof(uint64_t)});
    auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
    ASSERT_TRUE(s == KStatus::SUCCESS);
  }
  
  std::list<shared_ptr<TsBlockSpan>> blocks;
  std::vector<KwTsSpan> ts_span = {{INT64_MIN, INT64_MAX}};
  TsBlockItemFilterParams params{db_id, table_id, entity_id, ts_span};
  auto s = mem_seg_mgr_.GetBlockSpans(params, blocks);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(blocks.size(), 1);
  auto block = blocks.front();
  ASSERT_EQ(block->GetEntityID(), entity_id);
  ASSERT_EQ(block->GetTableID(), table_id);
  ASSERT_EQ(block->GetRowNum(), row_num);
  std::vector<AttributeInfo> schema;
  char* value;
  TsBitmap bitmap;
  // TODO(zqh): hide the following code temporarily
  // s = block->GetFixLenColAddr(0, &value, bitmap);
  // ASSERT_TRUE(s == KStatus::SUCCESS);
  // for (size_t i = 0; i < row_num; i++) {
  //   ASSERT_EQ(block->GetTS(i), 10086);
  //   ASSERT_EQ(KUint64(value + 8 * i), row_value + i);
  // }
  for (auto v : values) {
    delete v;
  }
}

TEST_F(TsMemSegMgrTest, DiffEntityAndSearch) {
  uint64_t row_value = 123456789;
  uint32_t db_id = 22;
  TSTableID table_id = 33;
  uint32_t row_num = 100;
  uint32_t entity_num = 10;
  std::list<uint64_t*> values;
  for (size_t i = 0; i < row_num; i++) {
    uint64_t* cur_value = new uint64_t(row_value + i);
    values.push_back(cur_value);
    TSMemSegRowData tmp_data(db_id, table_id, 9, i % entity_num + 1);
    tmp_data.SetData(10086 + i, 1009, TSSlice{reinterpret_cast<char*>(cur_value), sizeof(uint64_t)});
    auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
    ASSERT_TRUE(s == KStatus::SUCCESS);
  }
  std::list<shared_ptr<TsBlockSpan>> blocks;
  for (size_t j = 1; j <= entity_num; j++) {
    std::vector<KwTsSpan> ts_span = {{INT64_MIN, INT64_MAX}};
    TsBlockItemFilterParams params{db_id, table_id, j, ts_span};
    blocks.clear();
    auto s = mem_seg_mgr_.GetBlockSpans(params, blocks);
    ASSERT_TRUE(s == KStatus::SUCCESS);
    ASSERT_EQ(blocks.size(), 1);
    auto block = blocks.front();
    ASSERT_EQ(block->GetEntityID(), j);
    ASSERT_EQ(block->GetTableID(), table_id);
    ASSERT_EQ(block->GetRowNum(), row_num / entity_num);
    std::vector<AttributeInfo> schema;
    AttributeInfo dest_type;
    char* value;
    TsBitmap bitmap;
    // TODO(zqh): hide the following code temporarily
    // s = block->GetFixLenColAddr(0, &value, bitmap);
    // ASSERT_TRUE(s == KStatus::SUCCESS);
    // for (size_t i = 0; i < block->GetRowNum(); i++) {
    //   ASSERT_EQ(block->GetTS(i), 10086 + i * 10 + j - 1);
    //   ASSERT_EQ(KUint64(value + 8 * i), row_value + i * 10 + j - 1);
    // }
  }
  for (auto v : values) {
    delete v;
  }
}

TEST_F(TsMemSegMgrTest, DiffVersionAndSearch) {
  uint64_t row_value = 123456789;
  TSEntityID entity_id = 11;
  uint32_t db_id = 22;
  TSTableID table_id = 33;
  uint32_t row_num = 10;
  uint32_t version_num = 2;
  std::list<uint64_t*> values;
  for (size_t i = 0; i < row_num; i++) {
    uint64_t* cur_value = new uint64_t(row_value + i);
    values.push_back(cur_value);
    TSMemSegRowData tmp_data(db_id, table_id, 9 + i % version_num, entity_id);
    tmp_data.SetData(10086 + i, 1009, TSSlice{reinterpret_cast<char*>(cur_value), sizeof(uint64_t)});
    auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
    ASSERT_TRUE(s == KStatus::SUCCESS);
  }
  std::list<shared_ptr<TsBlockSpan>> blocks;
  std::vector<KwTsSpan> ts_span = {{INT64_MIN, INT64_MAX}};
  TsBlockItemFilterParams params{db_id, table_id, entity_id, ts_span};
  auto s = mem_seg_mgr_.GetBlockSpans(params, blocks);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(blocks.size(), version_num);
  int j = 0;
  for (auto block : blocks) {
    ASSERT_EQ(block->GetEntityID(), entity_id);
    ASSERT_EQ(block->GetTableID(), table_id);
    ASSERT_EQ(block->GetRowNum(), row_num / version_num);
    std::vector<AttributeInfo> schema;
    AttributeInfo dest_type;
    char* value;
    TsBitmap bitmap;
    // TODO(zqh): hide the following code temporarily
    // s = block->GetFixLenColAddr(0, &value, bitmap);
    // ASSERT_TRUE(s == KStatus::SUCCESS);
    // for (size_t i = 0; i < row_num / version_num; i++) {
    //   ASSERT_EQ(block->GetTS(i), 10086 + i * version_num + j);
    //   ASSERT_EQ(KUint64(value + i * 8), row_value + i * version_num + j);
    // }
    j++;
  }  
  for (auto v : values) {
    delete v;
  }
}

TEST_F(TsMemSegMgrTest, DiffTableAndSearch) {
  uint64_t row_value = 123456789;
  TSEntityID entity_id = 11;
  uint32_t db_id = 22;
  TSTableID table_id = 33;
  uint32_t row_num = 24;
  uint32_t table_num = 4;
  uint32_t version_num = 2;
  std::list<uint64_t*> values;
  for (size_t i = 0; i < row_num / table_num / version_num; i++) {
    for (size_t tbl_id = 0; tbl_id < table_num; tbl_id++) {
      for (size_t v_num = 0; v_num < version_num; v_num++) {
        uint64_t* cur_value = new uint64_t(row_value + i);
        values.push_back(cur_value);
        TSMemSegRowData tmp_data(db_id, table_id + tbl_id, 9 + v_num, entity_id);
        tmp_data.SetData(10086 + i, 1009 + i, TSSlice{reinterpret_cast<char*>(cur_value), sizeof(uint64_t)});
        auto s = mem_seg_mgr_.PutData({reinterpret_cast<char*>(&tmp_data), sizeof(tmp_data)}, tmp_data.entity_id, 1);
        ASSERT_TRUE(s == KStatus::SUCCESS);
      }
    }
  }
  for (size_t i = 0; i < table_num; i++) {
    std::list<shared_ptr<TsBlockSpan>> blocks;
    std::vector<KwTsSpan> ts_span = {{INT64_MIN, INT64_MAX}};
    TsBlockItemFilterParams params{db_id, table_id + i, entity_id, ts_span};
    auto s = mem_seg_mgr_.GetBlockSpans(params, blocks);
    ASSERT_TRUE(s == KStatus::SUCCESS);
    ASSERT_EQ(blocks.size(), version_num);
    for (auto block : blocks) {
      ASSERT_EQ(block->GetEntityID(), entity_id);
      ASSERT_EQ(block->GetTableID(), table_id + i);
      ASSERT_EQ(block->GetRowNum(), row_num / version_num / table_num);
    }
  }
  for (auto v : values) {
    delete v;
  }
}
