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

#include "test_util.h"
#include "ts_table.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_ts_table";  // NOLINT
const std::string TestBigTableInstance::kw_home_ = kDbPath;    // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";     // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTsTable : public TestBigTableInstance {
 public:
  TsTable *table_{nullptr};
  KTableKey table_id_ = 10086;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  TestTsTable() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    table_ = new TsTable(ctx_, kDbPath, 10086);
  }

  ~TestTsTable() {
    table_->DropAll(ctx_);
    if (table_ != nullptr) {
      delete table_;
      table_ = nullptr;
    }
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  std::vector<TagInfo> GetTagSchema(roachpb::CreateTsTable* meta) {
    std::vector<TagInfo> tag_schema;
    KStatus s;
    for (int i = 0 ; i < meta->k_column_size() ; i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                               static_cast<uint32_t>(col_var.length), 0,
                                               static_cast<uint32_t>(col_var.length),
                                               static_cast<TagType>(col_var.col_flag)}));
      }
    }
    return tag_schema;
  }

  void Create(roachpb::CreateTsTable* meta, const std::vector<RangeGroup>& range_groups) {
    std::unordered_map<uint64_t, int8_t> ranges;
    KStatus s = table_->Init(ctx_, ranges);
    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> metric_schema;
    for (int i = 0 ; i < meta->k_column_size() ; i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                               static_cast<uint32_t>(col_var.length), 0,
                                               static_cast<uint32_t>(col_var.length),
                                               static_cast<TagType>(col_var.col_flag)}));
      } else {
        metric_schema.push_back(std::move(col_var));
      }
    }

    s = table_->Create(ctx_, metric_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> table_range;
    for (size_t i = 0; i < range_groups.size(); i++) {
      s = table_->CreateEntityGroup(ctx_, range_groups[i], tag_schema, &table_range);
      EXPECT_EQ(s, KStatus::SUCCESS);
    }
  }

  void ConstructRoachpbTable(roachpb::CreateTsTable* meta, int clumn_num, int tag_num,
                             uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id_);
    table->set_table_name("table_" + std::to_string(table_id_));
    table->set_partition_interval(partition_interval);
    table->set_hash_num(g_testcase_hash_num);
    meta->set_allocated_ts_table(table);

    for (int i = 0; i < clumn_num; i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type(roachpb::DataType::TIMESTAMP);
      column->set_storage_len(8);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // first column name: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    for (int i = 0; i< tag_num; i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type(roachpb::DataType::TIMESTAMP);
      column->set_storage_len(8);
      column->set_column_id(tag_num + 1 + i);
      if (i == 0) {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
      } else {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }
  }
};

// create and drop
TEST_F(TestTsTable, empty) {
}

// create table with column
TEST_F(TestTsTable, create) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 3, 1);
  RangeGroup test_range{101, 0};
  std::vector<RangeGroup> ranges{test_range};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());
}

// create table with many columns
TEST_F(TestTsTable, createHuge) {
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 10000, 100);
  RangeGroup test_range{101, 0};
  std::vector<RangeGroup> ranges{test_range};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());
}

// crate many entity group
TEST_F(TestTsTable, createEntityGroup) {
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 5, 1);
  std::vector<RangeGroup> ranges;
  int range_grp_num = 5;
  for (size_t i = 0; i < range_grp_num; i++) {
    ranges.push_back({100 + i, 0});
  }
  Create(&meta, ranges);
  auto tage_schema = GetTagSchema(&meta);
  std::shared_ptr<TsEntityGroup> entity_group;
  for (size_t i = 0; i < range_grp_num; i++) {
    KStatus s = table_->CreateEntityGroup(ctx_, {100 + i, 0}, tage_schema, &entity_group);
    EXPECT_EQ(s, KStatus::FAIL);
  }
}

// multi-create entity group
TEST_F(TestTsTable, multiCreateEntityGroup) {
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 5, 1);
  std::vector<RangeGroup> ranges;
  Create(&meta, ranges);
  int range_grp_num = 50;
  int thread_num = 5;
  std::vector<std::thread> threads;
  std::vector<TagInfo> tage_schema = GetTagSchema(&meta);
  for (size_t i = 0; i < thread_num; i++) {
    threads.push_back(std::thread([=]{
      std::shared_ptr<TsEntityGroup> entity_group;
      std::vector<TagInfo> tage_schema1 = tage_schema;
      for (size_t i = 0; i < range_grp_num; i++) {
        kwdbContext_t cur_context;
        kwdbContext_p cur_ctx_ = &cur_context;
        kwdbts::InitServerKWDBContext(cur_ctx_);
        table_->CreateEntityGroup(cur_ctx_, {100 + i, 0}, tage_schema1, &entity_group);
      }
    }));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  std::vector<std::shared_ptr<TsEntityGroup>> leader_entity_groups;
  KStatus s = table_->GetAllLeaderEntityGroup(ctx_, &leader_entity_groups);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(leader_entity_groups.size(), range_grp_num);
}

// create and drop entity group
TEST_F(TestTsTable, dropEntityGroup) {
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 5, 1);
  std::vector<RangeGroup> ranges;
  KStatus s;
  int range_grp_num = 5;
  for (size_t i = 0; i < range_grp_num; i++) {
    ranges.push_back({100 + i, 0});
  }
  Create(&meta, ranges);
  for (size_t i = 0; i < range_grp_num; i++) {
    s = table_->DropEntityGroup(ctx_, 100 + i, true);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  auto tage_schema = GetTagSchema(&meta);
  std::shared_ptr<TsEntityGroup> entity_group;
  for (size_t i = 0; i < range_grp_num; i++) {
    s = table_->CreateEntityGroup(ctx_, {100 + i, 0}, tage_schema, &entity_group);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
}

// update many entity group
// 2.0.4 no use leader to check
// TEST_F(TestTsTable, UpdateEntityGroup) {
//   roachpb::CreateTsTable meta;
//   ConstructRoachpbTable(&meta, 5, 1);
//   std::vector<RangeGroup> ranges;
//   KStatus s;
//   int range_grp_num = 5;
//   for (size_t i = 0; i < range_grp_num; i++) {
//     ranges.push_back({100 + i, 0});
//   }
//   Create(&meta, ranges);
//   std::vector<std::shared_ptr<TsEntityGroup>> leader_entity_groups;
//   s = table_->GetAllLeaderEntityGroup(ctx_, &leader_entity_groups);
//   EXPECT_EQ(s, KStatus::SUCCESS);
//   EXPECT_EQ(leader_entity_groups.size(), range_grp_num);
//   for (size_t i = 0; i < range_grp_num; i++) {
//     s = table_->UpdateEntityGroup(ctx_, {100 + i, 1});
//     EXPECT_EQ(s, KStatus::SUCCESS);
//   }
//   s = table_->GetAllLeaderEntityGroup(ctx_, &leader_entity_groups);
//   EXPECT_EQ(s, KStatus::SUCCESS);
//   EXPECT_EQ(leader_entity_groups.size(), 0);
// }

// create many entity group
TEST_F(TestTsTable, GetEntityGroup) {
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 5, 1);
  std::vector<RangeGroup> ranges;
  KStatus s;
  int range_grp_num = 5;
  for (size_t i = 0; i < range_grp_num; i++) {
    ranges.push_back({100 + i, 3});
  }
  Create(&meta, ranges);
  std::shared_ptr<TsEntityGroup> entity_grp;
  for (size_t i = 0; i < range_grp_num; i++) {
    s = table_->GetEntityGroup(ctx_, 100 + i, &entity_grp);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(entity_grp->HashRange().range_group_id, 100 + i);
    EXPECT_EQ(entity_grp->HashRange().typ, 3);
  }
  for (size_t i = 0; i < range_grp_num; i++) {
    s = table_->UpdateEntityGroup(ctx_, {100 + i, 7});
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  for (size_t i = 0; i < range_grp_num; i++) {
    s = table_->GetEntityGroup(ctx_, 100 + i, &entity_grp);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(entity_grp->HashRange().range_group_id, 100 + i);
    EXPECT_EQ(entity_grp->HashRange().typ, 7);
  }
}

TEST_F(TestTsTable, SharedLruUnorderedMap) {
  int lru_size = 10;
  kwdbts::SharedLruUnorderedMap<KTableKey, TsTable> tables_cache = kwdbts::SharedLruUnorderedMap<KTableKey, TsTable>(lru_size);
  tables_cache.Init();

  for (KTableKey table_id = 1; table_id <= 100; table_id++) {
    std::shared_ptr<TsTable> t = std::make_shared<TsTable>(ctx_, "abc", table_id);
    tables_cache.Put(table_id, t);
  }
  ASSERT_EQ(tables_cache.Size(), lru_size);

  tables_cache.Clear();
  ASSERT_EQ(tables_cache.Size(), 0);

  std::vector<std::shared_ptr<TsTable>> tmp_vector;
  for (KTableKey table_id = 1; table_id <= 100; table_id++) {
    std::shared_ptr<TsTable> t = std::make_shared<TsTable>(ctx_, "abc", table_id);
    tmp_vector.push_back(t);
    tables_cache.Put(table_id, t);
  }
  ASSERT_EQ(tables_cache.Size(), 100);

  tables_cache.Clear(3);
  ASSERT_EQ(tables_cache.Size(), 100);

  tmp_vector.clear();
  tables_cache.Clear(3);
  ASSERT_EQ(tables_cache.Size(), 97);

  {
    KTableKey table_id = 1;
    std::shared_ptr<TsTable> t = std::make_shared<TsTable>(ctx_, "abc", table_id);
    tmp_vector.push_back(t);
    tables_cache.Put(table_id, t);
  }
  ASSERT_EQ(tables_cache.Size(), lru_size);

  tables_cache.SetCapacity(3);
  ASSERT_EQ(tables_cache.Size(), 3);
}
