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

#include "engine.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

RangeGroup kTestRange{101, 0};

class TestMetaIterator : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestMetaIterator() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // Clean up file directory
    TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  }

  ~TestMetaIterator() {
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }
};

TEST_F(TestMetaIterator, meta_iter) {
  ErrorInfo error_info;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "t", cur_table_id);

  std::vector<RangeGroup> ranges{{101, 0}, {102, 0}, {103, 1}};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range1, tbl_range2, tbl_range3;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, 101, &tbl_range1), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, 102, &tbl_range2), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, 103, &tbl_range3), KStatus::SUCCESS);

  auto manager1 = tbl_range1->GetSubEntityGroupManager();
//  MMapMetricsTable* root_et = manager1->GetRootTable(error_info);
//  ASSERT_EQ(error_info.errmsg, "");
//  manager1->ReleasePartitionTable(root_et);

  // tbl_range1 create subgroup
  for (int i = 1; i <= 5; ++i) {
    TsSubEntityGroup* bt = manager1->GetSubGroup(i, error_info, true);
    ASSERT_EQ(error_info.errmsg, "");
    ASSERT_NE(bt, nullptr);
//    manager1->ReleasePartitionTable(bt);
  }
  // tbl_range1 allocate entity id
  int entity_num1 = 2300;
  for (int i = 0; i < entity_num1; i++) {
    string tag = "p_tag_" + std::to_string(i);
    uint64_t tag_hash = GetConsistentHashId(tag.data(), tag.size(), g_testcase_hash_num);
    SubGroupID group_id;
    EntityID entity_id;
    ASSERT_GE(manager1->AllocateEntity(tag, tag_hash, &group_id, &entity_id), 0);
    ASSERT_GE(group_id, 1);
    ASSERT_GE(entity_id, 1);
  }

  MetaIterator* iter1;
  ASSERT_EQ(ts_table->GetMetaIterator(ctx_, &iter1, 1), KStatus::SUCCESS);
  ResultSet res;
  k_uint32 count1;
  std::vector<EntityResultIndex> entity_list1;
  ASSERT_EQ(iter1->Next(&entity_list1, &res, &count1), KStatus::SUCCESS);
  EXPECT_EQ(count1, ONE_FETCH_COUNT);
  EXPECT_EQ(entity_list1[0].entityGroupId, 101);
  EXPECT_EQ(entity_list1[0].subGroupId, 1);
  EXPECT_EQ(entity_list1[0].entityId, 1);
  ASSERT_EQ(iter1->Next(&entity_list1, &res, &count1), KStatus::SUCCESS);
  EXPECT_EQ(count1, ONE_FETCH_COUNT);
  ASSERT_EQ(iter1->Next(&entity_list1, &res, &count1), KStatus::SUCCESS);
  EXPECT_EQ(count1, entity_num1 - 2 * ONE_FETCH_COUNT);
  ASSERT_EQ(iter1->Next(&entity_list1, &res, &count1), KStatus::SUCCESS);
  EXPECT_EQ(count1, 0);
  delete iter1;

  auto manager2 = tbl_range2->GetSubEntityGroupManager();
//  root_et = manager2->GetRootTable(error_info);
//  ASSERT_EQ(error_info.errmsg, "");
//  manager1->ReleasePartitionTable(root_et);

  // tbl_range2 create subgroup
  for (int i = 1; i <= 3; ++i) {
    TsSubEntityGroup* bt = manager2->GetSubGroup(i, error_info, true);
    ASSERT_EQ(error_info.errmsg, "");
    ASSERT_EQ(bt->GetID(), i);
  }
  // tbl_range2 allocate entity id
  int entity_num2 = 1200;
  for (int i = 0; i < entity_num2; i++) {
    string tag = "p_tag_" + std::to_string(i);
    uint64_t tag_hash = GetConsistentHashId(tag.data(), tag.size(), g_testcase_hash_num);
    SubGroupID group_id;
    EntityID entity_id;
    ASSERT_GE(manager2->AllocateEntity(tag, tag_hash, &group_id, &entity_id), 0);
    ASSERT_GE(group_id, 1);
    ASSERT_GE(entity_id, 1);
  }

  MetaIterator* iter2;
  ASSERT_EQ(ts_table->GetMetaIterator(ctx_, &iter2, 1), KStatus::SUCCESS);
  k_uint32 count2;
  std::vector<EntityResultIndex> entity_list2;
  ASSERT_EQ(iter2->Next(&entity_list2, &res, &count2), KStatus::SUCCESS);
  EXPECT_EQ(count2, ONE_FETCH_COUNT);
  ASSERT_EQ(iter2->Next(&entity_list2, &res, &count2), KStatus::SUCCESS);
  EXPECT_EQ(count2, ONE_FETCH_COUNT);
  ASSERT_EQ(iter2->Next(&entity_list2, &res, &count2), KStatus::SUCCESS);
  EXPECT_EQ(count2, ONE_FETCH_COUNT);
  ASSERT_EQ(iter2->Next(&entity_list2, &res, &count2), KStatus::SUCCESS);
  EXPECT_EQ(count2, entity_num1 + entity_num2 - 3 * ONE_FETCH_COUNT);
  ASSERT_EQ(iter2->Next(&entity_list2, &res, &count2), KStatus::SUCCESS);
  EXPECT_EQ(count2, 0);
  delete iter2;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}
