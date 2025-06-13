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
#include "st_config.h"
#include "st_group_manager.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db/";  // NOLINT
const string TestBigTableInstance::kw_home_ = "./test_db/";  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

void ConstructSchema(vector<AttributeInfo>* schema, KTableKey table_id) {
  // create table
  Def_Column(col_1, 1, "k_timestamp", DATATYPE::TIMESTAMP64_LSN, 0, 8, 8, 0, AINFO_NOT_NULL, 8, 1, 0);
  Def_Column(col_2, 2, "c1", DATATYPE::INT64, 16, 8, 8, 0, 0, 8, 1, 0);
  Def_Column(col_3, 3, "c2", DATATYPE::DOUBLE, 24, 8, 8, 0, 0, 8, 1, 0);
  *schema = {std::move(col_1), std::move(col_2), std::move(col_3)};
}

class TestEntityTableManager : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  uint64_t table_id_ = 10001;
  RangeGroup ranges_{1001, 0};
  SubEntityGroupManager* et_manager_;
  vector<AttributeInfo> schema_;
  string tbl_sub_path_;
  MMapRootTableManager* root_bt_manager_;

  TestEntityTableManager() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    tbl_sub_path_ = std::to_string(table_id_) + "/" + std::to_string(ranges_.range_group_id) + "/";
    ConstructSchema(&schema_, table_id_);
  }

  virtual void SetUp() {
    TestBigTableInstance::SetUp();
    system(("rm -rf " + kDbPath + "/*").c_str());
    if (access(kDbPath.c_str(), 0)) {
      system(("mkdir " + kDbPath).c_str());
    }
    ErrorInfo err_info;
    {
      MakeDirectory(kDbPath + tbl_sub_path_);
      int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
      MMapMetricsTable* tmp_bt = new MMapMetricsTable();
      string bt_path = nameToEntityBigTablePath(std::to_string(table_id_));
      if (tmp_bt->open(bt_path, kDbPath, tbl_sub_path_, MMAP_CREAT_EXCL, err_info) >= 0 ||
          err_info.errcode == KWECORR) {
        tmp_bt->create(schema_, 1, tbl_sub_path_, kwdbts::EngineOptions::iot_interval, encoding, err_info, false);
      }
      if (err_info.errcode < 0) {
        tmp_bt->setObjectReady();
        tmp_bt->remove();
        delete tmp_bt;
        fprintf(stderr, "createTable fail, table_id[%lu], msg[%s]", table_id_, err_info.errmsg.c_str());
      } else {
        tmp_bt->incRefCount();
        root_bt_manager_ = new MMapRootTableManager(kDbPath, tbl_sub_path_, table_id_);
        root_bt_manager_->PutTable(1, tmp_bt);
      }
    }
    et_manager_ = new SubEntityGroupManager(root_bt_manager_);
    et_manager_->OpenInit(kDbPath, tbl_sub_path_, table_id_, err_info);
    ASSERT_EQ(err_info.errmsg, "");
  }

  virtual void TearDown() {
    delete et_manager_;
    et_manager_ = nullptr;
    delete root_bt_manager_;
  }

  ~TestEntityTableManager() {

    delete et_manager_;
    et_manager_ = nullptr;
  }
};

TEST_F(TestEntityTableManager, group) {
  ErrorInfo err_info;
  TsSubEntityGroup* g1_bt = et_manager_->CreateSubGroup(0, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  ASSERT_EQ(g1_bt->GetID(), 0);
  // et_manager_->ReleasePartitionTable(g1_bt);
  TsTimePartition* sg1_bt = et_manager_->CreatePartitionTable(1704280045000, 0, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  ASSERT_EQ(sg1_bt->getSchemaInfoIncludeDropped().size(), 3);
  et_manager_->ReleasePartitionTable(sg1_bt);

  TsSubEntityGroup* g2_bt = et_manager_->CreateSubGroup(1, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  ASSERT_EQ(g2_bt->GetID(), 1);
  // et_manager_->ReleasePartitionTable(g2_bt);
  TsTimePartition* sg2_bt = et_manager_->CreatePartitionTable(1704280045000, 1, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  ASSERT_EQ(sg2_bt->getSchemaInfoIncludeDropped().size(), 3);
  et_manager_->ReleasePartitionTable(sg2_bt);

  et_manager_->CreatePartitionTable(1704280045000, 0, err_info);
  ASSERT_LT(err_info.errcode, 0);
  err_info.clear();
  // reopen et_manager
  {
    delete et_manager_;
    et_manager_ = new SubEntityGroupManager(root_bt_manager_);
    et_manager_->OpenInit(kDbPath, tbl_sub_path_, table_id_, err_info);
  }
  {
    // delete group table
    et_manager_->DropPartitionTable(1704280045000, 0, false, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    et_manager_->DropPartitionTable(1704280045000 + iot_interval_*2, 0, false, err_info);
    ASSERT_EQ(err_info.errcode, 0);

    et_manager_->DropPartitionTable(1704280045000, 1, false, err_info);
    ASSERT_EQ(err_info.errmsg, "");

    et_manager_->GetPartitionTable(1704280045000, 0, err_info);
    ASSERT_LT(err_info.errcode, 0);
    err_info.clear();
  }

  et_manager_->DropAll(false, err_info);
  ASSERT_EQ(err_info.errmsg, "");
}

TEST_F(TestEntityTableManager, partition) {
  ErrorInfo err_info;
  timestamp64 p1_time = 3600;
  {
    // create ubgroup SubGroupTable(0. 1. 2)
    TsSubEntityGroup* g1_bt = et_manager_->CreateSubGroup(0, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(g1_bt->GetID(), 0);
    // et_manager_->ReleasePartitionTable(g1_bt);
    TsTimePartition* sg1_bt = et_manager_->CreatePartitionTable(p1_time, 0, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(sg1_bt->getSchemaInfoIncludeDropped().size(), 3);
    et_manager_->ReleasePartitionTable(sg1_bt);

    TsSubEntityGroup* g2_bt = et_manager_->CreateSubGroup(1, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(g2_bt->GetID(), 1);
    // et_manager_->ReleasePartitionTable(g2_bt);
    TsTimePartition* sg2_bt = et_manager_->CreatePartitionTable(p1_time, 1, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(sg2_bt->getSchemaInfoIncludeDropped().size(), 3);
    et_manager_->ReleasePartitionTable(sg2_bt);

    TsSubEntityGroup* g3_bt = et_manager_->CreateSubGroup(2, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(g3_bt->GetID(), 2);
    // et_manager_->ReleasePartitionTable(g3_bt);
    TsTimePartition* sg3_bt = et_manager_->CreatePartitionTable(p1_time, 2, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(sg3_bt->getSchemaInfoIncludeDropped().size(), 3);
    et_manager_->ReleasePartitionTable(sg3_bt);

    // recreate group1 failed
    TsTimePartition* g2_bt2 = et_manager_->CreatePartitionTable(p1_time, 1, err_info);
    ASSERT_LT(err_info.errcode, 0);
    ASSERT_EQ(g2_bt2, nullptr);
    err_info.clear();
  }
  {
    timestamp64 p2_time = 7200;
    // SubGroup1 create SubGroupTable, partition 7200
    TsTimePartition* g2_bt = et_manager_->CreatePartitionTable(p2_time, 1, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(g2_bt->getSchemaInfoIncludeDropped().size(), 3);
    et_manager_->ReleasePartitionTable(g2_bt);
  }
}

/* partition table cache test
 *
*/
TEST_F(TestEntityTableManager, cache) {
  ErrorInfo err_info;
  timestamp64 p1_time = iot_interval_;
  SubGroupID group_id = 1;
  TsSubEntityGroup* g1_bt = et_manager_->CreateSubGroup(group_id, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  ASSERT_EQ(g1_bt->GetID(), group_id);
  {
    TsTimePartition* tbl1 = et_manager_->CreatePartitionTable(p1_time, group_id, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ReleaseTable(tbl1);
  }
  {
    TsTimePartition* table1 = et_manager_->GetPartitionTable(p1_time, group_id, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    TsTimePartition* table2 = et_manager_->GetPartitionTable(p1_time, group_id, err_info);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ((intptr_t)table1, (intptr_t)table2);
    ASSERT_EQ(table1->refCount(), 3);
    ReleaseTable(table1);
    ReleaseTable(table2);
    ASSERT_EQ(table1->refCount(), 1);
  }
  {
    intptr_t p1_mem = 0 ;
    {
      TsTimePartition* p1_tbl = et_manager_->GetPartitionTable(p1_time, group_id, err_info);
      p1_mem = (intptr_t) p1_tbl;
      ReleaseTable(p1_tbl);
    }

    timestamp64 p_time = p1_time + iot_interval_;
    for (int i = 0 ; i < 20 ; i++) {
      TsTimePartition* m_table = et_manager_->CreatePartitionTable(p_time, group_id, err_info);
      ASSERT_EQ(err_info.errmsg, "");
      ReleaseTable(m_table);
      p_time += iot_interval_;
    }
    {
      TsTimePartition* tmp = new TsTimePartition(root_bt_manager_, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP); // 测试防止分区MMapMetricsTable使用相同内存地址
      // Test to prevent partition MMapMetricsTable from using the same memory address
      TsTimePartition* p1_tbl = et_manager_->GetPartitionTable(p1_time, group_id, err_info);
      ASSERT_NE(p1_mem, (intptr_t) p1_tbl);
      ReleaseTable(p1_tbl);
      delete tmp;
    }
  }
  et_manager_->DropAll(false ,err_info);
  ASSERT_EQ(err_info.errmsg, "");
}

TEST_F(TestEntityTableManager, entityid) {
  ErrorInfo err_info;
  for (int i = 0; i < 1010; i++) {
    string tag = "p_tag_" + std::to_string(i);
    uint64_t tag_hash = GetConsistentHashId(tag.data(), tag.size(), g_testcase_hash_num);
    SubGroupID group_id;
    EntityID entity_id;
    int err_code = et_manager_->AllocateEntity(tag, tag_hash, &group_id, &entity_id);
    if (err_code < 0){
      ASSERT_GE(err_code, 0);
    }
    ASSERT_GE(err_code, 0);
    ASSERT_GE(group_id, 1);
    ASSERT_GE(entity_id, 1);
    TsSubEntityGroup* gp_bt = et_manager_->GetSubGroup(group_id, err_info, false);
    ASSERT_EQ(err_info.errmsg, "");
    ASSERT_EQ(gp_bt->GetID(), group_id);
  }
}
