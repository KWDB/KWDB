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

#include <thread>
#include "test_util.h"
#include "ts_table.h"
#include "payload_builder.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_ts_entitygroup";
const std::string TestBigTableInstance::kw_home_ = kDbPath;
const string TestBigTableInstance::db_name_ = "tsdb";
const uint64_t TestBigTableInstance::iot_interval_ = 3600;
const int kThreadNum = 2;

class TestTsEntityGroup : public TestBigTableInstance {
 public:
  TsTable* table_;
  KTableKey table_id_ = 10086;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  uint64_t leader_group_id_ = 123;
  uint64_t follower_group_id_ = 124;
  uint64_t other_group_id_ = 125;
  std::shared_ptr<TsEntityGroup> entity_group_leader_;
  std::shared_ptr<TsEntityGroup> entity_group_follower_;
  std::shared_ptr<TsEntityGroup> entity_group_other_;

  TestTsEntityGroup() {
    string cmd = "rm -rf " + kw_home_ + "/" + intToString(table_id_);
    system(cmd.c_str());
    ctx_ = &context_;
    KStatus s = InitServerKWDBContext(ctx_);
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, 5, 3);
    std::vector<RangeGroup> ranges;
    ranges.push_back({leader_group_id_, 0});
    ranges.push_back({follower_group_id_, 1});
    ranges.push_back({other_group_id_, 8});
    CreateTable(&meta, ranges);

    s = table_->GetEntityGroup(ctx_, leader_group_id_, &entity_group_leader_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = table_->GetEntityGroup(ctx_, follower_group_id_, &entity_group_follower_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = table_->GetEntityGroup(ctx_, other_group_id_, &entity_group_other_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestTsEntityGroup() {
    entity_group_leader_.reset();
    entity_group_follower_.reset();
    entity_group_other_.reset();
    table_->DropAll(ctx_);
    delete table_;
  }

  void CreateTable(roachpb::CreateTsTable* meta, const std::vector<RangeGroup>& range_groups) {
    table_ = new TsTable(ctx_, kDbPath, 10086);
    std::unordered_map<uint64_t, int8_t> ranges;
    KStatus s = table_->Init(ctx_, ranges);
    s = table_->DropAll(ctx_);
    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> metric_schema;
    for (int i = 0; i < meta->k_column_size(); i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                               static_cast<uint32_t>(col_var.length),
                                               0, static_cast<uint32_t>(col_var.length),
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

  void GenPayloadData(KTimestamp primary_tag, KTimestamp start_ts, int count, TSSlice* payload, int inc_step = 1) {
    std::vector<AttributeInfo> data_schema;
    KStatus s = table_->GetDataSchemaExcludeDropped(ctx_, &data_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = entity_group_leader_->GetCurrentTagSchemaExcludeDropped(&tag_schema);
    PayloadBuilder payload_gen(tag_schema, data_schema);
    // tag values are primarytag 
    for (int i = 0; i < tag_schema.size(); i++) {
      payload_gen.SetTagValue(i, reinterpret_cast<char*>(&primary_tag), sizeof(KTimestamp));
    }
    payload_gen.SetDataRows(count);

    for (size_t i = 0; i < data_schema.size(); i++) {
      for (size_t j = 0; j < count; j++) {
        KTimestamp col_value = start_ts + j * inc_step;
        payload_gen.SetColumnValue(j, i,
                                   reinterpret_cast<char*>(&col_value), sizeof(KTimestamp));
      }
    }
    auto ret = payload_gen.Build(payload);
    EXPECT_TRUE(ret);
  }

  void ConstructRoachpbTable(roachpb::CreateTsTable* meta, int column_num, int tag_num,
                             uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable* table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id_);
    table->set_table_name("table_" + std::to_string(table_id_));
    table->set_partition_interval(partition_interval);
    meta->set_allocated_ts_table(table);

    for (int i = 0; i < column_num; i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type(roachpb::DataType::TIMESTAMP);
      column->set_storage_len(8);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    for (int i = 0; i < tag_num; i++) {
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

  bool CheckEqualCol(ResultSet& rs, int row, int col, KTimestamp expect) {  // NOLINT
    char* tmp = reinterpret_cast<char*>(rs.data[col][0]->mem);
    char* col_p = tmp + row * (col != 0 ? 8 : 16);
    return expect == KTimestamp(col_p);
  }

  int CheckIterRows(TsStorageIterator* iter1, k_int32 expect_rows, k_uint32 scan_cols_size) {
    ResultSet res{scan_cols_size};
    k_uint32 ret_cnt;
    int total_rows = 0;
    bool is_finished = false;
    do {
      KStatus s = iter1->Next(&res, &ret_cnt, &is_finished);
      if (s != KStatus::SUCCESS) {
        return -1;
      }
      total_rows += ret_cnt;
    } while (!is_finished);
    if (total_rows != expect_rows) {
      std::cout << "--------total rows:" << total_rows << ", expect: " << expect_rows << std::endl;
    }
    return (expect_rows - total_rows);
  }
};

TEST_F(TestTsEntityGroup, empty) {
  EXPECT_TRUE(table_->IsExist());
}

// insert some data
TEST_F(TestTsEntityGroup, create) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res{(k_uint32) scan_tags.size()};
  uint32_t count;
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  KStatus s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id_list.size(), 0);

  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, 3, &payload);
  EXPECT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = entity_group_leader_->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = entity_group_follower_->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = entity_group_other_->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  EXPECT_EQ(s, KStatus::SUCCESS);
  free(payload.data);

  primary_tags.push_back(&primary_key);
  res.clear();
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem), primary_key);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + 3};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  res.setColumnNum(scan_cols.size());
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                              {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);

  k_uint32 ret_cnt;
  res.clear();
  int total_rows = 0;
  bool is_finished = false;
  do {
    ASSERT_EQ(iter1->Next(&res, &ret_cnt, &is_finished), KStatus::SUCCESS);
    for (size_t i = 0; i < ret_cnt; i++) {
      ASSERT_TRUE(CheckEqualCol(res, i, 0, start_ts + total_rows + i));
      ASSERT_TRUE(CheckEqualCol(res, i, 1, start_ts + total_rows + i));
    }
    total_rows += ret_cnt;
  } while (!is_finished);
  EXPECT_EQ(total_rows, 3);
  delete iter1;

  ASSERT_EQ(entity_group_follower_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                                {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_follower_, {}, false, false), KStatus::SUCCESS);

  res.clear();
  total_rows = 0;
  is_finished = false;
  do {
    ASSERT_EQ(iter1->Next(&res, &ret_cnt, &is_finished), KStatus::SUCCESS);
    for (size_t i = 0; i < ret_cnt; i++) {
      ASSERT_TRUE(CheckEqualCol(res, i, 0, start_ts + total_rows + i));
    }
    total_rows += ret_cnt;
  } while (!is_finished);
  EXPECT_EQ(total_rows, 3);
  delete iter1;

  ASSERT_EQ(entity_group_other_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                             {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_other_, {}, false, false), KStatus::SUCCESS);

  res.clear();
  total_rows = 0;
  is_finished = false;
  do {
    ASSERT_EQ(iter1->Next(&res, &ret_cnt, &is_finished), KStatus::SUCCESS);
    for (size_t i = 0; i < ret_cnt; i++) {
      ASSERT_TRUE(CheckEqualCol(res, i, 0, start_ts + total_rows + i));
    }
    total_rows += ret_cnt;
  } while (!is_finished);
  EXPECT_EQ(total_rows, 3);
  delete iter1;
}

// insert data some times.
TEST_F(TestTsEntityGroup, InsertSometimes) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 30;
  int batch_count = 501;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload);
  for (size_t i = 0; i < batch_times; i++) {
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = entity_group_leader_->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result,kwdbts::DedupRule::KEEP);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  free(payload.data);
  primary_tags.push_back(&primary_key);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem), primary_key);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_times * batch_count};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                              {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count, scan_cols.size()));
  delete iter1;
}


// insert times , data cross partition
TEST_F(TestTsEntityGroup, InsertCrossPartitionSometimes) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 30;
  int batch_count = iot_interval_ + 2;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload, 10000);
  for (size_t i = 0; i < batch_times; i++) {
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = entity_group_leader_->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  free(payload.data);
  primary_tags.push_back(&primary_key);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem), primary_key);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_times * batch_count * 10000};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                              {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count, scan_cols.size()));
  delete iter1;
}

// multi-insert to same table
TEST_F(TestTsEntityGroup, mulitiInsert) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 30;
  int batch_count = 100;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload);

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreadNum; i++) {
    threads.push_back(std::thread([=] {
      kwdbContext_t ctx_1;
      KStatus s = InitServerKWDBContext(&ctx_1);
      for (size_t i = 0; i < batch_times; i++) {
        uint16_t inc_entity_cnt;
        uint32_t inc_unordered_cnt;
        DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
        s = entity_group_leader_->PutData(&ctx_1, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result,kwdbts::DedupRule::KEEP);
        if (s != KStatus::SUCCESS) {
          std::cout << "put data error." << i << std::endl;
        }
      }
    }));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  free(payload.data);
  primary_tags.push_back(&primary_key);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem), primary_key);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_times * batch_count};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                              {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count * kThreadNum, scan_cols.size()));
  delete iter1;
}

// multi-insert to different partition in same table
TEST_F(TestTsEntityGroup, mulitiInsertCrossPartition) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 30;
  int batch_count = iot_interval_ + 3;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload, 1000);

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreadNum; i++) {
    threads.push_back(std::thread([=] {
      kwdbContext_t ctx_1;
      KStatus s = InitServerKWDBContext(&ctx_1);
      for (size_t i = 0; i < batch_times; i++) {
        uint16_t inc_entity_cnt;
        uint32_t inc_unordered_cnt;
        DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
        s = entity_group_leader_->PutData(&ctx_1, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
        if (s != KStatus::SUCCESS) {
          std::cout << "put data error." << i << std::endl;
        }
      }
    }));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  free(payload.data);
  primary_tags.push_back(&primary_key);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem), primary_key);


  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_count * 1000};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                              {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count * kThreadNum, scan_cols.size()));
  delete iter1;
}

// multi-insert to differt tables.
TEST_F(TestTsEntityGroup, mulitiInsertMultiEntity) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 300;
  int batch_count = 104;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload);

  TSSlice payload1;
  GenPayloadData(primary_key + 1, start_ts, batch_count, &payload1);

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreadNum; i++) {
    threads.push_back(std::thread([=] {
      kwdbContext_t ctx_1;
      KStatus s = InitServerKWDBContext(&ctx_1);
      int ent_count = 0;
      int ent_count1 = 0;
      unsigned int random = 123;
      while (true) {
        if (ent_count >= batch_times && ent_count1 >= batch_times) {
          break;
        }
        TSSlice cur_payload = payload1;
        // insert to random table, exit after batch_times
        if (rand_r(&random) % 2 == 0 && ent_count < batch_times) {
          cur_payload = payload;
          ent_count++;
        } else {
          if (ent_count1 >= batch_times) {
            if (ent_count < batch_times) {
              cur_payload = payload;
              ent_count++;
            } else {
              break;
            }
          } else {
            ent_count1++;
          }
        }
        uint16_t inc_entity_cnt;
        uint32_t inc_unordered_cnt;
        DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
        s = entity_group_leader_->PutData(&ctx_1, cur_payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
        if (s != KStatus::SUCCESS) {
          std::cout << "put data error." << i << std::endl;
        }
      }
    }));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  free(payload.data);
  free(payload1.data);
  primary_tags.push_back(&primary_key);
  KTimestamp primary_key1 = primary_key + 1;
  primary_tags.push_back(&primary_key1);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2);
  EXPECT_EQ(entity_id_list.size(), 2);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem) + KTimestamp(entity_id_list[1].mem), primary_key + primary_key + 1);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_times * batch_count};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
            {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);

  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count * kThreadNum, scan_cols.size()));
  delete iter1;

  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[1].subGroupId, {entity_id_list[1].entityId},
            {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count * kThreadNum, scan_cols.size()));
  delete iter1;
}

// insert and delete data
TEST_F(TestTsEntityGroup, InsertAndDel) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 3;
  int batch_count = 502;

  uint64_t del_count;
  vector<DelRowSpan> del_rows;
  KwTsSpan del_ts_span{start_ts + 1, start_ts + 10};
  std::string p_tag((char*) (&primary_key), 8);  // NOLINT
  s = entity_group_leader_->DeleteData(ctx_, p_tag, 0, {del_ts_span}, &del_rows, &del_count, 0, false);
  ASSERT_EQ(s, KStatus::SUCCESS);


  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload);
  for (size_t i = 0; i < batch_times; i++) {
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = entity_group_leader_->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  free(payload.data);
  primary_tags.push_back(&primary_key);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(KTimestamp(entity_id_list[0].mem), primary_key);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_times * batch_count};
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
            {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * batch_count, scan_cols.size()));
  delete iter1;

  for (size_t i = 0; i < 3; i++) {
    s = entity_group_leader_->DeleteData(ctx_, p_tag, 0, {del_ts_span}, &del_rows, &del_count, 0, false);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (i == 0) {
      ASSERT_EQ(del_count, 10 * batch_times);
    } else {
      ASSERT_EQ(del_count, 0);
    }
  }
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
            {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  EXPECT_EQ(0, CheckIterRows(iter1, batch_times * (batch_count - 10), scan_cols.size()));
  delete iter1;
}
