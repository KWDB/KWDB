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

const string db_name_ = "./max_ts_table";     // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTsTableMaxTS : public testing::Test {
 public:
  TsTable *table_{nullptr};
  KTableKey table_id_ = 10086;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  std::vector<AttributeInfo> metric_schema_;
  std::vector<TagInfo> tag_schema_;
  TestTsTableMaxTS() {
    setenv("KW_HOME", db_name_.c_str(), 1);
    setenv("KW_IOT_INTERVAL", "36000", 1);
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    table_ = new TsTableImpl(ctx_, db_name_, 10086);
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta);
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo attr_info;
      KStatus s = TsEntityGroup::GetColAttributeInfo(ctx_, col, attr_info, i == 0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (attr_info.isAttrType(COL_GENERAL_TAG) || attr_info.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema_.push_back(std::move(TagInfo{col.column_id(), attr_info.type,
                                              static_cast<uint32_t>(attr_info.length), 0,
                                              static_cast<uint32_t>(attr_info.size),
                                              attr_info.isAttrType(COL_PRIMARY_TAG) ? PRIMARY_TAG : GENERAL_TAG,
                                              attr_info.flag}));
      } else {
        metric_schema_.push_back(std::move(attr_info));
      }
    }
    auto s = table_->Create(ctx_, metric_schema_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> table_range;
    s = table_->CreateEntityGroup(ctx_, {1, 0}, tag_schema_, &table_range);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestTsTableMaxTS() {
    delete table_;
    table_ = nullptr;
    System("rm -rf " + db_name_ + "/10086");
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  void RestartTable() {
    delete table_;
    table_ = new TsTableImpl(ctx_, db_name_, 10086);
    ErrorInfo err_info;
    std::unordered_map<k_uint64, int8_t> range_groups;
    range_groups[1] = 0;
    auto s = table_->Init(ctx_, range_groups, err_info);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  void ConstructRoachpbTable(roachpb::CreateTsTable* meta) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id_);
    table->set_table_name("table_" + std::to_string(table_id_));
    meta->set_allocated_ts_table(table);

    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type(roachpb::DataType::TIMESTAMP);
    column->set_storage_len(8);
    column->set_column_id(1);
    column->set_name("k_timestamp");  // first column name: k_timestamp
  
    column = meta->mutable_k_column()->Add();
    column->set_storage_type(roachpb::DataType::TIMESTAMP);
    column->set_storage_len(8);
    column->set_column_id(1);
    column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
    column->set_name("tag_1");
  }

  void GenPayloadData(KTimestamp primary_tag, KTimestamp start_ts, int count, TSSlice* payload, int inc_step = 1) {
    PayloadBuilder payload_gen(tag_schema_, metric_schema_);
    // tag values are primarytag 
    for (int i = 0; i < tag_schema_.size(); i++) {
      payload_gen.SetTagValue(i, reinterpret_cast<char*>(&primary_tag), sizeof(KTimestamp));
    }
    payload_gen.SetDataRows(count);

    for (size_t i = 0; i < metric_schema_.size(); i++) {
      for (size_t j = 0; j < count; j++) {
        KTimestamp col_value = start_ts + j * inc_step;
        payload_gen.SetColumnValue(j, i, reinterpret_cast<char*>(&col_value), sizeof(KTimestamp));
      }
    }
    auto ret = payload_gen.Build(payload);
    EXPECT_TRUE(ret);
  }
};

// create and drop
TEST_F(TestTsTableMaxTS, empty) {
  EXPECT_TRUE(table_->IsExist());
}

TEST_F(TestTsTableMaxTS, InsertOneRecord) {
  uint64_t primary_key = 123;
  timestamp64 start_ts = 8640000;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, 1, &payload);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result;
  auto s = table_->PutData(ctx_, 1, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
  EXPECT_EQ(s, KStatus::SUCCESS);
  free(payload.data);
  EntityResultIndex entity_id;
  s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, 1);
}

TEST_F(TestTsTableMaxTS, InsertManyTags) {
  uint64_t primary_key = 123;
  uint32_t entity_num = 4;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result;
  for (size_t i = 0; i < entity_num; i++) {
    TSSlice payload;
    GenPayloadData(primary_key + i, start_ts - i, 1, &payload);
    auto s = table_->PutData(ctx_, 1, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
    EXPECT_EQ(s, KStatus::SUCCESS);
    free(payload.data);
  }
  EntityResultIndex entity_id;
  auto s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, 1);
}

TEST_F(TestTsTableMaxTS, InsertManyTags1) {
  uint64_t primary_key = 123;
  uint32_t entity_num = 4;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result;
  for (size_t i = 0; i < entity_num; i++) {
    TSSlice payload;
    GenPayloadData(primary_key + i, start_ts + i, 1, &payload);
    auto s = table_->PutData(ctx_, 1, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
    EXPECT_EQ(s, KStatus::SUCCESS);
    free(payload.data);
  }
  EntityResultIndex entity_id;
  auto s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, entity_num);
}

TEST_F(TestTsTableMaxTS, restart) {
  uint64_t primary_key = 123;
  uint32_t entity_num = 4;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result;
  for (size_t i = 0; i < entity_num; i++) {
    TSSlice payload;
    GenPayloadData(primary_key + i, start_ts + i, 1, &payload);
    auto s = table_->PutData(ctx_, 1, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
    EXPECT_EQ(s, KStatus::SUCCESS);
    free(payload.data);
  }
  EntityResultIndex entity_id;
  auto s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, entity_num);
  RestartTable();
  s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, entity_num);
}

TEST_F(TestTsTableMaxTS, deleteSomeData) {
  uint64_t primary_key = 123;
  uint32_t entity_num = 4;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result;
  for (size_t i = 0; i < entity_num; i++) {
    TSSlice payload;
    GenPayloadData(primary_key + i, start_ts + i, 1, &payload);
    auto s = table_->PutData(ctx_, 1, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
    EXPECT_EQ(s, KStatus::SUCCESS);
    free(payload.data);
  }
  EntityResultIndex entity_id;
  auto s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, entity_num);

  uint64_t del_primary_key = 123 + entity_num - 1;
  std::string primary_keys_str(reinterpret_cast<char*>(&del_primary_key), 8);
  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back(KwTsSpan{INT64_MIN, INT64_MAX});
  uint64_t count;
  s = table_->DeleteData(ctx_, 1, primary_keys_str, ts_spans, &count, 0);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(count, 1);

  s = table_->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_EQ(entity_id.subGroupId, 1);
  EXPECT_EQ(entity_id.entityId, entity_num - 1);
}
