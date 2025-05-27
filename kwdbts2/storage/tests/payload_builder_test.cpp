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
#include "payload_builder.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_payload_builder";  // NOLINT
const std::string TestBigTableInstance::kw_home_ = kDbPath;    // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";     // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTsPayloadBuilder : public TestBigTableInstance {
 public:
  std::vector<TagInfo> tag_schema_;
  std::vector<uint32_t> actual_cols_;
  std::vector<AttributeInfo> data_schema_;
  std::vector<uint32_t> hps_total_;
  TsTable *table_{nullptr};
  KTableKey table_id_ = 10086;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  TestTsPayloadBuilder() {
    string rm_path = "rm -rf " + kDbPath;
    system(rm_path.c_str());
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    table_ = new TsTable(ctx_, kDbPath, 10086);
    for (size_t i = 0; i <= g_testcase_hash_num; i++) {
      hps_total_.push_back(i);
    }
  }

  ~TestTsPayloadBuilder() {
    if (table_ != nullptr) {
      table_->DropAll(ctx_);
      delete table_;
      table_ = nullptr;
    }
  }

  std::vector<TagInfo> GetTagSchema(roachpb::CreateTsTable* meta) {
    std::vector<TagInfo> tag_schema;
    KStatus s;
    for (int i = 0 ; i < meta->k_column_size() ; i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
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
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
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
    s = table_->GetTagSchema(ctx_, range_groups[0], &tag_schema_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = table_->GetDataSchemaExcludeDropped(ctx_, &data_schema_);
    for (auto col : data_schema_) {
      actual_cols_.push_back(actual_cols_.size());
    }
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  void ConstructRoachpbTable(roachpb::CreateTsTable* meta, int clumn_num, int tag_num) {
    roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id_);
    table->set_table_name("table_" + std::to_string(table_id_));
    table->set_hash_num(g_testcase_hash_num);
    meta->set_allocated_ts_table(table);

    for (int i = 0; i < clumn_num; i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type(roachpb::DataType::TIMESTAMP);
      column->set_storage_len(8);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // The first column is the timestamp, and name is: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    // add tag columns
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

  void ConstructRoachpbTable2(roachpb::CreateTsTable* meta, int clumn_num, int tag_num, bool pritag_var) {
    roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id_);
    table->set_table_name("table_" + std::to_string(table_id_));
    table->set_hash_num(g_testcase_hash_num);
    meta->set_allocated_ts_table(table);

    for (int i = 0; i < clumn_num; i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type(roachpb::DataType::TIMESTAMP);
      column->set_storage_len(8);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // The first column is the timestamp, and name is: k_timestamp
      } else {
        if (i % 2 == 0) {
          column->set_storage_type(roachpb::DataType::VARCHAR);
          column->set_storage_len(20);
        }
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    // add tag columns
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
      if (i % 2 == 0 && (i != 0 || pritag_var)) {
        column->set_storage_type(roachpb::DataType::VARCHAR);
        column->set_storage_len(20);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }
  }
  TSSlice GenPayload(KTimestamp primary_tag, int data_count) {
    PayloadBuilder pay_build(tag_schema_, data_schema_);
    for (size_t i = 0; i < tag_schema_.size(); i++) {
      KTimestamp cur_value = primary_tag + i;
      if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
        string aa = intToString(cur_value);
        pay_build.SetTagValue(i, aa.data(), aa.length());
      } else {
        pay_build.SetTagValue(i,
          reinterpret_cast<char*>(&cur_value), sizeof(KTimestamp));
      }
    }
    pay_build.SetDataRows(data_count);
    for (size_t j = 0; j < data_count; j++) {
      for (size_t i = 0; i < data_schema_.size(); i++) {
        KTimestamp cur_value = primary_tag + i + j;
        if (data_schema_[i].type == DATATYPE::VARSTRING) {
          string aa = intToString(cur_value);
          pay_build.SetColumnValue(j, i, aa.data(), aa.length());
        } else {
          pay_build.SetColumnValue(j, i,
            reinterpret_cast<char*>(&cur_value), sizeof(KTimestamp));
        }
      }
    }
    TSSlice payload_slice;
    bool s = pay_build.Build(&payload_slice, g_testcase_hash_num);
    EXPECT_EQ(s, true);
    return payload_slice;
  }
};

// Create and delete empty tables
TEST_F(TestTsPayloadBuilder, empty) {
}

// Test simple data types
TEST_F(TestTsPayloadBuilder, create) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, 100, 4);
  std::vector<RangeGroup> ranges{{101, 0}};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());
  int count = 100;
  KTimestamp primary_tag = 10010;
  TSSlice payload_slice = GenPayload(primary_tag, count);
  Payload payload(data_schema_, actual_cols_, payload_slice);
  for (size_t i = 0; i < count; i++) {
    for (size_t j = 0; j < data_schema_.size(); j++) {
      ASSERT_EQ(KTimestamp(payload.GetColumnAddr(i, j)), primary_tag + i + j);
    }
  }
  free(payload_slice.data);
}

// Test data with variable length type fields
TEST_F(TestTsPayloadBuilder, create_1) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable2(&meta, 100, 2, false);
  std::vector<RangeGroup> ranges{{101, 0}};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());
  int count = 10;
  KTimestamp primary_tag = 10086;
  TSSlice payload_slice = GenPayload(primary_tag, count);
  Payload payload(data_schema_, actual_cols_, payload_slice);
  // checkout data in payload is ok.
  for (size_t i = 0; i < count; i++) {
    for (size_t j = 0; j < data_schema_.size(); j++) {
      if (data_schema_[j].type == DATATYPE::VARSTRING) {
        char* addr = payload.GetVarColumnAddr(i, j);
        string aa = intToString(primary_tag + i + j);
        ASSERT_EQ(aa.length(), KUint16(addr));
        ASSERT_EQ(aa, string(addr + 2, aa.length()));
      } else {
        ASSERT_EQ(KTimestamp(payload.GetColumnAddr(i, j)), primary_tag + i + j);
      }
    }
  }
  free(payload_slice.data);
}

// Test tag data with variable length type fields
TEST_F(TestTsPayloadBuilder, create_2) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable2(&meta, 1, 100, false);
  std::vector<RangeGroup> ranges{{101, 0}};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());

  int count = 10;
  KTimestamp primary_tag = 9876500;
  TSSlice payload_slice = GenPayload(primary_tag, count);

  std::shared_ptr<TsEntityGroup> entity_grp;
  KStatus s = table_->GetEntityGroup(ctx_, 101, &entity_grp);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = entity_grp->PutData(ctx_, payload_slice);
  EXPECT_EQ(s, KStatus::SUCCESS);
  free(payload_slice.data);

  TagIterator* iter;
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tag_schema_.size(); i++) {
    scan_tags.push_back(i);
  }
  s = table_->GetTagIterator(ctx_, scan_tags, hps_total_, &iter, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ResultSet rs{(k_uint32) scan_tags.size()};
  std::vector<EntityResultIndex> entity_id_list;
  k_uint32 count_1 = 0;
  s = iter->Next(&entity_id_list, &rs, &count_1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(count_1, 1);
  for (size_t i = 0; i < tag_schema_.size(); i++) {
    KTimestamp cur_value = primary_tag + i;
    const Batch* batch = rs.data[i][0];
    if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING ||
        tag_schema_[i].m_data_type == DATATYPE::VARBINARY) {
      string value_str = intToString(cur_value);
      EXPECT_EQ(batch->getVarColDataLen(0), value_str.length() + 1);
      EXPECT_EQ(cur_value, atoi(reinterpret_cast<char*>(batch->getVarColData(0))));
    } else {
      char* col_addr = reinterpret_cast<char*>(batch->mem);
      if (!tag_schema_[i].isPrimaryTag()) {
        col_addr = col_addr + k_per_null_bitmap_size;
      }
      EXPECT_EQ(KTimestamp(col_addr), cur_value);
    }
  }
  delete iter;
}

// Test primary tag data with variable length type fields
TEST_F(TestTsPayloadBuilder, create_3) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable2(&meta, 1, 1, true);
  std::vector<RangeGroup> ranges{{101, 0}};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());

  int count = 10;
  KTimestamp primary_tag = 9800;
  TSSlice payload_slice = GenPayload(primary_tag, count);

  std::shared_ptr<TsEntityGroup> entity_grp;
  KStatus s = table_->GetEntityGroup(ctx_, 101, &entity_grp);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = entity_grp->PutData(ctx_, payload_slice);
  EXPECT_EQ(s, KStatus::SUCCESS);
  free(payload_slice.data);

  TagIterator* iter;
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tag_schema_.size(); i++) {
    scan_tags.push_back(i);
  }
  
  s = table_->GetTagIterator(ctx_, scan_tags,hps_total_, &iter, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ResultSet rs{(k_uint32) scan_tags.size()};
  std::vector<EntityResultIndex> entity_id_list;
  k_uint32 count_1 = 0;
  s = iter->Next(&entity_id_list, &rs, &count_1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(count_1, 1);
  for (size_t i = 0; i < tag_schema_.size(); i++) {
    KTimestamp cur_value = primary_tag + i;
    string value_str = intToString(cur_value);
    const Batch* batch = rs.data[i][0];
    if (tag_schema_[i].isPrimaryTag()) {
      if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
        // primary key store as  char[xxx]
        string a(reinterpret_cast<char*>(batch->mem));
        EXPECT_EQ(a, value_str);
      } else {
        EXPECT_EQ(KTimestamp(batch->mem), cur_value);
      }
    } else {
      if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
        EXPECT_EQ(batch->getVarColDataLen(0), value_str.length() + 1);
        EXPECT_EQ(cur_value, atoi(reinterpret_cast<char*>(batch->getVarColData(0))));
      } else {
        void* col_addr = batch->mem + k_per_null_bitmap_size;
        EXPECT_EQ(KTimestamp(col_addr), cur_value);
      }
    }
  }
  delete iter;
}

// Test all columns to include fields of variable length type
TEST_F(TestTsPayloadBuilder, create_4) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable2(&meta, 100, 100, true);
  std::vector<RangeGroup> ranges{{101, 0}};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());

  int count = 10;
  KTimestamp primary_tag = 12345678;
  TSSlice payload_slice = GenPayload(primary_tag, count);

  std::shared_ptr<TsEntityGroup> entity_grp;
  KStatus s = table_->GetEntityGroup(ctx_, 101, &entity_grp);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = entity_grp->PutData(ctx_, payload_slice);
  EXPECT_EQ(s, KStatus::SUCCESS);
  free(payload_slice.data);

  TagIterator* iter;
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tag_schema_.size(); i++) {
    scan_tags.push_back(i);
  }
  s = table_->GetTagIterator(ctx_, scan_tags,hps_total_, &iter, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ResultSet rs{(k_uint32) scan_tags.size()};
  std::vector<EntityResultIndex> entity_id_list;
  k_uint32 count_1 = 0;
  s = iter->Next(&entity_id_list, &rs, &count_1);
  EXPECT_EQ(entity_id_list.size(), 1);
  EXPECT_EQ(count_1, 1);
  for (size_t i = 0; i < tag_schema_.size(); i++) {
    KTimestamp cur_value = primary_tag + i;
    string value_str = intToString(cur_value);
    const Batch* batch = rs.data[i][0];
    if (tag_schema_[i].isPrimaryTag()) {
      if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
        // primary key store as  char[xxx]
        string a(reinterpret_cast<char*>(batch->mem));
        EXPECT_EQ(a, value_str);
      } else {
        EXPECT_EQ(KTimestamp(batch->mem), cur_value);
      }
    } else {
      if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
        EXPECT_EQ(batch->getVarColDataLen(0), value_str.length() + 1);
        EXPECT_EQ(cur_value, atoi(reinterpret_cast<char*>(batch->getVarColData(0))));
      } else {
        void* col_addr = batch->mem + k_per_null_bitmap_size;
        EXPECT_EQ(KTimestamp(col_addr), cur_value);
      }
    }
  }
  delete iter;
}

// Test all columns to include fields of variable length type, multiple insertions
TEST_F(TestTsPayloadBuilder, create_5) {
  EXPECT_FALSE(table_->IsExist());
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable2(&meta, 100, 100, true);
  std::vector<RangeGroup> ranges{{101, 0}};
  Create(&meta, ranges);
  EXPECT_TRUE(table_->IsExist());


  int count = 10;
  KTimestamp primary_tag = 12345678;
  int entity_num = 1;
  std::shared_ptr<TsEntityGroup> entity_grp;
  KStatus s = table_->GetEntityGroup(ctx_, 101, &entity_grp);
  EXPECT_EQ(s, KStatus::SUCCESS);
  for (size_t i = 0; i < entity_num; i++) {
    TSSlice payload_slice = GenPayload(primary_tag, count);
    s = entity_grp->PutData(ctx_, payload_slice);
    EXPECT_EQ(s, KStatus::SUCCESS);
    free(payload_slice.data);
  }

  TagIterator* iter;
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tag_schema_.size(); i++) {
    scan_tags.push_back(i);
  }
  s = table_->GetTagIterator(ctx_, scan_tags,hps_total_, &iter, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ResultSet rs{(k_uint32) scan_tags.size()};
  std::vector<EntityResultIndex> entity_id_list;
  k_uint32 count_1 = 0;
  s = iter->Next(&entity_id_list, &rs, &count_1);
  EXPECT_EQ(entity_id_list.size(), entity_num);
  EXPECT_EQ(count_1, entity_num);
  for (size_t i = 0; i < tag_schema_.size(); i++) {
    KTimestamp cur_value = primary_tag + i;
    string value_str = intToString(cur_value);
    const Batch* batch = rs.data[i][0];
    EXPECT_EQ(batch->count, entity_num);
    char* batch_mem = reinterpret_cast<char*>(batch->mem);
    for (size_t j = 0; j < entity_num; j++) {
      if (tag_schema_[i].isPrimaryTag()) {
        if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
          // primary key store as  char[xxx]
          string a(batch_mem + j * (tag_schema_[i].m_size + 8));
          EXPECT_EQ(a, value_str);
        } else {
          EXPECT_EQ(KTimestamp(batch_mem + j * 8), cur_value);
        }
      } else {
        if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
          EXPECT_EQ(batch->getVarColDataLen(0), value_str.length() + 1);
          EXPECT_EQ(cur_value, atoi(reinterpret_cast<char*>(batch->getVarColData(0))));
        } else {
          void* col_addr = batch_mem + k_per_null_bitmap_size + j * (8 + k_per_null_bitmap_size);
          EXPECT_EQ(KTimestamp(col_addr), cur_value);
        }
      }
    }
  }
  delete iter;
}
