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
#include <thread>

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_ts_nocore";
const std::string TestBigTableInstance::kw_home_ = kDbPath;
const string TestBigTableInstance::db_name_ = "tsdb";   // database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

bool ReachMetaMaxBlock(BLOCK_ID cur_block_id) {
  return cur_block_id >= 3;
}

class TestTsBLockItemMaxNoCore : public TestBigTableInstance {
 public:
  TsTable *table_;
  KTableKey table_id_ = 10086;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  uint64_t leader_group_id_ = 123;
  std::shared_ptr<TsEntityGroup> entity_group_leader_;
  TestTsBLockItemMaxNoCore() {
    string cmd = "rm -rf " + kw_home_ + "/" + intToString(table_id_);
    system(cmd.c_str());
    ctx_ = &context_;
    KStatus s = InitServerKWDBContext(ctx_);
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, 1, 1);
    std::vector<RangeGroup> ranges;
    ranges.push_back({leader_group_id_, 0});
    CreateTable(&meta, ranges);
    s = table_->GetEntityGroup(ctx_, leader_group_id_, &entity_group_leader_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  void CreateTable(roachpb::CreateTsTable* meta, const std::vector<RangeGroup>& range_groups) {
    table_ = new TsTable(ctx_, kDbPath, 10086);
    std::unordered_map<uint64_t, int8_t> ranges;
    KStatus s = table_->Init(ctx_, ranges);
    s = table_->DropAll(ctx_);
    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> metric_schema;
    for (int i = 0 ; i < meta->k_column_size() ; i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.push_back(TagInfo{col.column_id(), col_var.type,
                                               static_cast<uint32_t>(col_var.length),
                                               0, static_cast<uint32_t>(col_var.length),
                                               static_cast<TagType>(col_var.col_flag)});
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

  void GenPayloadData(KTimestamp primary_tag, KTimestamp start_ts, int count, TSSlice *payload, int inc_step = 1) {
    std::vector<AttributeInfo> data_schema;
    KStatus s = table_->GetDataSchemaExcludeDropped(ctx_, &data_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = entity_group_leader_->GetCurrentTagSchemaExcludeDropped(&tag_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    int header_size = Payload::header_size_;
    k_uint32 header_len = header_size;
    k_int16 primary_len_len = 2;
    // primary tag  timestamp type is 8.
    k_int32 primary_tag_len = 8;
    k_int32 tag_len_len = 4;
    // tag value    tag bitmap
    k_int32 tag_value_len_bitmap = (tag_schema.size() + 7) / 8;
    k_int32 tag_value_len =  + tag_schema.size() * 8 + tag_value_len_bitmap;
    // data part
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    // all column type is timestamp 
    k_int32 data_len = (count * 8 + bitmap_len) * data_schema.size() + count * 8;
    k_uint32 payload_length = header_len + primary_len_len + primary_tag_len
        + tag_len_len + tag_value_len + data_len_len + data_len;

    char* value = new char[payload_length];
    memset(value, 0, payload_length);
    char* value_idx = value;
    // header 
    KInt32(value_idx + Payload::row_num_offset_) = count;
    KUint32(value_idx + Payload::ts_version_offset_) = 1;
    value_idx += header_len;
    // set primary tag
    KInt16(value_idx) = primary_tag_len;
    value_idx += primary_len_len;
    KTimestamp(value_idx) = primary_tag;
    value_idx += primary_tag_len;
    // set tag
    KInt32(value_idx) = tag_value_len;
    value_idx += tag_len_len;
    value_idx += tag_value_len_bitmap;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      KTimestamp(value_idx) = primary_tag + i;
      value_idx += 8;
    }
    // set data_len_len
    KInt32(value_idx) = data_len;
    value_idx += data_len_len;
    for (size_t i = 0; i < data_schema.size(); i++) {
      value_idx += bitmap_len;
      for (size_t j = 0; j < count; j++) {
        KTimestamp(value_idx) = start_ts + j * inc_step;
        if (i == 0) {
          value_idx += sizeof(TimeStamp64LSN);
        } else {
          value_idx += 8;
        }
      }
    }
    *payload = {value, payload_length};
  }

  ~TestTsBLockItemMaxNoCore() {
    table_->DropAll(ctx_);
    delete table_;
  }

  int GetIterRows(TsStorageIterator* iter1, k_uint32 scan_cols_size) {
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
    return (total_rows);
  }

  void ConstructRoachpbTable(roachpb::CreateTsTable* meta, int clumn_num, int tag_num) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id_);
    table->set_table_name("table_" + std::to_string(table_id_));
    meta->set_allocated_ts_table(table);

    for (int i = 0; i < clumn_num; i++) {
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

// multi-insert to same table
TEST_F(TestTsBLockItemMaxNoCore, mulitiInsert) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scan_tags;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res;
  uint32_t count;
  KStatus s;
  KTimestamp primary_key = 8009001;
  KTimestamp start_ts = 1000000;
  int batch_times = 300;
  int thread_num = 2;
  int batch_count = 12;
  TSSlice payload;
  GenPayloadData(primary_key, start_ts, batch_count, &payload);

  std::vector<std::thread> threads;
  for (size_t i = 0; i < thread_num; i++) {
    threads.push_back(std::thread([=] {
      kwdbContext_t ctx_1;
      KStatus s = InitServerKWDBContext(&ctx_1);
      for (size_t i = 0; i < batch_times; i++) {
        uint16_t inc_entity_cnt;
        uint32_t inc_unordered_cnt;
        DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
        s = entity_group_leader_->PutData(&ctx_1, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
        if (s != KStatus::SUCCESS) {
          std::cout << "put data error." << i << std::endl;
        }
      }
    }));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  delete[] payload.data;
  primary_tags.push_back(&primary_key);
  scan_tags.push_back(0);
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  s = entity_group_leader_->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &entity_id_list, &res, &count, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);

  // check result
  KwTsSpan ts_span = {start_ts, start_ts + batch_times * batch_count};
  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back(ts_span);
  std::vector<k_uint32> scan_cols = {0};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(entity_group_leader_->GetIterator(ctx_, entity_id_list[0].subGroupId, {entity_id_list[0].entityId}, ts_spans, ts_type,
    scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_leader_, {}, false, false), KStatus::SUCCESS);
  int iter_count = GetIterRows(iter1, scan_cols.size());
  EXPECT_TRUE(iter_count % batch_count == 0);
  delete iter1;
}
