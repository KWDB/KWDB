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

std::string kDbPath = "./test_db";  // NOLINT The current directory is the storage directory for the big table

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT database path
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

RangeGroup kTestRange{1, 0};
class TestEngine : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestEngine() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // Clean up file directory
    KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  }

  ~TestEngine() {
    // CLOSE engine
    // ts_engine_->Close();
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  // Store in header
  int row_num_ = 5;
};

TEST_F(TestEngine, entityidlist) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "tagiter1", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 3600;
  // insert value
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  int cnt = 2000;
  char* data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
    delete[] data_value;
  }

  // getentitylist
  timestamp64 *check_ts = new timestamp64[cnt/2+2];
  std::vector<void*> primary_tags;
  KTimestamp check_start_ts1 = 3600;
  for (int i= 0, k = 0; i < cnt ; i += 2, k++) {
      check_start_ts1 = start_ts1 + i * 100;
      check_ts[k] = check_start_ts1;
      primary_tags.emplace_back(reinterpret_cast<void*>(&check_ts[k]));
  }
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<uint32_t> scan_tags = {1,2};
  ResultSet res{(k_uint32) scan_tags.size()};
  uint32_t count;
  std::vector<uint64_t/*index_id*/> tags_index_id{};
  std::vector<void*> tags{};
  ASSERT_EQ(ts_table->GetEntityIdList(ctx_, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags,
                                      &entity_id_list, &res, &count, 1), KStatus::SUCCESS);
  ASSERT_EQ(count, primary_tags.size());
  for (int idx = 0; idx < primary_tags.size(); idx++) {
    ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
    ASSERT_EQ(entity_id_list[idx].entityGroupId, kTestRange.range_group_id);
    ASSERT_EQ(memcmp(primary_tags[idx], entity_id_list[idx].mem, sizeof(uint64_t)), 0);
  }
  // check tagcolumn
  k_uint32 tagval = 0;
  for (int tagidx = 0; tagidx < scan_tags.size(); tagidx++) {
    for (int i = 0; i < res.data[tagidx][0]->count; i++) {
      if (tagidx == 0) {
        memcpy(&tagval, res.data[tagidx][0]->mem + i*(sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size,
              sizeof(tagval));
        ASSERT_EQ(tagval, start_ts1+(i)*100);
      }else {
        // var
        char* rec_ptr = static_cast<char*>(res.data[tagidx][0]->getVarColData(i));
        ASSERT_EQ(rec_ptr, std::to_string(start_ts1+(i)*100));
      }
    }
  }
  delete []check_ts;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}


TEST_F(TestEngine, tagiterator) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "tagiter2", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600;
  // insert value
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  int cnt = 1000;
  char* data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
    delete[] data_value;
  }

  // tag iterator
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2};
  std::vector<k_uint32> hps;
  make_hashpoint(&hps);
  TagIterator *iter;
  ASSERT_EQ(ts_table->GetTagIterator(ctx_, scan_tags,hps, &iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint32 fetch_total_count = 0;
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 all_idx = 0;
  k_uint32 tag_val = 0;
  do {
    ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
    if (count == 0) {
      break;
    }
    // check entity id
    for (int idx = 0; idx < entity_id_list.size(); idx++) {
      ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
      memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
      ASSERT_EQ(ptag, start_ts1+(idx+fetch_total_count)*100);
    }
    // check tag column
    for (int tag_idx = 0; tag_idx < scan_tags.size(); tag_idx++) {
      for (int i = 0; i < res.data[tag_idx][0]->count; i++) {
        if (tag_idx == 0) {
          memcpy(&tag_val, res.data[tag_idx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
          ASSERT_EQ(tag_val, start_ts1 + (i + fetch_total_count) * 100);
        } else {
          // var
          char* rec_ptr = static_cast<char*>(res.data[tag_idx][0]->getVarColData(i));
          ASSERT_EQ(rec_ptr, std::to_string(start_ts1+(i)*100));
        }
      }
    }
    fetch_total_count += count;
    entity_id_list.clear();
    res.clear();
  }while(count);
  ASSERT_EQ(fetch_total_count, cnt);
  iter->Close();
  delete iter;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, updatetag) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "tagiter2", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600;
  // insert value
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  int cnt = 20;
  char* data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
    delete[] data_value;
  }
  // update
  data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 1, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(tbl_range->PutEntity(ctx_, payload1, 0), KStatus::SUCCESS);
    delete[] data_value;
  }
  // tag iterator
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2};
  std::vector<k_uint32> hps;
  for (uint32_t i =0; i< HASHPOINT_RANGE; i++) {
    hps.push_back(i);
  }
  TagIterator *iter;
  ASSERT_EQ(ts_table->GetTagIterator(ctx_, scan_tags,hps, &iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint32 fetch_total_count = 0;
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 all_idx = 0;
  k_uint32 tag_val = 0;
  do {
    ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
    if (count == 0) {
      break;
    }
    // check entity id
    for (int idx = 0; idx < entity_id_list.size(); idx++) {
      ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
      memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
      ASSERT_EQ(ptag, start_ts1+(idx+fetch_total_count)*100);
    }
    // check tag column
    for (int tag_idx = 0; tag_idx < scan_tags.size(); tag_idx++) {
      for (int i = 0; i < res.data[tag_idx][0]->count; i++) {
        if (tag_idx == 0) {
          memcpy(&tag_val, res.data[tag_idx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
          ASSERT_EQ(tag_val, start_ts1 + 1 + (i + fetch_total_count) * 100);
        } else {
          // var
          char* rec_ptr = static_cast<char*>(res.data[tag_idx][0]->getVarColData(i));
          ASSERT_EQ(rec_ptr, std::to_string(start_ts1 + 1 + (i)*100));
        }
      }
    }
    fetch_total_count += count;
    entity_id_list.clear();
    res.clear();
  }while(count);
  ASSERT_EQ(fetch_total_count, cnt);
  iter->Close();
  delete iter;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, altertag) {

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "tagiter4", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600;
  // insert value
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  int cnt = 2;
  char* data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
    delete[] data_value;
  }
  // add new tag column
  std::string msg;
  const int begin_column_id = 10;
  int col_cnt = meta.k_column_size();
  int begin_tag_idx = col_cnt - 3;
  uint32_t cur_version = 1;
  uint32_t new_version = cur_version + 1;
  roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::INT);
  column->set_storage_len(sizeof(int32_t));
  column->set_column_id(11);
  column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
  column->set_name("tag" + std::to_string(begin_column_id));
  ASSERT_EQ(ts_table->AlterTable(ctx_, kwdbts::AlterType::ADD_COLUMN, column, cur_version, new_version, msg), KStatus::SUCCESS);
  // insert new data
  for (int i = cnt; i < cnt + cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    KUint32(data_value + Payload::ts_version_offset_) = new_version;
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
    delete[] data_value;
  }
  cur_version = new_version;
  new_version = cur_version + 1;

  // tag iterator
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2, 3};
  std::vector<k_uint32> hps;
  make_hashpoint(&hps);
  TagIterator *iter;
  ASSERT_EQ(ts_table->GetTagIterator(ctx_, scan_tags,hps, &iter, cur_version), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint32 fetch_total_count = 0;
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 all_idx = 0;
  k_uint32 tag_val = 0;
  do {
    ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
    if (count == 0) {
      break;
    }
    // check entity id
    for (int idx = 0; idx < entity_id_list.size(); idx++) {
      ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
      memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
      ASSERT_EQ(ptag, start_ts1+(idx+fetch_total_count)*100);
    }
    // check tag column
    for (int tag_idx = 0; tag_idx < scan_tags.size(); tag_idx++) {
      if (fetch_total_count == 0 && scan_tags[tag_idx] == 3) {
        // ASSERT_EQ(res.data[tag_idx].empty(), true);
        continue;
      }
      for (int i = 0; i <  res.data[tag_idx][0]->count; i++) {
        const auto& col = meta.k_column(begin_tag_idx + scan_tags[tag_idx]);
        if (col.storage_type() != roachpb::DataType::VARCHAR) {
          memcpy(&tag_val, res.data[tag_idx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
          ASSERT_EQ(tag_val, start_ts1 + (i + fetch_total_count) * 100);
        } else {
          // var
          char* rec_ptr = static_cast<char*>(res.data[tag_idx][0]->getVarColData(i));
          ASSERT_EQ(rec_ptr, std::to_string(start_ts1+(i + fetch_total_count)*100));
        }
      }
    }
    fetch_total_count += count;
    entity_id_list.clear();
    res.clear();
  }while(count);
  ASSERT_EQ(fetch_total_count, cnt + cnt);
  iter->Close();
  delete iter;
  // drop table
  // ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}
