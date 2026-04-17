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

#include <cstdio>
#include "ts_test_base.h"
#include "test_util.h"
#include "ts_engine.h"
#include "ts_lru_block_cache.h"
#include "ts_table.h"
#include <atomic>

using namespace kwdbts;

const string engine_root_path = "./tsdb";
extern atomic<int> destroyed_entity_block_file_count;
extern atomic<int> created_entity_block_file_count;

class TestV2Iterator : public TsEngineTestBase {
 public:
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  const std::vector<AttributeInfo>* metric_schema_;
  std::vector<TagInfo> tag_schema_;
  std::shared_ptr<TagTable> tag_table_;

  TestV2Iterator() : metric_schema_(nullptr) {
    EngineOptions::is_single_node_ = true;
    InitContext();
    InitEngine(engine_root_path);
  }

  void CreateTable(TSTableID table_id, std::shared_ptr<TsTable>& ts_table) {
    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTable(&pb_meta, table_id);
    auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    bool is_dropped = false;
    s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr_);
    ASSERT_EQ(s , KStatus::SUCCESS);
    s = table_schema_mgr_->GetMetricMeta(1, &metric_schema_);
    ASSERT_EQ(s , KStatus::SUCCESS);
    s = table_schema_mgr_->GetTagMeta(1, tag_schema_);
    ASSERT_EQ(s , KStatus::SUCCESS);
    s = table_schema_mgr_->GetTagSchema(ctx_, &tag_table_);
    ASSERT_EQ(s , KStatus::SUCCESS);
  }

  void InsertTag(TSTableID table_id, TSEntityID dev_id, TS_OSN osn) {
    timestamp64 start_ts = 3600;
    auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id, 1, dev_id, 1, start_ts);
    TsRawPayload::SetOSN(pay_load, osn);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    auto s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  void InsertMetric(TSTableID table_id, TSEntityID dev_id, timestamp64 ts, TS_OSN osn) {
    auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id, 1, dev_id, 1, ts);
    TsRawPayload::SetOSN(pay_load, osn);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    auto s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  void UpdateTag(TSTableID table_id, TSEntityID dev_id, TS_OSN osn, int up_num) {
    timestamp64 start_ts = 3600;
    auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id, 1, dev_id, 1, start_ts);
    TsRawPayload::SetOSN(pay_load, osn);
    bool is_dropped;
    auto s = engine_->PutEntity(ctx_, table_id, 1, &pay_load, 1, 1, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  void DeleteTag(TSTableID table_id, TSEntityID dev_id, TS_OSN osn, int del_num) {
    auto pkey = GetPrimaryKey(table_id, dev_id);
    uint64_t count;
    bool is_dropped;
    auto s = engine_->DeleteEntities(ctx_, table_id, 1, {pkey}, &count, 0, is_dropped, osn);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ASSERT_EQ(count, del_num);
  }

  void CheckTagInfo(std::shared_ptr<TsTable> ts_table, TS_OSN osn, int tag_num) {
    std::vector<uint32_t> scan_tags{0};
    std::vector<HashIdSpan> hps;
    make_hashpoint(&hps);
    BaseEntityIterator* iter;
    ts_table->GetTagIterator(ctx_, scan_tags, &hps, &iter, 1, osn);
    ResultSet res{(k_uint32) scan_tags.size()};
    k_uint32 fetch_total_count = 0;
    k_uint64 ptag = 0;
    k_uint32 count = 0;
    std::vector<EntityResultIndex> entity_id_list;
    do {
      ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
      if (count == 0) {
        break;
      }
      fetch_total_count += count;
      entity_id_list.clear();
      res.clear();
    }while(count);
    ASSERT_EQ(fetch_total_count, tag_num);
    iter->Close();
    delete iter;
  }

  std::string GetPrimaryKey(TSTableID table_id, TSEntityID dev_id) {
    uint64_t pkey_len = 0;
    for (size_t i = 0; i < tag_schema_.size(); i++) {
      if (tag_schema_[i].isPrimaryTag()) {
        pkey_len += tag_schema_[i].m_size;
      }
    }
    char* mem = reinterpret_cast<char*>(malloc(pkey_len));
    memset(mem, 0, pkey_len);
    std::string dev_str = intToString(dev_id);
    size_t offset = 0;
    for (size_t i = 0; i < tag_schema_.size(); i++) {
      if (tag_schema_[i].isPrimaryTag()) {
        if (tag_schema_[i].m_data_type == DATATYPE::VARSTRING) {
          memcpy(mem + offset, dev_str.data(), dev_str.length());
        } else {
          memcpy(mem + offset, (char*)(&dev_id), tag_schema_[i].m_size);
        }
        offset += tag_schema_[i].m_size;
      }
    }
    auto ret = std::string{mem, pkey_len};
    free(mem);
    return ret;
  }
};

TEST_F(TestV2Iterator, basic) {
  TSTableID table_id = 999;
  std::shared_ptr<TsTable> ts_table;
  CreateTable(table_id, ts_table);
  for (size_t i = 0; i < 10; i++) {
    auto pkey = GetPrimaryKey(table_id, 111 + i);
    assert(!tag_table_->hasPrimaryKey(pkey.data(), pkey.length()));
    InsertTag(table_id, 111 + i, 100 + i * 100);
  }
  CheckTagInfo(ts_table, UINT64_MAX, 10);
  CheckTagInfo(ts_table, 99, 0);
  CheckTagInfo(ts_table, 700, 7);
  CheckTagInfo(ts_table, 1000, 10);
  uint64_t count;
  auto s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 700, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 7);
}

TEST_F(TestV2Iterator, basic_metric) {
  TSTableID table_id = 999;
  std::shared_ptr<TsTable> ts_table;
  CreateTable(table_id, ts_table);
  std::vector<std::string> primarykeys;
  std::vector<void*> primarykeys_void;
  for (size_t i = 0; i < 10; i++) {
    auto pkey = GetPrimaryKey(table_id, 111 + i);
    assert(!tag_table_->hasPrimaryKey(pkey.data(), pkey.length()));
    InsertTag(table_id, 111 + i, 100 + i * 100);
    InsertMetric(table_id, 111 + i, 3700, 100 + i * 100 + 1);
    primarykeys.push_back(pkey);
    primarykeys_void.push_back(primarykeys[i].data());
  }
  CheckTagInfo(ts_table, UINT64_MAX, 10);
  CheckTagInfo(ts_table, 99, 0);
  CheckTagInfo(ts_table, 1000, 10);
  uint64_t count;
  auto s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 700, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 13);
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 701, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 14);
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 1001, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 20);
  std::vector<EntityResultIndex> entity_store;
  s = ts_table->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, 701, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 7);

  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  std::vector<uint32_t> scan_tags;
  scan_tags.push_back(0);
  std::vector<HashIdSpan> hps;
  ResultSet res;
  uint32_t count_32;
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  primarykeys_void, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 701);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 7);
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  primarykeys_void, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 301);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 3);
}

TEST_F(TestV2Iterator, basic_update) {
  TSTableID table_id = 999;
  std::shared_ptr<TsTable> ts_table;
  CreateTable(table_id, ts_table);
  std::vector<std::string> primarykeys;
  std::vector<void*> primarykeys_void;
  for (size_t i = 0; i < 10; i++) {
    auto pkey = GetPrimaryKey(table_id, 111 + i);
    assert(!tag_table_->hasPrimaryKey(pkey.data(), pkey.length()));
    InsertTag(table_id, 111 + i, 100 + i * 100);
    UpdateTag(table_id, 111 + i, 10000 + i * 100, 1);
    primarykeys.push_back(pkey);
    primarykeys_void.push_back(primarykeys[i].data());
  }
  CheckTagInfo(ts_table, UINT64_MAX, 10);
  CheckTagInfo(ts_table, 700, 7);
  CheckTagInfo(ts_table, 1000, 10);
  CheckTagInfo(ts_table, 11000, 10);
  std::vector<EntityResultIndex> entity_store;
  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  std::vector<uint32_t> scan_tags;
  scan_tags.push_back(0);
  std::vector<HashIdSpan> hps;
  ResultSet res;
  uint32_t count_32;
  auto s = ts_table->GetEntityIdList(ctx_,  primarykeys_void, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 701);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 7);
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  primarykeys_void, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 301);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 3);
}

TEST_F(TestV2Iterator, basic_update_1) {
  TSTableID table_id = 999;
  std::shared_ptr<TsTable> ts_table;
  CreateTable(table_id, ts_table);
  auto pkey = GetPrimaryKey(table_id, 111);
  InsertTag(table_id, 111, 100 );
  for (size_t i = 1; i <= 10; i++) {
    UpdateTag(table_id, 111, 10000 + i * 100, 1);
  }
  CheckTagInfo(ts_table, UINT64_MAX, 1);
  CheckTagInfo(ts_table, 700, 1);
  CheckTagInfo(ts_table, 1000, 1);
  CheckTagInfo(ts_table, 11000, 1);
  std::vector<EntityResultIndex> entity_store;
  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  std::vector<uint32_t> scan_tags;
  scan_tags.push_back(0);
  std::vector<HashIdSpan> hps;
  ResultSet res;
  uint32_t count_32;
  auto s = ts_table->GetEntityIdList(ctx_,  {pkey.data()}, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 001);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 0);
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  {pkey.data()}, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 301);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 1);
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  {pkey.data()}, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 10001);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 1);
}

TEST_F(TestV2Iterator, basic_del) {
  TSTableID table_id = 999;
  std::shared_ptr<TsTable> ts_table;
  CreateTable(table_id, ts_table);
  std::vector<std::string> primarykeys;
  std::vector<void*> primarykeys_void;
  for (size_t i = 0; i < 10; i++) {
    auto pkey = GetPrimaryKey(table_id, 111 + i);
    assert(!tag_table_->hasPrimaryKey(pkey.data(), pkey.length()));
    InsertTag(table_id, 111 + i, 100 + i * 100);
    DeleteTag(table_id, 111 + i, 10000 + i * 100, 1);
    primarykeys.push_back(pkey);
    primarykeys_void.push_back(primarykeys[i].data());
  }
  CheckTagInfo(ts_table, UINT64_MAX, 0);
  CheckTagInfo(ts_table, 700, 7);
  CheckTagInfo(ts_table, 1000, 10);
  CheckTagInfo(ts_table, 10499, 5);
  CheckTagInfo(ts_table, 11000, 0);

  std::vector<EntityResultIndex> entity_store;
  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  std::vector<uint32_t> scan_tags;
  scan_tags.push_back(0);
  std::vector<HashIdSpan> hps;
  ResultSet res;
  uint32_t count_32;
  auto s = ts_table->GetEntityIdList(ctx_,  primarykeys_void, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 701);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 7);
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  primarykeys_void, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 301);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 3);
}
TEST_F(TestV2Iterator, basic_del_1) {
  TSTableID table_id = 999;
  std::shared_ptr<TsTable> ts_table;
  CreateTable(table_id, ts_table);
  auto pkey = GetPrimaryKey(table_id, 111);
  for (size_t i = 1; i <= 10; i++) {
    assert(!tag_table_->hasPrimaryKey(pkey.data(), pkey.length()));
    InsertTag(table_id, 111, 100 + i * 100 - 50);
    DeleteTag(table_id, 111, 100 + i * 100, 1);
  }
  CheckTagInfo(ts_table, UINT64_MAX, 0);
  CheckTagInfo(ts_table, 150, 1);
  CheckTagInfo(ts_table, 201, 0);
  CheckTagInfo(ts_table, 249, 0);
  CheckTagInfo(ts_table, 250, 1);

  std::vector<EntityResultIndex> entity_store;
  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  std::vector<uint32_t> scan_tags;
  scan_tags.push_back(0);
  std::vector<HashIdSpan> hps;
  ResultSet res;
  uint32_t count_32;
  auto s = ts_table->GetEntityIdList(ctx_,  {pkey.data()}, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 701);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 0);
  entity_store.clear();
  s = ts_table->GetEntityIdList(ctx_,  {pkey.data()}, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags, &hps, &entity_store, &res, &count_32, 1, 351);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 1);
  ASSERT_EQ(res.data.size(), 1);
}
