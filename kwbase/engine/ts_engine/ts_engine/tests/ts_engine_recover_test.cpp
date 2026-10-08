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

#include <fcntl.h>
#include <unistd.h>
#include "cm_kwdb_context.h"
#include "ts_test_base.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
class TsEngineRecoverTest : public TsEngineTestBase {
 public:
  TsEngineRecoverTest() {
    InitContext();
    InitEngine(engine_root_path);
    ctx_->ts_engine = engine_;
  }

  void Restart() {
    ASSERT_TRUE(engine_ != nullptr);
    InitEngine(opts_.db_path, false);
    ctx_->ts_engine = engine_;
  }

  void CreateTable(TSTableID  table_id) {
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, table_id);
    std::shared_ptr<TsTable> ts_table;
    KStatus s = engine_->CreateTsTable(ctx_, table_id, &meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  void InsertData(TSTableID table_id, TSEntityID dev_id, timestamp64 start_ts, int num, KTimestamp interval = 1000, TS_OSN osn = 10) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, dev_id, num, start_ts);
    TsRawPayload::SetHashPoint(pay_load, 2);
    TsRawPayload::SetOSN(pay_load, osn);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    EXPECT_EQ(s , KStatus::SUCCESS);
    free(pay_load.data);
  }
  void UpdateTag(TSTableID table_id, TSEntityID dev_id, TS_OSN osn) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, dev_id, 0, 0);
    TsRawPayload::SetHashPoint(pay_load, 2);
    TsRawPayload::SetOSN(pay_load, osn);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = engine_->PutEntity(ctx_, table_id, 1, &pay_load, 1, 0, is_dropped);
    EXPECT_EQ(s , KStatus::SUCCESS);
    free(pay_load.data);
  }

  uint64_t GetDataNum(TSTableID table_id, TSEntityID dev_id, KwTsSpan ts_span, int64_t& dev_num) {
    std::shared_ptr<TsTable> ts_table_dest;
    bool is_dropped = false;
    auto s = engine_->GetTsTable(ctx_, table_id, ts_table_dest, is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
    auto ts_table_v2 = dynamic_pointer_cast<TsTableImpl>(ts_table_dest);
    std::vector<EntityResultIndex> devs;
    if (dev_id > 0) {
      uint32_t entity_id = 0;
      uint32_t sub_group_id = 0;
      auto pkey = GetPrimaryKey(table_id, dev_id);
      auto find = ts_table_v2->GetSchemaManager()->GetTagTable()->hasPrimaryKey(pkey.data(), pkey.length(), entity_id, sub_group_id);
      devs.push_back({EntityResultIndex(1, entity_id, sub_group_id)});
    } else {
        std::vector<uint32_t> scan_tags{0};
      std::vector<HashIdSpan> hps;
      hps.push_back({0, UINT64_MAX});
      BaseEntityIterator* iter;
      ts_table_v2->GetTagIterator(ctx_, scan_tags, &hps, &iter, 1, UINT64_MAX);
      ResultSet res{(k_uint32) scan_tags.size()};
      k_uint32 fetch_total_count = 0;
      k_uint64 ptag = 0;
      k_uint32 count = 0;
      do {
        std::vector<EntityResultIndex> devs1;
        EXPECT_EQ(iter->Next(&devs1, &res, &count), KStatus::SUCCESS);
        if (count == 0) {
          break;
        }
        devs.insert(devs.end(), devs1.begin(), devs1.end());
        fetch_total_count += count;
      }while(count);
      iter->Close();
      delete iter;
    }
    uint64_t row_count = 0;
    ctx_->ts_engine = engine_;
    ts_table_v2->GetEntityRowCount(ctx_, devs, {ts_span}, UINT64_MAX, &row_count);
    dev_num = devs.size();
    return row_count;
  }
  void UpdateTag(TSTableID table_id, TSEntityID dev_id, TS_OSN osn, int up_num) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    timestamp64 start_ts = 3600;
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, dev_id, 1, start_ts);
    TsRawPayload::SetOSN(pay_load, osn);
    s = engine_->PutEntity(ctx_, table_id, 1, &pay_load, 1, 1, is_dropped);
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
};

TEST_F(TsEngineRecoverTest, empty) {
  Restart();
}

TEST_F(TsEngineRecoverTest, update5Times) {
  TSTableID table_id = 10032;
  CreateTable(table_id);
  InsertData(table_id, 1, 12345, 5, 1000, 1700000);
  for (size_t i = 1; i <= 5; i++) {
    UpdateTag(table_id, 1, 1700000 + 1000 * i);
  }
  int64_t dev_num = 0;
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 5);
  Restart();
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 5);
}

TEST_F(TsEngineRecoverTest, delete5Times) {
  TSTableID table_id = 10032;
  CreateTable(table_id);
  for (size_t i = 1; i <= 5; i++) {
    InsertData(table_id, 1, 12345 + 10000 * i, 1, 1000, 1700000 + 100 * i);
    DeleteTag(table_id, 1, 1700000 + 100 * i + 10, 1);
  }
  int64_t dev_num = 0;
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 0);
  Restart();
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 0);
}

TEST_F(TsEngineRecoverTest, updataDeleteTimes) {
  TSTableID table_id = 10032;
  CreateTable(table_id);
  int times = 2;
  for (size_t i = 1; i <= times; i++) {
    InsertData(table_id, 1, 12345 + 10000 * i, 1, 1000, 1700000 + 100 * i);
    UpdateTag(table_id, 1, 1700000 + 100 * i + 5);
    if (i < times) {
      DeleteTag(table_id, 1, 1700000 + 100 * i + 10, 1);
    }
  }
  int64_t dev_num = 0;
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 1);
  Restart();
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 1);
}

TEST_F(TsEngineRecoverTest, insertDisorderByOSN) {
  TSTableID table_id = 10032;
  CreateTable(table_id);
  InsertData(table_id, 1, 12346, 1, 1000, 1700000);
  InsertData(table_id, 1, 12345, 1, 1000, 1700000 - 100);
  int64_t dev_num = 0;
  ASSERT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 2);
  ASSERT_EQ(GetDataNum(table_id, 0, {INT64_MIN, INT64_MAX}, dev_num), 2);
  ASSERT_EQ(dev_num, 1);
  Restart();
  EXPECT_EQ(GetDataNum(table_id, 1, {INT64_MIN, INT64_MAX}, dev_num), 2);
  EXPECT_EQ(GetDataNum(table_id, 0, {INT64_MIN, INT64_MAX}, dev_num), 2);
  ASSERT_EQ(dev_num, 1);
}
