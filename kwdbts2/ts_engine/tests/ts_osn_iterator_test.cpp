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
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;

const string engine_root_path = "./tsdb";
class TestV2IteratorByOSN : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineImpl *engine_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;
  TSTableID table_id_ = 999;
  std::shared_ptr<TsTable> ts_table_;
  const std::vector<AttributeInfo>* metric_schema_{nullptr};
  std::vector<TagInfo> tag_schema_;
  DATATYPE ts_col_type_;
   std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;

  virtual void SetUp() override {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

 public:
  TestV2IteratorByOSN() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineImpl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);

    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTable(&pb_meta, table_id_);
    s = engine_->CreateTsTable(ctx_, table_id_, &pb_meta, ts_table_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    bool is_dropped = false;
    s = engine_->GetTableSchemaMgr(ctx_, table_id_, is_dropped, table_schema_mgr_);
    EXPECT_EQ(s , KStatus::SUCCESS);
    s = table_schema_mgr_->GetMetricMeta(1, &metric_schema_);
    EXPECT_EQ(s , KStatus::SUCCESS);
    s = table_schema_mgr_->GetTagMeta(1, tag_schema_);
    EXPECT_EQ(s , KStatus::SUCCESS);
    ts_col_type_ = table_schema_mgr_->GetTsColDataType();
  }

  ~TestV2IteratorByOSN() {
    if (engine_) {
      delete engine_;
    }
  }
    std::string GetPrimaryKey(TSEntityID dev_id) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id_, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    uint64_t pkey_len = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        pkey_len += tag_schema[i].m_size;
      }
    }
    char* mem = reinterpret_cast<char*>(malloc(pkey_len));
    memset(mem, 0, pkey_len);
    std::string dev_str = intToString(dev_id);
    size_t offset = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        if (tag_schema[i].m_data_type == DATATYPE::VARSTRING) {
          memcpy(mem + offset, dev_str.data(), dev_str.length());
        } else {
          memcpy(mem + offset, (char*)(&dev_id), tag_schema[i].m_size);
        }
        offset += tag_schema[i].m_size;
      }
    }
    auto ret = std::string{mem, pkey_len};
    free(mem);
    return ret;
  }
};

// insert one tag, then scan tags by osn ranges.
TEST_F(TestV2IteratorByOSN, basic_insert) {
  uint32_t table_version = 1;
  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts);
  TsRawPayload::SetOSN(pay_load, 1760000);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  bool is_dropped = false;
  auto s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, 1760000});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 1760000);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_INSERT);
  delete iter;
  osn_spans.clear();
  osn_spans.push_back({0, 1760000 - 1});
  entity_id_list.clear();
  rs.clear();
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 0);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED);
  delete iter;
  osn_spans.clear();
  osn_spans.push_back({1760000 + 1, UINT64_MAX});
  entity_id_list.clear();
  rs.clear();
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 0);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED);
  delete iter;

  kwdbts::TsIterator *m_iter;
  uint32_t total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1760000});
  rs.clear();
  rs.setColumnNum(4);
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(rs.data[0][0]->mem), 3600);
  ASSERT_EQ(KUint64(rs.data[1][0]->mem), 1760000);
  ASSERT_EQ(KUint8(rs.data[2][0]->mem), 1);
  ASSERT_EQ(KUint8(rs.data[3][0]->mem), 0);
  delete m_iter;
}

// insert one tag, then update tag twice. then scan by osn ranges.
TEST_F(TestV2IteratorByOSN, basic_udpate) {
  uint32_t table_version = 1;
  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts);
  TsRawPayload::SetOSN(pay_load, 1760000);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  bool is_dropped = false;
  auto s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsRawPayload::SetOSN(pay_load, 1770000);
  s = engine_->PutEntity(ctx_, table_id_, 1, &pay_load, 1, 0, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsRawPayload::SetOSN(pay_load, 1780000);
  s = engine_->PutEntity(ctx_, table_id_, 1, &pay_load, 1, 0, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(pay_load.data);
  
  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, 1770000});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint32_t total = 0;
  do {
    s = iter->Next(&entity_id_list, &rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total, 1);
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 0);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED);
  delete iter;

  kwdbts::TsIterator *m_iter;
  ResultSet m_rs;
  m_rs.setColumnNum(4);
  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1770000});
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(m_rs.data[0][0]->mem), 3600);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1760000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 1);
  ASSERT_EQ(KUint8(m_rs.data[3][0]->mem), 0);
  delete m_iter;
  m_iter = nullptr;

  osn_spans.clear();
  osn_spans.push_back({0, 1780000});
  entity_id_list.clear();
  rs.clear();
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  total = 0;
  do {
    s = iter->Next(&entity_id_list, &rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total, 1);
  op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 1780000);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE);
  delete iter;

  total = 0;
  m_rs.clear();
  osn_spans.clear();
  osn_spans.push_back({0, 1780000});
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  2);
  ASSERT_EQ(m_rs.data[0][0]->mem, nullptr);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1780000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 2);
  ASSERT_EQ(KUint8(m_rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64(m_rs.data[0][1]->mem), 3600);
  ASSERT_EQ(KUint64(m_rs.data[1][1]->mem), 1760000);
  ASSERT_EQ(KUint8(m_rs.data[2][1]->mem), 1);
  ASSERT_EQ(KUint8(m_rs.data[3][1]->mem), 0);
  delete m_iter;

  uint64_t pkey = 1;
  std::vector<void*> pkeys;
  std::string pkey_str = GetPrimaryKey(pkey);
  pkeys.push_back(pkey_str.data());
  osn_spans.clear();
  osn_spans.push_back({0, UINT64_MAX});
  entity_id_list.clear();
  ResultSet res;
  res.setColumnNum(1);
  s = ts_table_->GetEntityIdListByOSN(ctx_, pkeys, osn_spans, scan_cols, {2}, &entity_id_list, &res, &count, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_id_list.size(), 1);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KUint64(res.data[0][0]->mem), pkey);
}

// insert one tag, then delete tag twice. then insert again, delete again, then scan by osn ranges.
TEST_F(TestV2IteratorByOSN, basic_delete) {
  // insert tag at 1760000
  uint32_t table_version = 1;
  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts);
  TsRawPayload::SetOSN(pay_load, 1760000);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  bool is_dropped = false;
  auto s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  uint64_t d_count = 0;
  // delete tag at 1770000
  s = engine_->DeleteRangeEntities(ctx_, table_id_, 1, {0, UINT64_MAX}, &d_count, d_count, is_dropped, 1770000);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(d_count, 1);
  free(pay_load.data);
  pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts + 1000);
  TsRawPayload::SetOSN(pay_load, 1780000);
  // insert tag at 1780000
  s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // delete tag at 1790000
  s = engine_->DeleteRangeEntities(ctx_, table_id_, 1, {0, UINT64_MAX}, &d_count, d_count, is_dropped, 1790000);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(d_count, 1);
  free(pay_load.data);

  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, 1760000});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint32_t total = 0;
  uint32_t count;
  do {
    s = iter->Next(&entity_id_list, &rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  0);
  delete iter;

  osn_spans.clear();
  osn_spans.push_back({0, 1780000});
  entity_id_list.clear();
  rs.clear();
  total = 0;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    s = iter->Next(&entity_id_list, &rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 1770000);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
  delete iter;

  kwdbts::TsIterator *m_iter;
  ResultSet m_rs;
  m_rs.setColumnNum(4);
  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1780000});
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(m_rs.data[0][0]->mem,  nullptr);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1770000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 3);
  ASSERT_EQ(KUint8(m_rs.data[3][0]->mem), 0);
  delete m_iter;
  m_iter = nullptr;

  osn_spans.clear();
  osn_spans.push_back({0, 1790000});
  entity_id_list.clear();
  rs.clear();
  total = 0;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    s = iter->Next(&entity_id_list, &rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  2);
  op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 1770000);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
  op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[1].op_with_osn.get());
  ASSERT_TRUE(op_osn != nullptr);
  ASSERT_TRUE(op_osn->osn == 1790000);
  ASSERT_TRUE(op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
  delete iter;

  total = 0;
  m_rs.clear();
  osn_spans.clear();
  osn_spans.push_back({0, 1790000});
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  2);
  ASSERT_EQ(m_rs.data[0][0]->mem,  nullptr);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1770000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 3);
  ASSERT_EQ(KUint8(m_rs.data[3][0]->mem), 0);
  ASSERT_EQ(m_rs.data[0][1]->mem,  nullptr);
  ASSERT_EQ(KUint64(m_rs.data[1][1]->mem), 1790000);
  ASSERT_EQ(KUint8(m_rs.data[2][1]->mem), 3);
  ASSERT_EQ(KUint8(m_rs.data[3][1]->mem), 0);
  delete m_iter;
  m_iter = nullptr;

  uint64_t pkey = 1;
  std::string pkey_str = GetPrimaryKey(pkey);
  std::vector<void*> pkeys;
  pkeys.push_back(pkey_str.data());
  osn_spans.clear();
  osn_spans.push_back({0, UINT64_MAX});
  entity_id_list.clear();
  ResultSet res;
  res.setColumnNum(1);
  s = ts_table_->GetEntityIdListByOSN(ctx_, pkeys, osn_spans, scan_cols, {2}, &entity_id_list, &res, &count, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_id_list.size(), 2);
  ASSERT_EQ(count, 2);
  ASSERT_EQ(res.data[0].size(), 1);
  ASSERT_EQ(KUint64(res.data[0][0]->mem), pkey);
  ASSERT_EQ(KUint64((char*)(res.data[0][0]->mem) + 216), pkey);
}

// insert one tag, then insert some metric datas
TEST_F(TestV2IteratorByOSN, basic_metric_insert) {
  uint32_t table_version = 1;
  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts);
  TsRawPayload::SetOSN(pay_load, 1760000);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  bool is_dropped = false;
  auto s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsRawPayload::SetOSN(pay_load, 1770000);
  s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsRawPayload::SetOSN(pay_load, 1780000);
  s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(pay_load.data);

  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, 0});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  delete iter;

  kwdbts::TsIterator *m_iter;
  uint32_t total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1760000 - 1});
  rs.clear();
  rs.setColumnNum(4);
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  0);
  delete m_iter;
  m_iter = nullptr;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1760000});
  rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(rs.data[1][0]->mem), 1760000);
  ASSERT_EQ(KUint8(rs.data[2][0]->mem), 1);
  ASSERT_EQ(KUint8(rs.data[3][0]->mem), 0);
  delete m_iter;
  m_iter = nullptr;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1780000});
  rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(rs.data[1][0]->mem), 1780000);
  ASSERT_EQ(KUint8(rs.data[2][0]->mem), 1);
  ASSERT_EQ(KUint8(rs.data[3][0]->mem), 0);
  delete m_iter;
  m_iter = nullptr;
}

// insert some metric datas, and then delete some metric data.
TEST_F(TestV2IteratorByOSN, basic_metric_delete) {
  uint32_t table_version = 1;
  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts);
  TsRawPayload::SetOSN(pay_load, 1760000);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  bool is_dropped = false;
  auto s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t pkey_mem = 1;
  std::string pkey = GetPrimaryKey(pkey_mem);
  std::vector<KwTsSpan> ts_spans;
  uint64_t r_count;
  ts_spans.push_back({0, 3600});
  s = engine_->DeleteData(ctx_, table_id_, 1, pkey, ts_spans, &r_count, 0, 1770000, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsRawPayload::SetOSN(pay_load, 1780000);
  s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ts_spans.clear();
  ts_spans.push_back({0, 13600});
  s = engine_->DeleteData(ctx_, table_id_, 1, pkey, ts_spans, &r_count, 0, 1790000, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsRawPayload::SetOSN(pay_load, 1800000);
  s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ts_spans.clear();
  ts_spans.push_back({0, 23600});
  s = engine_->DeleteData(ctx_, table_id_, 1, pkey, ts_spans, &r_count, 0, 1810000, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(pay_load.data);

  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, 1760000 - 1});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  delete iter;

  kwdbts::TsIterator *m_iter;
  ResultSet m_rs;
  uint32_t total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1770000 - 1});
  m_rs.clear();
  m_rs.setColumnNum(4);
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  0);
  delete m_iter;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1780000});
  m_rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  // while osn= 1780000, has one valid metric row ,but this row deleted later, so not return.
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1770000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 4);
  ASSERT_EQ(KUint64(m_rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 8), 3600);
  delete m_iter;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1790000});
  m_rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  2);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1770000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 4);
  ASSERT_EQ(KUint64(m_rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 8), 3600);

  ASSERT_EQ(KUint64((char*)(m_rs.data[1][0]->mem) + 8), 1790000);
  ASSERT_EQ(KUint8((char*)(m_rs.data[2][0]->mem) + 1), 4);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 16), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 24), 13600);

  delete m_iter;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({0, 1810000});
  m_rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  3);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1770000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 4);
  ASSERT_EQ(KUint64(m_rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 8), 3600);
  ASSERT_EQ(KUint64((char*)(m_rs.data[1][0]->mem) + 8), 1790000);
  ASSERT_EQ(KUint8((char*)(m_rs.data[2][0]->mem) + 1), 4);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 16), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 24), 13600);

  ASSERT_EQ(KUint64((char*)(m_rs.data[1][0]->mem) + 16), 1810000);
  ASSERT_EQ(KUint8((char*)(m_rs.data[2][0]->mem) + 2), 4);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 32), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 40), 23600);
  delete m_iter;
}
// tag insert then delete all metric data. osn range return empty row, else return nothing.
TEST_F(TestV2IteratorByOSN, only_tag_data_exist) {
  uint32_t table_version = 1;
  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema_, tag_schema_ ,table_id_, table_version, 1, 1, start_ts);
  TsRawPayload::SetOSN(pay_load, 1760000);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    bool is_dropped = false;
  auto s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t pkey_mem = 1;
  std::string pkey = GetPrimaryKey(pkey_mem);
  std::vector<KwTsSpan> ts_spans;
  uint64_t r_count;
  ts_spans.push_back({0, 45600});

  s = engine_->DeleteData(ctx_, table_id_, 1, pkey, ts_spans, &r_count, 0, 1770000, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({1760000 + 1, 1770000 - 1});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  delete iter;

  kwdbts::TsIterator *m_iter;
  ResultSet m_rs;
  uint32_t total = 0;
  osn_spans.clear();
  osn_spans.push_back({1760000 + 1, 1770000 - 1});
  m_rs.clear();
  m_rs.setColumnNum(4);
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  0);
  delete m_iter;
  m_iter = nullptr;

  entity_id_list.clear();
  osn_spans.clear();
  osn_spans.push_back({1760000, 1770000 - 1});
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  delete iter;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({1760000, 1770000 - 1});
  m_rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  // only find tag row. tag status is insert.
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1760000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 1);
  ASSERT_EQ(KUint8(m_rs.data[3][0]->mem), 1);
  delete m_iter;
  m_iter = nullptr;

  entity_id_list.clear();
  osn_spans.clear();
  osn_spans.push_back({1760000 + 1, 1770000});
  s = ts_table_->GetTagIteratorByOSN(ctx_, table_version, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  delete iter;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({1760000 + 1, 1770000});
  m_rs.clear();
  s = ts_table_->GetMetricIteratorByOSN(ctx_, table_version, scan_cols, entity_id_list, osn_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&m_rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(KUint64(m_rs.data[1][0]->mem), 1770000);
  ASSERT_EQ(KUint8(m_rs.data[2][0]->mem), 4);
  ASSERT_EQ(KUint64(m_rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64((char*)(m_rs.data[3][0]->mem) + 8), 45600);
  delete m_iter;

  free(pay_load.data);
}
