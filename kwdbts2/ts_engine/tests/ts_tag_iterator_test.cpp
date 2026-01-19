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
#include "libkwdbts2.h"
#include "ts_engine.h"
#include "test_util.h"
#include "../../mmap/include/mmap/mmap_ptag_hash_index.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT The current directory is the storage directory for the big table

RangeGroup kTestRange{1, 0};
class TestEngine : public ::testing::Test {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngineImpl* ts_engine_;

  TestEngine() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;
    opts_.is_single_node_ = true;

    fs::remove_all(kDbPath);
    // Clean up file directory
    auto engine = new TSEngineImpl(opts_);
    KStatus s = engine->Init(ctx_);
    ts_engine_ = engine;
  }

  ~TestEngine() {
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  // Store in header
  int row_num_ = 5;
};

TEST_F(TestEngine, tagiterator) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  auto s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges, false);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  bool is_dropped = false;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = ts_engine_->GetTableSchemaMgr(ctx_, cur_table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600;
  TSSlice data_value{};
  data_value = GenRowPayload(*metric_schema, tag_schema ,cur_table_id, 1, start_ts1, 1, start_ts1);

  TSTableID ts_id(cur_table_id);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, ts_id, 0, &data_value, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, true);
  ASSERT_EQ(s , KStatus::SUCCESS);
  free(data_value.data);

  int cnt = 1;

  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {0};
  std::unordered_set<k_uint32> hps;
  make_hashpoint(&hps);
  BaseEntityIterator *iter;
  ASSERT_EQ(ts_table->GetTagIterator(ctx_, scan_tags,hps, &iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint32 fetch_total_count = 0;
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  do {
    ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
    if (count == 0) {
      break;
    }
    // check entity id
    for (int idx = 0; idx < entity_id_list.size(); idx++) {
      ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
      memcpy(&ptag, entity_id_list[idx].mem.get(), sizeof(ptag));
      ASSERT_EQ(ptag, start_ts1+(idx+fetch_total_count)*100);
    }
    fetch_total_count += count;
    entity_id_list.clear();
    res.clear();
  }while(count);
  ASSERT_EQ(fetch_total_count, cnt);
  iter->Close();
  delete iter;
  ts_table.reset();
}

TEST(TsTagHashIndexTest, MultiInsert) {
  string index_name = "11.tag.ht";
  System("rm -rf 11.tag.ht");
  int primary_key_length = 8;
  MMapHashIndex* m_index_ = new MMapPTagHashIndex(primary_key_length);
  ErrorInfo err_info;
  auto errcode = m_index_->open(index_name, "./", "./", MMAP_CREATOPEN, err_info);
  assert(errcode == 0);
  std::vector<uint64_t> pkeys;
  int thread_num = 10;
  int insert_rows = 10000;

  for (size_t i = 0; i < insert_rows * thread_num; i++) {
    pkeys.push_back(10086 + i);
  }

  std::vector<std::thread> threads;
  for (size_t i = 0; i < thread_num; i++) {
    int idx = i;
    threads.push_back(thread([&](int index) {
        for (size_t j = 0; j < insert_rows; j++) {
          auto err_code = m_index_->insert((char*)(&pkeys[index * insert_rows + j]), primary_key_length, 1, index * insert_rows + j + 1);
          ASSERT_TRUE(err_code == 0);
        }
    }, idx));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }

  for (size_t i = 0; i < insert_rows * thread_num; i++) {
    auto ret = m_index_->get((char*)(&pkeys[i]), primary_key_length);
    ASSERT_EQ(ret.first, 1);
    ASSERT_EQ(ret.second, i + 1);
  }
  delete m_index_;
}
