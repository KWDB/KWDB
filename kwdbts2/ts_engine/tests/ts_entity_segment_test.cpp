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

#include "ts_entity_segment.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <list>
#include <memory>
#include <numeric>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_block.h"
#include "ts_entity_segment_data.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_vgroup.h"
#include "ts_lru_block_cache.h"

using namespace kwdbts;  // NOLINT
using namespace roachpb;

class TsEntitySegmentTest : public ::testing::Test {
 protected:
  std::unique_ptr<TsEngineSchemaManager> mgr;

  EngineOptions opts;

  std::unique_ptr<TsVGroup> vgroup;
  kwdbContext_t ctx;

  void CreateTable(TSTableID table_id, const std::vector<DataType> &metric_types,
                   const std::vector<AttributeInfo>** metric_schema, std::vector<TagInfo> *tag_schema,
                   std::shared_ptr<TsTableSchemaManager> &schema_mgr) {
    CreateTsTable meta;

    ConstructRoachpbTableWithTypes(&meta, table_id, metric_types);
    ASSERT_EQ(mgr->CreateTable(nullptr, 1, table_id, &meta), SUCCESS);
    ASSERT_EQ(mgr->GetTableSchemaMgr(table_id, schema_mgr), KStatus::SUCCESS);
    ASSERT_EQ(schema_mgr->GetMetricMeta(1, metric_schema), KStatus::SUCCESS);
    ASSERT_EQ(schema_mgr->GetTagMeta(1, *tag_schema), KStatus::SUCCESS);
  }

  void SimpleInsert();

 public:
  TsEntitySegmentTest() { EngineOptions::mem_segment_max_size = INT32_MAX; }

  ~TsEntitySegmentTest() { KWDBDynamicThreadPool::GetThreadPool().Stop(); }

  void SetUp() override {
    System("rm -rf schema");
    System("rm -rf db001-123");

    mgr = std::make_unique<TsEngineSchemaManager>("schema");
    std::shared_mutex wal_level_mutex;
    TsHashRWLatch tag_lock(EngineOptions::vgroup_max_num * 2 , RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK);
    mgr->Init(nullptr);
    opts.db_path = "db001-123";
    vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    EXPECT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);
  }
};

void TsEntitySegmentTest::SimpleInsert() {
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t total_insert_row_num = 0;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                     DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
  {
    for (int i = 0; i < 10; ++i) {
      TSEntityID dev_id = 1 + i * 123;
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i * 123, 103 + i * 1000, 123, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
      ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
    }

    ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);

    auto entity_segment = partitions[0]->GetEntitySegment();
    ASSERT_NE(entity_segment, nullptr);

    for (int i = 0; i < 10; ++i) {
      {
        // scan [500, INT64_MAX]
        std::vector<STScanRange> spans{{{500, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i);
        int row_idx = 0;
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          std::unique_ptr<TsBitmapBase> bitmap;
          char *ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
          std::vector<char *> col_values;
          col_values.resize(3);
          s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            EXPECT_EQ(block_span->GetTS(idx), 500 + row_idx + idx);
            EXPECT_EQ(*(timestamp64 *)(ts_col + idx * 8), 500 + row_idx + idx);
            EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
            EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
            EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
            kwdbts::DataFlags flag;
            TSSlice data;
            s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
            EXPECT_EQ(s, KStatus::SUCCESS);
            string str(data.data, 10);
            EXPECT_EQ(str, "varstring_");
            ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
          }
          row_idx += block_span->GetRowNum();
        }
        if (i >= 1) {
          EXPECT_EQ(row_idx, (i - 1) * 1000 + 623);
        } else {
          EXPECT_EQ(row_idx, 0);
        }
      }
      {
        // scan [INT64_MIN, 622]
        std::vector<STScanRange> spans{{{INT64_MIN, 622}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i > 0 ? 1 : 0);
        int row_idx = 0;
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          std::unique_ptr<TsBitmapBase> bitmap;
          char *ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          std::vector<char *> col_values;
          col_values.resize(3);
          s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            EXPECT_EQ(block_span->GetTS(idx), 123 + row_idx + idx);
            EXPECT_EQ(*(timestamp64 *)(ts_col + idx * 8), 123 + row_idx + idx);
            EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
            EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
            EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
            kwdbts::DataFlags flag;
            TSSlice data;
            s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
            EXPECT_EQ(s, KStatus::SUCCESS);
            string str(data.data, 10);
            EXPECT_EQ(str, "varstring_");
            ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
          }
          row_idx += block_span->GetRowNum();
        }
        EXPECT_EQ(row_idx, i > 0 ? 500 : 0);
      }
      {
        // scan [INT64_MIN, INT64_MAX]
        std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i);
        int row_idx = 0;
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          std::unique_ptr<TsBitmapBase> bitmap;
          char *ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
          std::vector<char *> col_values;
          col_values.resize(3);
          s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            EXPECT_EQ(block_span->GetTS(idx), 123 + row_idx + idx);
            EXPECT_EQ(*(timestamp64 *)(ts_col + idx * 8), 123 + row_idx + idx);
            EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
            EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
            EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
            kwdbts::DataFlags flag;
            TSSlice data;
            s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
            EXPECT_EQ(s, KStatus::SUCCESS);
            string str(data.data, 10);
            EXPECT_EQ(str, "varstring_");
            ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
          }
          row_idx += block_span->GetRowNum();
        }
        EXPECT_EQ(row_idx, i * EngineOptions::max_rows_per_block);
        entity_row_num += row_idx;
      }
    }

    current = vgroup->CurrentVersion();
    partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    std::vector<std::shared_ptr<TsLastSegment>> result = partitions[0]->GetAllLastSegments();
    ASSERT_EQ(result.size(), 3);
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    for (int j = 0; j < result.size(); ++j) {
      for (int i = 0; i < 10; ++i) {
        std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_span;
        result[j]->GetBlockSpans(filter, block_span, schema_mgr, schema);
        for (auto block : block_span) {
          last_row_num += block->GetRowNum();
        }
      }
    }
    int64_t last_total_row_num = 0;
    for (int j = 0; j < result.size(); ++j) {
      std::list<shared_ptr<TsBlockSpan>> block_span;
      result[j]->GetBlockSpans(block_span, mgr.get());
      for (auto block : block_span) {
        last_total_row_num += block->GetRowNum();
      }
    }
    EXPECT_EQ(last_total_row_num, last_row_num);
    EXPECT_EQ(last_total_row_num, total_insert_row_num - entity_row_num);
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
  }
}

TEST_F(TsEntitySegmentTest, simpleInsertNoBlockCache) {
  TsLRUBlockCache::GetInstance().SetMaxMemorySize(0);
  SimpleInsert();
}

TEST_F(TsEntitySegmentTest, simpleInsertSmallBlockCache) {
  TsLRUBlockCache::GetInstance().SetMaxMemorySize(1024);
  SimpleInsert();
}

TEST_F(TsEntitySegmentTest, simpleInsertMedianBlockCache) {
  TsLRUBlockCache::GetInstance().SetMaxMemorySize(256 * 1024 * 1024);
  SimpleInsert();
}

TEST_F(TsEntitySegmentTest, simpleInsertDefaultBlockCache) {
  TsLRUBlockCache::GetInstance().SetMaxMemorySize(EngineOptions::block_cache_max_size);
  SimpleInsert();
}

TEST_F(TsEntitySegmentTest, simpleInsertLargeBlockCache) {
  TsLRUBlockCache::GetInstance().SetMaxMemorySize((uint64_t)20 * 1024 * 1024 * 1024);
  SimpleInsert();
}

TEST_F(TsEntitySegmentTest, simpleInsertExtraLargeBlockCache) {
  TsLRUBlockCache::GetInstance().SetMaxMemorySize((uint64_t)128 * 1024 * 1024 * 1024);
  SimpleInsert();
}

TEST_F(TsEntitySegmentTest, simpleInsertDoubleCompact) {
  EngineOptions::g_dedup_rule = DedupRule::KEEP;
  EngineOptions::max_compact_num = 20;
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t total_insert_row_num = 0;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  {
    TSTableID table_id1 = 123;
    const std::vector<AttributeInfo>* metric_schema1;
    std::vector<TagInfo> tag_schema1;
    std::shared_ptr<TsTableSchemaManager> schema_mgr1;
    std::vector<DataType> metric_types1{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                        DataType::VARCHAR};
    CreateTable(table_id1, metric_types1, &metric_schema1, &tag_schema1, schema_mgr1);

    TSTableID table_id2 = 124;
    const std::vector<AttributeInfo>* metric_schema2;
    std::vector<TagInfo> tag_schema2;
    std::shared_ptr<TsTableSchemaManager> schema_mgr2;
    std::vector<DataType> metric_types2{DataType::TIMESTAMP, DataType::BIGINT, DataType::VARCHAR};
    CreateTable(table_id2, metric_types2, &metric_schema2, &tag_schema2, schema_mgr2);

    kwdbContext_t ctx;
    EngineOptions opts;
    EngineOptions::mem_segment_max_size = INT32_MAX;
    std::shared_mutex wal_level_mutex;
    TsHashRWLatch tag_lock(EngineOptions::vgroup_max_num * 2 , RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK);
    opts.db_path = "db001-123";
    auto vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    vgroup->Init(&ctx);
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);

    for (int i = 0; i < 10; ++i) {
      TSEntityID dev_id = 1 + i * 123;
      TSTableID table_id = i % 2 == 0 ? table_id1 : table_id2;
      auto metric_schema = i % 2 == 0 ? metric_schema1 : metric_schema2;
      auto tag_schema = i % 2 == 0 ? tag_schema1 : tag_schema2;
      auto schema_mgr = i % 2 == 0 ? schema_mgr1 : schema_mgr2;
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i * 123, 103 + i * 1000, 123, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    }

    ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);
    vgroup->Vacuum(&ctx, false);

    for (int i = 0; i < 10; ++i) {
      TSEntityID dev_id = 1 + i * 123;
      TSTableID table_id = i % 2 == 0 ? table_id1 : table_id2;
      auto metric_schema = i % 2 == 0 ? metric_schema1 : metric_schema2;
      auto tag_schema = i % 2 == 0 ? tag_schema1 : tag_schema2;
      auto schema_mgr = i % 2 == 0 ? schema_mgr1 : schema_mgr2;
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i * 123, 103 + i * 1000, 123, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    }

    ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);

    vgroup->Vacuum(&ctx, false);

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);

    auto entity_segment = partitions[0]->GetEntitySegment();
    ASSERT_NE(entity_segment, nullptr);

    for (int i = 0; i < 10; ++i) {
      TSTableID table_id = i % 2 == 0 ? table_id1 : table_id2;
      auto schema_mgr = i % 2 == 0 ? schema_mgr1 : schema_mgr2;
      auto metric_schema = i % 2 == 0 ? metric_schema1 : metric_schema2;
      auto tag_schema = i % 2 == 0 ? tag_schema1 : tag_schema2;
      {
        // scan [500, INT64_MAX]
        std::vector<STScanRange> spans{{{500, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i * 2);
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          std::unique_ptr<TsBitmapBase> bitmap;
          char *ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
          std::vector<char *> col_values;
          col_values.resize(metric_schema->size() - 1);
          s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
          if (i % 2 == 0) {
            EXPECT_EQ(s, KStatus::SUCCESS);
            s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
            EXPECT_EQ(s, KStatus::SUCCESS);
            s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
            EXPECT_EQ(s, KStatus::SUCCESS);
          }
          uint16_t pre_count;
          if (block_span->HasPreAgg()) {
            block_span->GetPreCount(0, nullptr, pre_count);
            EXPECT_EQ(pre_count, block_span->GetRowNum());
          }
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            if (i % 2 == 0) {
              EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
              EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
              EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
              kwdbts::DataFlags flag;
              TSSlice data;
              s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
              EXPECT_EQ(s, KStatus::SUCCESS);
              string str(data.data, 10);
              EXPECT_EQ(str, "varstring_");
            } else {
              EXPECT_LE(*(int64_t *)(col_values[0] + idx * 8), 10240);
              kwdbts::DataFlags flag;
              TSSlice data;
              s = block_span->GetVarLenTypeColAddr(idx, 2, flag, data);
              EXPECT_EQ(s, KStatus::SUCCESS);
              string str(data.data, 10);
              EXPECT_EQ(str, "varstring_");
            }
          }
        }
      }
      {
        // scan [INT64_MIN, 622]
        std::vector<STScanRange> spans{{{INT64_MIN, 622}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i > 0 ? 2 : 0);
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          std::unique_ptr<TsBitmapBase> bitmap;
          char *ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          std::vector<char *> col_values;
          col_values.resize(metric_schema->size() - 1);
          s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          if (i % 2 == 0) {
            s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
            EXPECT_EQ(s, KStatus::SUCCESS);
            s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
            EXPECT_EQ(s, KStatus::SUCCESS);
          }
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            if (i % 2 == 0) {
              EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
              EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
              EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
              kwdbts::DataFlags flag;
              TSSlice data;
              s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
              EXPECT_EQ(s, KStatus::SUCCESS);
              string str(data.data, 10);
              EXPECT_EQ(str, "varstring_");
            } else {
              EXPECT_LE(*(int64_t *)(col_values[0] + idx * 8), 10240);
              kwdbts::DataFlags flag;
              TSSlice data;
              s = block_span->GetVarLenTypeColAddr(idx, 2, flag, data);
              EXPECT_EQ(s, KStatus::SUCCESS);
              string str(data.data, 10);
              EXPECT_EQ(str, "varstring_");
            }
          }
        }
      }
      {
        // scan [INT64_MIN, INT64_MAX]
        std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i * 2);
        int row_idx = 0;
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          std::unique_ptr<TsBitmapBase> bitmap;
          char *ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
          std::vector<char *> col_values;
          col_values.resize(metric_schema->size() - 1);
          s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          if (i % 2 == 0) {
            s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
            EXPECT_EQ(s, KStatus::SUCCESS);
            s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
            EXPECT_EQ(s, KStatus::SUCCESS);
          }
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            if (i % 2 == 0) {
              EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
              EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
              EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
              kwdbts::DataFlags flag;
              TSSlice data;
              s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
              EXPECT_EQ(s, KStatus::SUCCESS);
              string str(data.data, 10);
              EXPECT_EQ(str, "varstring_");
            } else {
              EXPECT_LE(*(int64_t *)(col_values[0] + idx * 8), 10240);
              kwdbts::DataFlags flag;
              TSSlice data;
              s = block_span->GetVarLenTypeColAddr(idx, 2, flag, data);
              EXPECT_EQ(s, KStatus::SUCCESS);
              string str(data.data, 10);
              EXPECT_EQ(str, "varstring_");
            }
          }
          row_idx += block_span->GetRowNum();
        }
        entity_row_num += row_idx;
      }
    }

    current = vgroup->CurrentVersion();
    partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    std::vector<std::shared_ptr<TsLastSegment>> result = partitions[0]->GetAllLastSegments();
    ASSERT_EQ(result.size(), 6);
    for (int j = 0; j < result.size(); ++j) {
      for (int i = 0; i < 10; ++i) {
        TSTableID table_id = i % 2 == 0 ? table_id1 : table_id2;
        auto schema_mgr = i % 2 == 0 ? schema_mgr1 : schema_mgr2;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_span;
        result[j]->GetBlockSpans(filter, block_span, schema_mgr, schema);
        for (auto block : block_span) {
          last_row_num += block->GetRowNum();
        }
      }
    }
    int64_t last_total_row_num = 0;
    for (int j = 0; j < result.size(); ++j) {
      std::list<shared_ptr<TsBlockSpan>> block_span;
      result[j]->GetBlockSpans(block_span, mgr.get());
      for (auto block : block_span) {
        last_total_row_num += block->GetRowNum();
      }
    }
    EXPECT_EQ(last_total_row_num, last_row_num);
    EXPECT_EQ(last_total_row_num, total_insert_row_num - entity_row_num);
  }
}

TEST_F(TsEntitySegmentTest, TestEntityMinMaxRowNum) {
  EngineOptions::max_rows_per_block = 2000;
  EngineOptions::min_rows_per_block = 1000;

  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                     DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema;
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  std::vector<TSEntityID> dev_ids = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};
  std::vector<int> row_nums = {700, 800, 10, 20, 1500, 1400, 2001, 1999, 4000, 3000};
  {
    for (int i = 0; i < 10; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_ids[i], row_nums[i], 1 + 10000 * i, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_ids[i], &payload, false);
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    }
    ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    auto lastsegments = partitions[0]->GetAllLastSegments();
    EXPECT_EQ(lastsegments.size(), 1);

    auto entity_segment = partitions[0]->GetEntitySegment();
    ASSERT_NE(entity_segment, nullptr);

    struct Expect {
      int nblock_in_entity_segment;
      int row_num_in_entity_segment;
      int row_num_in_last_segment;
    };
    std::vector<Expect> expects{{1, 1500, 0}, {0, 0, 30}, {1, 2000, 900}, {2, 4000, 0}, {4, 7000, 0}};
    for (int i = 0; i < 5; ++i) {
      auto eid = i + 1;
      auto expect = expects[i];
      TsEntityItem entity_item;
      bool is_exist = false;
      ASSERT_EQ(entity_segment->GetEntityItem(eid, entity_item, is_exist), SUCCESS);

      if (is_exist) {
        std::vector<TsEntitySegmentBlockItemWithData> blk_items;
        ASSERT_EQ(entity_segment->GetAllBlockItems(eid, &blk_items), SUCCESS);
        ASSERT_EQ(blk_items.size(), expect.nblock_in_entity_segment);
        int nrow = std::accumulate(blk_items.begin(), blk_items.end(), 0,
                                   [](int sum, TsEntitySegmentBlockItemWithData& blk_item_data) { return sum + blk_item_data.block_item->n_rows; });
        EXPECT_EQ(nrow, expect.row_num_in_entity_segment);
      }

      auto last_segment = lastsegments[0];
      TsBlockItemFilterParams filter;
      filter.table_id = table_id;
      filter.db_id = 1;
      filter.entity_id = eid;
      filter.spans_ = {{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
      std::list<std::shared_ptr<TsBlockSpan>> spans;
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(schema_mgr->GetMetricSchema(0, &schema), KStatus::SUCCESS);
      auto s = last_segment->GetBlockSpans(filter, spans, schema_mgr, schema);
      auto nrow = std::accumulate(spans.begin(), spans.end(), 0,
                                  [](int sum, std::shared_ptr<TsBlockSpan> span) { return sum + span->GetRowNum(); });
      EXPECT_EQ(nrow, expect.row_num_in_last_segment);
      ASSERT_EQ(s, SUCCESS);
    }
  }
}

TEST_F(TsEntitySegmentTest, simpleCount) {
  EngineOptions::g_dedup_rule = DedupRule::KEEP;
  EngineOptions::max_compact_num = 20;
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t total_insert_row_num = 0;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  {
    TSTableID table_id = 123;
    const std::vector<AttributeInfo>* metric_schema;
    std::vector<TagInfo> tag_schema;
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                        DataType::VARCHAR};
    CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

    kwdbContext_t ctx;
    EngineOptions opts;
    EngineOptions::mem_segment_max_size = INT32_MAX;
    std::shared_mutex wal_level_mutex;
    TsHashRWLatch tag_lock(EngineOptions::vgroup_max_num * 2 , RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK);
    opts.db_path = "db001-123";
    auto vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    EXPECT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);

    for (int i = 1; i <= 10; ++i) {
      TSEntityID dev_id = i;
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 11 + i * 1000, 100, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
    }

    for (int i = 1; i <= 10; ++i) {
      TSEntityID dev_id = i;
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 11 + i * 1000, 10000 * 86400, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
    }
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);

    uint64_t sum = 0;
    uint64_t count_sum = 0;

    for (auto partition : partitions) {
      for (int i = 1; i <= 10; ++i) {
        // scan [INT64_MIN, INT64_MAX]
        uint64_t entity_num = 0;
        std::vector<KwTsSpan> ts_spans = {{INT64_MIN, INT64_MAX}};
        TsScanFilterParams filter{1, table_id, vgroup->GetVGroupID(), (TSEntityID)i, DATATYPE::TIMESTAMP64,
                                  UINT64_MAX, ts_spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        auto s = partition->GetBlockSpans(filter, &block_spans, schema_mgr, schema);
        EXPECT_EQ(s, KStatus::SUCCESS);
        int row_idx = 0;
        while (!block_spans.empty()) {
          auto block_span = block_spans.front();
          entity_num += block_span->GetRowNum();
          block_spans.pop_front();
        }
        sum += entity_num;
        ASSERT_EQ(entity_num, 11 + i * 1000);
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = (TSEntityID)i;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(s, KStatus::SUCCESS);
        ASSERT_TRUE(count_header.is_count_valid);
        ASSERT_EQ(count_header.valid_count, 11 + i * 1000);
        count_sum += count_header.valid_count;
      }
    }
    EXPECT_EQ(total_insert_row_num, sum);
    EXPECT_EQ(total_insert_row_num, count_sum);
  }
}

// for concurrent crash issue: ICXWWD
TEST_F(TsEntitySegmentTest, concurrentLRUBlockCacheAccess) {
  EngineOptions::block_cache_max_size = 1024 * 1024 * 1024;
  TsLRUBlockCache::GetInstance().unit_test_enabled = true;
  Defer defer([&]() { TsLRUBlockCache::GetInstance().unit_test_enabled = false; });
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                     DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);
  {
    for (int i = 0; i < 10; ++i) {
      TSEntityID dev_id = 1 + i * 123;
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i * 123, 103 + i * 1000, 123, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{metric_schema};
      p.ParsePayLoadStruct(payload);
      auto ptag = p.GetPrimaryTag();

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    }

    ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);

    auto entity_segment = partitions[0]->GetEntitySegment();
    ASSERT_NE(entity_segment, nullptr);

    auto AccessEntityBlock = [&]() {
      int entity_id = 124;
      // scan [500, INT64_MAX]
      std::vector<STScanRange> spans{{{500, INT64_MAX}, {0, UINT64_MAX}}};
      TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)entity_id, spans};
      std::list<shared_ptr<TsBlockSpan>> block_spans;
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
      EXPECT_EQ(s, KStatus::SUCCESS);
      EXPECT_EQ(block_spans.size(), 1);
    };

    std::shared_ptr<std::thread> entity_block_accessor;
    entity_block_accessor = std::make_shared<std::thread>(AccessEntityBlock);

    while (TsLRUBlockCache::GetInstance().unit_test_phase != TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_FIRST_INITIALIZING) {
      usleep(1000);
    }

    int entity_id = 124;
    // scan [500, INT64_MAX]
    std::vector<STScanRange> spans{{{500, INT64_MAX}, {0, UINT64_MAX}}};
    TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)entity_id, spans};
    std::list<shared_ptr<TsBlockSpan>> block_spans;
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(block_spans.size(), 1);

    entity_block_accessor->join();
  }
}

// for accessing column block crash issue: ZDP-49328
TEST_F(TsEntitySegmentTest, columnBlockCrashTest) {
  EngineOptions::block_cache_max_size = 1024 * 1024 * 1024;
  TsLRUBlockCache::GetInstance().unit_test_enabled = true;
  TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::COLUMN_BLOCK_CRASH_PHASE_NONE;
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                     DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);
  for (int i = 0; i < 10; ++i) {
    TSEntityID dev_id = 1 + i * 123;
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i * 123, 103 + i * 1000, 123, 1);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{metric_schema};
    p.ParsePayLoadStruct(payload);
    auto ptag = p.GetPrimaryTag();

    vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
    free(payload.data);
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);

  auto current = vgroup->CurrentVersion();
  auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1);

  auto entity_segment = partitions[0]->GetEntitySegment();
  ASSERT_NE(entity_segment, nullptr);
  {
    auto AccessColumnBlock = [&]() {
      // scan [INT64_MIN, INT64_MAX]
      int entity_id = 124;
      std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
      TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(entity_id), spans};
      std::list<shared_ptr<TsBlockSpan>> block_spans;
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
      EXPECT_EQ(s, KStatus::SUCCESS);
      EXPECT_EQ(block_spans.size(), 1);
      int row_idx = 0;
      ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
      while (!block_spans.empty()) {
        auto block_span = block_spans.front();
        block_spans.pop_front();
        std::unique_ptr<TsBitmapBase> bitmap;
        char *ts_col;
        s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
        std::vector<char *> col_values;
        col_values.resize(3);
        s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
        EXPECT_EQ(s, KStatus::SUCCESS);
        s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
        EXPECT_EQ(s, KStatus::SUCCESS);
        s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
        EXPECT_EQ(s, KStatus::SUCCESS);
        for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
          EXPECT_EQ(block_span->GetTS(idx), 123 + row_idx + idx);
          EXPECT_EQ(*(timestamp64 *)(ts_col + idx * 8), 123 + row_idx + idx);
          EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
          EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
          EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
          kwdbts::DataFlags flag;
          TSSlice data;
          s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
          EXPECT_EQ(s, KStatus::SUCCESS);
          string str(data.data, 10);
          EXPECT_EQ(str, "varstring_");
          ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
        }
        row_idx += block_span->GetRowNum();
      }
      EXPECT_EQ(row_idx, EngineOptions::max_rows_per_block);
    };

    std::shared_ptr<std::thread> column_block_accessor;
    column_block_accessor = std::make_shared<std::thread>(AccessColumnBlock);

    while (TsLRUBlockCache::GetInstance().unit_test_phase != TsLRUBlockCache::UNIT_TEST_PHASE::COLUMN_BLOCK_CRASH_PHASE_FIRST_INITIALIZING) {
      usleep(1000);
    }

    // scan [INT64_MIN, INT64_MAX]
    int entity_id = 124;
    std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
    TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(entity_id), spans};
    std::list<shared_ptr<TsBlockSpan>> block_spans;
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(block_spans.size(), 1);
    int row_idx = 0;
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
    while (!block_spans.empty()) {
      auto block_span = block_spans.front();
      block_spans.pop_front();
      std::unique_ptr<TsBitmapBase> bitmap;
      char *ts_col;
      s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
      TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE;
      std::vector<char *> col_values;
      col_values.resize(3);
      s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
      EXPECT_EQ(s, KStatus::SUCCESS);
      s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
      EXPECT_EQ(s, KStatus::SUCCESS);
      s = block_span->GetFixLenColAddr(3, &col_values[2], &bitmap);
      EXPECT_EQ(s, KStatus::SUCCESS);
      for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
        EXPECT_EQ(block_span->GetTS(idx), 123 + row_idx + idx);
        EXPECT_EQ(*(timestamp64 *)(ts_col + idx * 8), 123 + row_idx + idx);
        EXPECT_LE(*(int32_t *)(col_values[0] + idx * 4), 1024);
        EXPECT_LE(*(double *)(col_values[1] + idx * 8), 1024 * 1024);
        EXPECT_LE(*(int64_t *)(col_values[2] + idx * 8), 10240);
        kwdbts::DataFlags flag;
        TSSlice data;
        s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
        EXPECT_EQ(s, KStatus::SUCCESS);
        string str(data.data, 10);
        EXPECT_EQ(str, "varstring_");
        ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
      }
      row_idx += block_span->GetRowNum();
    }
    EXPECT_EQ(row_idx, EngineOptions::max_rows_per_block);

    column_block_accessor->join();
  }
}

// for accessing var column block issue
TEST_F(TsEntitySegmentTest, varColumnBlockTest) {
  EngineOptions::block_cache_max_size = 1024 * 1024 * 1024;
  TsLRUBlockCache::GetInstance().unit_test_enabled = true;
  TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_NONE;
  Defer defer([&]() {
    if (TsLRUBlockCache::GetInstance().unit_test_phase == TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_ONE_DONE) {
      TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_GET_VAR_COL_ADDR_DONE;
      while (TsLRUBlockCache::GetInstance().unit_test_phase !=
              TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE) {
        usleep(1000);
      }
    }
    if (TsLRUBlockCache::GetInstance().unit_test_phase == TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE) {
      TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE;
    }
  });

  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                     DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);
  ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
  for (int i = 0; i < 10; ++i) {
    TSEntityID dev_id = 1 + i * 123;
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i * 123, 103 + i * 1000, 123, 1);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{metric_schema};
    p.ParsePayLoadStruct(payload);
    auto ptag = p.GetPrimaryTag();

    vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
    free(payload.data);
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);

  auto current = vgroup->CurrentVersion();
  auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1);

  auto entity_segment = partitions[0]->GetEntitySegment();
  ASSERT_NE(entity_segment, nullptr);
  {
    auto AccessVarColumnBlock = [&]() {
      // scan [INT64_MIN, INT64_MAX]
      int entity_id = 124;
      std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
      TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(entity_id), spans};
      std::list<shared_ptr<TsBlockSpan>> block_spans;
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
      EXPECT_EQ(s, KStatus::SUCCESS);
      EXPECT_EQ(block_spans.size(), 1);
      int row_idx = 0;
      ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
      while (!block_spans.empty()) {
        auto block_span = block_spans.front();
        block_spans.pop_front();
        std::unique_ptr<TsBitmapBase> bitmap;
        for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
          kwdbts::DataFlags flag;
          TSSlice data;
          s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
          EXPECT_EQ(s, KStatus::SUCCESS);
          string str(data.data, 10);
          EXPECT_EQ(str, "varstring_");
          ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
        }
        row_idx += block_span->GetRowNum();
      }
      EXPECT_EQ(row_idx, EngineOptions::max_rows_per_block);
    };

    std::shared_ptr<std::thread> var_column_block_accessor;
    var_column_block_accessor = std::make_shared<std::thread>(AccessVarColumnBlock);

    while (TsLRUBlockCache::GetInstance().unit_test_phase != TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_ONE_DONE) {
      usleep(1000);
    }

    // scan [INT64_MIN, INT64_MAX]
    int entity_id = 124;
    std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
    TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), (TSEntityID)(entity_id), spans};
    std::list<shared_ptr<TsBlockSpan>> block_spans;
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    auto s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(block_spans.size(), 1);
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
    auto block_span = block_spans.front();
    block_spans.pop_front();
    std::unique_ptr<TsBitmapBase> bitmap;
    std::vector<char *> col_values;
    kwdbts::DataFlags flag;
    TSSlice data;
    s = block_span->GetVarLenTypeColAddr(0, 4, flag, data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
    if (TsLRUBlockCache::GetInstance().unit_test_phase !=
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE) {
      TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_GET_VAR_COL_ADDR_DONE;
      while (TsLRUBlockCache::GetInstance().unit_test_phase !=
          TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE) {
        usleep(1000);
      }
    }
    string str(data.data, 10);
    EXPECT_EQ(str, "varstring_");
    TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE;
    ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);

    var_column_block_accessor->join();
  }
}

TEST_F(TsEntitySegmentTest, varColumnCompression) {
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::VARCHAR};
  const std::vector<AttributeInfo> *metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);
  TSEntityID dev_id = 12306;
  for (int k = 0; k < 10; ++k) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 1000, 123 + k * 1000, 1);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{metric_schema};
    p.ParsePayLoadStruct(payload);
    auto ptag = p.GetPrimaryTag();
    vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false);
    free(payload.data);
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }
  ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);
  auto current = vgroup->CurrentVersion();
  auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1);

  auto entity_segment = partitions[0]->GetEntitySegment();
  ASSERT_NE(entity_segment, nullptr);

  auto root = entity_segment->GetPath();
  auto info = entity_segment->GetHandleInfo();

  auto path = root + "/" + DataBlockFileName(info.datablock_info.file_number);
  int fd = open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  uint32_t offset[3];
  ASSERT_GE(pread(fd, &offset, sizeof(offset), sizeof(TsAggAndBlockFileHeader)), 0);

  uint32_t compressed_len;
  ASSERT_GE(pread(fd, &compressed_len, sizeof(compressed_len),
                  offset[1] + 3 * sizeof(uint32_t) + 1 + sizeof(TsAggAndBlockFileHeader)),
            0);
  EXPECT_EQ(compressed_len, 4 + 1 + 1 + 8);
  close(fd);
}