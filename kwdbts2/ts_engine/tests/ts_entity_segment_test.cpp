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

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <numeric>
#include <string>
#include <thread>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_batch_data_worker.h"
#include "ts_block.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_db_schema_manager.h"
#include "ts_entity_segment_builder.h"
#include "ts_entity_segment_data.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_mem_segment_mgr.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"
#include "ts_vgroup.h"
#include "ts_lru_block_cache.h"

using namespace kwdbts;  // NOLINT
using namespace roachpb;

class TsEntitySegmentTest : public ::testing::Test {
 protected:
  std::unique_ptr<TsDBSchemaManager> db_schema_mgr = nullptr;
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
  static void SetUpTestCase() {
    KWDBDynamicThreadPool::GetThreadPool().InitImplicitly();
  }

  static void TearDownTestCase() {
    auto& pool = KWDBDynamicThreadPool::GetThreadPool();
    if (!pool.IsStop()) {
      pool.Stop();
    }
    KWDBDynamicThreadPool::Destroy();
  }

  TsEntitySegmentTest() {
    EngineOptions::mem_segment_max_size = INT32_MAX;
  }

  ~TsEntitySegmentTest() override = default;

  void SetUp() override {
    System("rm -rf db");
    System("rm -rf schema");
    System("rm -rf db001-123");

    db_schema_mgr = std::make_unique<TsDBSchemaManager>(".");
    mgr = std::make_unique<TsEngineSchemaManager>("schema", db_schema_mgr.get());
    std::shared_mutex wal_level_mutex;
    TsHashRWLatch tag_lock(EngineOptions::vgroup_max_num * 2 , RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK);
    mgr->Init(nullptr);
    db_schema_mgr->Init(mgr.get());
    opts.db_path = "db001-123";
    vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    EXPECT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);
  }
};

void TsEntitySegmentTest::SimpleInsert() {
  EngineOptions::max_rows_per_block = 20000;
  EngineOptions::min_rows_per_block = 20000;
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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
      ASSERT_EQ(TsLRUBlockCache::GetInstance().VerifyCacheMemorySize(), true);
    }

    EngineOptions::max_rows_per_block = 1000;
    EngineOptions::min_rows_per_block = 1000;
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

// Verifies TsEntityBlock::CreateFromCompressedSpan reconstructs a block whose rows are identical to
// the on-disk entity block it was encoded from. This is the decode step used by the snapshot write
// path to route small-row batches into the last segment. The factory consumes the batch wire
// format (BlockSpanHeader + entity-format compressed block); it only reads past the header, so the
// header region is zero-filled and header fields are supplied via TsEntityBlockSpanMeta. Exercises
// the block_version-aware decompression (LoadColData) for fixed- and var-length columns.
TEST_F(TsEntitySegmentTest, CreateFromCompressedSpanRoundTrip) {
  EngineOptions::min_rows_per_block = 100;
  EngineOptions::max_rows_per_block = 1000;

  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE,
                                     DataType::BIGINT, DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  // Insert one entity with enough rows to form an entity segment block (>= min_rows_per_block).
  TSEntityID dev_id = 7;
  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 100, 100, 1);
  TsRawPayload p{metric_schema};
  p.ParsePayLoadStruct(payload);
  auto ptag = p.GetPrimaryTag();
  ASSERT_EQ(vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false), KStatus::SUCCESS);
  free(payload.data);
  ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);

  std::shared_ptr<MMapMetricsTable> scan_schema;
  ASSERT_EQ(schema_mgr->GetMetricSchema(1, &scan_schema), KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_attrs = scan_schema->getSchemaInfoExcludeDroppedPtr();

  // Read the flushed entity block back as a span.
  auto current = vgroup->CurrentVersion();
  auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1u);
  auto entity_segment = partitions[0]->GetEntitySegment();
  ASSERT_NE(entity_segment, nullptr);
  std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
  TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), dev_id, spans};
  std::list<shared_ptr<TsBlockSpan>> block_spans;
  ASSERT_EQ(entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, scan_schema), KStatus::SUCCESS);
  ASSERT_EQ(block_spans.size(), 1u);
  auto orig_span = block_spans.front();
  int n_rows = orig_span->GetRowNum();
  ASSERT_GT(n_rows, 0);

  // Snapshot the original column values for later comparison.
  std::vector<timestamp64> orig_ts(n_rows);
  std::vector<int32_t> orig_int(n_rows);
  std::vector<double> orig_double(n_rows);
  std::vector<int64_t> orig_bigint(n_rows);
  std::vector<std::string> orig_varchar(n_rows);
  for (int idx = 0; idx < n_rows; ++idx) {
    std::unique_ptr<TsBitmapBase> bitmap;
    char* col = nullptr;
    ASSERT_EQ(orig_span->GetFixLenColAddr(0, &col, &bitmap), KStatus::SUCCESS);
    orig_ts[idx] = *reinterpret_cast<timestamp64*>(col + idx * sizeof(timestamp64));
    ASSERT_EQ(orig_span->GetFixLenColAddr(1, &col, &bitmap), KStatus::SUCCESS);
    orig_int[idx] = *reinterpret_cast<int32_t*>(col + idx * sizeof(int32_t));
    ASSERT_EQ(orig_span->GetFixLenColAddr(2, &col, &bitmap), KStatus::SUCCESS);
    orig_double[idx] = *reinterpret_cast<double*>(col + idx * sizeof(double));
    ASSERT_EQ(orig_span->GetFixLenColAddr(3, &col, &bitmap), KStatus::SUCCESS);
    orig_bigint[idx] = *reinterpret_cast<int64_t*>(col + idx * sizeof(int64_t));
    DataFlags flag;
    TSSlice data;
    ASSERT_EQ(orig_span->GetVarLenTypeColAddr(idx, 4, flag, data), KStatus::SUCCESS);
    orig_varchar[idx] = std::string(data.data, data.len);
  }

  // Build the batch wire-format payload: BlockSpanHeader (unread by the factory) + the entity
  // compressed block bytes produced by the read path (exactly what TsBatchData carries).
  TsBufferBuilder compressed;
  ASSERT_EQ(orig_span->GetCompressData(&compressed), KStatus::SUCCESS);
  size_t header_size = TsBatchData::block_span_data_header_size_;
  std::string blob;
  blob.assign(header_size, '\0');
  TSSlice comp = compressed.AsSlice();
  blob.append(comp.data, comp.len);

  TsEntityBlockSpanMeta meta;
  meta.n_cols = metric_attrs->size() + 1;
  meta.n_rows = n_rows;
  meta.block_version = orig_span->GetBlockVersion();
  meta.min_ts = orig_span->GetFirstTS();
  meta.max_ts = orig_span->GetLastTS();
  meta.first_osn = orig_span->GetFirstOSN();
  meta.last_osn = orig_span->GetLastOSN();
  orig_span->GetMinAndMaxOSN(meta.min_osn, meta.max_osn);

  std::shared_ptr<TsEntityBlock> decoded;
  TSSlice block_span_data{blob.data(), blob.size()};
  ASSERT_EQ(TsEntityBlock::CreateFromCompressedSpan(table_id, dev_id, orig_span->GetTableVersion(), meta,
                                                     header_size, block_span_data, metric_attrs, decoded),
            KStatus::SUCCESS);
  ASSERT_NE(decoded, nullptr);

  // Wrap in a span (scan version == block table version -> no conversion) and compare every column.
  std::shared_ptr<TsBlockSpan> decoded_span;
  ASSERT_EQ(TsBlockSpan::MakeNewBlockSpan(nullptr, vgroup->GetVGroupID(), dev_id, decoded, 0, n_rows, scan_schema,
                                           schema_mgr, decoded_span),
            KStatus::SUCCESS);
  ASSERT_EQ(decoded_span->GetRowNum(), n_rows);
  for (int idx = 0; idx < n_rows; ++idx) {
    EXPECT_EQ(decoded_span->GetTS(idx), orig_ts[idx]);
    std::unique_ptr<TsBitmapBase> bitmap;
    char* col = nullptr;
    EXPECT_EQ(decoded_span->GetFixLenColAddr(0, &col, &bitmap), KStatus::SUCCESS);
    EXPECT_EQ(*reinterpret_cast<timestamp64*>(col + idx * sizeof(timestamp64)), orig_ts[idx]);
    EXPECT_EQ(decoded_span->GetFixLenColAddr(1, &col, &bitmap), KStatus::SUCCESS);
    EXPECT_EQ(*reinterpret_cast<int32_t*>(col + idx * sizeof(int32_t)), orig_int[idx]);
    EXPECT_EQ(decoded_span->GetFixLenColAddr(2, &col, &bitmap), KStatus::SUCCESS);
    EXPECT_EQ(*reinterpret_cast<double*>(col + idx * sizeof(double)), orig_double[idx]);
    EXPECT_EQ(decoded_span->GetFixLenColAddr(3, &col, &bitmap), KStatus::SUCCESS);
    EXPECT_EQ(*reinterpret_cast<int64_t*>(col + idx * sizeof(int64_t)), orig_bigint[idx]);
    DataFlags flag;
    TSSlice data;
    EXPECT_EQ(decoded_span->GetVarLenTypeColAddr(idx, 4, flag, data), KStatus::SUCCESS);
    EXPECT_EQ(std::string(data.data, data.len), orig_varchar[idx]);
  }
}

// Verifies the snapshot write routing: a snapshot batch is committed to a new last segment (and
// remains readable) instead of becoming an entity segment block. Exercises
// TsVGroup::WriteBatchData -> WriteBatchToLastSegment -> GetLastSegmentBuilder -> PutBlockSpan, and
// FinishWriteBatchData's last-segment finalize + version registration.
TEST_F(TsEntitySegmentTest, WriteBatchRoutesToLastSegment) {
  // Phase 1: build an entity block (min=1 so the 50 rows form one entity block) and capture its
  // compressed bytes + header metadata.
  EngineOptions::min_rows_per_block = 1;
  EngineOptions::max_rows_per_block = 1000;

  TSTableID table_id = 456;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE,
                                     DataType::BIGINT, DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  TSEntityID dev_id = 9;
  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 50, 200, 1);
  TsRawPayload p{metric_schema};
  p.ParsePayLoadStruct(payload);
  auto ptag = p.GetPrimaryTag();
  ASSERT_EQ(vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false), KStatus::SUCCESS);
  free(payload.data);
  ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);

  std::shared_ptr<MMapMetricsTable> scan_schema;
  ASSERT_EQ(schema_mgr->GetMetricSchema(1, &scan_schema), KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_attrs = scan_schema->getSchemaInfoExcludeDroppedPtr();

  auto current = vgroup->CurrentVersion();
  auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1u);
  // 50 rows with min_rows_per_block=1 all land in the entity segment; no last segment yet.
  ASSERT_TRUE(partitions[0]->GetAllLastSegments().empty());
  auto entity_segment = partitions[0]->GetEntitySegment();
  ASSERT_NE(entity_segment, nullptr);
  std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
  TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), dev_id, spans};
  std::list<shared_ptr<TsBlockSpan>> block_spans;
  ASSERT_EQ(entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, scan_schema), KStatus::SUCCESS);
  ASSERT_EQ(block_spans.size(), 1u);
  auto orig_span = block_spans.front();
  int n_rows = orig_span->GetRowNum();

  TsBufferBuilder compressed;
  ASSERT_EQ(orig_span->GetCompressData(&compressed), KStatus::SUCCESS);
  TsEntityBlockSpanMeta meta;
  meta.n_cols = metric_attrs->size() + 1;
  meta.n_rows = n_rows;
  meta.block_version = orig_span->GetBlockVersion();
  meta.min_ts = orig_span->GetFirstTS();
  meta.max_ts = orig_span->GetLastTS();
  meta.first_osn = orig_span->GetFirstOSN();
  meta.last_osn = orig_span->GetLastOSN();
  orig_span->GetMinAndMaxOSN(meta.min_osn, meta.max_osn);

  // Phase 2: feed the compressed bytes back as a snapshot batch via the public batch-write
  // entrypoint. WriteBatchData routes every non-empty snapshot/restore batch to the last segment.
  size_t header_size = TsBatchData::block_span_data_header_size_;
  std::string blob;
  blob.assign(header_size, '\0');
  EncodeFixedTimestamp64(blob.data() + TsBatchData::min_ts_offset_in_span_data_, meta.min_ts);
  EncodeFixedTimestamp64(blob.data() + TsBatchData::max_ts_offset_in_span_data_, meta.max_ts);
  EncodeFixed64(blob.data() + TsBatchData::min_osn_offset_in_span_data_, meta.min_osn);
  EncodeFixed64(blob.data() + TsBatchData::max_osn_offset_in_span_data_, meta.max_osn);
  EncodeFixed64(blob.data() + TsBatchData::first_osn_offset_in_span_data_, meta.first_osn);
  EncodeFixed64(blob.data() + TsBatchData::last_osn_offset_in_span_data_, meta.last_osn);
  EncodeFixed32(blob.data() + TsBatchData::n_cols_offset_in_span_data_, meta.n_cols);
  EncodeFixed32(blob.data() + TsBatchData::n_rows_offset_in_span_data_, meta.n_rows);
  EncodeFixed32(blob.data() + TsBatchData::block_version_offset_in_span_data_, meta.block_version);
  TSSlice comp = compressed.AsSlice();
  blob.append(comp.data, comp.len);

  timestamp64 p_time = convertTsToPTime(meta.min_ts, static_cast<DATATYPE>((*metric_attrs)[0].type));
  TSSlice data{blob.data(), blob.size()};
  TsVGroup::LastSegBuilderMap builders;
  ASSERT_EQ(vgroup->WriteBatchData(table_id, 1, dev_id, p_time, 2, data, TsDataSource::Snapshot, builders),
            KStatus::SUCCESS);
  ASSERT_EQ(vgroup->FinishWriteBatchData(builders), KStatus::SUCCESS);

  // A new last segment must be committed (registered in the version) and contain the routed rows.
  current = vgroup->CurrentVersion();
  partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1u);
  auto lastsegs = partitions[0]->GetAllLastSegments();
  ASSERT_EQ(lastsegs.size(), 1u);
  std::list<shared_ptr<TsBlockSpan>> last_spans;
  ASSERT_EQ(lastsegs[0]->GetBlockSpans(last_spans, mgr.get()), KStatus::SUCCESS);
  int last_rows = 0;
  for (auto& bs : last_spans) {
    last_rows += bs->GetRowNum();
  }
  EXPECT_EQ(last_rows, n_rows);
}

// Regression test for the removal of EnsurePartitionBusyForBatch: a batch write (which only
// appends a new last-segment file) must be safe to run concurrently with compaction. Previously
// the partition was marked BatchDataWriting so PartitionCompact skipped it; that exclusion is
// gone, and this test exercises the real overlap - Compact() compacts existing last segments
// while WriteBatchData builds a new one - asserting no deadlock/crash and no row loss.
TEST_F(TsEntitySegmentTest, BatchWriteConcurrentWithCompact) {
  EngineOptions::min_rows_per_block = 1;
  EngineOptions::max_rows_per_block = 1000;

  TSTableID table_id = 789;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE,
                                     DataType::BIGINT, DataType::VARCHAR};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  // Insert one entity and flush, leaving its rows in the entity segment.
  auto insert_flush = [&](TSEntityID dev_id, int n_rows, timestamp64 start_ts) -> KStatus {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, n_rows, start_ts, 1);
    TsRawPayload p{metric_schema};
    p.ParsePayLoadStruct(payload);
    auto ptag = p.GetPrimaryTag();
    auto s = vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
    free(payload.data);
    if (s != KStatus::SUCCESS) return s;
    return vgroup->Flush();
  };

  // Snapshot an entity's flushed entity-segment block into a snapshot batch blob (the same wire
  // format WriteBatchData consumes). Returns the block row count, or -1 on failure.
  auto build_batch_blob = [&](TSEntityID dev_id, std::string& blob, timestamp64& p_time_out) -> int {
    std::shared_ptr<MMapMetricsTable> scan_schema;
    if (schema_mgr->GetMetricSchema(1, &scan_schema) != KStatus::SUCCESS) return -1;
    const std::vector<AttributeInfo>* metric_attrs = scan_schema->getSchemaInfoExcludeDroppedPtr();
    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    if (partitions.size() != 1u) return -1;
    auto entity_segment = partitions[0]->GetEntitySegment();
    if (entity_segment == nullptr) return -1;
    std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
    TsBlockItemFilterParams filter{0, table_id, vgroup->GetVGroupID(), dev_id, spans};
    std::list<shared_ptr<TsBlockSpan>> block_spans;
    if (entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, scan_schema) != KStatus::SUCCESS) {
      return -1;
    }
    if (block_spans.empty()) return -1;
    auto orig_span = block_spans.front();
    int n_rows = orig_span->GetRowNum();
    TsBufferBuilder compressed;
    if (orig_span->GetCompressData(&compressed) != KStatus::SUCCESS) return -1;
    TsEntityBlockSpanMeta meta;
    meta.n_cols = metric_attrs->size() + 1;
    meta.n_rows = n_rows;
    meta.block_version = orig_span->GetBlockVersion();
    meta.min_ts = orig_span->GetFirstTS();
    meta.max_ts = orig_span->GetLastTS();
    meta.first_osn = orig_span->GetFirstOSN();
    meta.last_osn = orig_span->GetLastOSN();
    orig_span->GetMinAndMaxOSN(meta.min_osn, meta.max_osn);
    size_t header_size = TsBatchData::block_span_data_header_size_;
    blob.assign(header_size, '\0');
    EncodeFixedTimestamp64(blob.data() + TsBatchData::min_ts_offset_in_span_data_, meta.min_ts);
    EncodeFixedTimestamp64(blob.data() + TsBatchData::max_ts_offset_in_span_data_, meta.max_ts);
    EncodeFixed64(blob.data() + TsBatchData::min_osn_offset_in_span_data_, meta.min_osn);
    EncodeFixed64(blob.data() + TsBatchData::max_osn_offset_in_span_data_, meta.max_osn);
    EncodeFixed64(blob.data() + TsBatchData::first_osn_offset_in_span_data_, meta.first_osn);
    EncodeFixed64(blob.data() + TsBatchData::last_osn_offset_in_span_data_, meta.last_osn);
    EncodeFixed32(blob.data() + TsBatchData::n_cols_offset_in_span_data_, meta.n_cols);
    EncodeFixed32(blob.data() + TsBatchData::n_rows_offset_in_span_data_, meta.n_rows);
    EncodeFixed32(blob.data() + TsBatchData::block_version_offset_in_span_data_, meta.block_version);
    TSSlice comp = compressed.AsSlice();
    blob.append(comp.data, comp.len);
    p_time_out = convertTsToPTime(meta.min_ts, static_cast<DATATYPE>((*metric_attrs)[0].type));
    return n_rows;
  };

  // Setup: batch-write dev_id=9 so a level-0 last segment exists for the concurrent Compact()
  // to actually compact (otherwise Compact skips for lack of last segments).
  ASSERT_EQ(insert_flush(9, 50, 1000), KStatus::SUCCESS);
  std::string setup_blob;
  timestamp64 setup_ptime = 0;
  int setup_rows = build_batch_blob(9, setup_blob, setup_ptime);
  ASSERT_GT(setup_rows, 0);
  {
    TSSlice data{setup_blob.data(), setup_blob.size()};
    TsVGroup::LastSegBuilderMap setup_builders;
    ASSERT_EQ(vgroup->WriteBatchData(table_id, 1, 9, setup_ptime, 2, data, TsDataSource::Snapshot,
                                     setup_builders),
              KStatus::SUCCESS);
    ASSERT_EQ(vgroup->FinishWriteBatchData(setup_builders), KStatus::SUCCESS);
  }

  // Concurrent batch target: snapshot dev_id=10 for the batch that will race with Compact().
  ASSERT_EQ(insert_flush(10, 50, 2000), KStatus::SUCCESS);
  std::string batch_blob;
  timestamp64 batch_ptime = 0;
  int batch_rows = build_batch_blob(10, batch_blob, batch_ptime);
  ASSERT_GT(batch_rows, 0);

  // Concurrent phase: WriteBatchData opens a last-segment builder (no Finish yet) while
  // Compact() compacts the existing last segments. A barrier starts both threads together.
  KStatus batch_status = KStatus::FAIL;
  KStatus compact_status = KStatus::FAIL;
  TsVGroup::LastSegBuilderMap batch_builders;
  std::atomic<int> ready{0};
  std::thread batch_thread([&]() {
    ready.fetch_add(1, std::memory_order_seq_cst);
    while (ready.load(std::memory_order_seq_cst) < 2) {
      // Spin-wait until both threads reach the barrier so the operations overlap.
    }
    TSSlice data{batch_blob.data(), batch_blob.size()};
    batch_status = vgroup->WriteBatchData(table_id, 1, 10, batch_ptime, 2, data, TsDataSource::Snapshot,
                                          batch_builders);
  });
  std::thread compact_thread([&]() {
    ready.fetch_add(1, std::memory_order_seq_cst);
    while (ready.load(std::memory_order_seq_cst) < 2) {
      // Spin-wait until both threads reach the barrier so the operations overlap.
    }
    compact_status = vgroup->Compact();
  });
  batch_thread.join();
  compact_thread.join();

  // Both must finish without deadlock/crash. Compact() returns SUCCESS whether it compacted or
  // skipped; the point is it must not skip *because of* a concurrent batch write any more.
  EXPECT_EQ(batch_status, KStatus::SUCCESS);
  EXPECT_EQ(compact_status, KStatus::SUCCESS);

  ASSERT_EQ(vgroup->FinishWriteBatchData(batch_builders), KStatus::SUCCESS);

  // Verify no rows were lost: the setup last segment (setup_rows) and the concurrent batch
  // (batch_rows) must both still be readable across the (possibly compacted) last segments.
  auto current = vgroup->CurrentVersion();
  auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
  ASSERT_EQ(partitions.size(), 1u);
  int total_last_rows = 0;
  for (auto& ls : partitions[0]->GetAllLastSegments()) {
    std::list<shared_ptr<TsBlockSpan>> spans;
    ASSERT_EQ(ls->GetBlockSpans(spans, mgr.get()), KStatus::SUCCESS);
    for (auto& bs : spans) {
      total_last_rows += bs->GetRowNum();
    }
  }
  EXPECT_GE(total_last_rows, setup_rows + batch_rows);
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
  EngineOptions::g_dedup_rule = DedupRule::KEEP_EXPERIMENTAL;
  EngineOptions::max_compact_num = 20;
  EngineOptions::max_rows_per_block = 20000;
  EngineOptions::min_rows_per_block = 20000;
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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    }

    EngineOptions::max_rows_per_block = 1000;
    EngineOptions::min_rows_per_block = 1000;

    ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);
    vgroup->Vacuum(&ctx, false);

    EngineOptions::max_rows_per_block = 20000;
    EngineOptions::min_rows_per_block = 20000;

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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    }

    EngineOptions::max_rows_per_block = 1000;
    EngineOptions::min_rows_per_block = 1000;

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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_ids[i], p, false);
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
    std::vector<Expect> expects{{1, 1500, 0}, {0, 0, 30}, {2, 2900, 0}, {2, 3999, 1}, {4, 7000, 0}};
    for (int i = 0; i < 5; ++i) {
      auto eid = i + 1;
      auto expect = expects[i];
      TsEntityItem entity_item;
      bool is_exist = false;
      ASSERT_EQ(entity_segment->GetEntityItem(eid, entity_item, is_exist), SUCCESS);

      if (is_exist) {
        std::vector<TsEntitySegmentBlockItemWithData> blk_items;
        ASSERT_EQ(entity_segment->GetAllBlockItems(eid, &blk_items), SUCCESS);
        ASSERT_EQ(blk_items.size(), expect.nblock_in_entity_segment) << eid;
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
  EngineOptions::g_dedup_rule = DedupRule::KEEP_EXPERIMENTAL;
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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
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
        TsEntityCountStats count_header{};
        count_header.entity_id = (TSEntityID)i;
        s = count_info->GetEntityCountStats(count_header);
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

      vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
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

    vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
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

    vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
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
    vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false);
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

TEST_F(TsEntitySegmentTest, BUG_IEOYSN) {
  EngineOptions::max_rows_per_block = 1500;
  EngineOptions::min_rows_per_block = 1500;
  // TSTableID table_ids[] = {333, 444, 555};
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::VARCHAR};
  const std::vector<AttributeInfo> *metric_schema = nullptr;
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  TSTableID table_id = 987654;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  auto env = &TsIOEnv::GetInstance();

  std::string tmp_path = "bug-IEOYSN";
  ASSERT_EQ(env->DeleteDir(tmp_path), SUCCESS);
  ASSERT_EQ(env->NewDirectory(tmp_path), SUCCESS);

  TsVersionManager v_mgr(env, tmp_path);
  ASSERT_EQ(v_mgr.Recover(false), SUCCESS);

  TsMemSegmentManager mem_mgr(vgroup.get(), &v_mgr);
  std::shared_ptr<TsTableSchemaManager> table_schema;
  ASSERT_EQ(mgr->GetTableSchemaMgr(table_id, table_schema), SUCCESS);

  std::vector<std::shared_ptr<TsMemSegment>> mem_segments;

  for (int k = 0; k < 10; ++k) {
    uint64_t dev_id = 100 + k / 2;
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 1000, 123 + k * 1000, 1);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{metric_schema};
    p.ParsePayLoadStruct(payload);
    // auto ptag = p.GetPrimaryTag();
    mem_mgr.PutData(&p, table_schema, dev_id);
    free(payload.data);
    mem_segments.push_back(mem_mgr.CurrentMemSegment());
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  
  auto current = v_mgr.Current();
  ASSERT_TRUE(current != nullptr);
  auto partitions = current->GetAllPartitions();
  ASSERT_EQ(partitions.size(), 1);
  auto p_version = partitions.begin()->second;
  TsEntitySegmentBuilder builder(env, tmp_path, mgr.get(), &v_mgr, p_version->GetPartitionIdentifier(), nullptr, TsDataSource::Flush);
  ASSERT_EQ(builder.Open(), SUCCESS);

  for (const auto &m : mem_segments) {
    std::list<std::shared_ptr<TsBlockSpan>> block_spans;
    ASSERT_EQ(m->GetBlockSpans(block_spans, mgr.get()), SUCCESS);
    ASSERT_EQ(block_spans.size(), 5);

    for (auto span : block_spans) {
      builder.PutBlockSpan(std::move(span));
    }
  }

  ASSERT_EQ(mgr->SetTableDropped(table_id), SUCCESS);

  TsVersionUpdate update;
  std::vector<std::shared_ptr<TsBlockSpan>> residual;
  TsSegmentWriteStats stats;
  ASSERT_EQ(builder.Compact(&update, &residual, &stats), SUCCESS);
}