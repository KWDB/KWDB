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
#include <iostream>
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_block.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_hash_latch.h"
#include "ts_lastsegment_builder.h"

using namespace kwdbts;  // NOLINT


class TsEntitySegmentTest : public ::testing::Test {
public:
  TsEntitySegmentTest() {}

  ~TsEntitySegmentTest() {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }
};

TEST_F(TsEntitySegmentTest, simpleInsert) {
  EngineOptions::max_rows_per_block = 1000;
  using namespace roachpb;
  int64_t total_insert_row_num = 0;
  int64_t entity_row_num = 0;
  int64_t last_row_num = 0;
  {
    System("rm -rf schema");
    System("rm -rf db001-123");
    CreateTsTable meta;
    TSTableID table_id = 123;
    ConstructRoachpbTableWithTypes(
      &meta, table_id,
      {DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT, DataType::VARCHAR});
    auto mgr = std::make_unique<TsEngineSchemaManager>("schema");
    auto s = mgr->CreateTable(nullptr, 1, table_id, &meta);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    s = mgr->GetTableSchemaMgr(table_id, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::vector<AttributeInfo> metric_schema;
    s = schema_mgr->GetMetricMeta(1, metric_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::filesystem::path path = "db001-123";
    std::shared_ptr<TsVGroupPartition> partition = std::make_shared<TsVGroupPartition>(path, 0, mgr.get(), 0, 1000000);
    partition->Open();
    for (int i = 0; i < 10; ++i) {
      std::unique_ptr<TsFile> last_segment;
      uint32_t file_number;
      partition->NewLastSegmentFile(&last_segment, &file_number);
      TsLastSegmentBuilder builder(mgr.get(), std::move(last_segment), file_number);
      auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1 + i * 123,
                                   103 + i * 1000, 123, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{payload, metric_schema};

      for (int j = 0; j < p.GetRowCount(); ++j) {
        s = builder.PutRowData(table_id, 1, 1 + i * 123, j, p.GetRowData(j));
        EXPECT_EQ(s, KStatus::SUCCESS);
      }
      builder.Finalize();
      partition->PublicLastSegment(file_number);
      total_insert_row_num += p.GetRowCount();
      free(payload.data);
    }

    // partition->Compact();
    EXPECT_EQ(partition->Compact(), KStatus::SUCCESS);

    TsEntitySegment* entity_segment = partition->GetEntitySegment();
    int sum = 0;
    for (int i = 0; i < 10; ++i) {
      {
        // scan [500, INT64_MAX]
        std::vector<STScanRange> spans{{{500, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID) (1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, 1, {0, 1, 2, 3, 4});
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i);
        int row_idx = 0;
        while(!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          TsBitmap bitmap;
          char* ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, bitmap);
          std::vector<char*> col_values;
          col_values.resize(3);
          s = block_span->GetFixLenColAddr(1, &col_values[0], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(2, &col_values[1], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(3, &col_values[2], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            EXPECT_EQ(block_span->GetTS(idx), 500 + row_idx + idx);
            EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 16), 500 + row_idx + idx);
            EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
            EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
            EXPECT_LE(*(int64_t *) (col_values[2] + idx * 8), 10240);
            kwdbts::DataFlags flag;
            TSSlice data;
            s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
            EXPECT_EQ(s, KStatus::SUCCESS);
            string str(data.data, 10);
            EXPECT_EQ(str, "varstring_");
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
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID) (1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, 1, {0, 1, 2, 3, 4});
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i > 0 ? 1 : 0);
        int row_idx = 0;
        while(!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          TsBitmap bitmap;
          char* ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          std::vector<char*> col_values;
          col_values.resize(3);
          s = block_span->GetFixLenColAddr(1, &col_values[0], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(2, &col_values[1], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(3, &col_values[2], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            EXPECT_EQ(block_span->GetTS(idx), 123 + row_idx + idx);
            EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 16), 123 + row_idx + idx);
            EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
            EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
            EXPECT_LE(*(int64_t *) (col_values[2] + idx * 8), 10240);
            kwdbts::DataFlags flag;
            TSSlice data;
            s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
            EXPECT_EQ(s, KStatus::SUCCESS);
            string str(data.data, 10);
            EXPECT_EQ(str, "varstring_");
          }
          row_idx += block_span->GetRowNum();
        }
        EXPECT_EQ(row_idx, i > 0 ? 500 : 0);
      }
      {
        // scan [INT64_MIN, INT64_MAX]
        std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_spans;
        s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, 1, {0, 1, 2, 3, 4});
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i);
        int row_idx = 0;
        while(!block_spans.empty()) {
          auto block_span = block_spans.front();
          sum += block_span->GetRowNum();
          block_spans.pop_front();
          TsBitmap bitmap;
          char* ts_col;
          s = block_span->GetFixLenColAddr(0, &ts_col, bitmap);
          std::vector<char*> col_values;
          col_values.resize(3);
          s = block_span->GetFixLenColAddr(1, &col_values[0], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(2, &col_values[1], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          s = block_span->GetFixLenColAddr(3, &col_values[2], bitmap);
          EXPECT_EQ(s, KStatus::SUCCESS);
          for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
            EXPECT_EQ(block_span->GetTS(idx), 123 + row_idx + idx);
            EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 16), 123 + row_idx + idx);
            EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
            EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
            EXPECT_LE(*(int64_t *) (col_values[2] + idx * 8), 10240);
            kwdbts::DataFlags flag;
            TSSlice data;
            s = block_span->GetVarLenTypeColAddr(idx, 4, flag, data);
            EXPECT_EQ(s, KStatus::SUCCESS);
            string str(data.data, 10);
            EXPECT_EQ(str, "varstring_");
          }
          row_idx += block_span->GetRowNum();
        }
        EXPECT_EQ(row_idx, i * EngineOptions::max_rows_per_block);
        entity_row_num += row_idx;
      }
    }

    std::vector<std::shared_ptr<TsLastSegment>> result = partition->GetLastSegmentMgr()->GetAllLastSegments();
    ASSERT_EQ(result.size(), 1);
    for (int j = 0; j < result.size(); ++j) {
      for (int i = 0; i < 10; ++i) {
        std::vector<STScanRange> spans{{{INT64_MIN, INT64_MAX}, {0, UINT64_MAX}}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID)(1 + i * 123), spans};
        std::list<shared_ptr<TsBlockSpan>> block_span;
        result[j]->GetBlockSpans(filter, block_span, schema_mgr, 1, {0, 1, 2, 3, 4});
        for (auto block : block_span) {
          last_row_num += block->GetRowNum();
        }
      }
    }
    int64_t last_total_row_num = 0;
    for (int j = 0; j < result.size(); ++j) {
        std::list<shared_ptr<TsBlockSpan>> block_span;
        result[j]->GetBlockSpans(block_span);
        for (auto block : block_span) {
          last_total_row_num += block->GetRowNum();
        }
    }
    EXPECT_EQ(last_total_row_num, last_row_num);
    EXPECT_EQ(last_total_row_num, total_insert_row_num - entity_row_num);
  }
}