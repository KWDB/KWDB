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
  {
    System("rm -rf schema");
    System("rm -rf db001-123");
    CreateTsTable meta;
    TSTableID table_id = 123;
    ConstructRoachpbTableWithTypes(
      &meta, table_id,
      {DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT, DataType::VARCHAR});
    auto mgr = std::make_unique<TsEngineSchemaManager>("schema");
    auto s = mgr->CreateTable(nullptr, table_id, &meta);
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
      free(payload.data);
    }

    // partition->Compact();
    EXPECT_EQ(partition->Compact(), KStatus::SUCCESS);

    TsEntitySegment* entity_segment = partition->GetEntitySegment();
    int sum = 0;
    for (int i = 0; i < 10; ++i) {
      {
        // scan [500, INT64_MAX]
        std::vector<KwTsSpan> spans{{500, INT64_MAX}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID) (1 + i * 123), spans};
        std::list<TsBlockSpan> block_spans;
        s = entity_segment->GetBlockSpans(filter, &block_spans);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i);
        int row_idx = 0;
        while(!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          for (int idx = 0; idx < block_span.nrow; ++idx) {
            TSSlice value;
            block_span.block->GetValueSlice(block_span.start_row + idx, 0, metric_schema, value);
            EXPECT_EQ(*(timestamp64*)value.data, 500 + row_idx + idx);
            EXPECT_EQ(block_span.block->GetTS(block_span.start_row + idx), 500 + row_idx + idx);
            block_span.block->GetValueSlice(block_span.start_row + idx, 1, metric_schema, value);
            EXPECT_LE(*(int32_t *) value.data, 1024);
            block_span.block->GetValueSlice(block_span.start_row + idx, 2, metric_schema, value);
            EXPECT_LE(*(double *) value.data, 1024 * 1024);
            block_span.block->GetValueSlice(block_span.start_row + idx, 3, metric_schema, value);
            EXPECT_LE(*(double *) value.data, 10240);
            block_span.block->GetValueSlice(block_span.start_row + idx, 4, metric_schema, value);
            string str(value.data, 10);
            EXPECT_EQ(str, "varstring_");
          }
          row_idx += block_span.nrow;
        }
        if (i >= 1) {
          EXPECT_EQ(row_idx, (i - 1) * 1000 + 623);
        } else {
          EXPECT_EQ(row_idx, 0);
        }
      }
      {
        // scan [INT64_MIN, 622]
        std::vector<KwTsSpan> spans{{INT64_MIN, 622}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID) (1 + i * 123), spans};
        std::list<TsBlockSpan> block_spans;
        s = entity_segment->GetBlockSpans(filter, &block_spans);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i > 0 ? 1 : 0);
        int row_idx = 0;
        while(!block_spans.empty()) {
          auto block_span = block_spans.front();
          block_spans.pop_front();
          for (int idx = 0; idx < block_span.nrow; ++idx) {
            EXPECT_EQ(block_span.block->GetTS(block_span.start_row + idx), 123 + row_idx + idx);
            TSSlice value;
            block_span.block->GetValueSlice(block_span.start_row + idx, 1, metric_schema, value);
            EXPECT_LE(*(int32_t *) value.data, 1024);
            block_span.block->GetValueSlice(block_span.start_row + idx, 2, metric_schema, value);
            EXPECT_LE(*(double *) value.data, 1024 * 1024);
            block_span.block->GetValueSlice(block_span.start_row + idx, 3, metric_schema, value);
            EXPECT_LE(*(double *) value.data, 10240);
            block_span.block->GetValueSlice(block_span.start_row + idx, 4, metric_schema, value);
            string str(value.data, 10);
            EXPECT_EQ(str, "varstring_");
          }
          row_idx += block_span.nrow;
        }
        EXPECT_EQ(row_idx, i > 0 ? 500 : 0);
      }
      {
        // scan [INT64_MIN, INT64_MAX]
        std::vector<KwTsSpan> spans{{INT64_MIN, INT64_MAX}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID)(1 + i * 123), spans};
        std::list<TsBlockSpan> block_spans;
        s = entity_segment->GetBlockSpans(filter, &block_spans);
        EXPECT_EQ(s, KStatus::SUCCESS);
        EXPECT_EQ(block_spans.size(), i);
        int row_idx = 0;
        while(!block_spans.empty()) {
          auto block_span = block_spans.front();
          sum += block_span.nrow;
          block_spans.pop_front();
          for (int idx = 0; idx < block_span.nrow; ++idx) {
            EXPECT_EQ(block_span.block->GetTS(block_span.start_row + idx), 123 + row_idx + idx);
            TSSlice value;
            block_span.block->GetValueSlice(block_span.start_row + idx, 1, metric_schema, value);
            EXPECT_LE(*(int32_t *) value.data, 1024);
            block_span.block->GetValueSlice(block_span.start_row + idx, 2, metric_schema, value);
            EXPECT_LE(*(double *) value.data, 1024 * 1024);
            block_span.block->GetValueSlice(block_span.start_row + idx, 3, metric_schema, value);
            EXPECT_LE(*(double *) value.data, 10240);
            block_span.block->GetValueSlice(block_span.start_row + idx, 4, metric_schema, value);
            string str(value.data, 10);
            EXPECT_EQ(str, "varstring_");
          }
          row_idx += block_span.block->GetRowNum();
        }
        EXPECT_EQ(row_idx, i * EngineOptions::max_rows_per_block);
      }
    }
    std::vector<std::shared_ptr<TsLastSegment>> result;
    partition->GetLastSegmentMgr()->GetCompactLastSegments(result);
    ASSERT_EQ(result.size(), 1);
    for (int j = 0; j < result.size(); ++j) {
      for (int i = 0; i < 10; ++i) {
        std::vector<KwTsSpan> spans{{INT64_MIN, INT64_MAX}};
        TsBlockItemFilterParams filter{0, table_id, (TSEntityID)(1 + i * 123), spans};
        std::list<TsBlockSpan> block_span;
        result[0]->GetBlockSpans(filter, &block_span);
        for (auto block : block_span) {
          sum += block.nrow;
        }
      }
    }
    std::cout << sum << std::endl;
  }
}