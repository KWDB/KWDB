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

#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>

#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "test_util.h"
#include "ts_block.h"
#include "ts_engine_schema_manager.h"
#include "ts_mem_segment_mgr.h"
#include "ts_payload.h"
#include "ts_table_schema_manager.h"

using namespace kwdbts;   // NOLINT
using namespace roachpb;  // NOLINT

std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::INT,    DataType::BIGINT, DataType::VARCHAR,
                                      DataType::FLOAT,     DataType::DOUBLE, DataType::VARCHAR};

class MemSegmentTester : public testing::Test {
 protected:
  TSTableID table_id = 123;
  uint32_t db_id = 122;

  std::unique_ptr<TsEngineSchemaManager> mgr;
  std::vector<TagInfo> tag_schema;
  const std::vector<AttributeInfo>* metric_schema = nullptr;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  std::shared_ptr<TsMemSegment> memseg;

 public:
  MemSegmentTester() {
    roachpb::CreateTsTable meta;
    fs::remove_all("schema");
    mgr = std::make_unique<TsEngineSchemaManager>("schema");
    ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
    mgr->CreateTable(nullptr, db_id, table_id, &meta);
    mgr->GetTableSchemaMgr(table_id, schema_mgr);
    schema_mgr->GetMetricMeta(1, &metric_schema);
    schema_mgr->GetTagMeta(1, tag_schema);
  }
  void SetUp() override { memseg = TsMemSegment::Create(12); }
};

TEST_F(MemSegmentTester, OSN_BUG) {
  ASSERT_NE(memseg, nullptr);
  for (int i = 0; i < 1000; ++i) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, i * 2);
    TsRawPayload pd(metric_schema);
    TsRawPayload::SetOSN(payload, i);
    ASSERT_EQ(pd.ParsePayLoadStruct(payload), SUCCESS);
    memseg->AllocRowNum(1);
    TSMemSegRowData* row_data = memseg->AllocOneRow(db_id, table_id, 1, 1, pd.GetRowData(0));
    row_data->SetData(i * 2, i);
    memseg->AppendOneRow(row_data);
    free(payload.data);
  }

  std::list<std::shared_ptr<TsBlockSpan>> blocks;
  ASSERT_EQ(memseg->GetBlockSpans(blocks, mgr.get()), SUCCESS);
  ASSERT_EQ(blocks.size(), 1);
  auto block = blocks.front();
  ASSERT_EQ(block->GetRowNum(), 1000);

  for (int i = 0; i < 1000; ++i) {
    EXPECT_EQ(*block->GetOSNAddr(i), i);
  }
}

TEST_F(MemSegmentTester, BackPressure) {
  std::vector<std::shared_ptr<TsMemSegment>> segments;
  for (int i = 0; i < 20; ++i) {
    segments.push_back(TsMemSegment::Create(12));
  }

  EXPECT_EQ(TsMemSegment::GetMemSegmentCount(), 21);
  EXPECT_EQ(TsMemSegment::IsApproachingLimit(), true);
  ASSERT_NE(memseg, nullptr);
  // test back pressure

  auto t1 = std::chrono::steady_clock::now();
  int n = 500;
  for (int i = 0; i < n; ++i) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, i * 2);
    TsRawPayload pd(metric_schema);
    TsRawPayload::SetOSN(payload, i);
    ASSERT_EQ(pd.ParsePayLoadStruct(payload), SUCCESS);
    memseg->AllocRowNum(1);
    TSMemSegRowData* row_data = memseg->AllocOneRow(db_id, table_id, 1, 1, pd.GetRowData(0));
    row_data->SetData(i * 2, i);
    memseg->AppendOneRow(row_data);
    free(payload.data);
  }
  auto t2 = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
  EXPECT_GE(duration, 2 * n);
}
