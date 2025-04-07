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
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_hash_latch.h"
#include "ts_lastsegment_builder.h"

using namespace kwdbts;  // NOLINT


class TsBlockSegmentTest : public ::testing::Test {
public:
  TsBlockSegmentTest() {}

  ~TsBlockSegmentTest() {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }
};

TEST_F(TsBlockSegmentTest, simpleInsert) {
  using namespace roachpb;
  {
    System("rm -rf schema");
    System("rm -rf db001-123");
    CreateTsTable meta;
    TSTableID table_id = 123;
    ConstructRoachpbTableWithTypes(
      &meta, table_id,
      {DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT});
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
    std::shared_ptr<TsVGroupPartition> partition = std::make_shared<TsVGroupPartition>(path, 0, mgr.get(), 0, 1000000, false);
    partition->Open();

    std::unique_ptr<TsLastSegment> last_segment;
    for (int i = 0; i < 10; ++i) {
      partition->NewLastSegment(&last_segment);
      TsLastSegmentBuilder builder(mgr.get(), last_segment);
      auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1 + i * 123, 10, 123, 1);
      TsRawPayloadRowParser parser{metric_schema};
      TsRawPayload p{payload, metric_schema};

      for (int j = 0; j < p.GetRowCount(); ++j) {
        s = builder.PutRowData(table_id, 1, 1 + i * 123, j, p.GetRowData(j));
        EXPECT_EQ(s, KStatus::SUCCESS);
      }
      builder.Finalize();
      builder.Flush();
      partition->PublicLastSegment(builder.Finish());
      free(payload.data);
    }

    //partition->Compact();
    EXPECT_EQ(partition->Compact(), KStatus::SUCCESS);
  }
}