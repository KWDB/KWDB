#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "test_util.h"
#include "ts_hash_latch.h"
#include "ts_mem_segment_mgr.h"
#include "ts_version.h"
#include "ts_vgroup.h"

using namespace kwdbts;
using namespace roachpb;

class TsMemSegmentProxyTest : public ::testing::Test {
 protected:
  std::unique_ptr<TsEngineSchemaManager> mgr;

  EngineOptions opts;

  std::unique_ptr<TsVGroup> vgroup;
  kwdbContext_t ctx;
  TsHashRWLatch tag_lock;

  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT,
                                     DataType::VARCHAR};

  void CreateTable(TSTableID table_id, const std::vector<DataType> &metric_types,
                   const std::vector<AttributeInfo> **metric_schema, std::vector<TagInfo> *tag_schema,
                   std::shared_ptr<TsTableSchemaManager> &schema_mgr) {
    CreateTsTable meta;

    ConstructRoachpbTableWithTypes(&meta, table_id, metric_types);
    ASSERT_EQ(mgr->CreateTable(nullptr, 1, table_id, &meta), SUCCESS);
    ASSERT_EQ(mgr->GetTableSchemaMgr(table_id, schema_mgr), KStatus::SUCCESS);
    ASSERT_EQ(schema_mgr->GetMetricMeta(1, metric_schema), KStatus::SUCCESS);
    ASSERT_EQ(schema_mgr->GetTagMeta(1, *tag_schema), KStatus::SUCCESS);
  }

  struct ExpectedRowDistribution {
   private:
    static std::pair<int64_t, int64_t> WriteToEntity(int64_t cnt) {
      int64_t entity = 0, last = 0;
      auto res = cnt % EngineOptions::max_rows_per_block;
      entity += (cnt - res);
      if (res < EngineOptions::min_rows_per_block) {
        last += res;
      } else {
        entity += res;
      }
      return {last, entity};
    }

   public:
    int64_t mem = 0, last = 0, entity = 0;

    void PutData(int64_t cnt) { mem += cnt; }
    void Flush() {
      auto [l, e] = WriteToEntity(mem);
      last += l;
      entity += e;
      mem = 0;
    }

    void Compact() {
      auto [l, e] = WriteToEntity(last);
      last = l;
      entity += e;
    }
  };

 public:
  TsMemSegmentProxyTest() : tag_lock(EngineOptions::vgroup_max_num * 2, RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK) {}
  void SetUp() override {
    // EngineOptions::g_dedup_rule = DedupRule::KEEP;
    std::string schema_path = "schema", db_path = "db001-123";
    fs::remove_all(schema_path);
    fs::remove_all(db_path);

    mgr = std::make_unique<TsEngineSchemaManager>(schema_path);
    std::shared_mutex wal_level_mutex;
    ASSERT_EQ(mgr->Init(nullptr), KStatus::SUCCESS);
    opts.db_path = db_path;

    vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    ASSERT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);
  }

  int64_t SumRowCount(const std::list<std::shared_ptr<TsBlockSpan>> &ts_block_spans) {
    int64_t sum = 0;
    for (auto &ts_block_span : ts_block_spans) {
      sum += ts_block_span->GetRowNum();
    }
    return sum;
  }

  std::array<int64_t, 3> GetRowDistributionFromVersion(const TsScanFilterParams &filter_params,
                                                       const TsPartitionVersion *p) {
    std::array<int64_t, 3> res{-1, -1, -1};
    [&]() {
      std::list<shared_ptr<TsBlockSpan>> ts_mem_block_spans;
      std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr;
      ASSERT_EQ(mgr->GetTableSchemaMgr(filter_params.table_id_, tbl_schema_mgr), KStatus::SUCCESS);
      auto metric_table = tbl_schema_mgr->GetCurrentMetricsTable();
      ASSERT_EQ(p->GetBlockSpans(filter_params, &ts_mem_block_spans, tbl_schema_mgr, metric_table, nullptr, false, true,
                                 true),
                KStatus::SUCCESS);

      std::list<shared_ptr<TsBlockSpan>> ts_last_block_spans;
      ASSERT_EQ(p->GetBlockSpans(filter_params, &ts_last_block_spans, tbl_schema_mgr, metric_table, nullptr, true,
                                 false, true),
                KStatus::SUCCESS);

      std::list<shared_ptr<TsBlockSpan>> ts_entity_block_spans;
      ASSERT_EQ(p->GetBlockSpans(filter_params, &ts_entity_block_spans, tbl_schema_mgr, metric_table, nullptr, true,
                                 true, false),
                KStatus::SUCCESS);
      res = {SumRowCount(ts_mem_block_spans), SumRowCount(ts_last_block_spans), SumRowCount(ts_entity_block_spans)};
    }();
    return res;
  }

  void DoCheckExpectation(const TsScanFilterParams &filter_params, const ExpectedRowDistribution &exp_dist,
                          const TsVGroupVersion *version) {
    auto partitions = version->GetAllPartitions();
    ASSERT_EQ(partitions.size(), 1);
    auto p = partitions.begin()->second;
    auto [mem_cnt, last_cnt, entity_cnt] = GetRowDistributionFromVersion(filter_params, p.get());
    EXPECT_EQ(mem_cnt, exp_dist.mem);
    EXPECT_EQ(last_cnt, exp_dist.last);
    EXPECT_EQ(entity_cnt, exp_dist.entity);
  }
};

TEST_F(TsMemSegmentProxyTest, SingleTable_Partition_Device) {
  EngineOptions::min_rows_per_block = 512;
  EngineOptions::max_rows_per_block = 4096;
  EngineOptions::max_last_segment_num = 0;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  const std::vector<AttributeInfo> *metric_schema;
  std::vector<TagInfo> tag_schema;
  TSTableID table_id = 1;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  std::vector<std::shared_ptr<const TsVGroupVersion>> versions;

  ExpectedRowDistribution dist;
  int n = 10;
  std::vector<int64_t> insert_row(n);
  std::vector<ExpectedRowDistribution> expected_row_distribution(n);
  for (int i = 0; i < n; ++i) {
    insert_row[i] = 103 + i * 1000;
    dist.PutData(insert_row[i]);
    expected_row_distribution[i] = dist;
    dist.Flush();
  }

  uint64_t init_lsn = 0x12345;
  std::vector<TS_OSN> lsn_vec(n);
  for (int i = 0; i < n; ++i) {
    TSEntityID dev_id = 1;
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, insert_row[i], 123, 1);
    auto lsn = init_lsn + static_cast<TS_OSN>(i * 54321);
    lsn_vec[i] = lsn;
    std::memcpy(payload.data, &lsn, sizeof(lsn));
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{metric_schema};
    p.ParsePayLoadStruct(payload);
    auto ptag = p.GetPrimaryTag();

    ASSERT_EQ(vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, &payload, false), SUCCESS);
    free(payload.data);
    versions.push_back(vgroup->CurrentVersion());
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  auto write_finish_version = vgroup->CurrentVersion();
  dist.Flush();
  auto write_finish_dist = dist;

  EXPECT_EQ(TsMemSegment::GetMemSegmentsCount(), 1);

  // query_check

  ASSERT_EQ(versions.size(), n);

  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr;
  ASSERT_EQ(mgr->GetTableSchemaMgr(table_id, tbl_schema_mgr), KStatus::SUCCESS);
  auto metric_table = tbl_schema_mgr->GetCurrentMetricsTable();

  std::vector<KwTsSpan> spans{{INT64_MIN, INT64_MAX}};
  TsScanFilterParams filter_params(0, table_id, vgroup->GetVGroupID(), 1, DATATYPE::TIMESTAMP64, UINT64_MAX, spans);
  for (int i = 0; i < n; ++i) {
    auto partitions = versions[i]->GetAllPartitions();
    ASSERT_EQ(partitions.size(), 1);

    auto p = partitions.begin()->second;

    auto [mem_cnt, last_cnt, entity_cnt] = GetRowDistributionFromVersion(filter_params, p.get());

    EXPECT_EQ(mem_cnt, insert_row[i]);
    EXPECT_EQ(last_cnt, expected_row_distribution[i].last);
    EXPECT_EQ(entity_cnt, expected_row_distribution[i].entity);

    uint64_t max_osn = 0;
    ASSERT_EQ(p->GetMaxOSN(1, table_id, 1, DATATYPE::TIMESTAMP64, max_osn), SUCCESS);
    EXPECT_EQ(max_osn, lsn_vec[i]);
  }

  {
    auto partitions = write_finish_version->GetAllPartitions();
    ASSERT_EQ(partitions.size(), 1);
    auto p = partitions.begin()->second;
    auto [mem_cnt, last_cnt, entity_cnt] = GetRowDistributionFromVersion(filter_params, p.get());
    EXPECT_EQ(mem_cnt, 0);
    EXPECT_EQ(last_cnt, write_finish_dist.last);
    EXPECT_EQ(entity_cnt, write_finish_dist.entity);
  }

  ASSERT_EQ(vgroup->Compact(), SUCCESS);
  auto compact_finish_version = vgroup->CurrentVersion();
  dist.Compact();
  auto compact_finish_dist = dist;
  {
    auto partitions = write_finish_version->GetAllPartitions();
    ASSERT_EQ(partitions.size(), 1);
    auto p = partitions.begin()->second;
    auto [mem_cnt, last_cnt, entity_cnt] = GetRowDistributionFromVersion(filter_params, p.get());
    EXPECT_EQ(mem_cnt, 0);
    EXPECT_EQ(last_cnt, compact_finish_dist.last);
    EXPECT_EQ(entity_cnt, compact_finish_dist.entity);
  }
}