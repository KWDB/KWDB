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

#include <unistd.h>

#include "ts_batch_data_worker.h"
#include "ts_coding.h"
#include "ts_test_base.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_engine.h"
#include "ts_entity_segment.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";

namespace {

TSSlice ExtractBatchTags(const TSSlice& batch) {
  uint16_t p_tag_size = DecodeFixed16(batch.data + TsBatchData::header_size_);
  uint32_t p_tag_offset = TsBatchData::header_size_ + sizeof(p_tag_size);
  uint32_t tags_data_offset = p_tag_offset + p_tag_size + sizeof(uint32_t);
  uint32_t tags_data_size = DecodeFixed32(batch.data + tags_data_offset - sizeof(uint32_t));
  return {batch.data + tags_data_offset, tags_data_size};
}

}  // namespace

// Verifies batch headers and block-span metadata are serialized with the
// expected little-endian field layout.
TEST(TsBatchDataTest, HeaderAndBlockSpanHeaderUseLittleEndianEncoding) {
  TsBatchData batch;
  const TS_OSN tag_osn = 0x0102030405060708ULL;
  const uint16_t hash_point = 0x1234;
  const uint32_t table_version = 0x90ABCDEF;
  const timestamp64 min_ts = 0x1112131415161718LL;
  const timestamp64 max_ts = 0x2122232425262728LL;
  const uint64_t min_osn = 0x3132333435363738ULL;
  const uint64_t max_osn = 0x4142434445464748ULL;
  const uint64_t first_osn = 0x5152535455565758ULL;
  const uint64_t last_osn = 0x6162636465666768ULL;
  const uint32_t n_cols = 0x11223344U;
  const uint32_t n_rows = 0x55667788U;
  const uint32_t block_version = 0xA1B2C3D4U;
  const std::string primary_tag{"pk"};
  const std::string tags{"tag"};

  batch.SetTagOSN(tag_osn);
  batch.SetHashPoint(hash_point);
  batch.SetTableVersion(table_version);
  batch.AddPrimaryTag({const_cast<char*>(primary_tag.data()), primary_tag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddBlockSpanDataHeader(0, min_ts, max_ts, min_osn, max_osn, first_osn, last_osn, n_cols, n_rows, block_version);
  batch.UpdateBatchDataInfo();

  TSSlice raw = batch.data_.AsSlice();
  auto expect_fixed16 = [&](size_t offset, uint16_t value) {
    char expected[sizeof(uint16_t)];
    EncodeFixed16(expected, value);
    EXPECT_EQ(std::string(raw.data + offset, sizeof(expected)), std::string(expected, sizeof(expected)));
  };
  auto expect_fixed32 = [&](size_t offset, uint32_t value) {
    char expected[sizeof(uint32_t)];
    EncodeFixed32(expected, value);
    EXPECT_EQ(std::string(raw.data + offset, sizeof(expected)), std::string(expected, sizeof(expected)));
  };
  auto expect_fixed64 = [&](size_t offset, const auto& value) {
    char expected[sizeof(uint64_t)];
    uint64_t raw_value = 0;
    static_assert(sizeof(value) == sizeof(raw_value), "expect 64-bit field");
    std::memcpy(&raw_value, &value, sizeof(raw_value));
    EncodeFixed64(expected, raw_value);
    EXPECT_EQ(std::string(raw.data + offset, sizeof(expected)), std::string(expected, sizeof(expected)));
  };

  expect_fixed16(TsBatchData::hash_point_id_offset_, hash_point);
  expect_fixed32(TsBatchData::data_length_offset_, static_cast<uint32_t>(batch.data_.size()));
  expect_fixed64(TsBatchData::tag_osn_offset_, tag_osn);
  expect_fixed32(TsBatchData::batch_version_offset_, CURRENT_BATCH_VERSION);
  expect_fixed32(TsBatchData::ts_version_offset_, table_version);
  expect_fixed32(TsBatchData::row_num_offset_, n_rows);
  EXPECT_EQ(static_cast<uint8_t>(raw.data[TsBatchData::row_type_offset_]), DataTagFlag::DATA_AND_TAG);

  expect_fixed16(TsBatchData::header_size_, static_cast<uint16_t>(primary_tag.size()));
  const size_t tags_size_offset = TsBatchData::header_size_ + sizeof(uint16_t) + primary_tag.size();
  expect_fixed32(tags_size_offset, static_cast<uint32_t>(tags.size()));

  const size_t block_offset = tags_size_offset + sizeof(uint32_t) + tags.size();
  expect_fixed32(block_offset + TsBatchData::length_offset_in_span_data_, static_cast<uint32_t>(raw.len - block_offset));
  expect_fixed64(block_offset + TsBatchData::min_ts_offset_in_span_data_, min_ts);
  expect_fixed64(block_offset + TsBatchData::max_ts_offset_in_span_data_, max_ts);
  expect_fixed64(block_offset + TsBatchData::min_osn_offset_in_span_data_, min_osn);
  expect_fixed64(block_offset + TsBatchData::max_osn_offset_in_span_data_, max_osn);
  expect_fixed64(block_offset + TsBatchData::first_osn_offset_in_span_data_, first_osn);
  expect_fixed64(block_offset + TsBatchData::last_osn_offset_in_span_data_, last_osn);
  expect_fixed32(block_offset + TsBatchData::n_cols_offset_in_span_data_, n_cols);
  expect_fixed32(block_offset + TsBatchData::n_rows_offset_in_span_data_, n_rows);
  expect_fixed32(block_offset + TsBatchData::block_version_offset_in_span_data_, block_version);
}

class TsBatchDataWorkerTest : public TsEngineTestBase {
 public:
  TsBatchDataWorkerTest() {
    EngineOptions::vgroup_max_num = 1;
    InitContext();
    InitEngine(engine_root_path);
  }
};

// Verifies that batch header fields are written back consistently after tags and
// block-span metadata are assembled into one batch payload.
TEST(TsBatchDataTest, UpdateBatchDataInfoRoundTrip) {
  TsBatchData batch;
  const TS_OSN tag_osn = 123;
  const uint16_t hash_point = 17;
  const uint32_t table_version = 9;
  const std::string ptag{"p1"};
  const std::string tags{"tag"};
  batch.SetTagOSN(tag_osn);
  batch.SetHashPoint(hash_point);
  batch.SetTableVersion(table_version);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddBlockSpanDataHeader(0, 100, 200, 11, 22, 33, 44, 5, 7, 3);
  batch.UpdateBatchDataInfo();

  EXPECT_EQ(batch.GetBatchVersion(), CURRENT_BATCH_VERSION);
  EXPECT_EQ(batch.GetTagOSN(), tag_osn);
  EXPECT_EQ(batch.GetHashPoint(), hash_point);
  EXPECT_EQ(batch.GetTableVersion(), table_version);
  EXPECT_EQ(batch.GetRowType(), DataTagFlag::DATA_AND_TAG);
  EXPECT_EQ(batch.GetRowCount(), 7U);
  EXPECT_EQ(batch.GetDataLength(), static_cast<uint32_t>(batch.data_.size()));
  EXPECT_TRUE(batch.HasBlockSpanData());
  EXPECT_EQ(batch.GetBlockSpanData().len, TsBatchData::block_span_data_header_size_);
}

// Recreates one entity as two restored block spans, then verifies the first read
// fills the tag cache and the second read reuses the cached serialized tag payload.
TEST_F(TsBatchDataWorkerTest, CacheTagValueAcrossBlockSpansOfSameEntity) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);

  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts = 10086000;
  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t read_job_id = 1;
  TSSlice backup_batch;
  uint32_t backup_row_num = 0;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id,
                             &backup_batch, &backup_row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GT(backup_row_num, 0U);
  std::string backup_data(backup_batch.data, backup_batch.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete engine_;
  Remove(engine_root_path);
  MakeDirectory(engine_root_path);
  engine_ = new TSEngineImpl(opts_);
  s = engine_->Init(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  MakeDirectory(engine_root_path + "/temp_db_");

  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice restore_batch{backup_data.data(), backup_data.size()};
  uint32_t restored_row_num = 0;
  uint64_t write_job_id = 2;
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &restore_batch, &restored_row_num,
                              TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(restored_row_num, backup_row_num);
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &restore_batch, &restored_row_num,
                              TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(restored_row_num, backup_row_num);
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  schema_mgr = nullptr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ts_table.reset();
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped, true, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);

  vector<EntityResultIndex> entity_indexes;
  s = ts_table->GetEntityIdByHashSpan(ctx_, {0, UINT32_MAX}, UINT64_MAX, entity_indexes);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_indexes.size(), 1UL);

  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  auto vgroup = engine_->GetTsVGroup(entity_indexes[0].subGroupId);
  auto current_version = vgroup->CurrentVersion();
  s = vgroup->GetBlockSpans(table_id, entity_indexes[0].entityId, {INT64_MIN, INT64_MAX},
                            DATATYPE::TIMESTAMP64, schema_mgr, 1, current_version, &block_spans);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(block_spans.size(), 2UL);

  TsReadBatchDataWorker worker(engine_, table_id, 1, {INT64_MIN, INT64_MAX}, 100, entity_indexes);
  s = worker.Init(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice first_batch;
  uint32_t first_row_num = 0;
  s = worker.ReadOnce(ctx_, &first_batch, &first_row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GT(first_row_num, 0U);
  auto first_stats = worker.GetTagCacheStatsForTest();
  EXPECT_EQ(first_stats.lookup_count, 1U);
  EXPECT_EQ(first_stats.fill_count, 1U);
  EXPECT_EQ(first_stats.hit_count, 0U);

  TSSlice second_batch;
  uint32_t second_row_num = 0;
  s = worker.ReadOnce(ctx_, &second_batch, &second_row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GT(second_row_num, 0U);
  auto second_stats = worker.GetTagCacheStatsForTest();
  EXPECT_EQ(second_stats.lookup_count, 1U);
  EXPECT_EQ(second_stats.fill_count, 1U);
  EXPECT_EQ(second_stats.hit_count, 1U);

  TSSlice first_tags = ExtractBatchTags(first_batch);
  TSSlice second_tags = ExtractBatchTags(second_batch);
  ASSERT_EQ(first_tags.len, second_tags.len);
  EXPECT_FALSE(isRowDeleted(first_tags.data, 1));
  EXPECT_FALSE(isRowDeleted(second_tags.data, 1));
  EXPECT_EQ(memcmp(first_tags.data, second_tags.data, first_tags.len), 0);
}

// Corrupts the persisted block-span length inside one batch payload and verifies
// the restore write path rejects the malformed batch layout.
TEST_F(TsBatchDataWorkerTest, RejectMalformedBatchLayout) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TsBatchData batch;
  const std::string ptag{"p"};
  const std::string tags{"t"};
  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddBlockSpanDataHeader(0, 100, 200, 1, 2, 3, 4, 5, 6, 7);
  batch.UpdateBatchDataInfo();

  uint32_t invalid_block_len = static_cast<uint32_t>(batch.GetBlockSpanData().len + 8);
  EncodeFixed32(batch.data_.data() + batch.block_span_data_offset_, invalid_block_len);

  TSSlice data = batch.data_.AsSlice();
  uint32_t row_num = 0;
  bool is_dropped = false;
  s = engine_->WriteBatchData(ctx_, table_id, 1, 1, &data, &row_num, TsDataSource::Restore, is_dropped);
  EXPECT_EQ(s, KStatus::FAIL);
}

// Corrupts the header data_length field so it no longer matches the actual batch
// buffer size and verifies the restore write path rejects the batch.
TEST_F(TsBatchDataWorkerTest, RejectMismatchedBatchDataLength) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TsBatchData batch;
  const std::string ptag{"p"};
  const std::string tags{"t"};
  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.UpdateBatchDataInfo();

  auto invalid_data_len = static_cast<uint32_t>(batch.data_.size() + 1);
  EncodeFixed32(batch.data_.data() + TsBatchData::data_length_offset_, invalid_data_len);

  TSSlice data = batch.data_.AsSlice();
  uint32_t row_num = 0;
  bool is_dropped = false;
  s = engine_->WriteBatchData(ctx_, table_id, 1, 1, &data, &row_num, TsDataSource::Restore, is_dropped);
  EXPECT_EQ(s, KStatus::FAIL);
}

// Marks one tag-only batch as DATA_AND_TAG in the header while leaving no trailing
// block-span payload and verifies the restore write path rejects it.
TEST_F(TsBatchDataWorkerTest, RejectDataAndTagHeaderWithoutBlockSpanPayload) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TsBatchData batch;
  const std::string ptag{"p"};
  const std::string tags{"t"};
  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.UpdateBatchDataInfo();

  uint8_t invalid_row_type = DataTagFlag::DATA_AND_TAG;
  memcpy(batch.data_.data() + TsBatchData::row_type_offset_, &invalid_row_type, sizeof(invalid_row_type));

  TSSlice data = batch.data_.AsSlice();
  uint32_t row_num = 0;
  bool is_dropped = false;
  s = engine_->WriteBatchData(ctx_, table_id, 1, 1, &data, &row_num, TsDataSource::Restore, is_dropped);
  EXPECT_EQ(s, KStatus::FAIL);
}

TEST_F(TsBatchDataWorkerTest, TestTsBatchDataWorker) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  // ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  timestamp64 start_ts = 10086000;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);

  // read batch job
  uint64_t read_job_id = 1;
  TSSlice data;
  uint32_t row_num;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id, &data, &row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::string backup_data = std::string(data.data, data.len);

  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // reopen
  delete engine_;
  Remove(engine_root_path);
  MakeDirectory(engine_root_path);
  engine_ = new TSEngineImpl(opts_);
  s = engine_->Init(ctx_);
  MakeDirectory(engine_root_path + "/temp_db_");
  EXPECT_EQ(s, KStatus::SUCCESS);

  // ConstructRoachpbTable(&pb_meta, table_id);
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint32_t n_rows;
  uint64_t write_job_id = 2;
  data.data = backup_data.data();
  data.len = backup_data.size();
  // first write
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &n_rows, TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(n_rows, row_num);
  // second write
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &n_rows, TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(n_rows, row_num);
  //  finish
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  for (uint32_t vgroup_id = 1; vgroup_id <= EngineOptions::vgroup_max_num; vgroup_id++) {
    auto vgroup = engine_->GetTsVGroup(vgroup_id);
    auto p = vgroup->CurrentVersion()->GetPartition(1, 10086);
    if (p == nullptr) {
      continue;
    }
    auto entity_segment = p->GetEntitySegment();
    uint32_t entity_id = entity_segment->GetEntityNum();
    assert(entity_id == 1);
    schema_mgr = nullptr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    KwTsSpan ts_span{INT64_MIN, INT64_MAX};
    KwOSNSpan osn_span{0, UINT64_MAX};
    STScanRange scan_range{ts_span, osn_span};
    TsBlockItemFilterParams filter{1, table_id, vgroup_id, entity_id, {scan_range}};
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  ASSERT_EQ(block_spans.size(), 2);
  while (!block_spans.empty()) {
    std::shared_ptr<TsBlockSpan> block_span = block_spans.front();
    std::unique_ptr<TsBitmapBase> bitmap;
    char *ts_col;
    s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<char *> col_values;
    col_values.resize(2);
    s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint64_t osn = *(uint64_t *) (block_span->GetOSNAddr(0));
    for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
      EXPECT_EQ(*(uint64_t *) (block_span->GetOSNAddr(idx)), osn);
      EXPECT_EQ(block_span->GetTS(idx), 10086000 + idx * 1000);
      EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 8), 10086000 + idx * 1000);
      EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
      EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
      kwdbts::DataFlags flag;
      TSSlice var_data;
      s = block_span->GetVarLenTypeColAddr(idx, 3, flag, var_data);
      EXPECT_EQ(s, KStatus::SUCCESS);
      string str(var_data.data, 10);
      EXPECT_EQ(str, "varstring_");
    }
    block_spans.pop_front();
  }
}

TEST_F(TsBatchDataWorkerTest, LoseData) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  // ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  timestamp64 start_ts = 10086000;
  // entity 1
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 10, 1000, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);

  // entity 2
  pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1000, start_ts);
  dedup_result = {0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);

  uint64_t read_job_id = 1;
  TSSlice data;
  uint32_t row_num;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id, &data, &row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::string backup_data1 = std::string(data.data, data.len);
  // read batch job [entity 1]
  TSSlice data1;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id, &data1, &row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::string backup_data2 = std::string(data1.data, data1.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // reopen
  delete engine_;
  Remove(engine_root_path);
  MakeDirectory(engine_root_path);
  engine_ = new TSEngineImpl(opts_);
  s = engine_->Init(ctx_);
  MakeDirectory(engine_root_path + "/temp_db_");
  EXPECT_EQ(s, KStatus::SUCCESS);

  // ConstructRoachpbTable(&pb_meta, table_id);
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 0, start_ts);
  dedup_result = {0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 10, 0, start_ts);
  dedup_result = {0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);

  uint32_t n_rows;
  // write entity 2
  uint64_t write_job_id = 3;
  data.data = backup_data2.data();
  data.len = backup_data2.size();
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &n_rows, TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(n_rows, 1000);
  //  finish
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // write entity 1
  write_job_id = 4;
  data.data = backup_data1.data();
  data.len = backup_data1.size();
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &n_rows, TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(n_rows, 1000);
  //  finish
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // read entity 1
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  for (uint32_t vgroup_id = 1; vgroup_id <= EngineOptions::vgroup_max_num; vgroup_id++) {
    auto vgroup = engine_->GetTsVGroup(vgroup_id);
    auto p = vgroup->CurrentVersion()->GetPartition(1, 10086);
    if (p == nullptr) {
      continue;
    }
    auto entity_segment = p->GetEntitySegment();
    uint32_t entity_num = entity_segment->GetEntityNum();
    assert(entity_num == 2);
    schema_mgr = nullptr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    KwTsSpan ts_span{INT64_MIN, INT64_MAX};
    KwOSNSpan osn_span{0, UINT64_MAX};
    STScanRange scan_range{ts_span, osn_span};
    TsBlockItemFilterParams filter{1, table_id, vgroup_id, 1, {scan_range}};
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  ASSERT_EQ(block_spans.size(), 1);
  while (!block_spans.empty()) {
    std::shared_ptr<TsBlockSpan> block_span = block_spans.front();
    std::unique_ptr<TsBitmapBase> bitmap;
    char *ts_col;
    s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<char *> col_values;
    col_values.resize(2);
    s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint64_t osn = *(uint64_t *) (block_span->GetOSNAddr(0));
    for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
      EXPECT_EQ(*(uint64_t *) (block_span->GetOSNAddr(idx)), osn);
      EXPECT_EQ(block_span->GetTS(idx), 10086000 + idx * 1000);
      EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 8), 10086000 + idx * 1000);
      EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
      EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
      kwdbts::DataFlags flag;
      TSSlice var_data;
      s = block_span->GetVarLenTypeColAddr(idx, 3, flag, var_data);
      EXPECT_EQ(s, KStatus::SUCCESS);
      string str(var_data.data, 10);
      EXPECT_EQ(str, "varstring_");
    }
    block_spans.pop_front();
  }
  block_spans.clear();

  // read entity 2
  for (uint32_t vgroup_id = 1; vgroup_id <= EngineOptions::vgroup_max_num; vgroup_id++) {
    auto vgroup = engine_->GetTsVGroup(vgroup_id);
    auto p = vgroup->CurrentVersion()->GetPartition(1, 10086);
    if (p == nullptr) {
      continue;
    }
    auto entity_segment = p->GetEntitySegment();
    uint32_t entity_num = entity_segment->GetEntityNum();
    assert(entity_num == 2);
    schema_mgr = nullptr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    KwTsSpan ts_span{INT64_MIN, INT64_MAX};
    KwOSNSpan osn_span{0, UINT64_MAX};
    STScanRange scan_range{ts_span, osn_span};
    TsBlockItemFilterParams filter{1, table_id, vgroup_id, 2, {scan_range}};
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  ASSERT_EQ(block_spans.size(), 1);
  while (!block_spans.empty()) {
    std::shared_ptr<TsBlockSpan> block_span = block_spans.front();
    std::unique_ptr<TsBitmapBase> bitmap;
    char *ts_col;
    s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<char *> col_values;
    col_values.resize(2);
    s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint64_t osn = *(uint64_t *) (block_span->GetOSNAddr(0));
    for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
      EXPECT_EQ(*(uint64_t *) (block_span->GetOSNAddr(idx)), osn);
      EXPECT_EQ(block_span->GetTS(idx), 10086000 + idx * 1000);
      EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 8), 10086000 + idx * 1000);
      EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
      EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
      kwdbts::DataFlags flag;
      TSSlice var_data;
      s = block_span->GetVarLenTypeColAddr(idx, 3, flag, var_data);
      EXPECT_EQ(s, KStatus::SUCCESS);
      string str(var_data.data, 10);
      EXPECT_EQ(str, "varstring_");
    }
    block_spans.pop_front();
  }
}

TEST_F(TsBatchDataWorkerTest, NoMetricData) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  // ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  timestamp64 start_ts = 10086000;

  // entity 1, no metric data
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 0, start_ts);
  uint16_t inc_entity_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, nullptr, &dedup_result);
  free(pay_load.data);

  // entity 2
  pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1000, 1000, start_ts);
  dedup_result = {0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, nullptr, &dedup_result);
  free(pay_load.data);

  uint64_t read_job_id = 1;
  TSSlice data;
  uint32_t row_num;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id, &data, &row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::string backup_data1 = std::string(data.data, data.len);
  // read batch job [entity 1]
  TSSlice data1;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id, &data1, &row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::string backup_data2 = std::string(data1.data, data1.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}
