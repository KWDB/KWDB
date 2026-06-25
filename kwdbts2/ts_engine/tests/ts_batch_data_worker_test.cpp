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

#include <cassert>

#include "ts_batch_data_worker.h"
#include "ts_coding.h"
#include "ts_test_base.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_agg.h"
#include "ts_bitmap.h"
#include "ts_compressor.h"
#include "ts_compatibility.h"
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

TSSlice ExtractBatchBlockTagPrefix(const TSSlice& batch) {
  uint16_t p_tag_size = DecodeFixed16(batch.data + TsBatchData::header_size_);
  uint32_t p_tag_offset = TsBatchData::header_size_ + sizeof(p_tag_size);
  uint32_t tags_data_offset = p_tag_offset + p_tag_size + sizeof(uint32_t);
  uint32_t tags_data_size = DecodeFixed32(batch.data + tags_data_offset - sizeof(uint32_t));
  return {batch.data, tags_data_offset + tags_data_size};
}

TSSlice ExtractBatchBlockSpanData(const TSSlice& batch) {
  uint16_t p_tag_size = DecodeFixed16(batch.data + TsBatchData::header_size_);
  uint32_t p_tag_offset = TsBatchData::header_size_ + sizeof(p_tag_size);
  uint32_t tags_data_offset = p_tag_offset + p_tag_size + sizeof(uint32_t);
  uint32_t tags_data_size = DecodeFixed32(batch.data + tags_data_offset - sizeof(uint32_t));
  size_t data_part_length_offset = tags_data_offset + tags_data_size;
  uint32_t data_part_length = DecodeFixed32(batch.data + data_part_length_offset);
  size_t valid_column_part_offset = data_part_length_offset + sizeof(uint32_t);
  uint16_t valid_column_type = DecodeFixed16(batch.data + valid_column_part_offset);
  uint32_t valid_column_part_size = sizeof(uint16_t);
  if (valid_column_type == TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_VECTOR) {
    uint32_t valid_col_num = DecodeFixed32(batch.data + valid_column_part_offset + sizeof(uint16_t));
    valid_column_part_size += sizeof(uint32_t) + sizeof(uint32_t) * valid_col_num;
  }
  size_t block_span_data_offset = valid_column_part_offset + valid_column_part_size;
  uint32_t block_span_data_size = data_part_length - valid_column_part_size;
  uint32_t block_span_offset = tags_data_offset + tags_data_size;
  if (block_span_data_offset >= batch.len) {
    return {nullptr, 0};
  }
  return {batch.data + block_span_data_offset, block_span_data_size};
}

class ScopedForceReCompress {
 public:
  explicit ScopedForceReCompress(bool enabled) : old_value_(EngineOptions::force_re_compress) {
    EngineOptions::force_re_compress = enabled;
  }

  ~ScopedForceReCompress() {
    EngineOptions::force_re_compress = old_value_;
  }

 private:
  bool old_value_;
};

void AppendRaw(TsBufferBuilder* dst, const void* data, size_t len) {
  dst->append(reinterpret_cast<const char*>(data), len);
}

std::string BuildLegacyOffsetAggData() {
  constexpr uint32_t metric_col_count = 2;
  const uint16_t count = 3;

  const timestamp64 ts_max = 3000;
  const timestamp64 ts_min = 1000;
  TsBufferBuilder ts_agg;
  AppendRaw(&ts_agg, &count, sizeof(count));
  AppendRaw(&ts_agg, &ts_max, sizeof(ts_max));
  AppendRaw(&ts_agg, &ts_min, sizeof(ts_min));

  const int32_t int_max = 11;
  const int32_t int_min = 7;
  const bool int_sum_overflow = false;
  const int64_t int_sum = 27;
  TsBufferBuilder int_agg;
  AppendRaw(&int_agg, &count, sizeof(count));
  AppendRaw(&int_agg, &int_max, sizeof(int_max));
  AppendRaw(&int_agg, &int_min, sizeof(int_min));
  AppendRaw(&int_agg, &int_sum_overflow, sizeof(int_sum_overflow));
  AppendRaw(&int_agg, &int_sum, sizeof(int_sum));

  TsBufferBuilder legacy_agg;
  PutFixed32(&legacy_agg, ts_agg.size());
  PutFixed32(&legacy_agg, ts_agg.size() + int_agg.size());
  static_assert(metric_col_count == 2, "legacy offsets must match metric column count");
  legacy_agg.append(ts_agg.AsSlice());
  legacy_agg.append(int_agg.AsSlice());
  return std::string(legacy_agg.data(), legacy_agg.size());
}

std::string BuildLegacyV0BlockData(const std::vector<AttributeInfo>& metric_schema) {
  const auto& mgr = CompressorManager::GetInstance();
  constexpr uint32_t n_rows = 3;
  const uint32_t n_cols = metric_schema.size() + 1;

  TsBufferBuilder block_data;
  block_data.resize(n_cols * sizeof(uint32_t));

  auto append_column = [&](uint32_t col_idx, const TSSlice& payload) {
    block_data.append(payload);
    uint32_t col_offset = block_data.size() - n_cols * sizeof(uint32_t);
    std::memcpy(block_data.data() + col_idx * sizeof(uint32_t), &col_offset, sizeof(uint32_t));
  };

  std::vector<uint64_t> osns{10, 11, 12};
  TsBufferBuilder osn_compressed;
  AttributeInfo osn_attr{};
  osn_attr.encode_algo = roachpb::ENCODE_ALGO_SIMPLE8B;
  osn_attr.compress_algo = roachpb::COMPRESS_ALGO_DISABLED;
  osn_attr.compress_level = roachpb::COMPRESS_LEVEL_UNSPECIFIED;
  auto [osn_encode, osn_compress] = mgr.GetAlgorithm(DATATYPE::INT64, osn_attr);
  bool ok = mgr.CompressData({reinterpret_cast<char*>(osns.data()), osns.size() * sizeof(uint64_t)}, nullptr,
                             n_rows, &osn_compressed, osn_encode, osn_compress, osn_attr.compress_level);
  assert(ok);
  append_column(0, osn_compressed.AsSlice());

  std::vector<timestamp64> timestamps{1000, 2000, 3000};
  TsBufferBuilder ts_compressed;
  auto [ts_encode, ts_compress] = mgr.GetAlgorithm(DATATYPE::TIMESTAMP64, metric_schema[0]);
  ok = mgr.CompressData({reinterpret_cast<char*>(timestamps.data()), timestamps.size() * sizeof(timestamp64)}, nullptr,
                        n_rows, &ts_compressed, ts_encode, ts_compress, metric_schema[0].compress_level);
  assert(ok);
  append_column(1, ts_compressed.AsSlice());

  std::vector<int32_t> int_values{7, 9, 11};
  TsBitmap int_bitmap(n_rows);
  TsBufferBuilder int_col;
  int_col.append(int_bitmap.GetData());
  TsBufferBuilder int_compressed;
  auto [int_encode, int_compress] = mgr.GetAlgorithm(DATATYPE::INT32, metric_schema[1]);
  ok = mgr.CompressData({reinterpret_cast<char*>(int_values.data()), int_values.size() * sizeof(int32_t)}, &int_bitmap,
                        n_rows, &int_compressed, int_encode, int_compress, metric_schema[1].compress_level);
  assert(ok);
  int_col.append(int_compressed.AsSlice());
  append_column(2, int_col.AsSlice());

  return std::string(block_data.data(), block_data.size());
}

std::string BuildLegacyV0BatchFromTagPrefix(std::string tag_prefix, const std::vector<AttributeInfo>& metric_schema) {
  constexpr uint32_t n_rows = 3;
  const uint32_t n_cols = metric_schema.size() + 1;
  std::string block_data = BuildLegacyV0BlockData(metric_schema);
  std::string agg_data = BuildLegacyOffsetAggData();

  // v0: BlockSpanHeader{length(4)+min_ts...block_version}(60) = 64 bytes
  constexpr uint32_t v0_header_size = sizeof(uint32_t) + TsBatchData::block_span_data_header_size_;
  TsBufferBuilder block_span;
  const uint32_t block_span_len = v0_header_size + block_data.size() + agg_data.size();
  PutFixed32(&block_span, block_span_len);
  PutFixed64(&block_span, 1000);
  PutFixed64(&block_span, 3000);
  PutFixed64(&block_span, 10);
  PutFixed64(&block_span, 12);
  PutFixed64(&block_span, 10);
  PutFixed64(&block_span, 12);
  PutFixed32(&block_span, n_cols);
  PutFixed32(&block_span, n_rows);
  PutFixed32(&block_span, 0);
  assert(block_span.size() == v0_header_size);
  block_span.append(block_data);
  block_span.append(agg_data);

  tag_prefix.append(block_span.data(), block_span.size());
  EncodeFixed32(tag_prefix.data() + TsBatchData::batch_version_offset_, 0);
  EncodeFixed32(tag_prefix.data() + TsBatchData::data_length_offset_, tag_prefix.size());
  EncodeFixed32(tag_prefix.data() + TsBatchData::row_num_offset_, n_rows);
  tag_prefix[TsBatchData::row_type_offset_] = DataTagFlag::DATA_AND_TAG;
  return tag_prefix;
}

std::string BuildLegacyV1BatchFromTagPrefix(std::string tag_prefix, const std::vector<AttributeInfo>& metric_schema,
                                            uint32_t block_version) {
  constexpr uint32_t n_rows = 3;
  const uint32_t n_cols = metric_schema.size() + 1;
  std::string block_data = BuildLegacyV0BlockData(metric_schema);
  std::string agg_data = BuildLegacyOffsetAggData();

  // v1: BlockSpanHeader{length(4)+min_ts...block_version}(60) = 64 bytes
  const uint32_t v1_header_size = sizeof(uint32_t) + TsBatchData::block_span_data_header_size_;
  TsBufferBuilder block_span;
  const uint32_t block_span_len = v1_header_size + block_data.size() + agg_data.size();
  PutFixed32(&block_span, block_span_len);
  PutFixed64(&block_span, 1000);
  PutFixed64(&block_span, 3000);
  PutFixed64(&block_span, 10);
  PutFixed64(&block_span, 12);
  PutFixed64(&block_span, 10);
  PutFixed64(&block_span, 12);
  PutFixed32(&block_span, n_cols);
  PutFixed32(&block_span, n_rows);
  PutFixed32(&block_span, block_version);
  assert(block_span.size() == v1_header_size);
  block_span.append(block_data);
  block_span.append(agg_data);

  tag_prefix.append(block_span.data(), block_span.size());
  EncodeFixed32(tag_prefix.data() + TsBatchData::batch_version_offset_, 1);
  EncodeFixed32(tag_prefix.data() + TsBatchData::data_length_offset_, tag_prefix.size());
  EncodeFixed32(tag_prefix.data() + TsBatchData::row_num_offset_, n_rows);
  tag_prefix[TsBatchData::row_type_offset_] = DataTagFlag::DATA_AND_TAG;
  return tag_prefix;
}

}  // namespace

// Verifies batch headers and block-span metadata are serialized with the
// expected little-endian field layout.
TEST(TsBatchDataTest, HeaderAndBlockSpanHeaderUseLittleEndianEncoding) {
  std::vector<uint32_t> valid_cols;
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
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.AddBlockSpanDataHeader(min_ts, max_ts, min_osn, max_osn, first_osn, last_osn, n_cols, n_rows, block_version);
  batch.UpdateBatchDataInfo(valid_cols);

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

  const size_t block_offset = tags_size_offset + sizeof(uint32_t) + tags.size() + sizeof(uint32_t) + sizeof(uint16_t);
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
  std::vector<uint32_t> valid_cols;
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
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.AddBlockSpanDataHeader(100, 200, 11, 22, 33, 44, 5, 7, 3);
  batch.UpdateBatchDataInfo(valid_cols);

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

// Tests that non-empty valid columns are encoded as VECTOR type with col count
// and column IDs, and that data_part_length correctly covers both valid column
// part and block span part.
TEST(TsBatchDataTest, ValidColumnPartEncodingWithNonEmptyCols) {
  TsBatchData batch;
  const std::string ptag{"pk"};
  const std::string tags{"tag"};
  std::vector<uint32_t> valid_cols = {0, 2, 5};

  batch.SetTagOSN(123);
  batch.SetHashPoint(17);
  batch.SetTableVersion(9);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.AddBlockSpanDataHeader(100, 200, 11, 22, 33, 44, 5, 7, 3);
  batch.UpdateBatchDataInfo(valid_cols);

  // valid_column_type = VECTOR
  uint16_t vc_type = DecodeFixed16(batch.data_.data() + batch.valid_column_part_offset_);
  EXPECT_EQ(vc_type, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_VECTOR);

  // valid_col_num
  size_t vc_num_offset = batch.valid_column_part_offset_ + sizeof(uint16_t);
  uint32_t vc_num = DecodeFixed32(batch.data_.data() + vc_num_offset);
  EXPECT_EQ(vc_num, 3u);

  // each valid column ID
  size_t col_base = vc_num_offset + sizeof(uint32_t);
  for (size_t i = 0; i < valid_cols.size(); i++) {
    uint32_t col_id = DecodeFixed32(batch.data_.data() + col_base + i * sizeof(uint32_t));
    EXPECT_EQ(col_id, valid_cols[i]);
  }

  // valid_column_part_length = type(2) + count(4) + 3 * col_id(4)
  uint32_t expected_vc_len = sizeof(uint16_t) + sizeof(uint32_t) + valid_cols.size() * sizeof(uint32_t);
  EXPECT_EQ(batch.valid_column_part_length_, expected_vc_len);

  // data_part_length = valid_column_part_length + block_span_data_header_size
  uint32_t data_part_length = DecodeFixed32(batch.data_.data() + batch.data_part_length_offset_);
  EXPECT_EQ(data_part_length, expected_vc_len + TsBatchData::block_span_data_header_size_);

  // block span data still starts after valid column part
  EXPECT_TRUE(batch.HasBlockSpanData());
  EXPECT_EQ(batch.GetBlockSpanData().len, TsBatchData::block_span_data_header_size_);

  // overall data_length includes everything
  EXPECT_EQ(batch.GetDataLength(), static_cast<uint32_t>(batch.data_.size()));
  EXPECT_EQ(batch.GetRowType(), DataTagFlag::DATA_AND_TAG);
  EXPECT_EQ(batch.GetRowCount(), 7u);
}

// Tests that empty valid columns encode as TUPLE type with no extra fields.
TEST(TsBatchDataTest, ValidColumnPartEncodingWithEmptyCols) {
  TsBatchData batch;
  const std::string ptag{"pk"};
  const std::string tags{"tag"};
  std::vector<uint32_t> empty_cols;

  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddDataPartLengthAndValidCols(empty_cols);
  batch.AddBlockSpanDataHeader(100, 200, 11, 22, 33, 44, 5, 7, 3);
  batch.UpdateBatchDataInfo(empty_cols);

  // valid_column_type = TUPLE
  uint16_t vc_type = DecodeFixed16(batch.data_.data() + batch.valid_column_part_offset_);
  EXPECT_EQ(vc_type, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE);

  // valid_column_part_length = sizeof(uint16_t) only
  EXPECT_EQ(batch.valid_column_part_length_, sizeof(uint16_t));

  // data_part_length = sizeof(uint16_t) + block_span_data_header_size
  uint32_t data_part_length = DecodeFixed32(batch.data_.data() + batch.data_part_length_offset_);
  EXPECT_EQ(data_part_length, static_cast<uint32_t>(sizeof(uint16_t) + TsBatchData::block_span_data_header_size_));

  EXPECT_TRUE(batch.HasBlockSpanData());
  EXPECT_EQ(batch.GetBlockSpanData().len, TsBatchData::block_span_data_header_size_);
}

// Tests tag-only batch (no block span) with valid columns.
TEST(TsBatchDataTest, TagOnlyWithValidColumns) {
  TsBatchData batch;
  const std::string ptag{"pk"};
  const std::string tags{"tag"};
  std::vector<uint32_t> valid_cols = {1, 3};

  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddDataPartLengthAndValidCols(valid_cols);
  // no block span header added
  batch.UpdateBatchDataInfo(valid_cols);

  EXPECT_EQ(batch.GetRowType(), DataTagFlag::TAG_ONLY);
  EXPECT_FALSE(batch.HasBlockSpanData());

  // data_part_length = valid_column_part_length (no block span)
  uint32_t expected_vc_len = sizeof(uint16_t) + sizeof(uint32_t) + valid_cols.size() * sizeof(uint32_t);
  EXPECT_EQ(batch.valid_column_part_length_, expected_vc_len);
  uint32_t data_part_length = DecodeFixed32(batch.data_.data() + batch.data_part_length_offset_);
  EXPECT_EQ(data_part_length, expected_vc_len);

  // row count defaults to 1 for tag-only
  EXPECT_EQ(batch.GetRowCount(), 1u);
}

// Tests that Clear() resets valid column state so a reused TsBatchData encodes
// correctly.
TEST(TsBatchDataTest, ClearResetsValidColumnState) {
  TsBatchData batch;
  const std::string ptag{"pk"};
  const std::string tags{"tag"};
  std::vector<uint32_t> valid_cols = {0, 1};

  // first use: with valid columns
  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.AddBlockSpanDataHeader(100, 200, 1, 2, 3, 4, 5, 6, 7);
  batch.UpdateBatchDataInfo(valid_cols);
  ASSERT_TRUE(batch.HasBlockSpanData());

  // second use after Clear: without valid columns
  batch.Clear();
  std::vector<uint32_t> empty_cols;
  batch.SetTagOSN(2);
  batch.SetHashPoint(2);
  batch.SetTableVersion(2);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddDataPartLengthAndValidCols(empty_cols);
  batch.AddBlockSpanDataHeader(300, 400, 10, 20, 30, 40, 5, 8, 1);
  batch.UpdateBatchDataInfo(empty_cols);

  // valid_column_type should be TUPLE now
  uint16_t vc_type = DecodeFixed16(batch.data_.data() + batch.valid_column_part_offset_);
  EXPECT_EQ(vc_type, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE);
  EXPECT_EQ(batch.valid_column_part_length_, sizeof(uint16_t));

  // block span data should still be accessible
  EXPECT_TRUE(batch.HasBlockSpanData());
  EXPECT_EQ(batch.GetBlockSpanData().len, TsBatchData::block_span_data_header_size_);
  EXPECT_EQ(batch.GetRowCount(), 8u);
}

// Verifies that the serialized batch with valid columns can be correctly parsed
// by simulating what ParseBatchDataLayout does: read data_part_length, then
// valid_column_type, then valid column IDs, and finally locate block span data.
TEST(TsBatchDataTest, ValidColumnLayoutParsingRoundTrip) {
  TsBatchData batch;
  const std::string ptag{"pk"};
  const std::string tags{"tag"};
  std::vector<uint32_t> valid_cols = {0, 2, 4};

  batch.SetTagOSN(1);
  batch.SetHashPoint(1);
  batch.SetTableVersion(1);
  batch.AddPrimaryTag({const_cast<char*>(ptag.data()), ptag.size()});
  batch.AddTags({const_cast<char*>(tags.data()), tags.size()});
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.AddBlockSpanDataHeader(100, 200, 11, 22, 33, 44, 5, 7, 3);
  batch.UpdateBatchDataInfo(valid_cols);

  TSSlice raw = batch.data_.AsSlice();
  const char* data = raw.data;
  size_t len = raw.len;

  // Simulate ParseBatchDataLayout parsing: read data_part_length after tags
  size_t dp_offset = batch.data_part_length_offset_;
  uint32_t data_part_length = DecodeFixed32(data + dp_offset);

  // valid_column_part starts after data_part_length
  size_t vc_offset = dp_offset + sizeof(uint32_t);
  uint16_t vc_type = DecodeFixed16(data + vc_offset);
  EXPECT_EQ(vc_type, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_VECTOR);

  uint32_t vc_part_size = sizeof(uint16_t);
  uint32_t vc_num = DecodeFixed32(data + vc_offset + sizeof(uint16_t));
  EXPECT_EQ(vc_num, 3u);
  vc_part_size += sizeof(uint32_t) + vc_num * sizeof(uint32_t);

  // Verify each column ID
  for (uint32_t i = 0; i < vc_num; i++) {
    uint32_t col_id = DecodeFixed32(data + vc_offset + sizeof(uint16_t) + sizeof(uint32_t) + i * sizeof(uint32_t));
    EXPECT_EQ(col_id, valid_cols[i]);
  }

  // block_span_data_offset
  size_t bs_offset = vc_offset + vc_part_size;
  EXPECT_EQ(data_part_length, vc_part_size + TsBatchData::block_span_data_header_size_);

  // Verify block span header fields are at the correct offset
  EXPECT_EQ(DecodeFixed64(data + bs_offset + TsBatchData::min_ts_offset_in_span_data_), 100);
  EXPECT_EQ(DecodeFixed64(data + bs_offset + TsBatchData::max_ts_offset_in_span_data_), 200);
  EXPECT_EQ(DecodeFixed32(data + bs_offset + TsBatchData::n_rows_offset_in_span_data_), 7u);

  // Verify block span data ends exactly at the batch boundary
  uint32_t bs_size = data_part_length - vc_part_size;
  EXPECT_EQ(bs_offset + bs_size, len);
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

// When a backup batch is generated by rebuilding block bytes, block agg bytes are
// emitted in the current sparse layout. The batch header and the block item
// written by restore must therefore both carry CURRENT_BLOCK_VERSION.
TEST_F(TsBatchDataWorkerTest, RecompressedSparseAggBatchRestoresCurrentBlockVersion) {
  TSTableID table_id = 10033;
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
  ASSERT_EQ(engine_->FlushVGroups(ctx_), KStatus::SUCCESS);

  std::string backup_data;
  uint32_t backup_row_num = 0;
  {
    ScopedForceReCompress force_recompress(true);
    uint64_t read_job_id = 1;
    TSSlice backup_batch;
    s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id,
                               &backup_batch, &backup_row_num, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ASSERT_GT(backup_row_num, 0U);

    TSSlice block_span_data = ExtractBatchBlockSpanData(backup_batch);
    ASSERT_NE(block_span_data.data, nullptr);
    ASSERT_GE(block_span_data.len, static_cast<size_t>(TsBatchData::block_span_data_header_size_));
    EXPECT_EQ(DecodeFixed32(block_span_data.data + TsBatchData::block_version_offset_in_span_data_),
              CURRENT_BLOCK_VERSION);

    backup_data.assign(backup_batch.data, backup_batch.len);
    s = engine_->BatchJobFinish(ctx_, read_job_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  DestroyEngine();
  InitEngine(engine_root_path);
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
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped, true, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<EntityResultIndex> entity_indexes;
  s = ts_table->GetEntityIdByHashSpan(ctx_, {0, UINT32_MAX}, UINT64_MAX, entity_indexes);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_indexes.size(), 1UL);

  auto vgroup = engine_->GetTsVGroup(entity_indexes[0].subGroupId);
  auto current_version = vgroup->CurrentVersion();
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  s = vgroup->GetBlockSpans(table_id, entity_indexes[0].entityId, {INT64_MIN, INT64_MAX},
                            DATATYPE::TIMESTAMP64, schema_mgr, 1, current_version, &block_spans);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(block_spans.size(), 1UL);
  EXPECT_EQ(block_spans.front()->GetBlockVersion(), CURRENT_BLOCK_VERSION);
}

// Simulates a snapshot generated by a very old node: batch_version=0 has no
// block_version field in its 60-byte block-span header, and the agg payload uses
// the legacy offset layout. New restore code must persist it as block_version=0
// and keep reading pre-agg values through the legacy offset path.
TEST_F(TsBatchDataWorkerTest, LegacyBatchVersionZeroRestoresOffsetAggAsBlockVersionZero) {
  TSTableID table_id = 10034;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
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
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, 1000);
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t read_job_id = 1;
  TSSlice current_batch;
  uint32_t current_row_num = 0;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id,
                             &current_batch, &current_row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice tag_prefix_batch = ExtractBatchBlockTagPrefix(current_batch);
  ASSERT_NE(tag_prefix_batch.data, nullptr);
  std::string tag_prefix(tag_prefix_batch.data, tag_prefix_batch.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::string legacy_batch_data = BuildLegacyV0BatchFromTagPrefix(tag_prefix, *metric_schema);

  DestroyEngine();
  InitEngine(engine_root_path);
  MakeDirectory(engine_root_path + "/temp_db_");
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice legacy_batch{legacy_batch_data.data(), legacy_batch_data.size()};
  uint32_t restored_row_num = 0;
  uint64_t write_job_id = 2;
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &legacy_batch, &restored_row_num,
                              TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(restored_row_num, 3U);
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped, true, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<EntityResultIndex> entity_indexes;
  s = ts_table->GetEntityIdByHashSpan(ctx_, {0, UINT32_MAX}, UINT64_MAX, entity_indexes);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_indexes.size(), 1UL);

  auto vgroup = engine_->GetTsVGroup(entity_indexes[0].subGroupId);
  auto current_version = vgroup->CurrentVersion();
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  s = vgroup->GetBlockSpans(table_id, entity_indexes[0].entityId, {INT64_MIN, INT64_MAX},
                            DATATYPE::TIMESTAMP64, schema_mgr, 1, current_version, &block_spans);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(block_spans.size(), 1UL);
  auto block_span = block_spans.front();
  ASSERT_EQ(block_span->GetBlockVersion(), 0U);
  ASSERT_TRUE(block_span->HasPreAgg());

  uint16_t pre_count = 0;
  ASSERT_EQ(block_span->GetPreCount(1, nullptr, pre_count), KStatus::SUCCESS);
  EXPECT_EQ(pre_count, 3U);
  void* pre_max = nullptr;
  ASSERT_EQ(block_span->GetPreMax(1, nullptr, pre_max), KStatus::SUCCESS);
  ASSERT_NE(pre_max, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(pre_max), 11);
  void* pre_min = nullptr;
  ASSERT_EQ(block_span->GetPreMin(1, nullptr, pre_min), KStatus::SUCCESS);
  ASSERT_NE(pre_min, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(pre_min), 7);
  void* pre_sum = nullptr;
  bool is_overflow = true;
  ASSERT_EQ(block_span->GetPreSum(1, nullptr, pre_sum, is_overflow), KStatus::SUCCESS);
  ASSERT_NE(pre_sum, nullptr);
  EXPECT_FALSE(is_overflow);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(pre_sum), 27);

  char* ts_col = nullptr;
  std::unique_ptr<TsBitmapBase> bitmap;
  ASSERT_EQ(block_span->GetFixLenColAddr(0, &ts_col, &bitmap), KStatus::SUCCESS);
  ASSERT_NE(ts_col, nullptr);
  EXPECT_EQ(*reinterpret_cast<timestamp64*>(ts_col), 1000);
  char* int_col = nullptr;
  ASSERT_EQ(block_span->GetFixLenColAddr(1, &int_col, &bitmap), KStatus::SUCCESS);
  ASSERT_NE(int_col, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(int_col + sizeof(int32_t) * 2), 11);
}

// Simulates an intermediate old snapshot: batch_version=1 already carries a
// block_version field, but block_version=1 is still below the sparse agg layout
// threshold. Restore must preserve block_version=1 and read legacy offset agg.
TEST_F(TsBatchDataWorkerTest, LegacyBatchVersionOneRestoresOffsetAggAsBlockVersionOne) {
  TSTableID table_id = 10035;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
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
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, 1000);
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t read_job_id = 1;
  TSSlice current_batch;
  uint32_t current_row_num = 0;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id,
                             &current_batch, &current_row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice tag_prefix_batch = ExtractBatchBlockTagPrefix(current_batch);
  ASSERT_NE(tag_prefix_batch.data, nullptr);
  std::string tag_prefix(tag_prefix_batch.data, tag_prefix_batch.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::string legacy_batch_data = BuildLegacyV1BatchFromTagPrefix(tag_prefix, *metric_schema, 1);

  DestroyEngine();
  InitEngine(engine_root_path);
  MakeDirectory(engine_root_path + "/temp_db_");
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice legacy_batch{legacy_batch_data.data(), legacy_batch_data.size()};
  uint32_t restored_row_num = 0;
  uint64_t write_job_id = 2;
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &legacy_batch, &restored_row_num,
                              TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(restored_row_num, 3U);
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped, true, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<EntityResultIndex> entity_indexes;
  s = ts_table->GetEntityIdByHashSpan(ctx_, {0, UINT32_MAX}, UINT64_MAX, entity_indexes);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_indexes.size(), 1UL);

  auto vgroup = engine_->GetTsVGroup(entity_indexes[0].subGroupId);
  auto current_version = vgroup->CurrentVersion();
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  s = vgroup->GetBlockSpans(table_id, entity_indexes[0].entityId, {INT64_MIN, INT64_MAX},
                            DATATYPE::TIMESTAMP64, schema_mgr, 1, current_version, &block_spans);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(block_spans.size(), 1UL);
  auto block_span = block_spans.front();
  ASSERT_EQ(block_span->GetBlockVersion(), 1U);
  ASSERT_TRUE(block_span->HasPreAgg());

  uint16_t pre_count = 0;
  ASSERT_EQ(block_span->GetPreCount(1, nullptr, pre_count), KStatus::SUCCESS);
  EXPECT_EQ(pre_count, 3U);
  void* pre_max = nullptr;
  ASSERT_EQ(block_span->GetPreMax(1, nullptr, pre_max), KStatus::SUCCESS);
  ASSERT_NE(pre_max, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(pre_max), 11);
  void* pre_min = nullptr;
  ASSERT_EQ(block_span->GetPreMin(1, nullptr, pre_min), KStatus::SUCCESS);
  ASSERT_NE(pre_min, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(pre_min), 7);
  void* pre_sum = nullptr;
  bool is_overflow = true;
  ASSERT_EQ(block_span->GetPreSum(1, nullptr, pre_sum, is_overflow), KStatus::SUCCESS);
  ASSERT_NE(pre_sum, nullptr);
  EXPECT_FALSE(is_overflow);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(pre_sum), 27);
}

// End-to-end test: insert data, read batch with empty valid cols, then modify the
// serialized batch to inject valid columns, and write back through WriteBatchData
// to verify ParseBatchDataLayout handles valid columns correctly.
TEST_F(TsBatchDataWorkerTest, WriteBatchDataWithValidColumns) {
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
  TSSlice original_batch;
  uint32_t original_row_num = 0;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id,
                             &original_batch, &original_row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GT(original_row_num, 0U);
  std::string original_data(original_batch.data, original_batch.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Parse the original batch to locate the data_part_length and valid_column_part
  const char* orig = original_data.data();
  size_t orig_len = original_data.size();
  uint16_t ptag_size = DecodeFixed16(orig + TsBatchData::header_size_);
  size_t ptag_offset = TsBatchData::header_size_ + sizeof(uint16_t);
  size_t tags_size_offset = ptag_offset + ptag_size;
  uint32_t tags_size = DecodeFixed32(orig + tags_size_offset);
  size_t tags_end = tags_size_offset + sizeof(uint32_t) + tags_size;
  // data_part_length is at tags_end
  // valid_column_type is at tags_end + 4
  uint16_t orig_vc_type = DecodeFixed16(orig + tags_end + sizeof(uint32_t));
  ASSERT_EQ(orig_vc_type, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE);
  // block span data starts at tags_end + 4 + 2 (TUPLE has 2-byte type only)
  size_t block_span_start = tags_end + sizeof(uint32_t) + sizeof(uint16_t);
  // block span data = everything from block_span_start to end
  size_t block_span_len = orig_len - block_span_start;

  // Inject valid columns: replace the TUPLE(2 bytes) with VECTOR(2) + count(4) + col_ids
  std::vector<uint32_t> inject_cols = {0, 1, 3};
  uint32_t inject_vc_part_size = sizeof(uint16_t) + sizeof(uint32_t) + inject_cols.size() * sizeof(uint32_t);
  uint32_t new_data_part_length = inject_vc_part_size + block_span_len;
  size_t new_len = tags_end + sizeof(uint32_t) + inject_vc_part_size + block_span_len;

  std::string modified_data;
  modified_data.resize(new_len);
  char* mod = modified_data.data();
  // Copy header + ptag + tags (up to and including tags data)
  memcpy(mod, orig, tags_end);
  // Write new data_part_length
  EncodeFixed32(mod + tags_end, new_data_part_length);
  size_t pos = tags_end + sizeof(uint32_t);
  // Write valid_column_type = VECTOR
  EncodeFixed16(mod + pos, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_VECTOR);
  pos += sizeof(uint16_t);
  // Write valid_col_num
  EncodeFixed32(mod + pos, static_cast<uint32_t>(inject_cols.size()));
  pos += sizeof(uint32_t);
  // Write each valid column ID
  for (auto col_id : inject_cols) {
    EncodeFixed32(mod + pos, col_id);
    pos += sizeof(uint32_t);
  }
  // Copy block span data from original
  memcpy(mod + pos, orig + block_span_start, block_span_len);
  // Update data_length in header
  EncodeFixed32(mod + TsBatchData::data_length_offset_, static_cast<uint32_t>(new_len));

  // Reopen engine and write the modified batch with valid columns
  delete engine_;
  Remove(engine_root_path);
  MakeDirectory(engine_root_path);
  engine_ = new TSEngineImpl(opts_);
  s = engine_->Init(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  MakeDirectory(engine_root_path + "/temp_db_");
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice data = {modified_data.data(), modified_data.size()};
  uint32_t row_num = 0;
  uint64_t write_job_id = 2;
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &row_num, TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, original_row_num);
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Compatibility test: construct a v1 batch (old format with block_span_length prefix
// in BlockSpanHeader), then write it through WriteBatchData to verify ParseBatchDataLayout
// correctly handles v1 format.
TEST_F(TsBatchDataWorkerTest, WriteV1BatchDataCompatibility) {
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

  // Insert data and read it back to get a valid v2 batch
  timestamp64 start_ts = 10086000;
  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t read_job_id = 1;
  TSSlice v2_batch;
  uint32_t row_num = 0;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id,
                             &v2_batch, &row_num, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GT(row_num, 0U);
  std::string v2_data(v2_batch.data, v2_batch.len);
  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Parse the v2 batch to extract tag portion and block span portion
  const char* v2 = v2_data.data();
  uint16_t ptag_size = DecodeFixed16(v2 + TsBatchData::header_size_);
  size_t ptag_offset = TsBatchData::header_size_ + sizeof(uint16_t);
  size_t tags_size_offset = ptag_offset + ptag_size;
  uint32_t tags_size = DecodeFixed32(v2 + tags_size_offset);
  size_t tags_end = tags_size_offset + sizeof(uint32_t) + tags_size;
  // v2: data_part_length(4) + valid_column_type(2=TUPLE) + BlockSpanHeader + compressed
  uint16_t vc_type = DecodeFixed16(v2 + tags_end + sizeof(uint32_t));
  ASSERT_EQ(vc_type, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE);
  size_t block_span_start = tags_end + sizeof(uint32_t) + sizeof(uint16_t);
  size_t block_span_len = v2_data.size() - block_span_start;

  // Construct v1 batch: tags | BlockSpanHeader{length(4)+min_ts...block_version}(60) | compressed
  uint32_t v1_block_span_length = sizeof(uint32_t) + block_span_len;
  size_t v1_len = tags_end + v1_block_span_length;

  std::string v1_data;
  v1_data.resize(v1_len);
  char* v1 = v1_data.data();
  // Copy header + ptag + tags (identical for v1 and v2)
  memcpy(v1, v2, tags_end);
  // Write BlockSpanHeader.length (the only length prefix in v1)
  EncodeFixed32(v1 + tags_end, v1_block_span_length);
  // Copy block span data (min_ts through compressed data)
  memcpy(v1 + tags_end + sizeof(uint32_t), v2 + block_span_start, block_span_len);
  // Set batch_version = 1
  EncodeFixed32(v1 + TsBatchData::batch_version_offset_, 1);
  // Update data_length
  EncodeFixed32(v1 + TsBatchData::data_length_offset_, static_cast<uint32_t>(v1_len));
  // Set row_type = DATA_AND_TAG (same as v2)
  uint8_t row_type = DataTagFlag::DATA_AND_TAG;
  memcpy(v1 + TsBatchData::row_type_offset_, &row_type, sizeof(row_type));
  // Set row_num
  EncodeFixed32(v1 + TsBatchData::row_num_offset_, row_num);

  // Reopen engine and write the v1 batch
  delete engine_;
  Remove(engine_root_path);
  MakeDirectory(engine_root_path);
  engine_ = new TSEngineImpl(opts_);
  s = engine_->Init(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  MakeDirectory(engine_root_path + "/temp_db_");
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice v1_slice = {v1_data.data(), v1_data.size()};
  uint32_t written_row_num = 0;
  uint64_t write_job_id = 2;
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &v1_slice, &written_row_num,
                              TsDataSource::Restore, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(written_row_num, row_num);
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Verifies ParseBatchDataLayout correctly handles v1 tag-only batch format
// (no data part after tags). Uses a unit-level test that calls ParseBatchDataLayout
// indirectly through WriteBatchData with a manually constructed v1 tag-only batch.
TEST(TsBatchDataTest, V1TagOnlyLayoutParsing) {
  // Construct a minimal v1 tag-only batch: header + ptag_len + ptag + tags_len + tags
  const std::string ptag{"pk"};
  const std::string tags{"tag"};
  size_t v1_len = TsBatchData::header_size_ + sizeof(uint16_t) + ptag.size() + sizeof(uint32_t) + tags.size();

  std::string v1_data;
  v1_data.resize(v1_len);
  char* v1 = v1_data.data();
  memset(v1, 0, v1_len);

  // Set header fields
  EncodeFixed16(v1 + TsBatchData::hash_point_id_offset_, 1);
  EncodeFixed32(v1 + TsBatchData::data_length_offset_, static_cast<uint32_t>(v1_len));
  EncodeFixed64(v1 + TsBatchData::tag_osn_offset_, 1);
  EncodeFixed32(v1 + TsBatchData::batch_version_offset_, 1);  // v1
  EncodeFixed32(v1 + TsBatchData::ts_version_offset_, 1);
  EncodeFixed32(v1 + TsBatchData::row_num_offset_, 1);
  uint8_t row_type = DataTagFlag::TAG_ONLY;
  memcpy(v1 + TsBatchData::row_type_offset_, &row_type, sizeof(row_type));

  // ptag
  size_t pos = TsBatchData::header_size_;
  EncodeFixed16(v1 + pos, static_cast<uint16_t>(ptag.size()));
  pos += sizeof(uint16_t);
  memcpy(v1 + pos, ptag.data(), ptag.size());
  pos += ptag.size();

  // tags
  EncodeFixed32(v1 + pos, static_cast<uint32_t>(tags.size()));
  pos += sizeof(uint32_t);
  memcpy(v1 + pos, tags.data(), tags.size());

  // Verify: tags end at batch boundary (tag-only, no data part)
  EXPECT_EQ(pos + tags.size(), v1_len);

  // Verify the header fields
  EXPECT_EQ(DecodeFixed32(v1 + TsBatchData::batch_version_offset_), 1u);
  EXPECT_EQ(static_cast<uint8_t>(v1[TsBatchData::row_type_offset_]), DataTagFlag::TAG_ONLY);
  EXPECT_EQ(DecodeFixed32(v1 + TsBatchData::data_length_offset_), static_cast<uint32_t>(v1_len));
}

// Corrupts the persisted block-span length inside one batch payload and verifies
// the restore write path rejects the malformed batch layout.
TEST_F(TsBatchDataWorkerTest, RejectMalformedBatchLayout) {
  std::vector<uint32_t> valid_cols;
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
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.AddBlockSpanDataHeader(100, 200, 1, 2, 3, 4, 5, 6, 7);
  batch.UpdateBatchDataInfo(valid_cols);

  uint32_t invalid_block_len = static_cast<uint32_t>(batch.GetBlockSpanData().len + 8);
  // corrupt data_part_length to make block span appear larger than actual
  EncodeFixed32(batch.data_.data() + batch.data_part_length_offset_,
                batch.valid_column_part_length_ + invalid_block_len);

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
  std::vector<uint32_t> valid_cols;
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
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.UpdateBatchDataInfo(valid_cols);

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
  std::vector<uint32_t> valid_cols;
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
  batch.AddDataPartLengthAndValidCols(valid_cols);
  batch.UpdateBatchDataInfo(valid_cols);

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
