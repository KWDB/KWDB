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
#include "ts_del_item_manager.h"
#include "sys_utils.h"
#include "ts_ts_lsn_span_utils.h"
#include "ts_table_del_info.h"
#include "ts_batch_data_worker.h"

using namespace kwdbts;  // NOLINT

class TsDelItemUtilTest : public ::testing::Test {
};

TEST(TsDelItemUtilTest, TsCrossNone) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {12, 100};
  del_range.osn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == scan_range.ts_span.begin);
  ASSERT_TRUE(result[0].ts_span.end == scan_range.ts_span.end);
  ASSERT_TRUE(result[0].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[0].osn_span.end == scan_range.osn_span.end);
}

TEST(TsDelItemUtilTest, TsCrossOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  STDelRange del_range;
  del_range.ts_span = {11, 100};
  del_range.osn_span = {0, 90};
  std::vector<STScanRange> result;
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 11);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].osn_span.begin == 91);
  ASSERT_TRUE(result[0].osn_span.end == scan_range.osn_span.end);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 10);
  ASSERT_TRUE(result[1].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[1].osn_span.end == scan_range.osn_span.end);
}

TEST(TsDelItemUtilTest, TsCrossSome) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.osn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 1);
  ASSERT_TRUE(result[0].ts_span.end == 8);
  ASSERT_TRUE(result[0].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[0].osn_span.end == scan_range.osn_span.end);
}

TEST(TsDelItemUtilTest, TsCrossSome1) {
  STScanRange scan_range;
  scan_range.ts_span = {11, 100};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 15};
  del_range.osn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 16);
  ASSERT_TRUE(result[0].ts_span.end == 100);
  ASSERT_TRUE(result[0].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[0].osn_span.end == scan_range.osn_span.end);
}

TEST(TsDelItemUtilTest, TsCrossLeftOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {2, 11};
  del_range.osn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 1);
  ASSERT_TRUE(result[0].ts_span.end == 1);
  ASSERT_TRUE(result[0].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[0].osn_span.end == scan_range.osn_span.end);
}

TEST(TsDelItemUtilTest, TsCrossAll) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {1, 100};
  del_range.osn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 0);
}

TEST(TsDelItemUtilTest, TsCrossSomeLsnCrossNull) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.osn_span = {101, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[0].osn_span.end == scan_range.osn_span.end);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[1].osn_span.end == scan_range.osn_span.end);
}

TEST(TsDelItemUtilTest, TsCrossSomeLsnCrossOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.osn_span = {100, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].osn_span.begin == 0);
  ASSERT_TRUE(result[0].osn_span.end == 99);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[1].osn_span.end == 100);
}

TEST(TsDelItemUtilTest, TsCrossSomeLsnCrossSome) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.osn_span = {90, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].osn_span.begin == 0);
  ASSERT_TRUE(result[0].osn_span.end == 89);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[1].osn_span.end == 100);
}

TEST(TsDelItemUtilTest, TsCrossSomeLsnCrossLeftOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {1, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.osn_span = {2, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].osn_span.begin == 1);
  ASSERT_TRUE(result[0].osn_span.end == 1);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[1].osn_span.end == 100);
}

TEST(TsDelItemUtilTest, TsCrossSomeLsnCrossAll) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.osn_span = {1, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.osn_span = {1, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin= 1);
  ASSERT_TRUE(result[0].ts_span.end == 8);
  ASSERT_TRUE(result[0].osn_span.begin == scan_range.osn_span.begin);
  ASSERT_TRUE(result[0].osn_span.end == 100);
}

TEST(TsDelItemUtilTest, mergeSortedSpans) {
  std::list<KwTsSpan> raw_spans;
  raw_spans.push_back({-1000, -500});
  raw_spans.push_back({-600, -100});
  raw_spans.push_back({-1100, 100});
  std::vector<KwTsSpan> ret_spans;
  MergeTsSpans(raw_spans, &ret_spans);
  ASSERT_TRUE(ret_spans.size() == 1);
  ASSERT_TRUE(ret_spans[0].begin == -1100);
  ASSERT_TRUE(ret_spans[0].end == 100);
}

TEST(TsDelItemUtilTest, mergeSortedSpans1) {
  std::list<KwTsSpan> raw_spans;
  raw_spans.push_back({-600, -100});
  raw_spans.push_back({0, 500});
  raw_spans.push_back({0, 200});
  raw_spans.push_back({300, 500});
  raw_spans.push_back({-10, 700});
  std::vector<KwTsSpan> ret_spans;
  MergeTsSpans(raw_spans, &ret_spans);
  ASSERT_TRUE(ret_spans.size() == 2);
  ASSERT_TRUE(ret_spans[0].begin == -600);
  ASSERT_TRUE(ret_spans[1].end == 700);
}

TEST(TsDelItemUtilTest, mergeSortedSpans2) {
  std::list<KwTsSpan> raw_spans;
  raw_spans.push_back({-600, -500});
  raw_spans.push_back({-400, -300});
  raw_spans.push_back({-200, 200});
  raw_spans.push_back({300, 500});
  raw_spans.push_back({600, 700});
  std::vector<KwTsSpan> ret_spans;
  MergeTsSpans(raw_spans, &ret_spans);
  ASSERT_TRUE(ret_spans.size() == 5);
 }

 TEST(TsDelItemUtilTest, snapshot_pack) {
  char tmp[128];
  memset(tmp, 'a', 128);
  uint32_t package_id = 10086;
  TSTableID tbl_id = 12345;
  uint32_t tbl_version = 456787;
  TSSlice batch_data{tmp, 50};
  uint32_t row_num = 300;
  TSSlice del_data{tmp, 100};
  TSSlice data;
  bool s = STPackageSnapshotData::PackageData(package_id, tbl_id, tbl_version, batch_data, row_num, del_data, &data);
  ASSERT_TRUE(s);

  uint32_t package_id_1;
  TSTableID tbl_id_1;
  uint32_t tbl_version_1;
  TSSlice batch_data_1;
  uint32_t row_num_1;
  TSSlice del_data_1;
  STPackageSnapshotData::UnpackageData(data, package_id_1, tbl_id_1, tbl_version_1, batch_data_1, row_num_1, del_data_1);
  ASSERT_EQ(package_id, package_id_1);
  ASSERT_EQ(tbl_id, tbl_id_1);
  ASSERT_EQ(tbl_version, tbl_version_1);
  ASSERT_EQ(row_num, row_num_1);
  ASSERT_EQ(batch_data.len, batch_data_1.len);
  ASSERT_EQ(del_data.len, del_data_1.len);
  free(data.data);
}

TEST(TsDelItemUtilTest, snapshot_pack_301_version) {
  char tmp[128];
  memset(tmp, 'a', 128);
  uint32_t package_id = 10086;
  TSTableID tbl_id = 12345;
  uint32_t tbl_version = 456787;
  TsBatchData batch_work;
  batch_work.SetHashPoint(1111);
  batch_work.UpdateBatchDataInfo();
  TSSlice batch_data{batch_work.data_.data(), batch_work.data_.length()};
  uint32_t row_num = 300;
  TSSlice del_data{tmp, 100};
  TSSlice data;

   // package_id + table_id + table_version + row_num + data
  size_t data_len = 4 + 8 + 4 + 4 + batch_data.len;
  char* data_with_rownum = reinterpret_cast<char*>(malloc(data_len));
  data.data = data_with_rownum;
  data.len = data_len;
  KUint32(data_with_rownum) = package_id;
  data_with_rownum += 4;
  KUint64(data_with_rownum) = tbl_id;
  data_with_rownum += 8;
  KUint32(data_with_rownum) = tbl_version;
  data_with_rownum += 4;
  KInt32(data_with_rownum) = row_num;
  data_with_rownum += 4;
  memcpy(data_with_rownum, batch_data.data, batch_data.len);

  uint32_t package_id_1;
  TSTableID tbl_id_1;
  uint32_t tbl_version_1;
  TSSlice batch_data_1;
  uint32_t row_num_1;
  TSSlice del_data_1{nullptr, 0};
  STPackageSnapshotData::UnpackageData(data, package_id_1, tbl_id_1, tbl_version_1, batch_data_1, row_num_1, del_data_1);
  ASSERT_EQ(package_id, package_id_1);
  ASSERT_EQ(tbl_id, tbl_id_1);
  ASSERT_EQ(tbl_version, tbl_version_1);
  ASSERT_EQ(row_num, row_num_1);
  ASSERT_EQ(batch_data.len, batch_data_1.len);
  ASSERT_EQ(del_data_1.len, 0);
  free(data.data);
}
