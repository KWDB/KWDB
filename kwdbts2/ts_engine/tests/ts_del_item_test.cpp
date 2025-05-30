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

using namespace kwdbts;  // NOLINT

class TsDelItemMgrTest : public ::testing::Test {
};

TEST(TsDelItemMgrTest, TsCrossNone) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {12, 100};
  del_range.lsn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == scan_range.ts_span.begin);
  ASSERT_TRUE(result[0].ts_span.end == scan_range.ts_span.end);
  ASSERT_TRUE(result[0].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[0].lsn_span.end == scan_range.lsn_span.end);
}

TEST(TsDelItemMgrTest, TsCrossOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  STDelRange del_range;
  del_range.ts_span = {11, 100};
  del_range.lsn_span = {0, 90};
  std::vector<STScanRange> result;
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 11);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].lsn_span.begin == 91);
  ASSERT_TRUE(result[0].lsn_span.end == scan_range.lsn_span.end);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 10);
  ASSERT_TRUE(result[1].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[1].lsn_span.end == scan_range.lsn_span.end);
}

TEST(TsDelItemMgrTest, TsCrossSome) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.lsn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 1);
  ASSERT_TRUE(result[0].ts_span.end == 8);
  ASSERT_TRUE(result[0].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[0].lsn_span.end == scan_range.lsn_span.end);
}

TEST(TsDelItemMgrTest, TsCrossSome1) {
  STScanRange scan_range;
  scan_range.ts_span = {11, 100};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 15};
  del_range.lsn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 16);
  ASSERT_TRUE(result[0].ts_span.end == 100);
  ASSERT_TRUE(result[0].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[0].lsn_span.end == scan_range.lsn_span.end);
}

TEST(TsDelItemMgrTest, TsCrossLeftOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {2, 11};
  del_range.lsn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 1);
  ASSERT_TRUE(result[0].ts_span.end == 1);
  ASSERT_TRUE(result[0].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[0].lsn_span.end == scan_range.lsn_span.end);
}

TEST(TsDelItemMgrTest, TsCrossAll) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {1, 100};
  del_range.lsn_span = {0, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 0);
}

TEST(TsDelItemMgrTest, TsCrossSomeLsnCrossNull) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.lsn_span = {101, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin == 1);
  ASSERT_TRUE(result[0].ts_span.end == 8);
  ASSERT_TRUE(result[0].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[0].lsn_span.end == scan_range.lsn_span.end);
}

TEST(TsDelItemMgrTest, TsCrossSomeLsnCrossOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.lsn_span = {100, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].lsn_span.begin == 0);
  ASSERT_TRUE(result[0].lsn_span.end == 99);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[1].lsn_span.end == 100);
}

TEST(TsDelItemMgrTest, TsCrossSomeLsnCrossSome) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {0, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.lsn_span = {90, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].lsn_span.begin == 0);
  ASSERT_TRUE(result[0].lsn_span.end == 89);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[1].lsn_span.end == 100);
}

TEST(TsDelItemMgrTest, TsCrossSomeLsnCrossLeftOne) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {1, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.lsn_span = {2, 200};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 2);
  ASSERT_TRUE(result[0].ts_span.begin == 9);
  ASSERT_TRUE(result[0].ts_span.end == 11);
  ASSERT_TRUE(result[0].lsn_span.begin == 1);
  ASSERT_TRUE(result[0].lsn_span.end == 1);
  ASSERT_TRUE(result[1].ts_span.begin == 1);
  ASSERT_TRUE(result[1].ts_span.end == 8);
  ASSERT_TRUE(result[1].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[1].lsn_span.end == 100);
}

TEST(TsDelItemMgrTest, TsCrossSomeLsnCrossAll) {
  STScanRange scan_range;
  scan_range.ts_span = {1, 11};
  scan_range.lsn_span = {1, 100};
  std::vector<STScanRange> result;
  STDelRange del_range;
  del_range.ts_span = {9, 11};
  del_range.lsn_span = {1, 100};
  LSNRangeUtil::MergeRangeCross(scan_range, del_range, &result);
  ASSERT_TRUE(result.size() == 1);
  ASSERT_TRUE(result[0].ts_span.begin= 1);
  ASSERT_TRUE(result[0].ts_span.end == 8);
  ASSERT_TRUE(result[0].lsn_span.begin == scan_range.lsn_span.begin);
  ASSERT_TRUE(result[0].lsn_span.end == 100);
}
