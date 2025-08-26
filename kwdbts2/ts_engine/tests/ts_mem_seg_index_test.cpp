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

#include "ts_mem_seg_index.h"

#include <gtest/gtest.h>

#include <numeric>
#include <vector>
#include <random>

#include "libkwdbts2.h"

using namespace kwdbts;

void InsertDuplicateTest(TsMemSegIndex &skiplist, int data) {
  TSMemSegRowData row{1, 1, 1, 1};
  row.SetData(1, 1, TSSlice{reinterpret_cast<char *>(&data), sizeof(data)});
  skiplist.InsertRowData(row);
}

TEST(TsMemSegIndexTest, InsertDuplicateKeys) {
  TsMemSegIndex skiplist;

  int n = 10000;
  std::vector<int> insert_data(n);
  std::iota(insert_data.begin(), insert_data.end(), 0);
  std::shuffle(insert_data.begin(), insert_data.end(), std::default_random_engine{0});

  for (int i = 0; i < 10000; ++i) {
    InsertDuplicateTest(skiplist, insert_data[i]);
  }

  SkiplistIterator iter(&skiplist);
  iter.SeekToFirst();
  int i = 0;
  while (iter.Valid()) {
    auto row_data = skiplist.ParseKey(iter.key());
    int data = *reinterpret_cast<int *>(row_data->row_data.data);
    ASSERT_EQ(data, insert_data[i]);
    iter.Next();
    ++i;
  }
  ASSERT_EQ(i, 10000);
}