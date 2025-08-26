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

void InsertDuplicateTest(TsMemSegIndex &skiplist, int data, int id = 1) {
  TSMemSegRowData row(id, id, id, id);
  row.SetData(id, id, TSSlice{reinterpret_cast<char *>(&data), sizeof(data)});
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

SkiplistIterator SeekHelper(TsMemSegIndex &skiplist, int id) {
  SkiplistIterator iter(&skiplist);
  char keybuf[TSMemSegRowData::GetKeyLen() + sizeof(TSMemSegRowData)];
  TSMemSegRowData *key = new (keybuf) TSMemSegRowData(id, id, id, id);
  key->SetData(id, id, TSSlice{nullptr, 0});
  key->GenKey(keybuf);
  iter.Seek(keybuf);
  return iter;
}

TEST(TsMemSegIndexTest, InsertDuplicateKeysAndSeek) {
  TsMemSegIndex skiplist;
  std::default_random_engine rng{0};

  int n = 5000;
  int nbatch = 10;
  std::vector<std::vector<int>> insert_data(nbatch);
  for (int i = 0; i < nbatch; ++i) {
    insert_data[i].resize(n);
    std::iota(insert_data[i].begin(), insert_data[i].end(), i * n);
    std::shuffle(insert_data[i].begin(), insert_data[i].end(), rng);
  }

  for (int i = 0; i < nbatch; ++i) {
    for (int j = 0; j < n; ++j) {
      InsertDuplicateTest(skiplist, insert_data[i][j], i);
    }
  }
  {
    SkiplistIterator iter(&skiplist);
    iter.SeekToFirst();
    int cnt = 0;
    while (iter.Valid()) {
      iter.Next();
      ++cnt;
    }
    ASSERT_EQ(cnt, nbatch * n);
  }
  {
    auto it = SeekHelper(skiplist, 5);
    ASSERT_TRUE(it.Valid());

    auto end = SeekHelper(skiplist, 6);

    int idx = 0;
    while (it.Valid() && it != end) {
      auto row_data = skiplist.ParseKey(it.key());
      int data = *reinterpret_cast<int *>(row_data->row_data.data);
      ASSERT_EQ(data, insert_data[5][idx]);
      it.Next();
      ++idx;
    }
    ASSERT_EQ(idx, n);
  }
}