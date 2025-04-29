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

#include "ts_bloomfilter.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <random>
#include <unordered_set>
#include "kwdb_type.h"
#include "libkwdbts2.h"

TEST(BloomFilter, Test1) {
  double p = 0.001;
  kwdbts::TsBloomFilterBuiler builder(p);
  std::mt19937_64 g{std::random_device()()};

  size_t nentity = 10000;
  size_t max_eid = 1000000;
  size_t min_eid = 100000;
  std::uniform_int_distribution<size_t> dist(min_eid, max_eid);
  std::vector<TSEntityID> eids(nentity);
  for (int i = 0; i < 10000; ++i) {
    eids[i] = dist(g);
    builder.Add(eids[i]);
  }

  auto filter = builder.Finalize();
  for (auto eid : eids) {
    EXPECT_TRUE(filter.MayExist(eid));
  }

  // Check if we can filter out non-exist entities.

  std::unordered_set<TSEntityID> exist_eid{eids.begin(), eids.end()};
  std::unordered_set<TSEntityID> nonexist_eid;
  while (nonexist_eid.size() != nentity) {
    auto eid = dist(g);
    if (exist_eid.find(eid) == exist_eid.end()) {
      nonexist_eid.insert(eid);
    }
  }

  // count false positive
  int n = std::count_if(nonexist_eid.begin(), nonexist_eid.end(),
                        [&filter](TSEntityID eid) { return filter.MayExist(eid); });
  double sigma = std::sqrt(nentity * p * (1 - p));
  EXPECT_LT(n, 10 * sigma);

  // serialization test;
  std::string data;
  filter.Serialize(&data);
  std::unique_ptr<kwdbts::TsBloomFilter> pfilter;
  auto s = kwdbts::TsBloomFilter::FromData({data.data(), data.size()}, &pfilter);
  ASSERT_EQ(s, kwdbts::SUCCESS);

  // check if consistent
  for (auto eid : exist_eid) {
    EXPECT_TRUE(pfilter->MayExist(eid));
  }
  for (auto eid : nonexist_eid) {
    EXPECT_EQ(pfilter->MayExist(eid), filter.MayExist(eid));
  }
}