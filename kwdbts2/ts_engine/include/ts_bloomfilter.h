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

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "kwdb_type.h"
#include "libkwdbts2.h"

namespace kwdbts {
// a simple bloom filter for filter entity_id in lastsegment;
class TsBloomFilter {
 private:
  int k_;     // number of hash funcs
  size_t m_;  // number of bits;
  std::string buffer_;

  size_t min_eid_, max_eid_;

  void CalcHash(TSEntityID eid, std::vector<size_t>*) const;
  void Record(const std::vector<size_t>& bits);

  static const char MAGIC[];
  TsBloomFilter() = default;

 public:
  explicit TsBloomFilter(const std::unordered_set<TSEntityID>& entities,
                         double p /*Probability of false positives*/);
  static KStatus FromData(TSSlice, std::unique_ptr<TsBloomFilter>*);
  void Serialize(std::string* dst) const;
  bool MayExist(TSEntityID entity_id) const;
};

class TsBloomFilterBuiler {
 private:
  double p_;
  std::unordered_set<TSEntityID> all_entities_;

 public:
  explicit TsBloomFilterBuiler(double p = 0.001) : p_(p) {}
  void Add(TSEntityID entity_id) { all_entities_.insert(entity_id); }
  TsBloomFilter Finalize() { return TsBloomFilter{all_entities_, p_}; }
};
}  // namespace kwdbts
