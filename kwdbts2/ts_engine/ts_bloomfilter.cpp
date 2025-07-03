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

#include <algorithm>
#include <cassert>
#include <cmath>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <random>
#include <string_view>
#include <tuple>
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_coding.h"

// functions that calculate m and k from given p and n,
// n: Number of items in the filter
// p: Probability of false positives, fraction between 0 and 1 or a number indicating 1-in-p
// m: Number of bits in the filter
// k: Number of hash functions
/*
 * n = ceil(m / (-k / log(1 - exp(log(p) / k))))
 * p = pow(1 - exp(-k / (m / n)), k)
 * m = ceil((n * log(p)) / log(1 / pow(2, log(2))));
 * k = round((m / n) * log(2));
 */

static std::tuple<size_t, int> GetMK(size_t n, double p) {
  static constexpr double denominator = -0.4804530139182015;
  size_t m = 1 + n * std::log(p) / denominator;
  int k = std::round(m * std::log(2) / n);
  return std::make_tuple(m, k);
}

static size_t hash1(TSEntityID eid) { return std::mt19937_64(eid)(); }
static size_t hash2(TSEntityID eid) { return std::ranlux48(eid)(); }

namespace kwdbts {
const char TsBloomFilter::MAGIC[] = "TSBLOOMFILTER";
void TsBloomFilter::CalcHash(TSEntityID eid, std::vector<size_t>* result) const {
  assert(result->size() == k_);
  auto h1 = hash1(eid);
  auto h2 = hash2(eid);
  for (int i = 0; i < k_; ++i) {
    size_t hash = h1 + i * h2 + i * i * i;
    size_t n = hash % m_;
    (*result)[i] = n;
  }
}

void TsBloomFilter::Record(const std::vector<size_t>& bit_pos) {
  for (auto p : bit_pos) {
    int idx = p / 8;
    int off = p % 8;
    buffer_[idx] |= 1 << off;
  }
}

TsBloomFilter::TsBloomFilter(const std::unordered_set<TSEntityID>& entities, double p) {
  assert(p > 0);
  auto [min_e, max_e] = std::minmax_element(entities.begin(), entities.end());
  min_eid_ = *min_e;
  max_eid_ = *max_e;

  auto [m, k] = GetMK(entities.size(), p);
  k_ = k;
  m_ = m;
  buffer_.resize((m_ + 7) / 8);

  m_ = buffer_.size() * 8;
  // use cubic enhanced double hashing, see:
  // https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
  std::vector<size_t> res(k_);
  for (auto eid : entities) {
    CalcHash(eid, &res);
    Record(res);
  }
}
bool TsBloomFilter::MayExist(TSEntityID entity_id) const {
  if (entity_id > max_eid_ || entity_id < min_eid_) {
    return false;
  }
  std::vector<size_t> res(k_);
  CalcHash(entity_id, &res);
  return std::all_of(res.begin(), res.end(), [this](size_t p) {
    int idx = p / 8;
    int off = p % 8;
    return buffer_[idx] & (1 << off);
  });
}

void TsBloomFilter::Serialize(std::string* dst) const {
  dst->clear();
  dst->append(MAGIC);
  PutFixed32(dst, k_);
  PutFixed64(dst, m_);
  PutFixed64(dst, min_eid_);
  PutFixed64(dst, max_eid_);
  dst->append(buffer_);
}

KStatus TsBloomFilter::FromData(TSSlice data, std::unique_ptr<TsBloomFilter>* filter) {
  size_t magic_len = std::strlen(MAGIC);
  if (data.len < magic_len || std::strncmp(data.data, MAGIC, magic_len) != 0) {
    LOG_ERROR("magic mismatch")
    return FAIL;
  }
  data.data += magic_len;
  data.len -= magic_len;

  filter->reset(new TsBloomFilter);

  uint32_t v;
  GetFixed32(&data, &v);
  (*filter)->k_ = v;
  GetFixed64(&data, &(*filter)->m_);
  GetFixed64(&data, &(*filter)->min_eid_);
  GetFixed64(&data, &(*filter)->max_eid_);
  if ((*filter)->m_ % 8 != 0) {
    LOG_ERROR("bloomfilter block corrupted");
    return FAIL;
  }
  size_t len = (*filter)->m_ / 8;
  assert(data.len == len);
  (*filter)->buffer_ = std::string_view{data.data, len};
  return SUCCESS;
}

}  // namespace kwdbts
