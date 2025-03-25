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

#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"

class TsInternalKeyComparator : public rocksdb::Comparator {
 public:
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    // assert(a.size() == b.size());
    // assert(a.size() == 20);

    return memcmp(a.data_, b.data_, a.size_);
    // latest version first.
    // int res2 = memcmp(b.data_ + 8, a.data_ + 8, 4);
    // int res3 = memcmp(a.data_ + 12, b.data_ + 12, 8);

    // return (res1 || res2) || res3;
  }

  const char* Name() const override { return "TsInternalKeyComparator"; }

  // nothing to do
  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {}
  void FindShortSuccessor(std::string* key) const override {}
};

rocksdb::Comparator* TsComparator();
